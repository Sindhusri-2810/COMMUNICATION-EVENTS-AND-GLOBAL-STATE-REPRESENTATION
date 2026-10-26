"""Core building blocks: messages, event log, network (channels) and base process."""
from __future__ import annotations

import itertools
import queue
import random
import threading
import time
from collections import Counter
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

# Application message kinds (report section 6)
REQUEST, QUERY, RESPONSE, RESULT = "REQUEST", "QUERY", "RESPONSE", "RESULT"
# Control message kinds (not application traffic)
MARKER = "MARKER"              # Chandy-Lamport snapshot marker
STOP = "STOP"                  # shut a process down
SNAPSHOT_CMD = "SNAPSHOT_CMD"  # ask the initiator to start a snapshot

Channel = Tuple[str, str]


@dataclass(frozen=True)
class Message:
    kind: str
    sender: str
    receiver: str
    payload: Any = None
    timestamp: int = 0   # Lamport timestamp of the send event
    msg_id: int = 0      # 0 = control message injected from outside the network


@dataclass(frozen=True)
class Event:
    seq: int             # position in the global log (E1, E2, ...)
    process: str
    kind: str            # local | send | receive | snapshot
    description: str
    lamport: int
    wall_time: float


class EventLog:
    """Thread-safe, ordered record of every event (report sections 6, 7, 10)."""

    def __init__(self, verbose: bool = True):
        self._lock = threading.Lock()
        self._events: List[Event] = []
        self.verbose = verbose

    def record(self, process: str, kind: str, description: str, lamport: int) -> Event:
        with self._lock:
            ev = Event(len(self._events) + 1, process, kind, description, lamport, time.time())
            self._events.append(ev)
            if self.verbose:
                print(f"[{ev.seq:>2}] {process}: {description}  (L={lamport})", flush=True)
            return ev

    @property
    def events(self) -> List[Event]:
        with self._lock:
            return list(self._events)


class Network:
    """Point-to-point FIFO channels with random delay and optional message loss.

    There is no shared memory or shared clock: processes only talk through send().
    """

    def __init__(self, names: List[str], channels: List[Channel], min_delay: float = 0.05,
                 max_delay: float = 0.25, loss_rate: float = 0.0, seed=None):
        self.names = list(names)
        self.channels = list(channels)
        self.inboxes: Dict[str, queue.Queue] = {n: queue.Queue() for n in names}
        self.min_delay, self.max_delay, self.loss_rate = min_delay, max_delay, loss_rate
        self._rng = random.Random(seed)
        self._lock = threading.Lock()
        self._ids = itertools.count(1)
        self._in_transit: Dict[Channel, List[Message]] = {c: [] for c in channels}
        self.dropped: Counter = Counter()

    def next_id(self) -> int:
        with self._lock:
            return next(self._ids)

    def incoming(self, name: str) -> List[str]:
        return [s for (s, d) in self.channels if d == name]

    def outgoing(self, name: str) -> List[str]:
        return [d for (s, d) in self.channels if s == name]

    def send(self, msg: Message) -> None:
        ch = (msg.sender, msg.receiver)
        if ch not in self._in_transit:
            raise ValueError(f"No channel {ch[0]} -> {ch[1]}")
        with self._lock:
            delay = self._rng.uniform(self.min_delay, self.max_delay)
            lost = msg.kind != MARKER and self._rng.random() < self.loss_rate
        time.sleep(delay)  # sender-side delay keeps each channel FIFO
        if lost:
            with self._lock:
                self.dropped[ch] += 1
            return
        with self._lock:
            self._in_transit[ch].append(msg)
        self.inboxes[msg.receiver].put(msg)

    def inject(self, receiver: str, msg: Message) -> None:
        """Deliver a control message that does not travel on a channel."""
        self.inboxes[receiver].put(msg)

    def receive(self, name: str) -> Message:
        msg = self.inboxes[name].get()
        if msg.msg_id:
            with self._lock:
                self._in_transit[(msg.sender, msg.receiver)].remove(msg)
        return msg

    def channel_contents(self, ch: Channel) -> List[Message]:
        with self._lock:
            return list(self._in_transit[ch])


class Process(threading.Thread):
    """A distributed process: local state + Lamport clock + a message loop.

    Also implements the Chandy-Lamport snapshot algorithm so a consistent
    global state can be captured while the system is still running.
    """

    def __init__(self, name: str, network: Network, log: EventLog, coordinator, initial_state: str):
        super().__init__(name=name, daemon=True)
        self.network, self.log, self.coordinator = network, log, coordinator
        self.state = initial_state
        self.clock = 0
        self.sent_count: Counter = Counter()   # per destination
        self.recv_count: Counter = Counter()   # per source
        self.error = None
        self._incoming = network.incoming(name)
        self._outgoing = network.outgoing(name)
        self._snap_recorded = False
        self._snap_open: set = set()
        self._snap_local: dict = {}
        self._snap_channels: Dict[str, List[str]] = {}
        self._snap_reported = False

    # ---- events -------------------------------------------------------
    def local_event(self, description: str) -> None:
        self.clock += 1
        self.log.record(self.name, "local", description, self.clock)

    @staticmethod
    def _label(kind: str, payload) -> str:
        if isinstance(payload, dict) and "req_id" in payload:
            return f"{kind}[{payload['req_id']}]"
        return kind

    def send(self, kind: str, dest: str, payload=None) -> None:
        self.clock += 1
        msg = Message(kind, self.name, dest, payload, self.clock, self.network.next_id())
        self.sent_count[dest] += 1
        self.log.record(self.name, "send", f"{self._label(kind, payload)} sent to {dest}", self.clock)
        self.network.send(msg)

    # ---- main loop ----------------------------------------------------
    def handle(self, msg: Message) -> None:  # overridden by subclasses
        raise NotImplementedError

    def run(self) -> None:
        try:
            self.main_loop()
        except Exception as exc:  # surface failures instead of hanging silently
            self.error = exc
            raise

    def main_loop(self) -> None:
        while True:
            msg = self.network.receive(self.name)
            if msg.kind == STOP:
                return
            if msg.kind == SNAPSHOT_CMD:
                self._start_snapshot()
            elif msg.kind == MARKER:
                self._on_marker(msg)
            else:
                self.clock = max(self.clock, msg.timestamp) + 1   # Lamport rule
                self.recv_count[msg.sender] += 1
                if self._snap_recorded and msg.sender in self._snap_open:
                    self._snap_channels[msg.sender].append(self._label(msg.kind, msg.payload))
                self.log.record(self.name, "receive",
                                f"{self._label(msg.kind, msg.payload)} received from {msg.sender}", self.clock)
                self.handle(msg)

    # ---- Chandy-Lamport snapshot -------------------------------------
    def _record_local_state(self) -> None:
        self._snap_recorded = True
        self._snap_local = {"state": self.state, "clock": self.clock,
                            "sent": dict(self.sent_count), "received": dict(self.recv_count)}
        self._snap_channels = {src: [] for src in self._incoming}
        self._snap_open = set(self._incoming)

    def _send_markers(self) -> None:
        for dest in self._outgoing:
            self.network.send(Message(MARKER, self.name, dest, None, self.clock, self.network.next_id()))

    def _start_snapshot(self) -> None:
        if self._snap_recorded:
            return
        self._record_local_state()
        self.log.record(self.name, "snapshot", "Snapshot started: local state recorded, markers sent", self.clock)
        self._send_markers()
        self._maybe_report()

    def _on_marker(self, msg: Message) -> None:
        if not self._snap_recorded:
            # First marker: that channel is recorded as empty, record own state.
            self._record_local_state()
            self.log.record(self.name, "snapshot",
                            f"MARKER received from {msg.sender}: local state recorded", self.clock)
            self._snap_open.discard(msg.sender)
            self._send_markers()
        else:
            self._snap_open.discard(msg.sender)
            self.log.record(self.name, "snapshot",
                            f"MARKER received from {msg.sender}: channel recording stopped", self.clock)
        self._maybe_report()

    def _maybe_report(self) -> None:
        if self._snap_recorded and not self._snap_open and not self._snap_reported:
            self._snap_reported = True
            self.coordinator.report(self.name, self._snap_local, self._snap_channels)
