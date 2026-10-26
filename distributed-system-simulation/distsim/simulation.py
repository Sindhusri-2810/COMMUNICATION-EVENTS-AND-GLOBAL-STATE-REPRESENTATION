"""Simulation driver: builds the system, runs it, collects and prints the global state."""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Dict, List, Optional

from .core import EventLog, Event, Message, Network, SNAPSHOT_CMD, STOP
from .processes import ClientProcess, DatabaseServer, ProcessingServer
from .snapshot import GlobalSnapshot, SnapshotCoordinator

# Communication channels C12, C23, C32, C21 (report section 8)
CHANNELS = [("P1", "P2"), ("P2", "P3"), ("P3", "P2"), ("P2", "P1")]


@dataclass
class SimulationResult:
    completed: bool
    timed_out: bool
    consistent: bool
    events: List[Event]
    final_states: Dict[str, str]
    channel_states: Dict[tuple, List[str]]
    results: list
    snapshot: Optional[GlobalSnapshot]


def run_simulation(num_requests: int = 1, min_delay: float = 0.05, max_delay: float = 0.25,
                   loss_rate: float = 0.0, seed=None, snapshot_after: Optional[float] = None,
                   timeout: float = 10.0, verbose: bool = True) -> SimulationResult:
    names = ["P1", "P2", "P3"]
    log = EventLog(verbose=verbose)
    net = Network(names, CHANNELS, min_delay, max_delay, loss_rate, seed)
    coord = SnapshotCoordinator(names)
    p1 = ClientProcess("P1", net, log, coord, num_requests)
    p2 = ProcessingServer("P2", net, log, coord, num_requests)
    p3 = DatabaseServer("P3", net, log, coord)
    procs = {"P1": p1, "P2": p2, "P3": p3}

    def say(text: str = "") -> None:
        if verbose:
            print(text, flush=True)

    say("----- DISTRIBUTED SYSTEM SIMULATION -----")
    for p in (p2, p3, p1):
        say(f"Process {p.name} Started")
    say()
    for p in (p2, p3, p1):
        p.start()

    deadline = time.time() + timeout
    if snapshot_after is not None:
        time.sleep(snapshot_after)
        net.inject("P1", Message(SNAPSHOT_CMD, "MAIN", "P1"))   # P1 initiates the snapshot

    completed = p1.done.wait(max(0.0, deadline - time.time()))
    snapshot = None
    if snapshot_after is not None and coord.done.wait(max(0.0, deadline - time.time())):
        snapshot = coord.result()

    for n in names:                       # shut everything down
        net.inject(n, Message(STOP, "MAIN", n))
    for p in procs.values():
        p.join(timeout=2)

    final_states = {n: p.state for n, p in procs.items()}
    channels = {ch: [m.kind for m in net.channel_contents(ch)] for ch in CHANNELS}
    consistent = all(not channels[c] for c in CHANNELS) and all(
        procs[s].sent_count[d] == procs[d].recv_count[s] for s, d in CHANNELS)

    say()
    if not completed:
        say(f"!! TIMEOUT: P1 did not receive all results within {timeout}s "
            f"(messages lost: {sum(net.dropped.values())}) - failure detected")
    if snapshot is not None:
        say(snapshot.format())
        say()
    say("----- GLOBAL STATE (final) -----")
    for n in names:
        say(f"{n} = {final_states[n]}")
    for ch in CHANNELS:
        say(f"Channel {ch[0]} -> {ch[1]} = {', '.join(channels[ch]) if channels[ch] else 'EMPTY'}")
    say(f"Global State = {'CONSISTENT' if consistent else 'INCONSISTENT'}")

    return SimulationResult(completed, not completed, consistent, log.events, final_states,
                            channels, p1.results, snapshot)
