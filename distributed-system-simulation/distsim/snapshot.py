"""Global-state representation: G = (S1, S2, S3, C12, C23, C32, C21)  (report section 8)."""
from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Dict, List, Tuple

Channel = Tuple[str, str]


@dataclass
class GlobalSnapshot:
    local_states: Dict[str, dict]
    channel_states: Dict[Channel, List[str]]

    def is_consistent(self) -> bool:
        """Every message counted as sent was either received before the cut or is
        recorded in its channel: sent(src->dst) == received(dst<-src) + |channel|."""
        for (src, dst), in_flight in self.channel_states.items():
            sent = self.local_states[src]["sent"].get(dst, 0)
            received = self.local_states[dst]["received"].get(src, 0)
            if sent != received + len(in_flight):
                return False
        return True

    def format(self, title: str = "GLOBAL STATE (Chandy-Lamport snapshot)") -> str:
        lines = [f"----- {title} -----"]
        for name in sorted(self.local_states):
            lines.append(f"{name} = {self.local_states[name]['state']}")
        for (src, dst) in sorted(self.channel_states, key=lambda c: (c[0], c[1])):
            msgs = self.channel_states[(src, dst)]
            lines.append(f"Channel {src} -> {dst} = {', '.join(msgs) if msgs else 'EMPTY'}")
        lines.append(f"Global State = {'CONSISTENT' if self.is_consistent() else 'INCONSISTENT'}")
        return "\n".join(lines)


class SnapshotCoordinator:
    """Collects the per-process pieces of a snapshot (the 'Global State Manager')."""

    def __init__(self, process_names: List[str]):
        self._expected = set(process_names)
        self._reports: Dict[str, Tuple[dict, Dict[str, List[str]]]] = {}
        self._lock = threading.Lock()
        self.done = threading.Event()

    def report(self, name: str, local: dict, channels: Dict[str, List[str]]) -> None:
        with self._lock:
            self._reports[name] = (local, channels)
            if set(self._reports) == self._expected:
                self.done.set()

    def result(self) -> GlobalSnapshot:
        with self._lock:
            locals_ = {n: r[0] for n, r in self._reports.items()}
            chans = {(src, dst): msgs for dst, r in self._reports.items() for src, msgs in r[1].items()}
        return GlobalSnapshot(locals_, chans)
