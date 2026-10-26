"""The three processes of the report: P1 client, P2 processing server, P3 database."""
from __future__ import annotations

import threading

from .core import Message, Process, REQUEST, QUERY, RESPONSE, RESULT

# P3's tiny in-memory "database"
DATABASE = {f"item-{i}": f"value-{i * i}" for i in range(1, 101)}


class ClientProcess(Process):
    """P1: IDLE -> WAITING (request sent) -> COMPLETED (result received)."""

    def __init__(self, name, network, log, coordinator, num_requests: int = 1):
        super().__init__(name, network, log, coordinator, "IDLE")
        self.num_requests = num_requests
        self.results = []
        self.done = threading.Event()

    def run(self) -> None:
        for i in range(1, self.num_requests + 1):
            self.local_event(f"Request {i} created")
            self.send(REQUEST, "P2", {"req_id": i, "key": f"item-{i}"})
        self.state = "WAITING"
        super().run()

    def handle(self, msg: Message) -> None:
        if msg.kind == RESULT:
            self.results.append(msg.payload)
            if len(self.results) == self.num_requests:
                self.state = "COMPLETED"
                self.done.set()


class ProcessingServer(Process):
    """P2: WAITING -> PROCESSING -> COMPLETED. Forwards queries to P3, results to P1."""

    def __init__(self, name, network, log, coordinator, expected_requests: int = 1):
        super().__init__(name, network, log, coordinator, "WAITING")
        self.expected = expected_requests
        self.received = 0
        self.finished = 0

    def handle(self, msg: Message) -> None:
        if msg.kind == REQUEST:
            self.received += 1
            self.state = "PROCESSING"
            key = str(msg.payload["key"]).strip().lower()           # the "computation"
            self.send(QUERY, "P3", {"req_id": msg.payload["req_id"], "key": key})
        elif msg.kind == RESPONSE:
            self.finished += 1
            self.send(RESULT, "P1", msg.payload)
            if self.finished == self.expected:
                self.state = "COMPLETED"
            else:
                self.state = "PROCESSING" if self.received > self.finished else "WAITING"


class DatabaseServer(Process):
    """P3: WAITING -> PROCESSING -> WAITING. Answers queries from P2."""

    def __init__(self, name, network, log, coordinator):
        super().__init__(name, network, log, coordinator, "WAITING")

    def handle(self, msg: Message) -> None:
        if msg.kind == QUERY:
            self.state = "PROCESSING"
            value = DATABASE.get(msg.payload["key"], "NOT_FOUND")    # database-like operation
            self.local_event(f"Query {msg.payload['req_id']} processed")
            self.send(RESPONSE, "P2", {"req_id": msg.payload["req_id"], "value": value})
            self.state = "WAITING"
