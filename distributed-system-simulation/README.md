# Distributed System Simulation

Python simulation of a small distributed system: **distributed program, process execution,
communication events and global-state representation** (Distributed Systems report, 2026–2027).

Three independent processes (each runs in its own thread, with no shared memory or shared clock)
cooperate only by passing messages:

```
P1 (Client) --REQUEST--> P2 (Processing Server) --QUERY--> P3 (Database Server)
P1 <--RESULT--           P2 <--RESPONSE--------------------P3
```

## Features

| Report section | Implementation |
|---|---|
| 5. Process execution & local states | `distsim/processes.py` (IDLE / WAITING / PROCESSING / COMPLETED) |
| 6. Send / receive events (E1–E10) | `EventLog` in `distsim/core.py` |
| 7. Event ordering, happened-before | Lamport logical clocks (`L=` in the log) |
| 8. Global state `G = (S1,S2,S3,C12,C23,C32,C21)` | `distsim/snapshot.py`, final state + live snapshot |
| 9. Algorithm / pseudocode | `distsim/processes.py` + `distsim/simulation.py` |
| 10. Message queues, state manager | `Network` (FIFO channels), `SnapshotCoordinator` |
| 12. Test cases TC01–TC07 | `tests/test_simulation.py` |
| 13. Delay, failure, concurrency, ordering | random delays, `--loss-rate`, multiple requests, Lamport clocks |

A **Chandy–Lamport snapshot** captures a consistent global state *while the system is running*
(e.g. `P1=WAITING, P2=PROCESSING, P3=WAITING, Channel P3->P2=RESPONSE`), not just at the end.

## Run

Requires Python 3.9+ and no third-party packages.

```bash
python main.py                                   # one request, as in the report
python main.py -n 3 --snapshot-after 0.12 --seed 4   # 3 requests + mid-run global snapshot
python main.py --loss-rate 1.0 --timeout 2       # communication failure -> timeout detected
python main.py --help
```

## Sample output

```
[ 1] P1: Request 1 created  (L=1)
[ 2] P1: REQUEST[1] sent to P2  (L=2)
[ 3] P2: REQUEST[1] received from P1  (L=3)
...
[10] P1: RESULT[1] received from P2  (L=10)

----- GLOBAL STATE (final) -----
P1 = COMPLETED
P2 = COMPLETED
P3 = WAITING
Channel P1 -> P2 = EMPTY
...
Global State = CONSISTENT
```

## Tests

```bash
python -m unittest discover -v
```

## Project layout

```
main.py                 CLI entry point
distsim/core.py         Message, EventLog, Network, Process (Lamport clock + snapshot logic)
distsim/processes.py    P1 client, P2 processing server, P3 database server
distsim/snapshot.py     GlobalSnapshot + SnapshotCoordinator
distsim/simulation.py   run_simulation(): builds, runs and reports
tests/                  unit tests (TC01, TC02, TC06, TC07, snapshot, clocks)
```
