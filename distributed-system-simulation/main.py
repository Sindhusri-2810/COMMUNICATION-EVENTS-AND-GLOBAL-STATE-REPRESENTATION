#!/usr/bin/env python3
"""Command-line entry point:  python main.py --help"""
import argparse

from distsim import run_simulation


def main() -> None:
    ap = argparse.ArgumentParser(description="Distributed system simulation (P1 -> P2 -> P3 -> P2 -> P1)")
    ap.add_argument("-n", "--requests", type=int, default=1, help="number of client requests (default 1)")
    ap.add_argument("--min-delay", type=float, default=0.05, help="min message delay in seconds")
    ap.add_argument("--max-delay", type=float, default=0.25, help="max message delay in seconds")
    ap.add_argument("--loss-rate", type=float, default=0.0, help="probability a message is lost (0-1)")
    ap.add_argument("--seed", type=int, default=None, help="random seed for reproducible delays")
    ap.add_argument("--snapshot-after", type=float, default=None, metavar="SEC",
                    help="capture a Chandy-Lamport global snapshot SEC seconds after start")
    ap.add_argument("--timeout", type=float, default=10.0, help="give up after this many seconds")
    ap.add_argument("-q", "--quiet", action="store_true", help="suppress output")
    a = ap.parse_args()
    run_simulation(a.requests, a.min_delay, a.max_delay, a.loss_rate, a.seed,
                   a.snapshot_after, a.timeout, verbose=not a.quiet)


if __name__ == "__main__":
    main()
