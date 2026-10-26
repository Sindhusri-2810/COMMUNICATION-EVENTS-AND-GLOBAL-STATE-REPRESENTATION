import unittest

from distsim import run_simulation

FAST = dict(min_delay=0.0, max_delay=0.01, verbose=False)


class TestSimulation(unittest.TestCase):
    def test_tc01_normal_request_has_ten_events_in_order(self):
        r = run_simulation(seed=1, **FAST)
        self.assertTrue(r.completed)
        self.assertEqual([e.process for e in r.events],
                         ["P1", "P1", "P2", "P2", "P3", "P3", "P3", "P2", "P2", "P1"])
        self.assertEqual([e.kind for e in r.events],
                         ["local", "send", "receive", "send", "receive", "local", "send",
                          "receive", "send", "receive"])

    def test_tc06_final_states(self):
        r = run_simulation(seed=1, **FAST)
        self.assertEqual(r.final_states, {"P1": "COMPLETED", "P2": "COMPLETED", "P3": "WAITING"})
        self.assertTrue(all(not m for m in r.channel_states.values()))
        self.assertTrue(r.consistent)

    def test_happened_before_lamport_clocks_increase(self):
        r = run_simulation(seed=1, **FAST)
        clocks = [e.lamport for e in r.events]
        self.assertEqual(clocks, sorted(clocks))
        self.assertEqual(len(set(clocks)), len(clocks))

    def test_tc02_multiple_requests(self):
        r = run_simulation(num_requests=5, seed=2, **FAST)
        self.assertTrue(r.completed and r.consistent)
        self.assertEqual(sorted(x["req_id"] for x in r.results), [1, 2, 3, 4, 5])
        self.assertEqual(r.final_states["P1"], "COMPLETED")

    def test_tc07_communication_failure_detected(self):
        r = run_simulation(loss_rate=1.0, timeout=0.5, seed=3, **FAST)
        self.assertTrue(r.timed_out)
        self.assertEqual(r.final_states["P1"], "WAITING")
        self.assertFalse(r.consistent)

    def test_snapshot_is_consistent(self):
        for seed in range(1, 6):
            r = run_simulation(num_requests=3, min_delay=0.02, max_delay=0.08, seed=seed,
                               snapshot_after=0.05, verbose=False)
            self.assertIsNotNone(r.snapshot)
            self.assertTrue(r.snapshot.is_consistent(), f"seed {seed}")


if __name__ == "__main__":
    unittest.main()
