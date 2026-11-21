"""
Unit tests for Load Balancing Schedulers.

Verifies:
1. Cyclic Round Robin distribution across healthy servers.
2. Unhealthy server exclusion from scheduling.
3. Proportional Weighted Round Robin distribution.
4. Handling of empty server pool.
"""

import unittest
from load_balancer.server_registry import ServerNode
from load_balancer.scheduler import RoundRobinScheduler, WeightedRoundRobinScheduler, SchedulerManager

class TestSchedulers(unittest.TestCase):

    def setUp(self):
        self.s1 = ServerNode("server-1", "Server 1", "127.0.0.1", 5001, weight=3)
        self.s2 = ServerNode("server-2", "Server 2", "127.0.0.1", 5002, weight=2)
        self.s3 = ServerNode("server-3", "Server 3", "127.0.0.1", 5003, weight=1)
        self.servers = [self.s1, self.s2, self.s3]

    def test_round_robin_cyclic_selection(self):
        """Verify strict cyclic selection: S1 -> S2 -> S3 -> S1 -> S2 -> S3."""
        rr = RoundRobinScheduler()
        selections = [rr.select(self.servers).id for _ in range(6)]
        expected = ["server-1", "server-2", "server-3", "server-1", "server-2", "server-3"]
        self.assertEqual(selections, expected)

    def test_round_robin_with_unhealthy_exclusion(self):
        """Verify that when Server 2 is excluded, routing is S1 -> S3 -> S1 -> S3."""
        rr = RoundRobinScheduler()
        # Pool with only healthy servers S1 and S3
        active_pool = [self.s1, self.s3]
        selections = [rr.select(active_pool).id for _ in range(4)]
        expected = ["server-1", "server-3", "server-1", "server-3"]
        self.assertEqual(selections, expected)

    def test_round_robin_empty_pool(self):
        """Verify None is returned when server list is empty."""
        rr = RoundRobinScheduler()
        self.assertIsNone(rr.select([]))

    def test_weighted_round_robin_proportional_distribution(self):
        """Verify Weighted Round Robin distributes traffic proportionally according to weights (3:2:1)."""
        wrr = WeightedRoundRobinScheduler()
        # Weights are S1=3, S2=2, S3=1 -> Total weight = 6 per cycle
        total_requests = 60
        counts = {}
        for _ in range(total_requests):
            chosen = wrr.select(self.servers)
            counts[chosen.id] = counts.get(chosen.id, 0) + 1

        self.assertEqual(counts["server-1"], 30)  # 3/6 = 50%
        self.assertEqual(counts["server-2"], 20)  # 2/6 = 33.3%
        self.assertEqual(counts["server-3"], 10)  # 1/6 = 16.7%

    def test_scheduler_manager_algorithm_switch(self):
        """Verify SchedulerManager properly switches strategies at runtime."""
        mgr = SchedulerManager(initial_algorithm="ROUND_ROBIN")
        self.assertEqual(mgr.current_algorithm, "ROUND_ROBIN")
        
        self.assertTrue(mgr.set_algorithm("WEIGHTED_ROUND_ROBIN"))
        self.assertEqual(mgr.current_algorithm, "WEIGHTED_ROUND_ROBIN")
        
        # Test invalid algorithm rejection
        self.assertFalse(mgr.set_algorithm("RANDOM_UNKNOWN"))
        self.assertEqual(mgr.current_algorithm, "WEIGHTED_ROUND_ROBIN")

if __name__ == "__main__":
    unittest.main()
