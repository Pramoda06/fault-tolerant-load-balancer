"""
Unit tests for Server Registry, State Machine, and Health Transitions.

Verifies:
1. Initial HEALTHY state and active pool inclusion.
2. Failure transition (HEALTHY -> UNHEALTHY) and pool exclusion.
3. Recovery transition (UNHEALTHY -> RECOVERING) with consecutive pass requirement.
4. Re-admission transition (RECOVERING -> HEALTHY) after threshold consecutive checks.
5. Consecutive check counters and metric timestamps.
"""

import time
import unittest
from load_balancer.server_registry import ServerRegistry

class TestServerRegistryAndLifecycle(unittest.TestCase):

    def setUp(self):
        # Recovery threshold set to 2 consecutive successful checks
        self.registry = ServerRegistry(recovery_threshold=2)
        self.s1 = self.registry.register_server("server-1", "Server 1", "127.0.0.1", 5001, weight=3)
        self.s2 = self.registry.register_server("server-2", "Server 2", "127.0.0.1", 5002, weight=2)
        self.s3 = self.registry.register_server("server-3", "Server 3", "127.0.0.1", 5003, weight=1)

    def test_initial_pool_contains_all_servers(self):
        """All newly registered servers start in HEALTHY state."""
        active = self.registry.get_active_pool()
        self.assertEqual(len(active), 3)
        self.assertTrue(all(s.status == "HEALTHY" for s in active))

    def test_failure_detection_and_pool_removal(self):
        """When server health check fails, state transitions to UNHEALTHY and it is removed from active pool."""
        t_fail = time.time()
        event_type, details = self.registry.update_health_result(
            server_id="server-2",
            is_healthy=False,
            latency_ms=12.5,
            check_time=t_fail
        )

        self.assertEqual(event_type, "FAILURE_DETECTED")
        self.assertEqual(self.s2.status, "UNHEALTHY")
        self.assertEqual(self.s2.consecutive_failures, 1)

        # Active pool must now only have S1 and S3
        active = self.registry.get_active_pool()
        self.assertEqual(len(active), 2)
        self.assertNotIn("server-2", [s.id for s in active])

    def test_recovery_lifecycle_and_readmission(self):
        """
        Verify the complete lifecycle:
        HEALTHY -> UNHEALTHY -> RECOVERING -> HEALTHY (Re-admitted)
        """
        # Step 1: Inject failure
        self.registry.update_health_result("server-2", is_healthy=False, latency_ms=10.0, check_time=time.time())
        self.assertEqual(self.s2.status, "UNHEALTHY")

        # Step 2: First successful check -> Should transition to RECOVERING, NOT yet HEALTHY
        t_rec1 = time.time()
        event1, _ = self.registry.update_health_result("server-2", is_healthy=True, latency_ms=8.0, check_time=t_rec1)
        self.assertEqual(event1, "RECOVERY_DETECTED")
        self.assertEqual(self.s2.status, "RECOVERING")
        self.assertEqual(self.s2.consecutive_successes, 1)

        # Still excluded from active routing pool while in RECOVERING state!
        self.assertNotIn("server-2", [s.id for s in self.registry.get_active_pool()])

        # Step 3: Second consecutive successful check -> Meets threshold (2), transitions to HEALTHY (Re-admitted)
        t_rec2 = time.time() + 2.0
        event2, _ = self.registry.update_health_result("server-2", is_healthy=True, latency_ms=7.5, check_time=t_rec2)
        self.assertEqual(event2, "SERVER_READMITTED")
        self.assertEqual(self.s2.status, "HEALTHY")
        self.assertEqual(self.s2.consecutive_successes, 2)

        # Must now be returned to the active routing pool!
        self.assertIn("server-2", [s.id for s in self.registry.get_active_pool()])

    def test_recovering_interrupted_by_failure(self):
        """If a server in RECOVERING fails another check, it reverts to UNHEALTHY."""
        # Fail -> Recover (1 check)
        self.registry.update_health_result("server-1", is_healthy=False, latency_ms=10.0, check_time=time.time())
        self.registry.update_health_result("server-1", is_healthy=True, latency_ms=8.0, check_time=time.time())
        self.assertEqual(self.s1.status, "RECOVERING")

        # Probe fails before second check
        event, _ = self.registry.update_health_result("server-1", is_healthy=False, latency_ms=15.0, check_time=time.time())
        self.assertEqual(event, "FAILURE_DETECTED")
        self.assertEqual(self.s1.status, "UNHEALTHY")
        self.assertEqual(self.s1.consecutive_successes, 0)

if __name__ == "__main__":
    unittest.main()
