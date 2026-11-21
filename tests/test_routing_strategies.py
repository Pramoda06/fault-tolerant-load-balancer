"""
Tests for the six routing strategies and runtime switching.

1. Round Robin ignores weights; Weighted Round Robin follows them.
2. Least Connections picks the server with the fewest active connections and the
   proxy maintains those counts correctly (also on failures).
3. Weighted Least Connections uses score = active_connections / weight.
4. Least Response Time uses measured response times and handles missing data.
5. IP Hash is sticky per client, only remaps clients of a failed server, and
   remaps them back after recovery.
6. Unhealthy servers are never selected by any strategy.
7. Switching strategy changes the backend routing and keeps weights, counters,
   metrics and server health.
"""

import time
import unittest
from collections import Counter
from unittest.mock import patch, MagicMock

from load_balancer.server_registry import ServerRegistry
from load_balancer.scheduler import (SchedulerManager, RoundRobinScheduler, WeightedRoundRobinScheduler,
                                     LeastConnectionsScheduler, WeightedLeastConnectionsScheduler,
                                     LeastResponseTimeScheduler, IPHashScheduler)
from load_balancer.metrics import MetricsCollector
from load_balancer.balancer import LoadBalancer

ALL = list(SchedulerManager.ALGORITHMS)


def make_registry(weights=(3, 2, 1)):
    reg = ServerRegistry(recovery_threshold=2)
    for i, w in enumerate(weights, start=1):
        reg.register_server(f"server-{i}", f"Server {i}", "127.0.0.1", 5000 + i, weight=w)
    return reg


def ok_response():
    r = MagicMock(status_code=200)
    r.json.return_value = {"status": "healthy"}
    return r


class TestStrategies(unittest.TestCase):

    def test_round_robin_ignores_weights(self):
        reg = make_registry((5, 1, 1))
        rr = RoundRobinScheduler()
        picks = [rr.select(reg.get_active_pool()).id for _ in range(30)]
        self.assertEqual(Counter(picks), {"server-1": 10, "server-2": 10, "server-3": 10})

    def test_weighted_round_robin_uses_weights(self):
        reg = make_registry((3, 2, 1))
        wrr = WeightedRoundRobinScheduler()
        picks = [wrr.select(reg.get_active_pool()).id for _ in range(60)]
        self.assertEqual(Counter(picks), {"server-1": 30, "server-2": 20, "server-3": 10})

    def test_least_connections_picks_fewest_active(self):
        reg = make_registry()
        lc = LeastConnectionsScheduler()
        for sid, n in (("server-1", 3), ("server-2", 1), ("server-3", 2)):
            reg.get_server(sid).active_connections = n
        self.assertEqual(lc.select(reg.get_active_pool()).id, "server-2")
        reg.get_server("server-2").active_connections = 5
        self.assertEqual(lc.select(reg.get_active_pool()).id, "server-3")

    def test_least_connections_ties_rotate_and_ignore_weights(self):
        reg = make_registry((5, 1, 1))
        lc = LeastConnectionsScheduler()
        picks = [lc.select(reg.get_active_pool()).id for _ in range(30)]     # all idle -> all tied
        self.assertEqual(Counter(picks), {"server-1": 10, "server-2": 10, "server-3": 10})

    def test_least_connections_simulated_concurrency(self):
        """Requests that stay open accumulate; LC keeps the in-flight counts balanced."""
        reg = make_registry()
        lc = LeastConnectionsScheduler()
        for _ in range(9):                       # 9 overlapping requests, none finished yet
            s = lc.select(reg.get_active_pool())
            reg.connection_started(s.id)
        self.assertEqual([s.active_connections for s in reg.get_all_servers()], [3, 3, 3])

    def test_weighted_least_connections_formula(self):
        reg = make_registry((3, 2, 1))
        wlc = WeightedLeastConnectionsScheduler()
        # scores: 3/3 = 1.0, 1/2 = 0.5, 1/1 = 1.0 -> server-2
        for sid, n in (("server-1", 3), ("server-2", 1), ("server-3", 1)):
            reg.get_server(sid).active_connections = n
        self.assertEqual(wlc.select(reg.get_active_pool()).id, "server-2")
        # scores: 2/3 = 0.67, 2/2 = 1.0, 1/1 = 1.0 -> server-1 (LC alone would tie S3)
        for sid, n in (("server-1", 2), ("server-2", 2), ("server-3", 1)):
            reg.get_server(sid).active_connections = n
        self.assertEqual(wlc.select(reg.get_active_pool()).id, "server-1")

    def test_weighted_least_connections_concurrency_follows_weights(self):
        reg = make_registry((3, 2, 1))
        wlc = WeightedLeastConnectionsScheduler()
        for _ in range(12):                      # 12 overlapping requests
            reg.connection_started(wlc.select(reg.get_active_pool()).id)
        self.assertEqual([s.active_connections for s in reg.get_all_servers()], [6, 4, 2])

    def test_weighted_least_connections_idle_ties_use_weights(self):
        reg = make_registry((3, 2, 1))
        wlc = WeightedLeastConnectionsScheduler()
        picks = [wlc.select(reg.get_active_pool()).id for _ in range(60)]
        self.assertEqual(Counter(picks), {"server-1": 30, "server-2": 20, "server-3": 10})

    def test_least_response_time_uses_measurements(self):
        reg = make_registry()
        lrt = LeastResponseTimeScheduler()
        for sid, ms in (("server-1", 30.0), ("server-2", 12.0), ("server-3", 20.0)):
            reg.connection_started(sid)
            reg.connection_finished(sid, ms)
        picks = [lrt.select(reg.get_active_pool()).id for _ in range(5)]
        self.assertEqual(set(picks), {"server-2"})
        self.assertAlmostEqual(reg.get_server("server-2").recent_response_ms(), 12.0)

    def test_least_response_time_missing_measurements_tried_first(self):
        reg = make_registry()
        lrt = LeastResponseTimeScheduler()
        reg.connection_finished("server-1", 5.0)          # only server-1 measured
        first = [lrt.select(reg.get_active_pool()).id for _ in range(2)]
        self.assertEqual(sorted(first), ["server-2", "server-3"])

    def test_least_response_time_ignores_stale_measurements(self):
        reg = make_registry()
        lrt = LeastResponseTimeScheduler()
        for sid, ms in (("server-1", 5.0), ("server-2", 15.0), ("server-3", 25.0)):
            reg.connection_finished(sid, ms)
        old = time.time() - 60                             # server-1's sample is 60 s old
        s1 = reg.get_server("server-1")
        s1._response_samples.clear()
        s1._response_samples.append((old, 5.0))
        self.assertIsNone(s1.recent_response_ms())
        self.assertEqual(lrt.select(reg.get_active_pool()).id, "server-1")   # re-measured first

    def test_failed_request_is_not_a_response_time_sample(self):
        reg = make_registry()
        reg.connection_started("server-1")
        reg.connection_finished("server-1", None)
        self.assertIsNone(reg.get_server("server-1").recent_response_ms())
        self.assertEqual(reg.get_server("server-1").active_connections, 0)

    def test_ip_hash_sticky_and_failover(self):
        reg = make_registry()
        ih = IPHashScheduler()
        clients = [f"10.0.0.{i}" for i in range(1, 41)]
        first = {ip: ih.select(reg.get_active_pool(), client_ip=ip).id for ip in clients}
        again = {ip: ih.select(reg.get_active_pool(), client_ip=ip).id for ip in clients}
        self.assertEqual(first, again)                                  # sticky
        self.assertEqual(len(set(first.values())), 3)                   # clients spread over servers
        reg.get_server("server-2").status = "UNHEALTHY"
        during = {ip: ih.select(reg.get_active_pool(), client_ip=ip).id for ip in clients}
        for ip in clients:
            self.assertNotEqual(during[ip], "server-2")
            if first[ip] != "server-2":
                self.assertEqual(during[ip], first[ip])                 # only server-2's clients moved
        reg.get_server("server-2").status = "HEALTHY"
        after = {ip: ih.select(reg.get_active_pool(), client_ip=ip).id for ip in clients}
        self.assertEqual(after, first)                                  # mapping restored

    def test_no_strategy_selects_unhealthy_server(self):
        for algo in ALL:
            reg = make_registry()
            reg.get_server("server-1").status = "UNHEALTHY"
            reg.get_server("server-3").status = "RECOVERING"
            mgr = SchedulerManager(algo)
            picks = {mgr.select_server(reg.get_active_pool(), client_ip=f"10.1.1.{i}").id for i in range(30)}
            self.assertEqual(picks, {"server-2"}, algo)


class TestProxyIntegration(unittest.TestCase):

    def make_lb(self, algo, weights=(3, 2, 1)):
        reg = make_registry(weights)
        mgr = SchedulerManager(algo)
        metrics = MetricsCollector()
        return reg, mgr, metrics, LoadBalancer(reg, mgr, metrics)

    @patch("load_balancer.balancer.requests.get")
    def test_active_connections_maintained_by_proxy(self, mock_get):
        reg, mgr, metrics, lb = self.make_lb("LEAST_CONNECTIONS")
        seen = []

        def fake_get(url, headers=None, timeout=None):
            port = int(url.split(":")[2].split("/")[0])
            node = next(s for s in reg.get_all_servers() if s.port == port)
            seen.append(node.active_connections)
            return ok_response()

        mock_get.side_effect = fake_get
        for _ in range(6):
            self.assertEqual(lb.handle_request()[0], 200)
        self.assertEqual(seen, [1] * 6)                                  # counted while forwarding
        self.assertTrue(all(s.active_connections == 0 for s in reg.get_all_servers()))
        self.assertTrue(all(s.recent_response_ms() is not None for s in reg.get_all_servers()))

    @patch("load_balancer.balancer.requests.get")
    def test_connection_released_on_backend_error(self, mock_get):
        reg, mgr, metrics, lb = self.make_lb("LEAST_CONNECTIONS")
        mock_get.side_effect = Exception("connection refused")
        code, _ = lb.handle_request()
        self.assertEqual(code, 503)
        self.assertTrue(all(s.active_connections == 0 for s in reg.get_all_servers()))
        self.assertTrue(all(s.recent_response_ms() is None for s in reg.get_all_servers()))

    @patch("load_balancer.balancer.requests.get")
    def test_ip_hash_uses_client_ip_through_proxy(self, mock_get):
        reg, mgr, metrics, lb = self.make_lb("IP_HASH")
        mock_get.return_value = ok_response()
        a = {lb.handle_request(client_ip="192.168.1.7")[1]["server_id"] for _ in range(10)}
        self.assertEqual(len(a), 1)

    @patch("load_balancer.balancer.requests.get")
    def test_switching_changes_routing_and_keeps_state(self, mock_get):
        reg, mgr, metrics, lb = self.make_lb("ROUND_ROBIN")
        mock_get.return_value = ok_response()
        reg.get_server("server-3").status = "UNHEALTHY"
        rr = Counter(lb.handle_request()[1]["server_id"] for _ in range(10))
        self.assertEqual(rr, {"server-1": 5, "server-2": 5})
        before = (metrics.total_requests, [s.requests_served for s in reg.get_all_servers()])
        self.assertTrue(mgr.set_algorithm("IP_HASH"))
        ih = Counter(lb.handle_request(client_ip="10.9.9.9")[1]["server_id"] for _ in range(10))
        self.assertEqual(len(ih), 1)                                     # sticky: one server only
        self.assertTrue(mgr.set_algorithm("WEIGHTED_ROUND_ROBIN"))
        wrr = Counter(lb.handle_request()[1]["server_id"] for _ in range(10))
        self.assertEqual(wrr, {"server-1": 6, "server-2": 4})            # weights 3:2 (S3 down)
        # switching kept weights, health, counters and metrics
        self.assertEqual([s.weight for s in reg.get_all_servers()], [3, 2, 1])
        self.assertEqual(reg.get_server("server-3").status, "UNHEALTHY")
        self.assertEqual(metrics.total_requests, before[0] + 20)
        self.assertEqual(sum(s.requests_served for s in reg.get_all_servers()), sum(before[1]) + 20)


if __name__ == "__main__":
    unittest.main()
