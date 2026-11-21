"""
Total backend failure with data loss: orchestration of detection, recovery and verification.

Sequence for disaster(strategy):

  1. inject a fault into EVERY registered backend (default: refused = data ports closed)
     and delete the backend-local primary database (simulated disk loss)
  2. the health monitor detects the failures; with 0 usable backends the service is CRITICAL
     and the proxy answers 503 (requests are not routed to unhealthy backends)
  3. stage-1 automatic recovery starts a replacement backend and verifies it with health checks
  4. as soon as a backend is ready, the data tier is rebuilt from durable storage with the
     selected strategy (backup / replica / wal / none) and its integrity is verified
  5. request routing is re-enabled only after step 4 succeeded; with no usable durable copy
     the data loss is reported and routing stays blocked until an operator acknowledges it
  6. the capacity manager then measures the restored workload and decides how many servers
     are needed (it starts a new observation window when service is restored)

Measured (from this simulation run, wall-clock):
  detection_ms     disaster start -> health monitor reports 0 usable backends
  backend_ready_ms disaster start -> first backend HEALTHY again
  data_restore_ms  duration of the data rebuild
  rto_ms           disaster start -> first client request served after recovery
  rpo              acknowledged records / writes / seconds of writes that were not restored
"""

import time
import threading
from typing import Any, Callable, Dict, Optional

IDLE, OUTAGE, RESTORING, RESTORED, DATA_LOSS, FAILED = (
    "IDLE", "OUTAGE", "RESTORING_DATA", "RESTORED", "DATA_LOSS_ACK_REQUIRED", "FAILED")


class DisasterRecovery:
    def __init__(self, registry, faults, datastore, metrics, capacity=None, strategies=("backup", "replica", "wal", "none"),
                 clock: Optional[Callable[[], float]] = None):
        self.registry = registry
        self.faults = faults
        self.datastore = datastore
        self.metrics = metrics
        self.capacity = capacity
        self.strategies = tuple(strategies)
        self._clock = clock or time.time
        self._lock = threading.RLock()
        self._reset_state()

    def _reset_state(self):
        self.state = IDLE
        self.strategy = None
        self.fault = None
        self.t_start = None
        self.t_detected = None
        self.t_backend_ready = None
        self.t_data_restored = None
        self.t_routing_restored = None
        self.records_before = None
        self.data_report = None
        self.message = "No disaster in progress."

    # ------------------------------------------------------------------ #
    def start(self, strategy: str, fault: str = "refused") -> Dict[str, Any]:
        if strategy not in self.strategies:
            raise ValueError(f"strategy must be one of {', '.join(self.strategies)}")
        with self._lock:
            if self.state in (OUTAGE, RESTORING):
                raise RuntimeError("a disaster scenario is already running")
            self._reset_state()
            self.strategy = strategy
            self.fault = fault
            self.t_start = self._clock()
            self.records_before = len(self.datastore.ledger)
            self.state = OUTAGE
            self.message = "All backends failing; backend-local data lost."
        # A total failure takes the replacement instances down as well. They are retired (process
        # stopped, unregistered) so that stage-1 recovery starts from a clean state and is not
        # blocked by its replacement limit.
        retired = self.capacity.retire_all_replacements() if self.capacity is not None else 0
        if self.capacity is not None:
            self.capacity.activate_all_standby()     # standby originals fail with the rest
        if retired:
            self.metrics.add_log(f"Total failure: {retired} replacement backend(s) lost and removed from the proxy.",
                                 level="ERROR", event_type="DISASTER")
        failed = []
        for s in self.registry.get_all_servers():
            try:
                self.faults.inject(s.id, fault)
                failed.append(s.name)
            except Exception as ex:
                self.metrics.add_log(f"Could not fail {s.name}: {ex}", level="ERROR", event_type="DISASTER")
        lost = self.datastore.wipe_backend_local()
        self.metrics.add_log(f"DISASTER: total backend failure ({fault}) on {', '.join(failed)}; backend-local data with "
                             f"{lost} records lost. Recovery strategy: {strategy}.", level="ERROR", event_type="DISASTER")
        return self.status()

    def routing_block_reason(self) -> Optional[str]:
        """Gate used by the load balancer: requests are not routed while data is not verified."""
        st = self.state
        if st == OUTAGE:
            return "TOTAL OUTAGE: data recovery pending; requests are not routed until data is restored and verified"
        if st == RESTORING:
            return "DATA RECOVERY IN PROGRESS: routing resumes after restore and integrity verification"
        if st in (DATA_LOSS, FAILED):
            return "DATA LOST: no usable durable copy; operator must acknowledge before service resumes"
        return None

    def tick(self):
        """Advance the scenario (called periodically)."""
        with self._lock:
            if self.state not in (OUTAGE,):
                return
            servers = self.registry.get_all_servers()
            usable = [s for s in servers if s.status == "HEALTHY"]
            now = self._clock()
            if self.t_detected is None and servers and not usable:
                self.t_detected = now
                self.metrics.add_log(f"TOTAL OUTAGE detected {1000 * (now - self.t_start):.0f} ms after the disaster: "
                                     f"0 usable backends, service CRITICAL.", level="ERROR", event_type="DISASTER")
            if not usable or self.t_detected is None:
                return
            self.t_backend_ready = now
            self.state = RESTORING
            self.message = f"{usable[0].name} is ready; restoring data with strategy '{self.strategy}'."
            strategy = self.strategy
        self.metrics.add_log(f"{usable[0].name} passed its health checks; restoring data ({strategy}) before routing "
                             f"resumes.", level="INFO", event_type="DISASTER")
        try:
            report = self.datastore.recover(strategy)
        except Exception as ex:
            report = {"strategy": strategy, "integrity_ok": False, "integrity": f"FAILED ({ex})", "notes": [str(ex)],
                      "source_available": False, "restored_records": 0, "rpo_records": None}
        with self._lock:
            if self.state != RESTORING:            # reset during the restore
                return
            self.data_report = report
            self.t_data_restored = self._clock()
            from load_balancer.datastore import STATE_DATA_LOST
            if self.datastore.state == STATE_DATA_LOST:
                self.state = DATA_LOSS
                self.message = (f"{report.get('rpo_records')} acknowledged record(s) cannot be recovered with strategy "
                                f"'{strategy}'. Manual intervention required.")
                lvl = "ERROR"
            elif not report.get("integrity_ok", False):
                self.state = FAILED
                self.message = f"Restored data failed integrity verification: {report.get('integrity')}"
                lvl = "ERROR"
            else:
                self.state = RESTORED
                self.message = (f"Data restored and verified ({report['restored_records']} records). "
                                + ("No acknowledged data lost." if report.get("zero_data_loss")
                                   else f"{report['rpo_records']} acknowledged record(s) LOST (RPO)."))
                lvl = "SUCCESS" if report.get("zero_data_loss") else "WARN"
            msg = self.message
        self.metrics.add_log(f"Disaster recovery: {msg}", level=lvl, event_type="DISASTER")
        if self.capacity is not None:
            self.capacity.notify()

    def acknowledge(self) -> bool:
        with self._lock:
            if self.state not in (DATA_LOSS, FAILED):
                return False
            if self.state == DATA_LOSS:
                self.datastore.accept_loss()
            self.state = RESTORED
            self.message = "Data loss acknowledged; service resumed with the restored (possibly empty) dataset."
        self.metrics.add_log(self.message, level="WARN", event_type="DISASTER")
        return True

    def on_request_served(self, server_id: str):
        with self._lock:
            if self.state == RESTORED and self.t_routing_restored is None:
                self.t_routing_restored = self._clock()
                rto = self.t_routing_restored - self.t_start
                self.metrics.add_log(f"Traffic restored after the disaster: RTO {rto * 1000:.0f} ms "
                                     f"(disaster start -> first request served by {server_id}).",
                                     level="SUCCESS", event_type="DISASTER")

    def reset(self):
        with self._lock:
            self._reset_state()

    # ------------------------------------------------------------------ #
    def status(self) -> Dict[str, Any]:
        with self._lock:
            def ms(t):
                return None if t is None or self.t_start is None else round((t - self.t_start) * 1000.0, 1)
            return {
                "state": self.state, "strategy": self.strategy, "fault": self.fault, "message": self.message,
                "routing_blocked": self.routing_block_reason(),
                "records_acknowledged_before": self.records_before,
                "timings_ms": {"detection": ms(self.t_detected), "backend_ready": ms(self.t_backend_ready),
                               "data_restored": ms(self.t_data_restored), "rto": ms(self.t_routing_restored)},
                "data_report": self.data_report,
                "strategies": list(self.strategies),
                "labels": {"timings_ms": "measured wall-clock time in this simulation run (not production RTO)"},
            }
