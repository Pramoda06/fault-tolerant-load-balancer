"""
Durable data tier for the disaster-recovery demonstration (SQLite, local files).

Separation of availability and durability:

  data/backend_local/primary.sqlite   the application's live data, attached to the backend
                                      tier. A total backend failure with data loss deletes it.
  data/durable/                       storage that does NOT belong to any backend process:
      wal.jsonl                       write-ahead log: one JSON line per committed write with a
                                      log sequence number (LSN) and a SHA-256 checksum
      replica.sqlite                  synchronous replica (updated before a write is acknowledged)
      snapshots/snap_<n>.sqlite       periodic backups (consistent copies via the SQLite backup API)
      manifest.json                   recovery manifest: snapshots with LSN + SHA-256, last WAL LSN,
                                      replica LSN, timeline

Write path (one lock, in this order): append to WAL and fsync -> apply to primary ->
apply to replica -> acknowledge. A write is "durable" once it is in the WAL.

Recovery strategies (recover()):
  backup   restore the newest snapshot whose SHA-256 matches the manifest and whose records
           pass their checksums (falls back to older snapshots). Writes after it are lost.
  replica  restore the replica after integrity checks. A replica also contains any accidental
           deletion or corruption that was replicated to it.
  wal      restore the newest valid snapshot (or start empty) and replay committed WAL entries
           with higher LSNs; replay stops at the first entry with a bad checksum or an LSN gap.
  none     no durable copy is used: every record is reported unrecoverable.

Measurement only: an in-memory "ack ledger" remembers what was acknowledged to clients, so the
experiment can count lost records (RPO). It is never used to restore data.

This is a single-machine simulation: "replica" and "backup" are files on the same disk, not
copies on another host or site.
"""

import os
import json
import time
import glob
import shutil
import sqlite3
import hashlib
import threading
from collections import deque
from typing import Any, Callable, Dict, List, Optional

STATE_OK = "OK"
STATE_LOST = "LOST"                    # backend-local data lost; recovery pending
STATE_RECOVERING = "RECOVERING"
STATE_DATA_LOST = "DATA_LOST"          # recovery could not restore every acknowledged record / no source


class DataUnavailable(Exception):
    pass


def record_checksum(rid, name, value, version) -> str:
    return hashlib.sha256(f"{rid}|{name}|{value}|{version}".encode()).hexdigest()[:16]


def entry_checksum(entry: dict) -> str:
    body = {k: entry[k] for k in sorted(entry) if k != "crc"}
    return hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()[:16]


def file_sha256(path: str) -> Optional[str]:
    if not os.path.exists(path):
        return None
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


SCHEMA = """
CREATE TABLE IF NOT EXISTS records (
    id INTEGER PRIMARY KEY, name TEXT NOT NULL, value TEXT NOT NULL,
    version INTEGER NOT NULL, updated_at REAL NOT NULL, lsn INTEGER NOT NULL, checksum TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
"""


class DataStore:
    def __init__(self, base_dir: str, snapshot_interval: float = 30.0, keep_snapshots: int = 5,
                 clock: Optional[Callable[[], float]] = None, metrics=None):
        self.base_dir = base_dir
        self.local_dir = os.path.join(base_dir, "backend_local")
        self.durable_dir = os.path.join(base_dir, "durable")
        self.snap_dir = os.path.join(self.durable_dir, "snapshots")
        self.primary_path = os.path.join(self.local_dir, "primary.sqlite")
        self.replica_path = os.path.join(self.durable_dir, "replica.sqlite")
        self.wal_path = os.path.join(self.durable_dir, "wal.jsonl")
        self.manifest_path = os.path.join(self.durable_dir, "manifest.json")
        self.snapshot_interval = snapshot_interval
        self.keep_snapshots = keep_snapshots
        self._clock = clock or time.time
        self.metrics = metrics
        self._lock = threading.RLock()
        self.state = STATE_OK
        self.last_recovery: Optional[Dict[str, Any]] = None
        self.ledger: Dict[int, dict] = {}            # measurement only: acknowledged state per record id
        self._ledger_lsn_ts: Dict[int, float] = {}   # measurement only: lsn -> ack time
        self._snap_thread = None
        self._stop = threading.Event()
        self._write_times = deque(maxlen=50)          # (log ms, own disk ms, live copy ms) of recent writes
        if not self._open_existing():
            self.reset()

    def _open_existing(self) -> bool:
        """Re-open data that survived a proxy restart (durable storage is not reset on start)."""
        if not os.path.exists(self.manifest_path) or not os.path.exists(self.replica_path):
            return False
        m = self._read_manifest()
        self._last_lsn = int(m.get("last_wal_lsn", 0))
        self.state = STATE_OK if os.path.exists(self.primary_path) else STATE_LOST
        self.ledger, self._ledger_lsn_ts = {}, {}
        if self.state == STATE_OK:
            c = sqlite3.connect(self.primary_path)
            for rid, version, checksum, lsn, ts in c.execute("SELECT id, version, checksum, lsn, updated_at FROM records"):
                self.ledger[rid] = {"version": version, "checksum": checksum, "lsn": lsn, "ts": ts}
                self._ledger_lsn_ts[lsn] = ts
            c.close()
        return True

    # ------------------------------------------------------------------ #
    # Files and connections                                                #
    # ------------------------------------------------------------------ #
    @staticmethod
    def _connect(path: str) -> sqlite3.Connection:
        conn = sqlite3.connect(path, timeout=5, check_same_thread=False)
        conn.executescript(SCHEMA)
        return conn

    def _log(self, msg, level="INFO", event="DATA"):
        if self.metrics:
            self.metrics.add_log(msg, level=level, event_type=event)

    def _read_manifest(self) -> dict:
        try:
            with open(self.manifest_path) as f:
                return json.load(f)
        except Exception:
            return {"version": 1, "timeline": 1, "snapshots": [], "last_wal_lsn": 0, "replica_lsn": 0}

    def _write_manifest(self, m: dict):
        m["updated_at"] = self._clock()
        tmp = self.manifest_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(m, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.manifest_path)

    def reset(self):
        """Start a fresh, empty dataset with empty durable storage."""
        with self._lock:
            for d in (self.local_dir, self.durable_dir):
                if os.path.isdir(d):
                    shutil.rmtree(d, ignore_errors=True)
            os.makedirs(self.local_dir, exist_ok=True)
            os.makedirs(self.snap_dir, exist_ok=True)
            for p in (self.primary_path, self.replica_path):
                c = self._connect(p)
                c.execute("INSERT OR REPLACE INTO meta VALUES ('last_lsn', '0')")
                c.commit()
                c.close()
            open(self.wal_path, "w").close()
            self._write_manifest({"version": 1, "timeline": 1, "snapshots": [], "last_wal_lsn": 0, "replica_lsn": 0})
            self.state = STATE_OK
            self.last_recovery = None
            self.ledger = {}
            self._ledger_lsn_ts = {}
            self._last_lsn = 0

    # ------------------------------------------------------------------ #
    # Write path                                                           #
    # ------------------------------------------------------------------ #
    def _apply(self, conn: sqlite3.Connection, e: dict):
        if e["op"] == "delete":
            conn.execute("DELETE FROM records WHERE id=?", (e["id"],))
        else:
            conn.execute("INSERT OR REPLACE INTO records (id, name, value, version, updated_at, lsn, checksum) "
                         "VALUES (?, ?, ?, ?, ?, ?, ?)",
                         (e["id"], e["name"], e["value"], e["version"], e["ts"], e["lsn"],
                          record_checksum(e["id"], e["name"], e["value"], e["version"])))
        conn.execute("INSERT OR REPLACE INTO meta VALUES ('last_lsn', ?)", (str(e["lsn"]),))

    def _commit(self, op: str, rid: Optional[int], name: Optional[str], value: Optional[str]) -> dict:
        with self._lock:
            if self.state != STATE_OK:
                raise DataUnavailable(f"data tier is {self.state}; writes are not accepted")
            if not os.path.exists(self.primary_path):
                raise DataUnavailable("primary data is missing")
            prim = self._connect(self.primary_path)
            try:
                if rid is None:
                    row = prim.execute("SELECT COALESCE(MAX(id), 0) FROM records").fetchone()
                    rid = max(row[0], max(self.ledger) if self.ledger else 0) + 1
                    version = 1
                else:
                    row = prim.execute("SELECT name, version FROM records WHERE id=?", (rid,)).fetchone()
                    if row is None:
                        raise KeyError(f"record {rid} not found")
                    name = name if name is not None else row[0]
                    version = row[1] + 1
                if op == "delete":
                    value = None
                lsn = self._last_lsn + 1
                e = {"lsn": lsn, "op": op, "id": rid, "name": name, "value": value, "version": version,
                     "ts": self._clock()}
                e["crc"] = entry_checksum(e)
                # 1) write-ahead log (durable before anything else)
                t_a = time.perf_counter()
                with open(self.wal_path, "a") as f:
                    f.write(json.dumps(e) + "\n")
                    f.flush()
                    os.fsync(f.fileno())
                t_b = time.perf_counter()
                # 2) primary
                self._apply(prim, e)
                prim.commit()
                t_c = time.perf_counter()
            finally:
                prim.close()
            # 3) synchronous replica: the client is answered only after this copy is written too
            rep = self._connect(self.replica_path)
            try:
                self._apply(rep, e)
                rep.commit()
            finally:
                rep.close()
            t_d = time.perf_counter()
            self._write_times.append(((t_b - t_a) * 1000, (t_c - t_b) * 1000, (t_d - t_c) * 1000))
            self._last_lsn = lsn
            m = self._read_manifest()
            m["last_wal_lsn"] = lsn
            m["replica_lsn"] = lsn
            self._write_manifest(m)
            # measurement-only ledger of acknowledged writes
            if op == "delete":
                self.ledger.pop(rid, None)
            else:
                self.ledger[rid] = {"version": version, "checksum": record_checksum(rid, name, value, version),
                                    "lsn": lsn, "ts": e["ts"]}
            self._ledger_lsn_ts[lsn] = e["ts"]
            return {"id": rid, "name": name, "value": value, "version": version, "lsn": lsn, "op": op}

    def create(self, name: str, value: str) -> dict:
        return self._commit("upsert", None, str(name), str(value))

    def update(self, rid: int, value: str) -> dict:
        return self._commit("upsert", int(rid), None, str(value))

    def delete(self, rid: int) -> dict:
        return self._commit("delete", int(rid), None, None)

    def list_records(self, limit: int = 200) -> List[dict]:
        with self._lock:
            if self.state in (STATE_LOST, STATE_RECOVERING) or not os.path.exists(self.primary_path):
                raise DataUnavailable(f"data tier is {self.state}")
            c = self._connect(self.primary_path)
            try:
                rows = c.execute("SELECT id, name, value, version, updated_at, lsn FROM records ORDER BY id DESC LIMIT ?",
                                 (limit,)).fetchall()
            finally:
                c.close()
            return [{"id": r[0], "name": r[1], "value": r[2], "version": r[3], "updated_at": r[4], "lsn": r[5]}
                    for r in rows]

    # ------------------------------------------------------------------ #
    # Backups                                                              #
    # ------------------------------------------------------------------ #
    def snapshot(self, reason: str = "manual") -> dict:
        with self._lock:
            if self.state != STATE_OK or not os.path.exists(self.primary_path):
                raise DataUnavailable(f"cannot back up: data tier is {self.state}")
            m = self._read_manifest()
            n = (max([s["n"] for s in m["snapshots"]]) if m["snapshots"] else 0) + 1
            path = os.path.join(self.snap_dir, f"snap_{n:04d}.sqlite")
            src = sqlite3.connect(self.primary_path)
            dst = sqlite3.connect(path)
            try:
                src.backup(dst)
                count = dst.execute("SELECT COUNT(*) FROM records").fetchone()[0]
                lsn = int(dst.execute("SELECT value FROM meta WHERE key='last_lsn'").fetchone()[0])
            finally:
                dst.close()
                src.close()
            info = {"n": n, "file": os.path.basename(path), "lsn": lsn, "records": count,
                    "sha256": file_sha256(path), "created_at": self._clock(), "reason": reason}
            m["snapshots"].append(info)
            while len(m["snapshots"]) > self.keep_snapshots:
                old = m["snapshots"].pop(0)
                try:
                    os.remove(os.path.join(self.snap_dir, old["file"]))
                except OSError:
                    pass
            self._write_manifest(m)
        self._log(f"Backup snapshot {info['file']} taken ({count} records, up to LSN {lsn}, {reason}).", "INFO", "DATA_BACKUP")
        return info

    def start_periodic(self, interval_getter: Callable[[], float]):
        def loop():
            last = time.time()
            while not self._stop.wait(1.0):
                iv = interval_getter()
                if iv and iv > 0 and time.time() - last >= iv:
                    last = time.time()
                    try:
                        if self.state == STATE_OK and self._last_lsn > 0:
                            self.snapshot("periodic")
                    except Exception as ex:
                        self._log(f"Periodic backup failed: {ex}", "ERROR", "DATA_BACKUP")
        self._snap_thread = threading.Thread(target=loop, daemon=True, name="PeriodicBackup")
        self._snap_thread.start()

    def stop(self):
        self._stop.set()

    # ------------------------------------------------------------------ #
    # Simulated failures                                                   #
    # ------------------------------------------------------------------ #
    def wipe_backend_local(self) -> int:
        """Total backend failure with data loss: the backend-local primary is gone."""
        with self._lock:
            n = 0
            if os.path.exists(self.primary_path):
                c = sqlite3.connect(self.primary_path)
                n = c.execute("SELECT COUNT(*) FROM records").fetchone()[0]
                c.close()
                os.remove(self.primary_path)
            self.state = STATE_LOST
        self._log(f"Backend-local data LOST: primary database with {n} records deleted (simulated disk loss). "
                  f"Durable storage (WAL, replica, backups) is separate and untouched.", "ERROR", "DATA_LOSS")
        return n

    def corrupt(self, target: str) -> str:
        with self._lock:
            if target == "snapshot":
                m = self._read_manifest()
                if not m["snapshots"]:
                    raise ValueError("no snapshot to corrupt")
                path = os.path.join(self.snap_dir, m["snapshots"][-1]["file"])
                with open(path, "r+b") as f:
                    f.seek(max(0, os.path.getsize(path) // 2))
                    b = f.read(16)
                    f.seek(max(0, os.path.getsize(path) // 2))
                    f.write(bytes((x ^ 0xFF) for x in b))
                msg = f"latest backup {m['snapshots'][-1]['file']} corrupted (bytes flipped; SHA-256 no longer matches)"
            elif target == "wal":
                with open(self.wal_path) as f:
                    lines = f.readlines()
                if not lines:
                    raise ValueError("WAL is empty")
                i = len(lines) // 2
                e = json.loads(lines[i])
                e["value"] = (e.get("value") or "") + " [tampered]"    # crc left unchanged -> mismatch
                lines[i] = json.dumps(e) + "\n"
                with open(self.wal_path, "w") as f:
                    f.writelines(lines)
                msg = f"WAL entry LSN {e['lsn']} corrupted (checksum no longer matches)"
            elif target == "replica":
                c = sqlite3.connect(self.replica_path)
                row = c.execute("SELECT id FROM records ORDER BY id LIMIT 1").fetchone()
                if row is None:
                    c.close()
                    raise ValueError("replica has no records")
                c.execute("UPDATE records SET value = value || ' [bit rot]' WHERE id=?", (row[0],))
                c.commit()
                c.close()
                msg = f"replica record {row[0]} silently corrupted (record checksum no longer matches)"
            else:
                raise ValueError("target must be snapshot, wal or replica")
        self._log(f"Simulated corruption: {msg}.", "WARN", "DATA_CORRUPTION")
        return msg

    # ------------------------------------------------------------------ #
    # Verification and recovery                                            #
    # ------------------------------------------------------------------ #
    @staticmethod
    def _verify_db(path: str):
        """(ok, rows, bad_ids, lsn, error) for a SQLite file."""
        try:
            c = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
            try:
                if c.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                    return False, [], [], 0, "SQLite integrity_check failed"
                rows = c.execute("SELECT id, name, value, version, updated_at, lsn, checksum FROM records").fetchall()
                lsn_row = c.execute("SELECT value FROM meta WHERE key='last_lsn'").fetchone()
            finally:
                c.close()
        except sqlite3.DatabaseError as ex:
            return False, [], [], 0, f"unreadable: {ex}"
        bad = [r[0] for r in rows if record_checksum(r[0], r[1], r[2], r[3]) != r[6]]
        return True, rows, bad, int(lsn_row[0]) if lsn_row else 0, None

    def _valid_snapshots(self, notes: List[str]):
        m = self._read_manifest()
        for s in reversed(m["snapshots"]):
            path = os.path.join(self.snap_dir, s["file"])
            if file_sha256(path) != s["sha256"]:
                notes.append(f"{s['file']}: SHA-256 mismatch with manifest (corrupted) - skipped")
                continue
            ok, rows, bad, lsn, err = self._verify_db(path)
            if not ok or bad:
                notes.append(f"{s['file']}: {err or str(len(bad)) + ' records fail their checksum'} - skipped")
                continue
            yield s, path

    def _replay(self, conn, from_lsn: int, notes: List[str]) -> int:
        """Apply valid committed WAL entries with LSN > from_lsn, in order. Returns last applied LSN."""
        expected = from_lsn + 1
        applied = from_lsn
        try:
            with open(self.wal_path) as f:
                lines = f.readlines()
        except OSError:
            notes.append("WAL missing - nothing to replay")
            return applied
        for no, line in enumerate(lines, 1):
            try:
                e = json.loads(line)
            except json.JSONDecodeError:
                notes.append(f"WAL line {no}: unreadable - replay stopped")
                break
            if e.get("lsn", 0) <= from_lsn:
                continue
            if e.get("crc") != entry_checksum(e):
                notes.append(f"WAL LSN {e.get('lsn')}: checksum mismatch - replay stopped; later entries not applied")
                break
            if e["lsn"] != expected:
                notes.append(f"WAL gap: expected LSN {expected}, found {e['lsn']} - replay stopped")
                break
            self._apply(conn, e)
            applied = e["lsn"]
            expected += 1
        conn.commit()
        return applied

    def recover(self, strategy: str) -> Dict[str, Any]:
        """Rebuild the backend-local primary from durable storage using one strategy."""
        t0 = time.perf_counter()
        before = self.timeline()                 # the story up to the failure (the log is archived after a restore)
        notes: List[str] = []
        source = None
        restored_lsn = 0
        ok_source = True
        with self._lock:
            self.state = STATE_RECOVERING
            tmp = self.primary_path + ".restore"
            if os.path.exists(tmp):
                os.remove(tmp)
            if strategy == "backup":
                found = next(self._valid_snapshots(notes), None)
                if found is None:
                    ok_source = False
                    notes.append("no valid backup snapshot available")
                else:
                    s, path = found
                    shutil.copyfile(path, tmp)
                    source = f"backup {s['file']} (LSN {s['lsn']}, {s['records']} records)"
                    restored_lsn = s["lsn"]
            elif strategy == "replica":
                if not os.path.exists(self.replica_path):
                    ok_source = False
                    notes.append("replica missing")
                else:
                    ok, rows, bad, lsn, err = self._verify_db(self.replica_path)
                    if not ok:
                        ok_source = False
                        notes.append(f"replica failed verification: {err}")
                    else:
                        shutil.copyfile(self.replica_path, tmp)
                        if bad:
                            c = sqlite3.connect(tmp)
                            c.executemany("DELETE FROM records WHERE id=?", [(b,) for b in bad])
                            c.commit()
                            c.close()
                            notes.append(f"replica: {len(bad)} record(s) failed their checksum and were excluded: {bad}")
                        source = f"synchronous replica (LSN {lsn})"
                        restored_lsn = lsn
            elif strategy == "wal":
                found = next(self._valid_snapshots(notes), None)
                if found is not None:
                    s, path = found
                    shutil.copyfile(path, tmp)
                    base = s["lsn"]
                    source = f"backup {s['file']} (LSN {base}) + WAL replay"
                else:
                    base = 0
                    source = "empty database + full WAL replay"
                    notes.append("no valid snapshot: replaying the WAL from LSN 1")
                c = self._connect(tmp)
                try:
                    restored_lsn = self._replay(c, base, notes)
                finally:
                    c.close()
            elif strategy == "none":
                ok_source = False
                notes.append("no backup, replica or log is used: lost records cannot be recovered")
            else:
                raise ValueError(f"unknown strategy {strategy!r}")

            if not os.path.exists(tmp):
                c = self._connect(tmp)
                c.execute("INSERT OR REPLACE INTO meta VALUES ('last_lsn', '0')")
                c.commit()
                c.close()
            # integrity verification of the restored dataset
            ok, rows, bad, lsn_meta, err = self._verify_db(tmp)
            integrity_ok = ok and not bad
            if bad:
                notes.append(f"restored data: {len(bad)} record(s) failed their checksum")
            os.replace(tmp, self.primary_path)
            restored = {r[0]: {"version": r[3], "checksum": r[6], "lsn": r[5]} for r in rows if r[0] not in bad}

            # measurement against the acknowledged writes (ledger) - not used for restoring
            expected = dict(self.ledger)
            missing = sorted(i for i in expected if i not in restored)
            stale = sorted(i for i in expected if i in restored and restored[i]["version"] < expected[i]["version"])
            unexpected = sorted(i for i in restored if i not in expected)
            last_ack_lsn = max(self._ledger_lsn_ts) if self._ledger_lsn_ts else 0
            lost_lsns = sorted(l for l in self._ledger_lsn_ts if l > restored_lsn)
            if lost_lsns:
                # RPO (time): from the last write that survived (or the first lost one) to the last acknowledged write
                survived_ts = self._ledger_lsn_ts.get(restored_lsn)
                start_ts = survived_ts if survived_ts is not None else self._ledger_lsn_ts[lost_lsns[0]]
                rpo_s = self._ledger_lsn_ts[lost_lsns[-1]] - start_ts
            else:
                rpo_s = 0.0
            data_loss = bool(missing or stale)
            report = {
                "strategy": strategy, "source": source, "source_available": ok_source,
                "restored_records": len(restored), "expected_records": len(expected),
                "expected_ids": sorted(expected)[-200:], "restored_ids": sorted(restored)[-200:],
                "missing_records": missing, "stale_records": stale,
                "unexpected_records": unexpected,     # e.g. records deleted after the backup came back
                "rpo_records": len(missing) + len(stale), "rpo_writes": len(lost_lsns),
                "rpo_seconds": round(max(0.0, rpo_s), 3), "restored_lsn": restored_lsn,
                "last_acknowledged_lsn": last_ack_lsn, "integrity_ok": integrity_ok,
                "integrity": "PASSED" if integrity_ok else f"FAILED ({err or str(len(bad)) + ' bad records'})",
                "notes": notes, "restore_ms": round((time.perf_counter() - t0) * 1000, 1),
                "zero_data_loss": not data_loss and ok_source,
                "timeline": before,
            }
            if not ok_source and expected:
                self.state = STATE_DATA_LOST
            elif data_loss:
                self.state = STATE_OK            # restored from a valid source, with documented loss
            else:
                self.state = STATE_OK
            if self.state == STATE_OK:
                self._new_timeline(restored_lsn)
            self.last_recovery = report
        lvl = "SUCCESS" if report["zero_data_loss"] else ("ERROR" if not ok_source else "WARN")
        self._log(f"Data recovery ({strategy}): {report['restored_records']}/{report['expected_records']} records "
                  f"restored from {source or 'nothing'}; integrity {report['integrity']}; "
                  f"{report['rpo_records']} acknowledged record(s) lost.", lvl, "DATA_RECOVERY")
        return report

    def _new_timeline(self, restored_lsn: int):
        """After a restore, continue on a new timeline: archive the old WAL, rebuild the replica
        from the restored primary and take a fresh baseline backup."""
        ts = int(self._clock() * 1000)
        if os.path.exists(self.wal_path):
            os.replace(self.wal_path, os.path.join(self.durable_dir, f"wal.archived.{ts}.jsonl"))
        open(self.wal_path, "w").close()
        shutil.copyfile(self.primary_path, self.replica_path)
        self._last_lsn = restored_lsn
        m = self._read_manifest()
        m["timeline"] = m.get("timeline", 1) + 1
        m["last_wal_lsn"] = restored_lsn
        m["replica_lsn"] = restored_lsn
        self._write_manifest(m)
        # the ledger now describes the restored data (lost writes are reported, not re-created)
        c = sqlite3.connect(self.primary_path)
        rows = c.execute("SELECT id, version, checksum, lsn, updated_at FROM records").fetchall()
        c.close()
        self.ledger = {r[0]: {"version": r[1], "checksum": r[2], "lsn": r[3], "ts": r[4]} for r in rows}
        self._ledger_lsn_ts = {l: t for l, t in self._ledger_lsn_ts.items() if l <= restored_lsn}

    def accept_loss(self):
        """Operator acknowledges unrecoverable data and resumes with what was restored (possibly empty)."""
        with self._lock:
            if self.state != STATE_DATA_LOST:
                return False
            self.state = STATE_OK
            self._new_timeline(0 if not self.last_recovery else self.last_recovery.get("restored_lsn", 0))
        self._log("Data loss acknowledged by the operator; service resumes with the restored (possibly empty) dataset.",
                  "WARN", "DATA_RECOVERY")
        return True

    # ------------------------------------------------------------------ #
    # Record map: where every record is stored right now                  #
    # ------------------------------------------------------------------ #
    @staticmethod
    def _versions(path: str) -> Optional[Dict[int, int]]:
        """{id: version} of the records in a SQLite file whose checksum is valid; None if unusable."""
        if not os.path.exists(path):
            return None
        ok, rows, bad, _lsn, _err = DataStore._verify_db(path)
        if not ok:
            return None
        return {r[0]: r[3] for r in rows if r[0] not in bad}

    def write_path_ms(self) -> Optional[Dict[str, float]]:
        """Mean time each step of the write path took over the recent writes (measured)."""
        w = list(self._write_times)
        if not w:
            return None
        n = len(w)
        return {"log_ms": round(sum(x[0] for x in w) / n, 2), "primary_ms": round(sum(x[1] for x in w) / n, 2),
                "replica_ms": round(sum(x[2] for x in w) / n, 2), "samples": n}

    def timeline(self, limit: int = 40) -> Dict[str, Any]:
        """
        The confirmed writes of the current log, in order, and how far each copy reaches:
            backup_lsn    newest VALID backup (what "backup only" restores)
            log_valid_lsn last entry that replay would apply (stops at the first bad or missing entry)
            replica_lsn   the live copy (synchronous: every confirmed write)
        Writes before the first log entry (data restored by an earlier recovery) are summarised as `base_lsn`.
        """
        with self._lock:
            notes: List[str] = []
            snap = next(self._valid_snapshots(notes), None)
            m = self._read_manifest()
            try:
                with open(self.wal_path) as f:
                    lines = f.readlines()
            except OSError:
                lines = []
            # same rule as recover("wal"): newest valid backup (or nothing), then replay entries after it in
            # order, stopping at the first unreadable, damaged or missing entry
            start = snap[0]["lsn"] if snap else 0
            events, expected, valid_until, broken = [], start + 1, start, False
            for line in lines:
                try:
                    e = json.loads(line)
                except json.JSONDecodeError:
                    broken = True
                    continue
                ok = e.get("crc") == entry_checksum(e)
                if e.get("lsn", 0) > start and not broken:
                    if ok and e.get("lsn") == expected:
                        valid_until = e["lsn"]
                        expected += 1
                    else:
                        broken = True
                op = "delete" if e.get("op") == "delete" else ("create" if e.get("version") == 1 else "update")
                events.append({"lsn": e.get("lsn"), "op": op, "id": e.get("id"), "ok": ok, "ts": e.get("ts")})
            base = (events[0]["lsn"] - 1) if events else int(m.get("last_wal_lsn", 0) or 0)
            replica_ok, _rows, replica_bad, replica_lsn, _err = self._verify_db(self.replica_path) \
                if os.path.exists(self.replica_path) else (False, [], [], 0, None)
            return {"events": events[-limit:], "hidden": max(0, len(events) - limit), "base_lsn": base,
                    "backup_lsn": snap[0]["lsn"] if snap else None, "backup_file": snap[0]["file"] if snap else None,
                    "backup_time": snap[0]["created_at"] if snap else None,
                    "log_valid_lsn": valid_until,
                    "replica_lsn": replica_lsn if replica_ok else None, "replica_bad_ids": replica_bad,
                    "last_lsn": self._last_lsn, "notes": notes}

    def record_map(self, limit: int = 80) -> Dict[str, Any]:
        """
        For every record: the version acknowledged to the client (the truth, from the measurement
        ledger) and the version each copy holds. "backup" and "wal" are exactly what the backup and
        backup + log strategies would restore right now (same validity checks as recover(): newest
        valid snapshot, log replay stops at the first bad or missing entry), so the preview is not a guess.
        """
        with self._lock:
            truth = {rid: e["version"] for rid, e in self.ledger.items()}
            primary = None
            if self.state not in (STATE_RECOVERING,) and os.path.exists(self.primary_path):
                primary = self._versions(self.primary_path)
            replica = self._versions(self.replica_path)
            notes: List[str] = []
            snap = next(self._valid_snapshots(notes), None)
            backup = self._versions(snap[1]) if snap else None
            base_lsn = snap[0]["lsn"] if snap else 0
            wal = dict(backup or {})
            try:
                with open(self.wal_path) as f:
                    lines = f.readlines()
            except OSError:
                lines = []
            expected = base_lsn + 1
            for line in lines:
                try:
                    e = json.loads(line)
                except json.JSONDecodeError:
                    break
                if e.get("lsn", 0) <= base_lsn:
                    continue
                if e.get("crc") != entry_checksum(e) or e["lsn"] != expected:
                    break
                if e["op"] == "delete":
                    wal.pop(e["id"], None)
                else:
                    wal[e["id"]] = e["version"]
                expected += 1
            copies = {"primary": primary, "backup": backup, "replica": replica, "wal": wal}
            ids = set(truth)
            for c in copies.values():
                ids.update(c or {})
            ids = sorted(ids)
            shown = ids[-limit:]

            def outcome(c):
                c = c or {}
                return {"restored": sum(1 for i in truth if c.get(i) == truth[i]),
                        "lost": sorted(i for i in truth if i not in c),
                        "stale": sorted(i for i in truth if i in c and c[i] < truth[i]),
                        "resurrected": sorted(i for i in c if i not in truth)}
            return {
                "ids": shown, "hidden": len(ids) - len(shown), "truth": {str(i): truth[i] for i in shown if i in truth},
                "copies": {k: (None if v is None else {str(i): v[i] for i in shown if i in v}) for k, v in copies.items()},
                "backup_file": snap[0]["file"] if snap else None, "backup_lsn": base_lsn if snap else None,
                "preview": {"backup": outcome(backup) if snap else outcome({}), "replica": outcome(replica),
                            "wal": outcome(wal), "none": outcome({})},
                "acknowledged": len(truth), "notes": notes,
                "timeline": self.timeline(), "write_path_ms": self.write_path_ms(),
            }

    # ------------------------------------------------------------------ #
    def status(self) -> Dict[str, Any]:
        with self._lock:
            m = self._read_manifest()
            primary_records = None
            if os.path.exists(self.primary_path) and self.state not in (STATE_RECOVERING,):
                try:
                    c = sqlite3.connect(self.primary_path)
                    primary_records = c.execute("SELECT COUNT(*) FROM records").fetchone()[0]
                    c.close()
                except sqlite3.DatabaseError:
                    primary_records = None
            replica_records = None
            try:
                c = sqlite3.connect(self.replica_path)
                replica_records = c.execute("SELECT COUNT(*) FROM records").fetchone()[0]
                c.close()
            except sqlite3.DatabaseError:
                pass
            wal_entries = 0
            try:
                with open(self.wal_path) as f:
                    wal_entries = sum(1 for _ in f)
            except OSError:
                pass
            snaps = []
            for s in m["snapshots"]:
                path = os.path.join(self.snap_dir, s["file"])
                snaps.append(dict(s, valid=file_sha256(path) == s["sha256"]))
            return {
                "state": self.state,
                "primary": {"path": "data/backend_local/primary.sqlite", "exists": os.path.exists(self.primary_path),
                            "records": primary_records, "last_lsn": self._last_lsn},
                "replica": {"mode": "synchronous", "records": replica_records, "lsn": m.get("replica_lsn")},
                "wal": {"entries": wal_entries, "last_lsn": m.get("last_wal_lsn"), "timeline": m.get("timeline")},
                "snapshots": snaps,
                "snapshot_interval_s": self.snapshot_interval,
                "manifest": {"last_valid_snapshot": next((s["file"] for s in reversed(snaps) if s["valid"]), None),
                             "last_valid_snapshot_lsn": next((s["lsn"] for s in reversed(snaps) if s["valid"]), None),
                             "last_wal_lsn": m.get("last_wal_lsn"), "timeline": m.get("timeline")},
                "acknowledged_records": len(self.ledger),
                "last_recovery": self.last_recovery,
                "labels": {"acknowledged_records": "measurement-only ledger (never used for restore)",
                           "storage": "local files on one machine (simulation of separate durable storage)"},
            }
