"""Automated test package for load balancer and network monitoring."""

import os
import tempfile

# Tests that import app.py must not touch the project's real data directory, and must not
# start background capacity changes or periodic backups that would make them timing-dependent.
os.environ.setdefault("LB_DATA_DIR", tempfile.mkdtemp(prefix="lb_test_data_"))
os.environ.setdefault("LB_CAPACITY", "0")
