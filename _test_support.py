"""Import the real server.py for offline tests.

server.py does no network or database work at import time, so tests exercise
the actual implementation instead of copies of it. This sets dummy
credentials, disables the session file, and moves into a scratch directory so
the SQLite files a test creates never land in the repo.

    from _test_support import server
"""
import os
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent

os.environ.setdefault("BSKY_HANDLE", "test.bsky.social")
os.environ.setdefault("BSKY_APP_PASSWORD", "test-password")
os.environ["BSKY_SESSION_FILE"] = ""
os.chdir(tempfile.mkdtemp(prefix="bsky-mcp-test-"))
sys.path.insert(0, str(HERE))

import server  # noqa: E402

__all__ = ["server", "HERE"]
