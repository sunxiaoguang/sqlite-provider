import ctypes
import os
import threading
import time
from pathlib import Path
from typing import Callable, TypeVar

T = TypeVar("T")

SQLITE_OK = 0
SQLITE_ERROR = 1
SQLITE_ROW = 100
SQLITE_DONE = 101
SQLITE_INTEGER = 1
SQLITE_FLOAT = 2
SQLITE_TEXT = 3
SQLITE_BLOB = 4
SQLITE_NULL = 5
SQLITE_TRANSIENT = ctypes.c_void_p(-1)


class sqlite3(ctypes.Structure):
    pass


class sqlite3_stmt(ctypes.Structure):
    pass


class DlInfo(ctypes.Structure):
    _fields_ = [
        ("dli_fname", ctypes.c_char_p),
        ("dli_fbase", ctypes.c_void_p),
        ("dli_sname", ctypes.c_char_p),
        ("dli_saddr", ctypes.c_void_p),
    ]


class SqliteError(RuntimeError):
    def __init__(self, rc: int, message: str):
        super().__init__(f"sqlite rc={rc}: {message}")
        self.rc = rc
        self.message = message


class AbiSqlite:
    def __init__(self, lib_path: str):
        self.lib_path = os.path.realpath(lib_path)
        self.lib = ctypes.CDLL(self.lib_path, mode=getattr(ctypes, "RTLD_LOCAL", 0))
        self._bind()
        self._assert_symbol_from_library("sqlite3_open")
        self._assert_symbol_from_library("sqlite3_exec")
        rc = self.lib.sqlite3_provider_init_default()
        if rc != SQLITE_OK:
            raise RuntimeError(f"sqlite3_provider_init_default failed: rc={rc}")

    def _bind(self) -> None:
        lib = self.lib
        lib.sqlite3_provider_init_default.argtypes = []
        lib.sqlite3_provider_init_default.restype = ctypes.c_int

        lib.sqlite3_open.argtypes = [ctypes.c_char_p, ctypes.POINTER(ctypes.POINTER(sqlite3))]
        lib.sqlite3_open.restype = ctypes.c_int
        lib.sqlite3_close.argtypes = [ctypes.POINTER(sqlite3)]
        lib.sqlite3_close.restype = ctypes.c_int

        lib.sqlite3_exec.argtypes = [
            ctypes.POINTER(sqlite3),
            ctypes.c_char_p,
            ctypes.c_void_p,
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_char_p),
        ]
        lib.sqlite3_exec.restype = ctypes.c_int
        lib.sqlite3_errmsg.argtypes = [ctypes.POINTER(sqlite3)]
        lib.sqlite3_errmsg.restype = ctypes.c_char_p
        lib.sqlite3_free.argtypes = [ctypes.c_void_p]
        lib.sqlite3_free.restype = None

        lib.sqlite3_prepare_v2.argtypes = [
            ctypes.POINTER(sqlite3),
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.POINTER(ctypes.POINTER(sqlite3_stmt)),
            ctypes.POINTER(ctypes.c_char_p),
        ]
        lib.sqlite3_prepare_v2.restype = ctypes.c_int
        lib.sqlite3_finalize.argtypes = [ctypes.POINTER(sqlite3_stmt)]
        lib.sqlite3_finalize.restype = ctypes.c_int
        lib.sqlite3_reset.argtypes = [ctypes.POINTER(sqlite3_stmt)]
        lib.sqlite3_reset.restype = ctypes.c_int
        lib.sqlite3_step.argtypes = [ctypes.POINTER(sqlite3_stmt)]
        lib.sqlite3_step.restype = ctypes.c_int

        lib.sqlite3_bind_int64.argtypes = [ctypes.POINTER(sqlite3_stmt), ctypes.c_int, ctypes.c_longlong]
        lib.sqlite3_bind_int64.restype = ctypes.c_int
        lib.sqlite3_bind_double.argtypes = [ctypes.POINTER(sqlite3_stmt), ctypes.c_int, ctypes.c_double]
        lib.sqlite3_bind_double.restype = ctypes.c_int
        lib.sqlite3_bind_text.argtypes = [
            ctypes.POINTER(sqlite3_stmt),
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_void_p,
        ]
        lib.sqlite3_bind_text.restype = ctypes.c_int

        lib.sqlite3_column_count.argtypes = [ctypes.POINTER(sqlite3_stmt)]
        lib.sqlite3_column_count.restype = ctypes.c_int
        lib.sqlite3_column_type.argtypes = [ctypes.POINTER(sqlite3_stmt), ctypes.c_int]
        lib.sqlite3_column_type.restype = ctypes.c_int
        lib.sqlite3_column_int64.argtypes = [ctypes.POINTER(sqlite3_stmt), ctypes.c_int]
        lib.sqlite3_column_int64.restype = ctypes.c_longlong
        lib.sqlite3_column_double.argtypes = [ctypes.POINTER(sqlite3_stmt), ctypes.c_int]
        lib.sqlite3_column_double.restype = ctypes.c_double
        lib.sqlite3_column_text.argtypes = [ctypes.POINTER(sqlite3_stmt), ctypes.c_int]
        lib.sqlite3_column_text.restype = ctypes.POINTER(ctypes.c_ubyte)
        lib.sqlite3_column_blob.argtypes = [ctypes.POINTER(sqlite3_stmt), ctypes.c_int]
        lib.sqlite3_column_blob.restype = ctypes.c_void_p
        lib.sqlite3_column_bytes.argtypes = [ctypes.POINTER(sqlite3_stmt), ctypes.c_int]
        lib.sqlite3_column_bytes.restype = ctypes.c_int

    def _assert_symbol_from_library(self, symbol: str) -> None:
        libc = ctypes.CDLL(None)
        if not hasattr(libc, "dladdr"):
            raise AssertionError("dladdr not available; cannot verify symbol origin")
        dladdr = libc.dladdr
        dladdr.argtypes = [ctypes.c_void_p, ctypes.POINTER(DlInfo)]
        dladdr.restype = ctypes.c_int
        fn = getattr(self.lib, symbol)
        addr = ctypes.cast(fn, ctypes.c_void_p).value
        info = DlInfo()
        rc = dladdr(ctypes.c_void_p(addr), ctypes.byref(info))
        if rc == 0 or not info.dli_fname:
            raise AssertionError(f"dladdr failed for {symbol}")
        resolved = os.path.realpath(info.dli_fname.decode("utf-8", errors="replace"))
        if resolved != self.lib_path:
            raise AssertionError(f"{symbol} resolved to {resolved}, expected {self.lib_path}")

    def open(self, path: str) -> "Connection":
        db = ctypes.POINTER(sqlite3)()
        rc = self.lib.sqlite3_open(path.encode("utf-8"), ctypes.byref(db))
        if rc != SQLITE_OK:
            raise SqliteError(rc, "sqlite3_open failed")
        return Connection(self, db)

    def raise_from_db(self, db: ctypes.POINTER(sqlite3), rc: int, fallback: str) -> None:
        msg_ptr = self.lib.sqlite3_errmsg(db)
        msg = msg_ptr.decode("utf-8", errors="replace") if msg_ptr else fallback
        raise SqliteError(rc, msg)


class Statement:
    def __init__(self, conn: "Connection", stmt: ctypes.POINTER(sqlite3_stmt)):
        self.conn = conn
        self.stmt = stmt
        self.closed = False

    def __enter__(self) -> "Statement":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def close(self) -> None:
        if self.closed:
            return
        rc = self.conn.api.lib.sqlite3_finalize(self.stmt)
        self.closed = True
        if rc != SQLITE_OK:
            self.conn.api.raise_from_db(self.conn.db, rc, "sqlite3_finalize failed")

    def reset(self) -> None:
        rc = self.conn.api.lib.sqlite3_reset(self.stmt)
        if rc != SQLITE_OK:
            self.conn.api.raise_from_db(self.conn.db, rc, "sqlite3_reset failed")

    def bind_int64(self, idx: int, value: int) -> None:
        rc = self.conn.api.lib.sqlite3_bind_int64(self.stmt, idx, value)
        if rc != SQLITE_OK:
            self.conn.api.raise_from_db(self.conn.db, rc, "sqlite3_bind_int64 failed")

    def bind_double(self, idx: int, value: float) -> None:
        rc = self.conn.api.lib.sqlite3_bind_double(self.stmt, idx, value)
        if rc != SQLITE_OK:
            self.conn.api.raise_from_db(self.conn.db, rc, "sqlite3_bind_double failed")

    def bind_text(self, idx: int, value: str) -> None:
        rc = self.conn.api.lib.sqlite3_bind_text(
            self.stmt,
            idx,
            value.encode("utf-8"),
            -1,
            SQLITE_TRANSIENT,
        )
        if rc != SQLITE_OK:
            self.conn.api.raise_from_db(self.conn.db, rc, "sqlite3_bind_text failed")

    def step(self) -> int:
        rc = self.conn.api.lib.sqlite3_step(self.stmt)
        if rc in (SQLITE_ROW, SQLITE_DONE):
            return rc
        self.conn.api.raise_from_db(self.conn.db, rc, "sqlite3_step failed")
        return rc


class Connection:
    def __init__(self, api: AbiSqlite, db: ctypes.POINTER(sqlite3)):
        self.api = api
        self.db = db
        self.closed = False

    def __enter__(self) -> "Connection":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def close(self) -> None:
        if self.closed:
            return
        rc = self.api.lib.sqlite3_close(self.db)
        self.closed = True
        if rc != SQLITE_OK:
            raise SqliteError(rc, "sqlite3_close failed")

    def exec(self, sql: str) -> None:
        err = ctypes.c_char_p()
        rc = self.api.lib.sqlite3_exec(
            self.db,
            sql.encode("utf-8"),
            None,
            None,
            ctypes.byref(err),
        )
        if rc != SQLITE_OK:
            msg = None
            if err.value is not None:
                msg = err.value.decode("utf-8", errors="replace")
                self.api.lib.sqlite3_free(ctypes.cast(err, ctypes.c_void_p))
            self.api.raise_from_db(self.db, rc, msg or "sqlite3_exec failed")

    def prepare(self, sql: str) -> Statement:
        stmt = ctypes.POINTER(sqlite3_stmt)()
        rc = self.api.lib.sqlite3_prepare_v2(
            self.db,
            sql.encode("utf-8"),
            -1,
            ctypes.byref(stmt),
            None,
        )
        if rc != SQLITE_OK:
            self.api.raise_from_db(self.db, rc, "sqlite3_prepare_v2 failed")
        return Statement(self, stmt)

    def query(self, sql: str) -> list[tuple]:
        rows: list[tuple] = []
        with self.prepare(sql) as stmt:
            col_count = self.api.lib.sqlite3_column_count(stmt.stmt)
            while True:
                rc = stmt.step()
                if rc == SQLITE_DONE:
                    break
                row = []
                for col in range(col_count):
                    value_type = self.api.lib.sqlite3_column_type(stmt.stmt, col)
                    if value_type == SQLITE_NULL:
                        row.append(None)
                    elif value_type == SQLITE_INTEGER:
                        row.append(self.api.lib.sqlite3_column_int64(stmt.stmt, col))
                    elif value_type == SQLITE_FLOAT:
                        row.append(self.api.lib.sqlite3_column_double(stmt.stmt, col))
                    elif value_type == SQLITE_BLOB:
                        ptr = self.api.lib.sqlite3_column_blob(stmt.stmt, col)
                        n = self.api.lib.sqlite3_column_bytes(stmt.stmt, col)
                        row.append(ctypes.string_at(ptr, n) if ptr else b"")
                    else:
                        ptr = self.api.lib.sqlite3_column_text(stmt.stmt, col)
                        n = self.api.lib.sqlite3_column_bytes(stmt.stmt, col)
                        row.append(ctypes.string_at(ptr, n).decode("utf-8") if ptr else "")
                rows.append(tuple(row))
        return rows

    def query_one(self, sql: str) -> tuple:
        rows = self.query(sql)
        if not rows:
            raise AssertionError("expected at least one row")
        return rows[0]


_API: AbiSqlite | None = None
_API_LOCK = threading.Lock()


def load_api() -> AbiSqlite:
    global _API
    if _API is not None:
        return _API
    with _API_LOCK:
        if _API is not None:
            return _API
        lib_path = os.environ.get("SQLITE3_SPI_LIB")
        if not lib_path:
            raise RuntimeError("SQLITE3_SPI_LIB is required for sqlite_provider_py tests")
        _API = AbiSqlite(lib_path)
        return _API


def open_db(
    path: Path,
    *,
    timeout: float = 0.025,
    synchronous: str = "NORMAL",
) -> Connection:
    """Open a database via sqlite-provider ABI shim with deterministic PRAGMAs."""
    conn = load_api().open(str(path))
    conn.exec("PRAGMA journal_mode=WAL;")
    conn.exec(f"PRAGMA synchronous={synchronous};")
    conn.exec("PRAGMA foreign_keys=ON;")
    conn.exec(f"PRAGMA busy_timeout={max(1, int(timeout * 1000))};")
    return conn


def retry_locked(
    op: Callable[[], T],
    *,
    max_retries: int = 30,
    base_sleep_s: float = 0.002,
) -> T:
    """Retry SQLITE_BUSY/SQLITE_LOCKED-style operations with bounded backoff."""
    for attempt in range(max_retries + 1):
        try:
            return op()
        except SqliteError as exc:
            if "locked" not in exc.message.lower() or attempt == max_retries:
                raise
            time.sleep(base_sleep_s * (attempt + 1))
    raise AssertionError("unreachable")
