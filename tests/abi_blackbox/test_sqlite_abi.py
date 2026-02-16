#!/usr/bin/env python3

import ctypes
import os
import unittest

SQLITE_OK = 0
SQLITE_ERROR = 1
SQLITE_ROW = 100
SQLITE_DONE = 101
SQLITE_INTEGER = 1
SQLITE_TEXT = 3
SQLITE_TRANSIENT = ctypes.c_void_p(-1)


class sqlite3(ctypes.Structure):
    pass


class sqlite3_stmt(ctypes.Structure):
    pass


def load_lib():
    lib_path = os.environ["SQLITE3_SPI_LIB"]
    lib = ctypes.CDLL(lib_path, mode=getattr(ctypes, "RTLD_LOCAL", 0))

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

    lib.sqlite3_prepare_v2.argtypes = [
        ctypes.POINTER(sqlite3),
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.POINTER(ctypes.POINTER(sqlite3_stmt)),
        ctypes.POINTER(ctypes.c_char_p),
    ]
    lib.sqlite3_prepare_v2.restype = ctypes.c_int
    lib.sqlite3_bind_int64.argtypes = [ctypes.POINTER(sqlite3_stmt), ctypes.c_int, ctypes.c_longlong]
    lib.sqlite3_bind_int64.restype = ctypes.c_int
    lib.sqlite3_bind_text.argtypes = [
        ctypes.POINTER(sqlite3_stmt),
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_void_p,
    ]
    lib.sqlite3_bind_text.restype = ctypes.c_int
    lib.sqlite3_step.argtypes = [ctypes.POINTER(sqlite3_stmt)]
    lib.sqlite3_step.restype = ctypes.c_int
    lib.sqlite3_column_int64.argtypes = [ctypes.POINTER(sqlite3_stmt), ctypes.c_int]
    lib.sqlite3_column_int64.restype = ctypes.c_longlong
    lib.sqlite3_column_type.argtypes = [ctypes.POINTER(sqlite3_stmt), ctypes.c_int]
    lib.sqlite3_column_type.restype = ctypes.c_int
    lib.sqlite3_column_text.argtypes = [ctypes.POINTER(sqlite3_stmt), ctypes.c_int]
    lib.sqlite3_column_text.restype = ctypes.POINTER(ctypes.c_ubyte)
    lib.sqlite3_column_bytes.argtypes = [ctypes.POINTER(sqlite3_stmt), ctypes.c_int]
    lib.sqlite3_column_bytes.restype = ctypes.c_int
    lib.sqlite3_finalize.argtypes = [ctypes.POINTER(sqlite3_stmt)]
    lib.sqlite3_finalize.restype = ctypes.c_int

    lib.sqlite3_errcode.argtypes = [ctypes.POINTER(sqlite3)]
    lib.sqlite3_errcode.restype = ctypes.c_int
    lib.sqlite3_errmsg.argtypes = [ctypes.POINTER(sqlite3)]
    lib.sqlite3_errmsg.restype = ctypes.c_char_p
    lib.sqlite3_free.argtypes = [ctypes.c_void_p]
    lib.sqlite3_free.restype = None

    rc = lib.sqlite3_provider_init_default()
    if rc != SQLITE_OK:
        raise RuntimeError(f"bootstrap failed rc={rc}")

    return lib


class SqliteAbiBlackboxTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.lib = load_lib()

    def open_db(self):
        db = ctypes.POINTER(sqlite3)()
        rc = self.lib.sqlite3_open(b":memory:", ctypes.byref(db))
        self.assertEqual(rc, SQLITE_OK)
        self.assertIsNotNone(db)
        return db

    def close_db(self, db):
        rc = self.lib.sqlite3_close(db)
        self.assertEqual(rc, SQLITE_OK)

    def exec_sql(self, db, sql):
        err = ctypes.c_char_p()
        rc = self.lib.sqlite3_exec(db, sql.encode("utf-8"), None, None, ctypes.byref(err))
        msg = None
        if err.value is not None:
            msg = err.value.decode("utf-8", errors="replace")
            self.lib.sqlite3_free(ctypes.cast(err, ctypes.c_void_p))
        return rc, msg

    def test_connection_lifecycle(self):
        db = self.open_db()
        self.close_db(db)

    def test_exec_workflow(self):
        db = self.open_db()
        try:
            rc, msg = self.exec_sql(db, "CREATE TABLE t(id INTEGER, name TEXT);")
            self.assertEqual(rc, SQLITE_OK)
            self.assertIsNone(msg)

            rc, msg = self.exec_sql(db, "INSERT INTO t VALUES(1, 'alice'), (2, 'bob');")
            self.assertEqual(rc, SQLITE_OK)
            self.assertIsNone(msg)
        finally:
            self.close_db(db)

    def test_prepare_bind_step_column_finalize(self):
        db = self.open_db()
        try:
            rc, _ = self.exec_sql(db, "CREATE TABLE t(id INTEGER, name TEXT);")
            self.assertEqual(rc, SQLITE_OK)

            insert_stmt = ctypes.POINTER(sqlite3_stmt)()
            rc = self.lib.sqlite3_prepare_v2(
                db,
                b"INSERT INTO t(id, name) VALUES(?, ?);",
                -1,
                ctypes.byref(insert_stmt),
                None,
            )
            self.assertEqual(rc, SQLITE_OK)

            rc = self.lib.sqlite3_bind_int64(insert_stmt, 1, 3)
            self.assertEqual(rc, SQLITE_OK)
            rc = self.lib.sqlite3_bind_text(insert_stmt, 2, b"carol", -1, SQLITE_TRANSIENT)
            self.assertEqual(rc, SQLITE_OK)
            rc = self.lib.sqlite3_step(insert_stmt)
            self.assertEqual(rc, SQLITE_DONE)
            rc = self.lib.sqlite3_finalize(insert_stmt)
            self.assertEqual(rc, SQLITE_OK)

            select_stmt = ctypes.POINTER(sqlite3_stmt)()
            rc = self.lib.sqlite3_prepare_v2(
                db,
                b"SELECT id, name FROM t WHERE id = ?;",
                -1,
                ctypes.byref(select_stmt),
                None,
            )
            self.assertEqual(rc, SQLITE_OK)
            rc = self.lib.sqlite3_bind_int64(select_stmt, 1, 3)
            self.assertEqual(rc, SQLITE_OK)

            rc = self.lib.sqlite3_step(select_stmt)
            self.assertEqual(rc, SQLITE_ROW)
            self.assertEqual(self.lib.sqlite3_column_type(select_stmt, 0), SQLITE_INTEGER)
            self.assertEqual(self.lib.sqlite3_column_int64(select_stmt, 0), 3)
            self.assertEqual(self.lib.sqlite3_column_type(select_stmt, 1), SQLITE_TEXT)

            text_ptr = self.lib.sqlite3_column_text(select_stmt, 1)
            text_len = self.lib.sqlite3_column_bytes(select_stmt, 1)
            self.assertIsNotNone(text_ptr)
            text = ctypes.string_at(text_ptr, text_len).decode("utf-8")
            self.assertEqual(text, "carol")

            rc = self.lib.sqlite3_step(select_stmt)
            self.assertEqual(rc, SQLITE_DONE)
            rc = self.lib.sqlite3_finalize(select_stmt)
            self.assertEqual(rc, SQLITE_OK)
        finally:
            self.close_db(db)

    def test_error_propagation(self):
        db = self.open_db()
        try:
            err = ctypes.c_char_p()
            rc = self.lib.sqlite3_exec(
                db,
                b"SELECT * FROM missing_table;",
                None,
                None,
                ctypes.byref(err),
            )
            self.assertNotEqual(rc, SQLITE_OK)
            self.assertEqual(rc, SQLITE_ERROR)
            self.assertIsNotNone(err.value)
            msg = err.value.decode("utf-8", errors="replace")
            self.lib.sqlite3_free(ctypes.cast(err, ctypes.c_void_p))
            self.assertIn("no such table", msg.lower())

            errcode = self.lib.sqlite3_errcode(db)
            self.assertNotEqual(errcode, SQLITE_OK)
            errmsg = self.lib.sqlite3_errmsg(db)
            self.assertIsNotNone(errmsg)
            self.assertIn("no such table", errmsg.decode("utf-8", errors="replace").lower())
        finally:
            self.close_db(db)


if __name__ == "__main__":
    unittest.main()
