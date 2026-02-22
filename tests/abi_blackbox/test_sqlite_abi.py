#!/usr/bin/env python3

import ctypes
import os
import subprocess
import sys
import tempfile
import unittest

SQLITE_OK = 0
SQLITE_ERROR = 1
SQLITE_ABORT = 4
SQLITE_NOMEM = 7
SQLITE_MISUSE = 21
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
    lib.sqlite3_busy_timeout.argtypes = [ctypes.POINTER(sqlite3), ctypes.c_int]
    lib.sqlite3_busy_timeout.restype = ctypes.c_int
    lib.sqlite3_backup_init.argtypes = [
        ctypes.POINTER(sqlite3),
        ctypes.c_char_p,
        ctypes.POINTER(sqlite3),
        ctypes.c_char_p,
    ]
    lib.sqlite3_backup_init.restype = ctypes.c_void_p
    lib.sqlite3_backup_step.argtypes = [ctypes.c_void_p, ctypes.c_int]
    lib.sqlite3_backup_step.restype = ctypes.c_int
    lib.sqlite3_backup_remaining.argtypes = [ctypes.c_void_p]
    lib.sqlite3_backup_remaining.restype = ctypes.c_int
    lib.sqlite3_backup_pagecount.argtypes = [ctypes.c_void_p]
    lib.sqlite3_backup_pagecount.restype = ctypes.c_int
    lib.sqlite3_backup_finish.argtypes = [ctypes.c_void_p]
    lib.sqlite3_backup_finish.restype = ctypes.c_int
    lib.sqlite3_blob_open.argtypes = [
        ctypes.POINTER(sqlite3),
        ctypes.c_char_p,
        ctypes.c_char_p,
        ctypes.c_char_p,
        ctypes.c_longlong,
        ctypes.c_int,
        ctypes.POINTER(ctypes.c_void_p),
    ]
    lib.sqlite3_blob_open.restype = ctypes.c_int
    lib.sqlite3_blob_read.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_int, ctypes.c_int]
    lib.sqlite3_blob_read.restype = ctypes.c_int
    lib.sqlite3_blob_write.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_int, ctypes.c_int]
    lib.sqlite3_blob_write.restype = ctypes.c_int
    lib.sqlite3_blob_bytes.argtypes = [ctypes.c_void_p]
    lib.sqlite3_blob_bytes.restype = ctypes.c_int
    lib.sqlite3_blob_close.argtypes = [ctypes.c_void_p]
    lib.sqlite3_blob_close.restype = ctypes.c_int
    lib.sqlite3_serialize.argtypes = [
        ctypes.POINTER(sqlite3),
        ctypes.c_char_p,
        ctypes.POINTER(ctypes.c_void_p),
        ctypes.POINTER(ctypes.c_int),
        ctypes.c_uint,
    ]
    lib.sqlite3_serialize.restype = ctypes.c_int
    lib.sqlite3_deserialize.argtypes = [
        ctypes.POINTER(sqlite3),
        ctypes.c_char_p,
        ctypes.c_void_p,
        ctypes.c_int,
        ctypes.c_uint,
    ]
    lib.sqlite3_deserialize.restype = ctypes.c_int
    lib.sqlite3_wal_checkpoint_v2.argtypes = [
        ctypes.POINTER(sqlite3),
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.POINTER(ctypes.c_int),
        ctypes.POINTER(ctypes.c_int),
    ]
    lib.sqlite3_wal_checkpoint_v2.restype = ctypes.c_int
    lib.sqlite3_table_column_metadata.argtypes = [
        ctypes.POINTER(sqlite3),
        ctypes.c_char_p,
        ctypes.c_char_p,
        ctypes.c_char_p,
        ctypes.POINTER(ctypes.c_char_p),
        ctypes.POINTER(ctypes.c_char_p),
        ctypes.POINTER(ctypes.c_int),
        ctypes.POINTER(ctypes.c_int),
        ctypes.POINTER(ctypes.c_int),
    ]
    lib.sqlite3_table_column_metadata.restype = ctypes.c_int

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
    lib.sqlite3_create_function_v2.argtypes = [
        ctypes.POINTER(sqlite3),
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_void_p,
    ]
    lib.sqlite3_create_function_v2.restype = ctypes.c_int
    lib.sqlite3_create_collation_v2.argtypes = [
        ctypes.POINTER(sqlite3),
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_void_p,
    ]
    lib.sqlite3_create_collation_v2.restype = ctypes.c_int
    lib.sqlite3_changes.argtypes = [ctypes.POINTER(sqlite3)]
    lib.sqlite3_changes.restype = ctypes.c_int
    lib.sqlite3_total_changes.argtypes = [ctypes.POINTER(sqlite3)]
    lib.sqlite3_total_changes.restype = ctypes.c_int
    lib.sqlite3_last_insert_rowid.argtypes = [ctypes.POINTER(sqlite3)]
    lib.sqlite3_last_insert_rowid.restype = ctypes.c_longlong
    lib.sqlite3_stmt_readonly.argtypes = [ctypes.POINTER(sqlite3_stmt)]
    lib.sqlite3_stmt_readonly.restype = ctypes.c_int
    lib.sqlite3_bind_parameter_count.argtypes = [ctypes.POINTER(sqlite3_stmt)]
    lib.sqlite3_bind_parameter_count.restype = ctypes.c_int
    lib.sqlite3_bind_parameter_index.argtypes = [ctypes.POINTER(sqlite3_stmt), ctypes.c_char_p]
    lib.sqlite3_bind_parameter_index.restype = ctypes.c_int
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
    lib.sqlite3_extended_errcode.argtypes = [ctypes.POINTER(sqlite3)]
    lib.sqlite3_extended_errcode.restype = ctypes.c_int
    lib.sqlite3_errmsg.argtypes = [ctypes.POINTER(sqlite3)]
    lib.sqlite3_errmsg.restype = ctypes.c_char_p
    lib.sqlite3_errstr.argtypes = [ctypes.c_int]
    lib.sqlite3_errstr.restype = ctypes.c_char_p
    lib.sqlite3_sleep.argtypes = [ctypes.c_int]
    lib.sqlite3_sleep.restype = ctypes.c_int
    lib.sqlite3_malloc64.argtypes = [ctypes.c_uint64]
    lib.sqlite3_malloc64.restype = ctypes.c_void_p
    lib.sqlite3_context_db_handle.argtypes = [ctypes.c_void_p]
    lib.sqlite3_context_db_handle.restype = ctypes.POINTER(sqlite3)
    lib.sqlite3_result_int64.argtypes = [ctypes.c_void_p, ctypes.c_longlong]
    lib.sqlite3_result_int64.restype = None
    lib.sqlite3_libversion.argtypes = []
    lib.sqlite3_libversion.restype = ctypes.c_char_p
    lib.sqlite3_libversion_number.argtypes = []
    lib.sqlite3_libversion_number.restype = ctypes.c_int
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
        return self.open_db_path(b":memory:")

    def open_db_path(self, path: bytes):
        db = ctypes.POINTER(sqlite3)()
        rc = self.lib.sqlite3_open(path, ctypes.byref(db))
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

    def test_busy_timeout_is_available(self):
        db = self.open_db()
        try:
            rc = self.lib.sqlite3_busy_timeout(db, 25)
            self.assertEqual(rc, SQLITE_OK)
            self.assertNotEqual(rc, SQLITE_MISUSE)
        finally:
            self.close_db(db)

    def test_backup_api_is_available(self):
        source = self.open_db()
        dest = self.open_db()
        try:
            rc, _ = self.exec_sql(source, "CREATE TABLE t(v INTEGER); INSERT INTO t VALUES(7),(9);")
            self.assertEqual(rc, SQLITE_OK)

            backup = self.lib.sqlite3_backup_init(dest, b"main", source, b"main")
            self.assertIsNotNone(backup)
            self.assertNotEqual(backup, 0)
            self.assertGreaterEqual(self.lib.sqlite3_backup_pagecount(backup), 0)
            self.assertGreaterEqual(self.lib.sqlite3_backup_remaining(backup), 0)
            rc = self.lib.sqlite3_backup_step(backup, -1)
            self.assertEqual(rc, SQLITE_OK)
            rc = self.lib.sqlite3_backup_finish(backup)
            self.assertEqual(rc, SQLITE_OK)

            stmt = ctypes.POINTER(sqlite3_stmt)()
            rc = self.lib.sqlite3_prepare_v2(dest, b"SELECT count(*) FROM t;", -1, ctypes.byref(stmt), None)
            self.assertEqual(rc, SQLITE_OK)
            rc = self.lib.sqlite3_step(stmt)
            self.assertEqual(rc, SQLITE_ROW)
            self.assertEqual(self.lib.sqlite3_column_int64(stmt, 0), 2)
            rc = self.lib.sqlite3_finalize(stmt)
            self.assertEqual(rc, SQLITE_OK)
        finally:
            self.close_db(source)
            self.close_db(dest)

    def test_blob_api_is_available(self):
        db = self.open_db()
        try:
            rc, _ = self.exec_sql(
                db,
                "CREATE TABLE t(id INTEGER PRIMARY KEY, payload BLOB); "
                "INSERT INTO t(id, payload) VALUES(1, zeroblob(4));",
            )
            self.assertEqual(rc, SQLITE_OK)

            blob = ctypes.c_void_p()
            rc = self.lib.sqlite3_blob_open(
                db, b"main", b"t", b"payload", 1, 1, ctypes.byref(blob)
            )
            self.assertEqual(rc, SQLITE_OK)
            self.assertIsNotNone(blob.value)
            self.assertEqual(self.lib.sqlite3_blob_bytes(blob), 4)

            write_buf = (ctypes.c_ubyte * 4)(1, 2, 3, 4)
            rc = self.lib.sqlite3_blob_write(blob, write_buf, 4, 0)
            self.assertEqual(rc, SQLITE_OK)

            read_buf = (ctypes.c_ubyte * 4)()
            rc = self.lib.sqlite3_blob_read(blob, read_buf, 4, 0)
            self.assertEqual(rc, SQLITE_OK)
            self.assertEqual(bytes(read_buf), b"\x01\x02\x03\x04")

            rc = self.lib.sqlite3_blob_close(blob)
            self.assertEqual(rc, SQLITE_OK)
        finally:
            self.close_db(db)

    def test_serialize_deserialize_is_available(self):
        source = self.open_db()
        dest = self.open_db()
        image = ctypes.c_void_p()
        try:
            rc, _ = self.exec_sql(source, "CREATE TABLE s(v INTEGER); INSERT INTO s VALUES(42);")
            self.assertEqual(rc, SQLITE_OK)

            image_len = ctypes.c_int()
            rc = self.lib.sqlite3_serialize(source, None, ctypes.byref(image), ctypes.byref(image_len), 0)
            self.assertEqual(rc, SQLITE_OK)
            self.assertIsNotNone(image.value)
            self.assertGreater(image_len.value, 0)

            rc = self.lib.sqlite3_deserialize(dest, None, image, image_len, 0)
            self.assertEqual(rc, SQLITE_OK)

            stmt = ctypes.POINTER(sqlite3_stmt)()
            rc = self.lib.sqlite3_prepare_v2(dest, b"SELECT v FROM s;", -1, ctypes.byref(stmt), None)
            self.assertEqual(rc, SQLITE_OK)
            rc = self.lib.sqlite3_step(stmt)
            self.assertEqual(rc, SQLITE_ROW)
            self.assertEqual(self.lib.sqlite3_column_int64(stmt, 0), 42)
            rc = self.lib.sqlite3_finalize(stmt)
            self.assertEqual(rc, SQLITE_OK)
        finally:
            if image.value is not None:
                self.lib.sqlite3_free(image)
            self.close_db(source)
            self.close_db(dest)

    def test_optional_api_failure_paths_sanitize_outputs(self):
        db = self.open_db()
        try:
            rc, _ = self.exec_sql(
                db,
                "CREATE TABLE t(id INTEGER PRIMARY KEY, payload BLOB); "
                "INSERT INTO t(id, payload) VALUES(1, zeroblob(4));",
            )
            self.assertEqual(rc, SQLITE_OK)

            blob = ctypes.c_void_p(0x1234)
            rc = self.lib.sqlite3_blob_open(
                db, b"main", b"t", b"payload", 9999, 1, ctypes.byref(blob)
            )
            self.assertNotEqual(rc, SQLITE_OK)
            self.assertIsNone(blob.value)

            image = ctypes.c_void_p(0x1234)
            image_len = ctypes.c_int(777)
            rc = self.lib.sqlite3_serialize(
                db,
                b"missing_schema",
                ctypes.byref(image),
                ctypes.byref(image_len),
                0,
            )
            self.assertNotEqual(rc, SQLITE_OK)
            self.assertIsNone(image.value)
            self.assertEqual(image_len.value, 0)

            self.assertEqual(self.lib.sqlite3_backup_step(None, 1), SQLITE_MISUSE)
            self.assertEqual(self.lib.sqlite3_backup_finish(None), SQLITE_MISUSE)
            self.assertEqual(self.lib.sqlite3_blob_read(None, None, 4, 0), SQLITE_MISUSE)
            self.assertEqual(self.lib.sqlite3_blob_write(None, None, 4, 0), SQLITE_MISUSE)
        finally:
            self.close_db(db)

    def test_wal_checkpoint_and_table_metadata_are_available(self):
        fd, path = tempfile.mkstemp(prefix="sqlite_provider_wal_", suffix=".db")
        os.close(fd)
        db = self.open_db_path(path.encode("utf-8"))
        try:
            rc, _ = self.exec_sql(
                db,
                "PRAGMA journal_mode=WAL; "
                "CREATE TABLE m(id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "name TEXT COLLATE NOCASE NOT NULL); "
                "INSERT INTO m(name) VALUES('a');",
            )
            self.assertEqual(rc, SQLITE_OK)

            log_size = ctypes.c_int(-1)
            checkpointed = ctypes.c_int(-1)
            rc = self.lib.sqlite3_wal_checkpoint_v2(
                db, None, 0, ctypes.byref(log_size), ctypes.byref(checkpointed)
            )
            self.assertEqual(rc, SQLITE_OK)
            self.assertGreaterEqual(log_size.value, 0)
            self.assertGreaterEqual(checkpointed.value, 0)

            data_type = ctypes.c_char_p()
            coll_seq = ctypes.c_char_p()
            not_null = ctypes.c_int(-1)
            primary_key = ctypes.c_int(-1)
            autoinc = ctypes.c_int(-1)
            rc = self.lib.sqlite3_table_column_metadata(
                db,
                None,
                b"m",
                b"name",
                ctypes.byref(data_type),
                ctypes.byref(coll_seq),
                ctypes.byref(not_null),
                ctypes.byref(primary_key),
                ctypes.byref(autoinc),
            )
            self.assertEqual(rc, SQLITE_OK)
            self.assertIsNotNone(data_type.value)
            self.assertIsNotNone(coll_seq.value)
            self.assertEqual(not_null.value, 1)
            self.assertEqual(primary_key.value, 0)
            self.assertEqual(autoinc.value, 0)
            self.assertEqual(data_type.value.decode("utf-8").lower(), "text")
            self.assertEqual(coll_seq.value.decode("utf-8").lower(), "nocase")
        finally:
            self.close_db(db)
            for suffix in ("", "-wal", "-shm"):
                try:
                    os.remove(path + suffix)
                except FileNotFoundError:
                    pass

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

    def test_extras_helpers_follow_backend_values(self):
        db = self.open_db()
        try:
            rc, _ = self.exec_sql(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);")
            self.assertEqual(rc, SQLITE_OK)
            rc, _ = self.exec_sql(db, "INSERT INTO t(name) VALUES('alice');")
            self.assertEqual(rc, SQLITE_OK)
            rc, _ = self.exec_sql(db, "INSERT INTO t(name) VALUES('bob');")
            self.assertEqual(rc, SQLITE_OK)

            self.assertEqual(self.lib.sqlite3_changes(db), 1)
            self.assertEqual(self.lib.sqlite3_total_changes(db), 2)
            self.assertEqual(self.lib.sqlite3_last_insert_rowid(db), 2)

            stmt = ctypes.POINTER(sqlite3_stmt)()
            rc = self.lib.sqlite3_prepare_v2(
                db,
                b"SELECT :foo, ?, @bar;",
                -1,
                ctypes.byref(stmt),
                None,
            )
            self.assertEqual(rc, SQLITE_OK)
            self.assertIsNotNone(stmt)

            self.assertEqual(self.lib.sqlite3_stmt_readonly(stmt), 1)
            self.assertEqual(self.lib.sqlite3_bind_parameter_count(stmt), 3)
            self.assertEqual(self.lib.sqlite3_bind_parameter_index(stmt, b"@bar"), 3)

            rc = self.lib.sqlite3_finalize(stmt)
            self.assertEqual(rc, SQLITE_OK)
        finally:
            self.close_db(db)

    def test_create_collation_v2_registers_custom_ordering(self):
        db = self.open_db()
        try:
            rc, _ = self.exec_sql(db, "CREATE TABLE t(name TEXT);")
            self.assertEqual(rc, SQLITE_OK)
            rc, _ = self.exec_sql(db, "INSERT INTO t VALUES('a'),('c'),('b');")
            self.assertEqual(rc, SQLITE_OK)

            cmp_fn = ctypes.CFUNCTYPE(
                ctypes.c_int,
                ctypes.c_void_p,
                ctypes.c_int,
                ctypes.c_void_p,
                ctypes.c_int,
                ctypes.c_void_p,
            )
            destroy_fn = ctypes.CFUNCTYPE(None, ctypes.c_void_p)
            destroyed = ctypes.c_int(0)

            @cmp_fn
            def reverse_binary(_ctx, lhs_len, lhs, rhs_len, rhs):
                lhs_bytes = ctypes.string_at(lhs, max(lhs_len, 0))
                rhs_bytes = ctypes.string_at(rhs, max(rhs_len, 0))
                if lhs_bytes < rhs_bytes:
                    return 1
                if lhs_bytes > rhs_bytes:
                    return -1
                return 0

            @destroy_fn
            def on_destroy(ctx):
                if ctx:
                    ctypes.cast(ctx, ctypes.POINTER(ctypes.c_int)).contents.value = 1

            rc = self.lib.sqlite3_create_collation_v2(
                db,
                b"reverse_bin",
                1,
                ctypes.byref(destroyed),
                reverse_binary,
                on_destroy,
            )
            self.assertEqual(rc, SQLITE_OK)

            stmt = ctypes.POINTER(sqlite3_stmt)()
            rc = self.lib.sqlite3_prepare_v2(
                db,
                b"SELECT name FROM t ORDER BY name COLLATE reverse_bin;",
                -1,
                ctypes.byref(stmt),
                None,
            )
            self.assertEqual(rc, SQLITE_OK)

            rows = []
            while True:
                rc = self.lib.sqlite3_step(stmt)
                if rc == SQLITE_ROW:
                    ptr = self.lib.sqlite3_column_text(stmt, 0)
                    ln = self.lib.sqlite3_column_bytes(stmt, 0)
                    rows.append(ctypes.string_at(ptr, ln).decode("utf-8"))
                elif rc == SQLITE_DONE:
                    break
                else:
                    self.fail(f"unexpected step rc={rc}")
            self.assertEqual(rows, ["c", "b", "a"])

            rc = self.lib.sqlite3_finalize(stmt)
            self.assertEqual(rc, SQLITE_OK)
        finally:
            self.close_db(db)
        self.assertEqual(destroyed.value, 1)

    def test_null_handle_error_apis_match_sqlite(self):
        self.assertEqual(self.lib.sqlite3_errcode(None), SQLITE_NOMEM)
        self.assertEqual(self.lib.sqlite3_extended_errcode(None), SQLITE_NOMEM)
        msg = self.lib.sqlite3_errmsg(None)
        self.assertEqual(msg.decode("utf-8", errors="replace"), "out of memory")

    def test_errstr_matches_sqlite_text(self):
        self.assertEqual(
            self.lib.sqlite3_errstr(SQLITE_ERROR).decode("utf-8", errors="replace"),
            "SQL logic error",
        )
        self.assertEqual(
            self.lib.sqlite3_errstr(SQLITE_ABORT).decode("utf-8", errors="replace"),
            "query aborted",
        )
        self.assertEqual(
            self.lib.sqlite3_errstr(21).decode("utf-8", errors="replace"),
            "bad parameter or other API misuse",
        )
        self.assertEqual(
            self.lib.sqlite3_errstr(SQLITE_DONE).decode("utf-8", errors="replace"),
            "no more rows available",
        )

    def test_libversion_available_before_explicit_init(self):
        script = """
import ctypes, os
lib = ctypes.CDLL(os.environ['SQLITE3_SPI_LIB'])
lib.sqlite3_libversion.argtypes = []
lib.sqlite3_libversion.restype = ctypes.c_char_p
lib.sqlite3_libversion_number.argtypes = []
lib.sqlite3_libversion_number.restype = ctypes.c_int
v = lib.sqlite3_libversion()
n = lib.sqlite3_libversion_number()
assert v is not None and len(v) > 0, (v, n)
assert n > 0, (v, n)
"""
        subprocess.run(
            [sys.executable, "-c", script],
            check=True,
            env=os.environ.copy(),
        )

    def test_sleep_returns_elapsed_millis(self):
        self.assertEqual(self.lib.sqlite3_sleep(0), 0)
        self.assertEqual(self.lib.sqlite3_sleep(1), 1)

    def test_malloc64_rejects_large_request(self):
        ptr = self.lib.sqlite3_malloc64(8)
        self.assertIsNotNone(ptr)
        self.lib.sqlite3_free(ctypes.c_void_p(ptr))

        too_large = 0x1_0000_0010
        self.assertIsNone(self.lib.sqlite3_malloc64(too_large))

    def test_context_db_handle_matches_open_handle(self):
        db = self.open_db()
        try:
            captured = ctypes.c_void_p()
            xfunc = ctypes.CFUNCTYPE(
                None, ctypes.c_void_p, ctypes.c_int, ctypes.POINTER(ctypes.c_void_p)
            )

            @xfunc
            def cb(ctx, argc, argv):
                captured.value = ctypes.cast(
                    self.lib.sqlite3_context_db_handle(ctx), ctypes.c_void_p
                ).value
                self.lib.sqlite3_result_int64(ctx, 1)

            rc = self.lib.sqlite3_create_function_v2(
                db,
                b"ctx_db_handle_probe",
                0,
                1,
                None,
                cb,
                None,
                None,
                None,
            )
            self.assertEqual(rc, SQLITE_OK)

            rc, msg = self.exec_sql(db, "SELECT ctx_db_handle_probe();")
            self.assertEqual(rc, SQLITE_OK)
            self.assertIsNone(msg)
            self.assertEqual(captured.value, ctypes.cast(db, ctypes.c_void_p).value)
        finally:
            self.close_db(db)


if __name__ == "__main__":
    unittest.main()
