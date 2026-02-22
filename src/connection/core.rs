use core::ptr::NonNull;

use crate::error::{Error, Result};
use crate::provider::{FeatureSet, OpenOptions, Sqlite3Api, Sqlite3Keying};
use crate::statement::Statement;

/// Safe wrapper around a `sqlite3*` connection.
pub struct Connection<'p, P: Sqlite3Api> {
    pub(crate) api: &'p P,
    pub(crate) db: NonNull<P::Db>,
}

impl<'p, P: Sqlite3Api> Connection<'p, P> {
    /// Open a connection using the provider SPI.
    pub fn open(api: &'p P, filename: &str, options: OpenOptions<'_>) -> Result<Self> {
        let db = unsafe { api.open(filename, options)? };
        Ok(Self { api, db })
    }

    /// Prepare a statement, using prepare_v3 when available.
    pub fn prepare(&self, sql: &str) -> Result<Statement<'_, 'p, P>> {
        let stmt = unsafe {
            if self.api.feature_set().contains(FeatureSet::PREPARE_V3) {
                self.api.prepare_v3(self.db, sql, 0)?
            } else {
                self.api.prepare_v2(self.db, sql)?
            }
        };
        Ok(Statement::new(self, stmt))
    }

    /// Prepare a statement with flags (requires prepare_v3 support).
    pub fn prepare_with_flags(&self, sql: &str, flags: u32) -> Result<Statement<'_, 'p, P>> {
        if !self.api.feature_set().contains(FeatureSet::PREPARE_V3) {
            return Err(Error::feature_unavailable("prepare_v3 unsupported"));
        }
        let stmt = unsafe { self.api.prepare_v3(self.db, sql, flags)? };
        Ok(Statement::new(self, stmt))
    }

    /// Expose the raw database handle.
    pub fn raw_handle(&self) -> NonNull<P::Db> {
        self.db
    }
}

impl<'p, P: Sqlite3Keying> Connection<'p, P> {
    /// Open a connection and immediately apply a key (SQLCipher-style).
    pub fn open_with_key(
        api: &'p P,
        filename: &str,
        options: OpenOptions<'_>,
        key: &[u8],
    ) -> Result<Self> {
        let db = unsafe { api.open(filename, options)? };
        if let Err(err) = unsafe { api.key(db, key) } {
            let _ = unsafe { api.close(db) };
            return Err(err);
        }
        Ok(Self { api, db })
    }

    /// Rekey the database (SQLCipher-style).
    pub fn rekey(&self, key: &[u8]) -> Result<()> {
        unsafe { self.api.rekey(self.db, key) }
    }
}

impl<'p, P: Sqlite3Api> Drop for Connection<'p, P> {
    fn drop(&mut self) {
        let _ = unsafe { self.api.close(self.db) };
    }
}
