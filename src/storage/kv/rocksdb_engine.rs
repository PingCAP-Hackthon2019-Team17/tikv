// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Debug, Display, Formatter};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use engine::rocks;
use engine::rocks::util::CFOptions;
use engine::rocks::{ColumnFamilyOptions, DBIterator, SeekKey, Writable, WriteBatch, DB};
use engine::Engines;
use engine::Error as EngineError;
use engine::{CfName, CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
use engine::{IterOption, Peekable};
use kvproto::kvrpcpb::Context;
use tempfile::{Builder, TempDir};

use crate::storage::{BlockCacheConfig, Key, Value};
use tikv_util::escape;
use tikv_util::worker::{Runnable, Scheduler, Worker};

use super::{
    Callback, CbContext, Cursor, Engine, Error, Iterator as EngineIterator, Modify, Result,
    ScanMode, Snapshot,
};

pub use engine::SyncSnapshot as RocksSnapshot;

const TEMP_DIR: &str = "";

enum Task {
    Write(Vec<Modify>, Callback<()>),
    Snapshot(Callback<RocksSnapshot>),
    Pause(Duration),
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            Task::Write(..) => write!(f, "write task"),
            Task::Snapshot(_) => write!(f, "snapshot task"),
            Task::Pause(_) => write!(f, "pause"),
        }
    }
}

struct Runner(Engines);

impl Runnable<Task> for Runner {
    fn run(&mut self, t: Task) {
        match t {
            Task::Write(modifies, cb) => cb((CbContext::new(), write_modifies(&self.0, modifies))),
            Task::Snapshot(cb) => cb((
                CbContext::new(),
                Ok(RocksSnapshot::new(Arc::clone(&self.0.kv))),
            )),
            Task::Pause(dur) => std::thread::sleep(dur),
        }
    }
}

struct RocksEngineCore {
    // only use for memory mode
    temp_dir: Option<TempDir>,
    worker: Worker<Task>,
}

impl Drop for RocksEngineCore {
    fn drop(&mut self) {
        if let Some(h) = self.worker.stop() {
            if let Err(e) = h.join() {
                safe_panic!("RocksEngineCore engine thread panicked: {:?}", e);
            }
        }
    }
}

/// The RocksEngine is based on `RocksDB`.
///
/// This is intended for **testing use only**.
#[derive(Clone)]
pub struct RocksEngine {
    core: Arc<Mutex<RocksEngineCore>>,
    sched: Scheduler<Task>,
    engines: Engines,
    not_leader: Arc<AtomicBool>,
}

impl RocksEngine {
    pub fn new(
        path: &str,
        cfs: &[CfName],
        cfs_opts: Option<Vec<CFOptions<'_>>>,
        shared_block_cache: bool,
    ) -> Result<RocksEngine> {
        info!("RocksEngine: creating for path"; "path" => path);
        let (path, temp_dir) = match path {
            TEMP_DIR => {
                let td = Builder::new().prefix("temp-rocksdb").tempdir().unwrap();
                (td.path().to_str().unwrap().to_owned(), Some(td))
            }
            _ => (path.to_owned(), None),
        };
        let mut worker = Worker::new("engine-rocksdb");
        let db = Arc::new(rocks::util::new_engine(&path, None, cfs, cfs_opts)?);
        // It does not use the raft_engine, so it is ok to fill with the same
        // rocksdb.
        let engines = Engines::new(db.clone(), db, shared_block_cache);
        box_try!(worker.start(Runner(engines.clone())));
        Ok(RocksEngine {
            sched: worker.scheduler(),
            core: Arc::new(Mutex::new(RocksEngineCore { temp_dir, worker })),
            not_leader: Arc::new(AtomicBool::new(false)),
            engines,
        })
    }

    pub fn trigger_not_leader(&self) {
        self.not_leader.store(true, Ordering::SeqCst);
    }

    pub fn pause(&self, dur: Duration) {
        self.sched.schedule(Task::Pause(dur)).unwrap();
    }

    pub fn get_rocksdb(&self) -> Arc<DB> {
        Arc::clone(&self.engines.kv)
    }

    pub fn stop(&self) {
        let mut core = self.core.lock().unwrap();
        if let Some(h) = core.worker.stop() {
            h.join().unwrap();
        }
    }
}

impl Display for RocksEngine {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "RocksDB")
    }
}

impl Debug for RocksEngine {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RocksDB [is_temp: {}]",
            self.core.lock().unwrap().temp_dir.is_some()
        )
    }
}

/// A builder to build a temporary `RocksEngine`.
///
/// Only used for test purpose.
#[must_use]
pub struct TestEngineBuilder {
    path: Option<PathBuf>,
    cfs: Option<Vec<CfName>>,
}

impl TestEngineBuilder {
    pub fn new() -> Self {
        Self {
            path: None,
            cfs: None,
        }
    }

    /// Customize the data directory of the temporary engine.
    ///
    /// By default, TEMP_DIR will be used.
    pub fn path(mut self, path: impl AsRef<Path>) -> Self {
        self.path = Some(path.as_ref().to_path_buf());
        self
    }

    /// Customize the CFs that engine will have.
    ///
    /// By default, engine will have all CFs.
    pub fn cfs(mut self, cfs: impl AsRef<[CfName]>) -> Self {
        self.cfs = Some(cfs.as_ref().to_vec());
        self
    }

    /// Build a `RocksEngine`.
    pub fn build(self) -> Result<RocksEngine> {
        let path = match self.path {
            None => TEMP_DIR.to_owned(),
            Some(p) => p.to_str().unwrap().to_owned(),
        };
        let cfs = self.cfs.unwrap_or_else(|| crate::storage::ALL_CFS.to_vec());
        let cfg_rocksdb = crate::config::DbConfig::default();
        let cache = BlockCacheConfig::default().build_shared_cache();
        let cfs_opts = cfs
            .iter()
            .map(|cf| match *cf {
                CF_DEFAULT => CFOptions::new(CF_DEFAULT, cfg_rocksdb.defaultcf.build_opt(&cache)),
                CF_LOCK => CFOptions::new(CF_LOCK, cfg_rocksdb.lockcf.build_opt(&cache)),
                CF_WRITE => CFOptions::new(CF_WRITE, cfg_rocksdb.writecf.build_opt(&cache)),
                CF_RAFT => CFOptions::new(CF_RAFT, cfg_rocksdb.raftcf.build_opt(&cache)),
                _ => CFOptions::new(*cf, ColumnFamilyOptions::new()),
            })
            .collect();
        RocksEngine::new(&path, &cfs, Some(cfs_opts), cache.is_some())
    }
}

fn write_modifies(engine: &Engines, modifies: Vec<Modify>) -> Result<()> {
    let wb = WriteBatch::default();
    for rev in modifies {
        let res = match rev {
            Modify::Delete(cf, k) => {
                if cf == CF_DEFAULT {
                    trace!("RocksEngine: delete"; "key" => %k);
                    wb.delete(k.as_encoded())
                } else {
                    trace!("RocksEngine: delete_cf"; "cf" => cf, "key" => %k);
                    let handle = rocks::util::get_cf_handle(&engine.kv, cf)?;
                    wb.delete_cf(handle, k.as_encoded())
                }
            }
            Modify::Put(cf, k, v) => {
                if cf == CF_DEFAULT {
                    trace!("RocksEngine: put"; "key" => %k, "value" => escape(&v));
                    wb.put(k.as_encoded(), &v)
                } else {
                    trace!("RocksEngine: put_cf"; "cf" => cf, "key" => %k, "value" => escape(&v));
                    let handle = rocks::util::get_cf_handle(&engine.kv, cf)?;
                    wb.put_cf(handle, k.as_encoded(), &v)
                }
            }
            Modify::DeleteRange(cf, start_key, end_key, notify_only) => {
                trace!(
                    "RocksEngine: delete_range_cf";
                    "cf" => cf,
                    "start_key" => %start_key,
                    "end_key" => %end_key,
                    "notify_only" => notify_only,
                );
                if !notify_only {
                    let handle = rocks::util::get_cf_handle(&engine.kv, cf)?;
                    wb.delete_range_cf(handle, start_key.as_encoded(), end_key.as_encoded())
                } else {
                    Ok(())
                }
            }
        };
        // TODO: turn the error into an engine error.
        if let Err(msg) = res {
            return Err(box_err!("{}", msg));
        }
    }
    engine.write_kv(&wb)?;
    Ok(())
}

impl Engine for RocksEngine {
    type Snap = RocksSnapshot;

    fn async_write(&self, _: &Context, modifies: Vec<Modify>, cb: Callback<()>) -> Result<()> {
        if modifies.is_empty() {
            return Err(Error::EmptyRequest);
        }
        box_try!(self.sched.schedule(Task::Write(modifies, cb)));
        Ok(())
    }

    fn async_snapshot(&self, _: &Context, cb: Callback<Self::Snap>) -> Result<()> {
        fail_point!("rockskv_async_snapshot", |_| Err(box_err!(
            "snapshot failed"
        )));
        let not_leader = {
            let mut header = kvproto::errorpb::Error::new();
            header.mut_not_leader().set_region_id(100);
            header
        };
        let _not_leader = not_leader.clone();
        fail_point!("rockskv_async_snapshot_not_leader", |_| {
            Err(Error::Request(not_leader))
        });
        if self.not_leader.load(Ordering::SeqCst) {
            return Err(Error::Request(_not_leader));
        }
        box_try!(self.sched.schedule(Task::Snapshot(cb)));
        Ok(())
    }
}

impl Snapshot for RocksSnapshot {
    type Iter = DBIterator<Arc<DB>>;

    fn get(&self, key: &Key) -> Result<Option<Value>> {
        trace!("RocksSnapshot: get"; "key" => %key);
        let v = box_try!(self.get_value(key.as_encoded()));
        Ok(v.map(|v| v.to_vec()))
    }

    fn multi_get(&self, keys: &Vec<Key>) -> Result<Vec<Option<Value>>> {
        let v = box_try!(self.multi_get_value(keys.iter().map(|k| k.as_encoded().as_ref()).collect()));
        Ok(v.into_iter().map(|v| v.map(|val| val.to_vec())).collect())
    }

    fn get_cf(&self, cf: CfName, key: &Key) -> Result<Option<Value>> {
        trace!("RocksSnapshot: get_cf"; "cf" => cf, "key" => %key);
        let v = box_try!(self.get_value_cf(cf, key.as_encoded()));
        Ok(v.map(|v| v.to_vec()))
    }

    fn multi_get_cf(&self, cf: CfName, keys: &Vec<Key>) -> Result<Vec<Option<Value>>> {
        let v = box_try!(self.multi_get_value_cf(cf, keys.iter().map(|k| k.as_encoded().as_ref()).collect()));
        Ok(v.into_iter().map(|v| v.map(|val| val.to_vec())).collect())
    }

    fn iter(&self, iter_opt: IterOption, mode: ScanMode) -> Result<Cursor<Self::Iter>> {
        trace!("RocksSnapshot: create iterator");
        let iter = self.db_iterator(iter_opt);
        Ok(Cursor::new(iter, mode))
    }

    fn iter_cf(
        &self,
        cf: CfName,
        iter_opt: IterOption,
        mode: ScanMode,
    ) -> Result<Cursor<Self::Iter>> {
        trace!("RocksSnapshot: create cf iterator");
        let iter = self.db_iterator_cf(cf, iter_opt)?;
        Ok(Cursor::new(iter, mode))
    }
}

impl<D: Deref<Target = DB> + Send> EngineIterator for DBIterator<D> {
    fn next(&mut self) -> bool {
        DBIterator::next(self)
    }

    fn prev(&mut self) -> bool {
        DBIterator::prev(self)
    }

    fn seek(&mut self, key: &Key) -> Result<bool> {
        Ok(DBIterator::seek(self, key.as_encoded().as_slice().into()))
    }

    fn seek_for_prev(&mut self, key: &Key) -> Result<bool> {
        Ok(DBIterator::seek_for_prev(
            self,
            key.as_encoded().as_slice().into(),
        ))
    }

    fn seek_to_first(&mut self) -> bool {
        DBIterator::seek(self, SeekKey::Start)
    }

    fn seek_to_last(&mut self) -> bool {
        DBIterator::seek(self, SeekKey::End)
    }

    fn valid(&self) -> bool {
        DBIterator::valid(self)
    }

    fn status(&self) -> Result<()> {
        DBIterator::status(self)
            .map_err(|e| EngineError::RocksDb(e))
            .map_err(From::from)
    }

    fn key(&self) -> &[u8] {
        DBIterator::key(self)
    }

    fn value(&self) -> &[u8] {
        DBIterator::value(self)
    }
}

#[cfg(test)]
mod tests {
    pub use super::super::perf_context::{PerfStatisticsDelta, PerfStatisticsInstant};
    use super::super::tests::*;
    use super::super::CFStatistics;
    use super::*;
    use tempfile::Builder;

    #[test]
    fn test_rocksdb() {
        let engine = TestEngineBuilder::new()
            .cfs(TEST_ENGINE_CFS)
            .build()
            .unwrap();
        test_base_curd_options(&engine)
    }

    #[test]
    fn test_rocksdb_linear() {
        let engine = TestEngineBuilder::new()
            .cfs(TEST_ENGINE_CFS)
            .build()
            .unwrap();
        test_linear(&engine);
    }

    #[test]
    fn test_rocksdb_statistic() {
        let engine = TestEngineBuilder::new()
            .cfs(TEST_ENGINE_CFS)
            .build()
            .unwrap();
        test_cfs_statistics(&engine);
    }

    #[test]
    fn rocksdb_reopen() {
        let dir = Builder::new().prefix("rocksdb_test").tempdir().unwrap();
        {
            let engine = TestEngineBuilder::new()
                .path(dir.path())
                .cfs(TEST_ENGINE_CFS)
                .build()
                .unwrap();
            must_put_cf(&engine, "cf", b"k", b"v1");
        }
        {
            let engine = TestEngineBuilder::new()
                .path(dir.path())
                .cfs(TEST_ENGINE_CFS)
                .build()
                .unwrap();
            assert_has_cf(&engine, "cf", b"k", b"v1");
        }
    }

    #[test]
    fn test_rocksdb_perf_statistics() {
        let engine = TestEngineBuilder::new()
            .cfs(TEST_ENGINE_CFS)
            .build()
            .unwrap();
        test_perf_statistics(&engine);
    }

    pub fn test_perf_statistics<E: Engine>(engine: &E) {
        must_put(engine, b"foo", b"bar1");
        must_put(engine, b"foo2", b"bar2");
        must_put(engine, b"foo3", b"bar3"); // deleted
        must_put(engine, b"foo4", b"bar4");
        must_put(engine, b"foo42", b"bar42"); // deleted
        must_put(engine, b"foo5", b"bar5"); // deleted
        must_put(engine, b"foo6", b"bar6");
        must_delete(engine, b"foo3");
        must_delete(engine, b"foo42");
        must_delete(engine, b"foo5");

        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut iter = snapshot
            .iter(IterOption::default(), ScanMode::Forward)
            .unwrap();

        let mut statistics = CFStatistics::default();

        let perf_statistics = PerfStatisticsInstant::new();
        iter.seek(&Key::from_raw(b"foo30"), &mut statistics)
            .unwrap();
        assert_eq!(perf_statistics.delta().internal_delete_skipped_count, 0);

        let perf_statistics = PerfStatisticsInstant::new();
        iter.near_seek(&Key::from_raw(b"foo55"), &mut statistics)
            .unwrap();
        assert_eq!(perf_statistics.delta().internal_delete_skipped_count, 2);

        let perf_statistics = PerfStatisticsInstant::new();
        iter.prev(&mut statistics);
        assert_eq!(perf_statistics.delta().internal_delete_skipped_count, 2);

        iter.prev(&mut statistics);
        assert_eq!(perf_statistics.delta().internal_delete_skipped_count, 3);

        iter.prev(&mut statistics);
        assert_eq!(perf_statistics.delta().internal_delete_skipped_count, 3);
    }
}
