mod opt;

use std::{
    collections::VecDeque,
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicUsize},
        Arc, RwLock, RwLockReadGuard,
    },
    thread::JoinHandle,
};

use bytes::{Bytes, BytesMut};
use crossbeam_channel::{bounded, select, Receiver, Sender};
use log::{debug, info, warn};
pub use opt::AgateOptions;
use skiplist::Skiplist;
use yatp::task::callback::Handle;

use crate::{
    closer::Closer,
    entry::{Entry, EntryRef},
    get_ts,
    iterator::is_deleted_or_expired,
    levels::LevelsController,
    manifest::ManifestFile,
    memtable::{MemTable, MemTables},
    ops::oracle::Oracle,
    opt::build_table_options,
    util::{has_any_prefixes, make_comparator},
    value::{self, Request, Value, ValuePointer, VALUE_FIN_TXN, VALUE_POINTER, VALUE_TXN},
    value_log::{ValueLog, ValueLogWrapper},
    wal::Wal,
    Error, Result, Table, TableBuilder, TableOptions,
};

const MEMTABLE_FILE_EXT: &str = ".mem";
const KV_WRITE_CH_CAPACITY: usize = 1000;

struct Closers {
    writes: Closer,
}

pub struct Core {
    closers: Closers,

    mts: RwLock<MemTables>,

    next_mem_fid: AtomicUsize,

    pub(crate) opts: AgateOptions,
    pub(crate) manifest: Arc<ManifestFile>,
    pub(crate) lvctl: LevelsController,
    pub(crate) vlog: ValueLogWrapper,
    write_channel: (Sender<Request>, Receiver<Request>),
    flush_channel: (Sender<Option<FlushTask>>, Receiver<Option<FlushTask>>),

    block_writes: AtomicBool,
    is_closed: AtomicBool,

    pub(crate) orc: Arc<Oracle>,
}

pub struct Agate {
    pub(crate) core: Arc<Core>,
    closer: Closer,
    pool: Arc<yatp::ThreadPool<yatp::task::callback::TaskCell>>,

    // TODO: replace flush thread with yatp pool, so we can control the total
    // cpu usage by control the unified yatp pool.
    flush_handle: Option<JoinHandle<()>>,
}

struct FlushTask {
    mt: Arc<MemTable>,
    drop_prefixes: Vec<Bytes>,
}

impl FlushTask {
    pub fn new(mt: Arc<MemTable>) -> Self {
        Self {
            mt,
            drop_prefixes: vec![],
        }
    }
}

impl Agate {
    /*
    pub fn get_with_ts(&self, key: &[u8], ts: u64) -> Result<Option<Bytes>> {
        let key = format::key_with_ts(key, ts);
        let view = self.core.memtable.view();
        if let Some(value) = view.get(&key) {
            return Ok(Some(value.clone()));
        }
        unimplemented!()
    }
    */

    fn new(core: Arc<Core>) -> Self {
        let closer = Closer::new();
        let pool = Arc::new(
            yatp::Builder::new("agatedb")
                .max_thread_count(core.opts.num_compactors * 8 + 2)
                .min_thread_count(core.opts.num_compactors * 5 + 2)
                .build_callback_pool(),
        );

        let flush_core = core.clone();
        let flush_handle = std::thread::spawn(move || flush_core.flush_memtable().unwrap());

        let write_core = core.clone();
        let pool_clone = pool.clone();
        pool.spawn(move |_: &mut Handle<'_>| {
            write_core
                .do_writes(
                    write_core.closers.writes.clone(),
                    write_core.clone(),
                    pool_clone,
                )
                .unwrap()
        });

        core.lvctl.start_compact(closer.clone(), pool.clone());

        Self {
            core,
            closer,
            pool,
            flush_handle: Some(flush_handle),
        }
    }

    pub fn get(&self, key: &Bytes) -> Result<Value> {
        self.core.get(key)
    }

    pub fn write_to_lsm(&self, request: Request) -> Result<()> {
        self.core.write_to_lsm(request)
    }

    pub fn write_requests(&self, request: Vec<Request>) -> Result<()> {
        self.core.write_requests(request)
    }
}

impl Drop for Agate {
    fn drop(&mut self) {
        self.core
            .block_writes
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.core.closers.writes.close();
        // Flush thread need to be closed before compaction threads. Because the flush
        // thread may stuck in add_l0_table forever when there are too many sst files
        // in level0, and at the same time all compaction threads have exited.
        // TODO: remove such closing order dependency
        self.core.flush_channel.0.send(None).unwrap();
        self.flush_handle.take().unwrap().join().unwrap();
        self.closer.close();
        self.pool.shutdown();
    }
}

impl Core {
    pub(crate) fn new(opts: &AgateOptions) -> Result<Self> {
        // create first mem table
        let orc = Arc::new(Oracle::new(opts));
        let manifest = Arc::new(ManifestFile::open_or_create_manifest_file(opts)?);
        let lvctl = LevelsController::new(opts, manifest.clone(), orc.clone())?;

        let (imm_tables, mut next_mem_fid) = Self::open_mem_tables(opts)?;
        let mt = Self::open_mem_table(opts, next_mem_fid)?;
        next_mem_fid += 1;

        let core = Self {
            closers: Closers {
                writes: Closer::new(),
            },
            mts: RwLock::new(MemTables::new(Arc::new(mt), imm_tables)),
            next_mem_fid: AtomicUsize::new(next_mem_fid),
            opts: opts.clone(),
            manifest,
            lvctl,
            vlog: ValueLogWrapper(Arc::new(ValueLog::new(opts.clone())?)),
            write_channel: bounded(KV_WRITE_CH_CAPACITY),
            flush_channel: crossbeam_channel::bounded(opts.num_memtables),
            block_writes: AtomicBool::new(false),
            is_closed: AtomicBool::new(false),
            orc,
        };

        core.orc.init_next_ts(core.max_version());

        // TODO: Initialize other structures.
        Ok(core)
    }

    pub fn max_version(&self) -> u64 {
        let v = self.mts.read().unwrap().max_version();
        v.max(self.lvctl.max_version())
    }

    fn memtable_file_path(opts: &AgateOptions, file_id: usize) -> PathBuf {
        opts.dir
            .join(format!("{:05}{}", file_id, MEMTABLE_FILE_EXT))
    }

    fn open_mem_table(opts: &AgateOptions, file_id: usize) -> Result<MemTable> {
        let path = Self::memtable_file_path(opts, file_id);
        let c = make_comparator();
        // TODO: refactor skiplist to use `u64`
        let skl = Skiplist::with_capacity(c, opts.arena_size() as usize, false);

        // We don't need to create the WAL for the skiplist in in-memory mode so return the memtable.
        if opts.in_memory {
            return Ok(MemTable::new(file_id, skl, None, opts.clone()));
        }

        let wal = Wal::open(file_id as u32, path, opts.clone())?;
        // TODO: delete WAL when skiplist ref count becomes zero

        let mem_table = MemTable::new(file_id, skl, Some(wal), opts.clone());

        mem_table.update_skip_list()?;

        Ok(mem_table)
    }

    fn open_mem_tables(opts: &AgateOptions) -> Result<(VecDeque<Arc<MemTable>>, usize)> {
        // We don't need to open any tables in in-memory mode.
        if opts.in_memory {
            return Ok((VecDeque::new(), 0));
        }

        let mut fids = vec![];
        let mut mts = VecDeque::new();

        for file in fs::read_dir(&opts.dir)? {
            let file = file?;
            let filename_ = file.file_name();
            let filename = filename_.to_string_lossy();
            if filename.ends_with(MEMTABLE_FILE_EXT) {
                let end = filename.len() - MEMTABLE_FILE_EXT.len();
                let fid: usize = filename[end - 5..end].parse().unwrap();
                fids.push(fid);
            }
        }

        fids.sort_unstable();

        for fid in &fids {
            let memtable = Self::open_mem_table(opts, *fid)?;
            mts.push_back(Arc::new(memtable));
        }

        let mut next_mem_fid = 0;

        if !fids.is_empty() {
            next_mem_fid = *fids.last().unwrap();
        }

        next_mem_fid += 1;

        Ok((mts, next_mem_fid))
    }

    fn new_mem_table(&self) -> Result<MemTable> {
        let file_id = self
            .next_mem_fid
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mt = Self::open_mem_table(&self.opts, file_id)?;
        Ok(mt)
    }

    pub fn is_closed(&self) -> bool {
        // TODO: check db closed
        false
    }

    pub(crate) fn get(&self, key: &Bytes) -> Result<Value> {
        if self.is_closed() {
            return Err(Error::DBClosed);
        }

        let view = self.mts.read()?.view();

        let mut max_value = Value::default();
        let version = get_ts(key);

        for table in view.tables() {
            let mut value = Value::default();

            if let Some((key, value_data)) = table.get_with_key(key) {
                value.decode(value_data.clone());
                value.version = get_ts(key);

                if value.meta == 0 && value.value.is_empty() {
                    continue;
                }
                if value.version == version {
                    return Ok(value);
                }
                // max_vs.version == 0 means it is not assigned a value yet.
                if max_value.version == 0 || max_value.version < value.version {
                    max_value = value;
                }
            }
        }

        // max_value will be used in level controller.
        self.lvctl.get(key, max_value, 0)
    }

    /// `write_to_lsm` will only be called in write thread (or write coroutine).
    ///
    /// By using a fine-grained lock approach, writing to LSM tree acquires:
    /// 1. read lock of memtable list (only block flush)
    /// 2. write lock of mutable memtable WAL (won't block mut-table read).
    /// 3. level controller lock (TBD)
    pub(crate) fn write_to_lsm(&self, request: Request) -> Result<()> {
        // TODO: Check entries and pointers.

        let memtables = self.mts.read()?;
        let mut_table = memtables.mut_table();

        for (idx, entry) in request.entries.into_iter().enumerate() {
            if self.opts.skip_vlog(&entry) {
                // deletion, tombstone, and small values
                mut_table.put(
                    entry.key,
                    Value {
                        value: entry.value,
                        meta: entry.meta & (!value::VALUE_POINTER),
                        user_meta: entry.user_meta,
                        expires_at: entry.expires_at,
                        version: 0,
                    },
                )?;
            } else {
                let mut vptr_buf = BytesMut::new();
                request.ptrs[idx].encode(&mut vptr_buf);
                // Write pointer to memtable.
                mut_table.put(
                    entry.key,
                    Value {
                        value: vptr_buf.freeze(),
                        meta: entry.meta | value::VALUE_POINTER,
                        user_meta: entry.user_meta,
                        expires_at: entry.expires_at,
                        version: 0,
                    },
                )?;
            }
        }

        if self.opts.sync_writes {
            mut_table.sync_wal()?;
        }
        Ok(())
    }

    /// Calling ensure_room_for_write requires locking whole memtable.
    pub fn ensure_room_for_write(&self) -> Result<()> {
        // we do not need to force flush memtable in in-memory mode as WAL is None.
        let mut mts = self.mts.write()?;
        let mut force_flush = false;

        if !force_flush && !self.opts.in_memory && mts.mut_table().should_flush_wal()? {
            force_flush = true;
        }

        let mem_size = mts.mut_table().skl.mem_size();

        if !force_flush && mem_size as u64 >= self.opts.mem_table_size {
            force_flush = true;
        }

        if !force_flush {
            return Ok(());
        }

        match self
            .flush_channel
            .0
            .try_send(Some(FlushTask::new(mts.mut_table())))
        {
            Ok(_) => {
                let memtable = self.new_mem_table()?;
                mts.use_new_table(Arc::new(memtable));

                debug!(
                    "memtable flushed, total={}, mt.size = {}",
                    mts.nums_of_memtable(),
                    mem_size
                );

                Ok(())
            }
            Err(_) => Err(Error::WriteNoRoom(())),
        }
    }

    /// build L0 table from memtable
    fn build_l0_table(flush_task: FlushTask, table_opts: TableOptions) -> TableBuilder {
        let mut iter = flush_task.mt.skl.iter_ref();
        let mut builder = TableBuilder::new(table_opts);
        iter.seek_to_first();
        while iter.valid() {
            if !flush_task.drop_prefixes.is_empty()
                && has_any_prefixes(iter.key(), &flush_task.drop_prefixes)
            {
                continue;
            }
            // TODO: reduce encode / decode by using something like flatbuffer
            let mut vs = Value::default();
            vs.decode(iter.value().clone());
            let vlog_len = if vs.meta & value::VALUE_POINTER != 0 {
                let mut vp = ValuePointer::default();
                vp.decode(&vs.value);
                vp.len
            } else {
                0
            };
            builder.add(iter.key(), &vs, vlog_len);
            iter.next();
        }
        builder
    }

    /// handle_flush_task must run serially.
    fn handle_flush_task(&self, flush_task: FlushTask) -> Result<()> {
        if flush_task.mt.skl.is_empty() {
            return Ok(());
        }
        let table_opts = build_table_options(&self.opts);
        let builder = Self::build_l0_table(flush_task, table_opts.clone());

        if builder.is_empty() {
            builder.finish();
            return Ok(());
        }

        let file_id = self.lvctl.reserve_file_id();
        let table = if self.opts.in_memory {
            let data = builder.finish();
            Table::open_in_memory(data, file_id, table_opts)?
        } else {
            Table::create(
                &crate::table::new_filename(file_id, &self.opts.dir),
                builder.finish(),
                table_opts,
            )?
        };

        self.lvctl.add_l0_table(table)?;

        Ok(())
    }

    pub(crate) fn get_mem_tables(&self) -> Vec<Arc<MemTable>> {
        let mut tables = vec![];
        let mts = self.mts.read().unwrap();
        tables.push(mts.mut_table());
        for idx in 0..mts.nums_of_memtable() - 1 {
            tables.push(mts.imm_table(idx));
        }
        tables
    }

    fn flush_memtable(&self) -> Result<()> {
        for task in self.flush_channel.1.clone() {
            if let Some(task) = task {
                let flush_id = task.mt.id();
                match self.handle_flush_task(task) {
                    Ok(_) => {
                        let mut mts = self.mts.write()?;
                        assert_eq!(flush_id, mts.imm_table(0).id());
                        mts.pop_imm();
                    }
                    Err(err) => {
                        eprintln!("error while flushing memtable to disk: {:?}", err);
                        std::thread::sleep(std::time::Duration::from_secs(1));
                    }
                }
            } else {
                break;
            }
        }

        Ok(())
    }

    /// Write requests should be only called in one thread. By calling this
    /// function, requests will be written into the LSM tree.
    ///
    // TODO: ensure only one thread calls this function by using Mutex.
    pub fn write_requests(&self, mut requests: Vec<Request>) -> Result<()> {
        if requests.is_empty() {
            return Ok(());
        }

        #[allow(clippy::needless_collect)]
        let dones: Vec<_> = requests.iter().map(|x| x.done.clone()).collect();

        let write = || {
            if let Some(ref vlog) = **self.vlog {
                vlog.write(&mut requests)?;
            }

            let mut cnt = 0;

            // Writing to LSM.
            for req in requests {
                if req.entries.is_empty() {
                    continue;
                }

                cnt += req.entries.len();

                while self.ensure_room_for_write().is_err() {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }

                self.write_to_lsm(req)?;
            }

            debug!("{} entries written", cnt);

            Ok(())
        };

        let result = write();

        for done in dones.into_iter().flatten() {
            done.send(result.clone()).unwrap();
        }

        result
    }

    pub(crate) fn send_to_write_channel(
        &self,
        entries: Vec<Entry>,
    ) -> Result<Receiver<Result<()>>> {
        if self.block_writes.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(Error::CustomError(
                "Writes are blocked, possibly due to dropping all data or close".to_string(),
            ));
        }

        let mut count = 0;
        let mut size = 0;
        for entry in &entries {
            size += entry.estimate_size(self.opts.value_threshold) as u64;
            count += 1;
        }
        if count >= self.opts.max_batch_count || size >= self.opts.max_batch_size {
            return Err(Error::TxnTooBig);
        }

        let (tx, rx) = bounded(1);
        // TODO: Get request from request pool.
        let req = Request {
            entries,
            ptrs: vec![],
            done: Some(tx),
        };
        self.write_channel.0.send(req)?;
        Ok(rx)
    }

    fn do_writes(
        &self,
        closer: Closer,
        core: Arc<Self>,
        pool: Arc<yatp::ThreadPool<yatp::task::callback::TaskCell>>,
    ) -> Result<()> {
        info!("Start doing writes.");

        let (pending_tx, pending_rx) = bounded(1);

        const STATUS_WRITE: usize = 0;
        const STATUS_CLOSED: usize = 1;

        let mut reqs = Vec::with_capacity(10);

        let status = loop {
            let req;

            // We wait until there is at least one request.
            select! {
                recv(core.write_channel.1) -> req_recv => {
                    req = req_recv.unwrap();
                }
                recv(closer.get_receiver()) -> _ => {
                    break STATUS_CLOSED;
                }
            }

            reqs.push(req);

            let status = loop {
                if reqs.len() >= 3 * KV_WRITE_CH_CAPACITY {
                    pending_tx.send(()).unwrap();
                    break STATUS_WRITE;
                }

                select! {
                    // Either push to pending, or continue to pick from write_channel.
                    recv(core.write_channel.1) -> req => {
                        let req = req.unwrap();
                        reqs.push(req);
                    }
                    send(pending_tx, ()) -> _ => {
                        break STATUS_WRITE;
                    }
                    recv(closer.get_receiver()) -> _ => {
                        break STATUS_CLOSED;
                    }
                }
            };

            if status == STATUS_CLOSED {
                break STATUS_CLOSED;
            } else if status == STATUS_WRITE {
                let rx = pending_rx.clone();
                let reqs = std::mem::replace(&mut reqs, Vec::with_capacity(10));
                let core = core.clone();
                pool.spawn(move |_: &mut Handle<'_>| {
                    if let Err(err) = core.write_requests(reqs) {
                        log::error!("failed to write: {:?}", err);
                    }
                    rx.recv().ok();
                })
            }
        };

        if status == STATUS_CLOSED {
            // All the pending request are drained.
            // Don't close the write_channel, because it has be used in several places.
            loop {
                select! {
                    recv(core.write_channel.1) -> req => {
                        reqs.push(req.unwrap());
                    }
                    default => {
                        if let Err(err) = core.write_requests(reqs) {
                            log::error!("failed to write: {:?}", err);
                        }
                        return Ok(());
                    }
                }
            }
        }

        unreachable!()
    }

    // Triggers a value log garbage collection.
    //
    // It picks value log files to perform GC based on statistics that are collected
    // during compactions.  If no such statistics are available, then log files are
    // picked in random order. The process stops as soon as the first log file is
    // encountered which does not result in garbage collection.
    //
    // When a log file is picked, it is first sampled. If the sample shows that we
    // can discard at least discardRatio space of that file, it would be rewritten.
    //
    // If a call to run_value_log_gc results in no rewrites, then an ErrNoRewrite is
    // thrown indicating that the call resulted in no file rewrites.
    //
    // We recommend setting discardRatio to 0.5, thus indicating that a file be
    // rewritten if half the space can be discarded.  This results in a lifetime
    // value log write amplification of 2 (1 from original write + 0.5 rewrite +
    // 0.25 + 0.125 + ... = 2). Setting it to higher value would result in fewer
    // space reclaims, while setting it to a lower value would result in more space
    // reclaims at the cost of increased activity on the LSM tree. discardRatio
    // must be in the range (0.0, 1.0), both endpoints excluded, otherwise an
    // ErrInvalidRequest is returned.
    //
    // Only one GC is allowed at a time. If another value log GC is running, or DB
    // has been closed, this would return an ErrRejected.
    //
    // Note: Every time GC is run, it would produce a spike of activity on the LSM
    // tree.
    pub fn run_value_log_gc(&self, discard_ratio: f64) -> Result<()> {
        if self.opts.in_memory {
            return Err(Error::ErrGCInMemoryMode);
        }
        if discard_ratio >= 1.0 || discard_ratio <= 0.0 {
            return Err(Error::ErrInvalidRequest);
        }

        self.vlog.as_ref().as_ref().unwrap().run_gc(discard_ratio)
    }

    fn rewrite(&self, log_file: &RwLockReadGuard<Wal>) -> Result<()> {
        let mut inner = self.vlog.as_ref().as_ref().unwrap().inner.write().unwrap();
        let fid = log_file.file_id();
        if inner
            .files_to_delete
            .iter()
            .find(|id| **id == fid)
            .is_some()
        {
            return Err(Error::Other(format!(
                "value log file already marked for deletion fid {}",
                fid
            )));
        }

        let max_fid = inner.max_fid;
        assert!(fid < max_fid);

        info!("Rewriting fid: {}", fid);
        let mut wb: Vec<Entry> = Vec::with_capacity(1000);
        let mut size = 0;
        let (mut count, mut moved) = (0, 0);

        let fe = |e: EntryRef| -> Result<()> {
            count += 1;
            if count % 100000 == 0 {
                debug!("Processing entry {:?}", count);
            }

            // spadea(todo): can we avoid clone?
            let val = self.get(&Bytes::from(e.key.to_vec()))?;
            if discard_entry(&e, &val) {
                return Ok(());
            }

            // Value is still present in value log

            if val.value.len() == 0 {
                return Err(Error::Other(format!("Empty value {:?}", val)));
            }

            let mut vp = ValuePointer::default();
            vp.decode(&val.value);

            // If the entry found from the LSM Tree points to a newer vlog file,
            // don't do anything.
            if vp.file_id > fid {
                return Ok(());
            }

            // If the entry found from the LSM Tree points to an offset greater than the one
            // read from vlog, don't do anything.
            // if vp.offset > e.offset {
            //     // todo: no offset now
            //     return Ok(());
            // }

            // If the entry read from LSM Tree and vlog file point to the same vlog file and offset,
            // insert them back into the DB.
            // NOTE: It might be possible that the entry read from the LSM Tree points to
            // an older vlog file. See the comments in the else part.
            if vp.file_id == fid && vp.offset == e.offset {
                moved += 1;
                let mut new_entry = e.to_owned();
                new_entry.meta = e.meta & (!(VALUE_POINTER | VALUE_TXN | VALUE_FIN_TXN));
                let es = new_entry.estimate_size(self.opts.value_threshold);
                // Consider size of value as well while considering the total size
                // of the batch. There have been reports of high memory usage in
                // rewrite because we don't consider the value size. See #1292.
                es += e.value.len();

                // Ensure length and size of wb is within transaction limits.
                if wb.len() + 1 >= self.opts.max_batch_count as usize
                    || size + es > self.opts.max_batch_size as usize
                {
                    self.batch_set(wb)?;
                    size = 0;
                    wb.clear();
                }

                wb.push(new_entry);
                size += es;
            } else {
                // It might be possible that the entry read from LSM Tree points to
                // an older vlog file.  This can happen in the following situation.
                // Assume DB is opened with
                // numberOfVersionsToKeep=1
                //
                // Now, if we have ONLY one key in the system "FOO" which has been
                // updated 3 times and the same key has been garbage collected 3
                // times, we'll have 3 versions of the movekey
                // for the same key "FOO".
                //
                // NOTE: moveKeyi is the gc'ed version of the original key with version i
                // We're calling the gc'ed keys as moveKey to simplify the
                // explanantion. We used to add move keys but we no longer do that.
                //
                // Assume we have 3 move keys in L0.
                // - moveKey1 (points to vlog file 10),
                // - moveKey2 (points to vlog file 14) and
                // - moveKey3 (points to vlog file 15).
                //
                // Also, assume there is another move key "moveKey1" (points to
                // vlog file 6) (this is also a move Key for key "FOO" ) on upper
                // levels (let's say 3). The move key "moveKey1" on level 0 was
                // inserted because vlog file 6 was GCed.
                //
                // Here's what the arrangement looks like
                // L0 => (moveKey1 => vlog10), (moveKey2 => vlog14), (moveKey3 => vlog15)
                // L1 => ....
                // L2 => ....
                // L3 => (moveKey1 => vlog6)
                //
                // When L0 compaction runs, it keeps only moveKey3 because the number of versions
                // to keep is set to 1. (we've dropped moveKey1's latest version)
                //
                // The new arrangement of keys is
                // L0 => ....
                // L1 => (moveKey3 => vlog15)
                // L2 => ....
                // L3 => (moveKey1 => vlog6)
                //
                // Now if we try to GC vlog file 10, the entry read from vlog file
                // will point to vlog10 but the entry read from LSM Tree will point
                // to vlog6. The move key read from LSM tree will point to vlog6
                // because we've asked for version 1 of the move key.
                //
                // This might seem like an issue but it's not really an issue
                // because the user has set the number of versions to keep to 1 and
                // the latest version of moveKey points to the correct vlog file
                // and offset. The stale move key on L3 will be eventually dropped
                // by compaction because there is a newer versions in the upper
                // levels.
            }

            Ok(())
        };

        let mut iter = log_file.iter().unwrap();
        while let Some(e) = iter.next()? {
            fe(e)?;
        }

        let mut batch_size = 1024;
        let mut loops = 0;
        let mut i = 0;
        while i < wb.len() {
            loops += 1;
            if batch_size == 0 {
                warn!("We shouldn't reach batch size of zero.");
                return Err(Error::ErrNoRewrite);
            }

            let end = usize::min(i + batch_size, wb.len());
            match self.batch_set(wb[i..end].to_vec()) {
                Err(Error::TxnTooBig) => {
                    batch_size = batch_size / 2;
                    continue;
                }
                e @ Err(_) => return e,
                _ => {}
            }
            i += batch_size;
        }

        info!("Processed {} entries in {} loops", wb.len(), loops);
        info!("Total entries: {}. Moved: {}", count, moved);
        info!("Removing fid: {}", fid);

        let mut delete_file_now = false;
        if inner.files_map.get(&fid).is_none() {
            return Err(Error::Other(format!("Unable to find fid {}", fid)));
        }

        // no other object holding it
        if Arc::strong_count(&self.vlog) == 1 {
            inner.files_map.remove(&fid);
            delete_file_now = true;
        } else {
            inner.files_to_delete.push(fid);
        }

        if delete_file_now {
            // todo
        }

        Ok(())
    }

    fn batch_set(&self, entries: Vec<Entry>) -> Result<()> {
        let done = self.send_to_write_channel(entries)?;
        done.recv().unwrap()
    }
}

fn discard_entry(e: &EntryRef, val: &Value) -> bool {
    if val.version != get_ts(e.key) {
        // Version not found. Discard.
        return true;
    }

    if is_deleted_or_expired(val.meta, val.expires_at) {
        return true;
    }

    if val.meta & VALUE_POINTER == 0 {
        // Key also stores the value in LSM. Discard.
        return true;
    }

    if val.meta & VALUE_FIN_TXN > 0 {
        // Just a txn finish entry. Discard.
        return true;
    }
    return false;
}

#[cfg(test)]
pub(crate) mod tests;
