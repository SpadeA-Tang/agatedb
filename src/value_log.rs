use std::{
    collections::{HashMap, HashSet},
    iter::FromIterator,
    ops::{Deref, DerefMut},
    os::unix::prelude::MetadataExt,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU32},
        Arc, RwLock,
    },
};

use bytes::{Bytes, BytesMut};
use log::{debug, error, info};

use crate::{
    discard::DiscardStats,
    error,
    value::{self, Request, ValuePointer},
    wal::{Header, Wal},
    AgateOptions, Error, Result,
};

fn vlog_file_path(dir: impl AsRef<Path>, fid: u32) -> PathBuf {
    dir.as_ref().join(format!("{:06}.vlog", fid))
}

pub(crate) struct ValueLogInner {
    /// `files_map` stores mapping from value log ID to WAL object.
    ///
    /// As we would concurrently read WAL, we need to wrap it with `RwLock`.
    /// TODO: use scheme like memtable to separate current vLog
    /// and previous logs, so as to reduce usage of `RwLock`.
    files_map: HashMap<u32, Arc<RwLock<Wal>>>,
    /// maximum file ID opened
    max_fid: u32,
    files_to_delete: Vec<u32>,
    num_entries_written: u32,
}

impl ValueLogInner {
    fn new() -> Self {
        Self {
            files_map: HashMap::new(),
            max_fid: 0,
            files_to_delete: vec![],
            num_entries_written: 0,
        }
    }

    fn drop_no_fail(&mut self) -> Result<()> {
        for wal in &mut self.files_map.values_mut() {
            let mut wal = wal.write()?;
            wal.mark_close_and_save();
        }

        Ok(())
    }

    pub(crate) fn files_map(&self) -> &HashMap<u32, Arc<RwLock<Wal>>> {
        &self.files_map
    }

    pub(crate) fn files_map_mut(&mut self) -> &mut HashMap<u32, Arc<RwLock<Wal>>> {
        &mut self.files_map
    }

    pub(crate) fn max_fid(&self) -> u32 {
        self.max_fid
    }

    pub(crate) fn files_to_delete(&self) -> &Vec<u32> {
        &self.files_to_delete
    }

    pub(crate) fn files_to_delete_mut(&mut self) -> &mut Vec<u32> {
        &mut self.files_to_delete
    }
}

impl Drop for ValueLogInner {
    fn drop(&mut self) {
        crate::util::no_fail(self.drop_no_fail(), "ValueLog::ValueLogInner::drop");
    }
}

/// ValueLog stores all value logs of an agatedb instance.
pub struct ValueLog {
    /// value log directory
    dir_path: PathBuf,
    /// value log file mapping, use `RwLock` to support concurrent read
    inner: Arc<RwLock<ValueLogInner>>,
    /// offset of next write
    writeable_log_offset: AtomicU32,
    opts: AgateOptions,

    gc_running: AtomicBool,
    discard_stats: DiscardStats,
}

impl ValueLog {
    /// Create value logs from agatedb options.
    /// If agate is created with in-memory mode, this function will return `None`.
    pub fn new(opts: AgateOptions) -> Result<Option<Self>> {
        let inner = if opts.in_memory {
            None
        } else {
            let inner = Self {
                discard_stats: DiscardStats::init_discard_stats(opts.clone())?,
                inner: Arc::new(RwLock::new(ValueLogInner::new())),
                dir_path: opts.value_dir.clone(),
                opts,
                writeable_log_offset: AtomicU32::new(0),
                gc_running: AtomicBool::new(false),
            };

            // TODO: garbage collection
            inner.open()?;
            Some(inner)
        };

        Ok(inner)
    }

    pub(crate) fn inner(&self) -> &Arc<RwLock<ValueLogInner>> {
        &self.inner
    }

    fn file_path(&self, fid: u32) -> PathBuf {
        vlog_file_path(&self.dir_path, fid)
    }

    /// Opens all vlog and put them into files map.
    ///
    /// Returns OK if there is no error.
    /// Returns Error when there are duplicated files or vlog file with invalid file name.
    fn populate_files_map(&self) -> Result<()> {
        let dir = std::fs::read_dir(&self.dir_path)?;
        let mut inner = self.inner.write().unwrap();
        for file in dir {
            let file = file?;
            match file.file_name().into_string() {
                Ok(filename) => {
                    if filename.ends_with(".vlog") {
                        let fid: u32 = filename[..filename.len() - 5].parse().map_err(|err| {
                            Error::InvalidFilename(format!("failed to parse file ID {:?}", err))
                        })?;
                        let wal = Wal::open(fid, file.path(), self.opts.clone())?;
                        let wal = Arc::new(RwLock::new(wal));
                        if inner.files_map.insert(fid, wal).is_some() {
                            return Err(Error::InvalidFilename(format!(
                                "duplicated vlog found {}",
                                fid
                            )));
                        }
                        if inner.max_fid < fid {
                            inner.max_fid = fid;
                        }
                    }
                }
                Err(filename) => {
                    return Err(Error::InvalidFilename(format!(
                        "Unrecognized filename {:?}",
                        filename
                    )));
                }
            }
        }
        Ok(())
    }

    /// Creates a new vlog file.
    fn create_vlog_file(&self) -> Result<(u32, Arc<RwLock<Wal>>)> {
        let mut inner = self.inner.write().unwrap();
        let fid = inner.max_fid + 1;
        let path = self.file_path(fid);
        let wal = Wal::open(fid, path, self.opts.clone())?;
        // TODO: only create new files
        let wal = Arc::new(RwLock::new(wal));
        assert!(inner.files_map.insert(fid, wal.clone()).is_none());
        assert!(inner.max_fid < fid);
        inner.max_fid = fid;
        // TODO: add vlog header
        self.writeable_log_offset
            .store(0, std::sync::atomic::Ordering::SeqCst);
        inner.num_entries_written = 0;
        Ok((fid, wal))
    }

    /// Gets sorted valid vlog files' ID set.
    fn sorted_fids(&self) -> Vec<u32> {
        let inner = self.inner.read().unwrap();
        let to_be_deleted: HashSet<u32> = HashSet::from_iter(inner.files_to_delete.iter().cloned());
        let mut result = inner
            .files_map
            .keys()
            .filter(|k| !to_be_deleted.contains(k))
            .cloned()
            .collect::<Vec<u32>>();
        // Hold read lock as short as we can
        drop(inner);

        // cargo clippy suggests using `sort_unstable`
        result.sort_unstable();
        result
    }

    /// Open value log directory
    fn open(&self) -> Result<()> {
        self.populate_files_map()?;
        // TODO: find empty files and iterate vlogs
        self.create_vlog_file()?;
        Ok(())
    }

    fn w_offset(&self) -> u32 {
        self.writeable_log_offset
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Write requests to vlog, and put vlog pointers back in `Request`.
    /// `write` should not be called concurrently, otherwise this will lead to wrong result.
    pub fn write(&self, requests: &mut [Request]) -> Result<()> {
        let result = self.write_inner(requests);
        if self.opts.sync_writes {
            let inner = self.inner.read().unwrap();
            let current_log_id = inner.max_fid;
            let current_log_ptr = inner.files_map.get(&current_log_id).unwrap().clone();
            let mut current_log = current_log_ptr.write().unwrap();
            drop(inner);
            current_log.sync()?;
        }
        result
    }

    pub fn write_inner(&self, requests: &mut [Request]) -> Result<()> {
        let inner = self.inner.read().unwrap();
        let mut current_log_id = inner.max_fid;
        let mut current_log = inner.files_map.get(&current_log_id).unwrap().clone();
        drop(inner);

        // `write` is called serially. There won't be two routines concurrently
        // calling this function. Therefore, we could bypass a lot of lock schemes
        // in this function.
        let write = |buf: &[u8], current_log_lck: &RwLock<Wal>| -> Result<()> {
            if buf.is_empty() {
                return Ok(());
            }
            let n = buf.len() as u32;
            let start = self
                .writeable_log_offset
                .fetch_add(n, std::sync::atomic::Ordering::SeqCst);
            let end_offset = start + n;

            // expand file size if space is not enough
            // TODO: handle value >= 4GB case
            let mut current_log = current_log_lck.write().unwrap();
            if end_offset >= current_log.size() {
                current_log.set_len(end_offset as u64)?;
            }
            // As `start..end_offset` is only used by current write routine, we
            // could safely unlock the log lock and copy data inside.
            let ptr = current_log.data()[start as usize..end_offset as usize].as_mut_ptr();
            drop(current_log);
            unsafe {
                std::ptr::copy_nonoverlapping(buf.as_ptr(), ptr, buf.len());
            }
            // ensure data are flushed to main memory
            std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst);

            let mut current_log = current_log_lck.write().unwrap();
            current_log.set_size(end_offset);
            drop(current_log);
            Ok(())
        };

        // `to_disk` returns `true` if we need a new vLog.
        let to_disk = |current_log: &RwLock<Wal>| -> Result<bool> {
            let inner = self.inner.read().unwrap();
            if self.w_offset() as u64 > self.opts.value_log_file_size
                || inner.num_entries_written > self.opts.value_log_max_entries
            {
                let mut current_log = current_log.write().unwrap();
                current_log.done_writing(self.w_offset())?;
                Ok(true)
            } else {
                Ok(false)
            }
        };

        let mut buf = BytesMut::new();
        for req in requests.iter_mut() {
            req.ptrs.clear();

            let mut written = 0;

            for mut entry in req.entries.iter_mut() {
                buf.clear();

                if self.opts.skip_vlog(entry) {
                    req.ptrs.push(ValuePointer::default());
                    continue;
                }

                let mut p = ValuePointer {
                    file_id: current_log_id,
                    offset: self.w_offset(),
                    ..Default::default()
                };

                let orig_meta = entry.meta;
                entry.meta &= !(value::VALUE_FIN_TXN | value::VALUE_TXN);

                let plen = Wal::encode_entry(&mut buf, entry);
                entry.meta = orig_meta;
                p.len = plen as u32;
                req.ptrs.push(p);
                write(&buf, &current_log)?;

                written += 1;
            }

            if to_disk(&current_log)? {
                let (log_id, log) = self.create_vlog_file()?;
                current_log_id = log_id;
                current_log = log;
            }

            let mut inner = self.inner.write().unwrap();
            inner.num_entries_written += written;
        }

        if to_disk(&current_log)? {
            self.create_vlog_file()?;
        }
        Ok(())
    }

    fn get_file(&self, value_ptr: &ValuePointer) -> Result<Arc<RwLock<Wal>>> {
        let inner = self.inner.read().unwrap();
        let file = inner.files_map.get(&value_ptr.file_id).cloned();
        if let Some(file) = file {
            let max_fid = inner.max_fid;
            if value_ptr.file_id == max_fid {
                let current_offset = self.w_offset();
                if value_ptr.offset >= current_offset {
                    return Err(Error::InvalidLogOffset(value_ptr.offset, current_offset));
                }
            }
            // If the file is not current log, we cannot get file size without acquiring lock.
            // Therefore, we don't check for offset overflow.
            Ok(file)
        } else {
            Err(Error::VlogNotFound(value_ptr.file_id))
        }
    }

    /// Read data from vlogs.
    /// The returned value is a `Bytes`, including the whole entry.
    /// You may need to manually decode it with `Wal::decode_wntry`.
    ///
    /// TODO: let user to decide when to unlock instead of blocking.
    /// TODO: return header together with k-v pair.
    pub(crate) fn read(&self, value_ptr: ValuePointer) -> Result<Bytes> {
        let log_file = self.get_file(&value_ptr)?;
        let r = log_file.read().unwrap();
        let mut buf = r.read(&value_ptr)?;
        let original_buf = buf.slice(..);
        drop(r);

        // TODO: verify checksum

        let mut header = Header::default();
        header.decode(&mut buf)?;
        let kv = buf;

        if (kv.len() as u32) < header.key_len + header.value_len {
            return Err(error::Error::InvalidValuePointer {
                vptr: value_ptr,
                kvlen: kv.len(),
                range: header.key_len..header.key_len + header.value_len,
            });
        }
        Ok(original_buf)
    }

    pub(crate) fn get_log_file(&self, fid: u32) -> Option<Arc<RwLock<Wal>>> {
        self.inner.read().unwrap().files_map.get(&fid).cloned()
    }

    pub(crate) fn pick_log(&self, discard_ratio: f64) -> Option<Arc<RwLock<Wal>>> {
        let inner = self.inner.read().unwrap();

        loop {
            // Pick a candidate that contains the largest amount of discardable data
            let (fid, discard) = self.discard_stats.max_discard();

            // max_discard will return fid=0 if it doesn't have any discard data. The
            // vlog files start from 1.
            if fid == 0 {
                info!("No file with discard stats");
            }
            let lf = match inner.files_map.get(&fid) {
                Some(lf) => lf.read().unwrap(),
                None => {
                    // This file was deleted but it's discard stats increased because of compactions. The file
                    // doesn't exist so we don't need to do anything. Skip it and retry.
                    self.discard_stats.update(fid, -1);
                    continue;
                }
            };

            let file_size = match lf.file().metadata() {
                Ok(meta) => meta.size(),
                Err(e) => {
                    error!(
                        "Unable to get stats for value log fid: {} err: {:?}",
                        fid, e
                    );
                    return None;
                }
            };

            let thr = file_size as f64 * discard_ratio;
            // rust does not support comparison between float
            if discard < thr as u64 {
                debug!(
                    "Discard: {} less than threshold: {} for file: {:?}",
                    discard,
                    thr,
                    lf.file_path()
                );
                return None;
            }

            if fid < inner.max_fid {
                info!(
                    "Found value log max discard fid: {} discard: {}",
                    fid, discard
                );

                return Some(inner.files_map.get(&fid).unwrap().clone());
            }

            return None;
        }
    }

    pub(crate) fn discard_stats(&self) -> &DiscardStats {
        &self.discard_stats
    }
}

#[derive(Clone)]
pub struct ValueLogWrapper(pub Arc<Option<ValueLog>>);

impl ValueLogWrapper {
    // for calling this, ValueLog must be some
    pub(crate) fn value_log(&self) -> &ValueLog {
        self.0.as_ref().as_ref().unwrap()
    }
}

impl Deref for ValueLogWrapper {
    type Target = Arc<Option<ValueLog>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ValueLogWrapper {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Drop for ValueLogWrapper {
    fn drop(&mut self) {
        if self.0.as_ref().is_none() {
            return;
        }
        if Arc::strong_count(&self.0) > 2 {
            return;
        }

        let mut inner = self.0.as_ref().as_ref().unwrap().inner.write().unwrap();
        let files_to_delete = std::mem::take(&mut inner.files_to_delete);
        for fid in files_to_delete {
            inner.files_map.remove(&fid).unwrap();
            self.value_log().discard_stats().update(fid, -1);
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use value::VALUE_POINTER;

    use super::*;
    use crate::entry::Entry;

    #[test]
    fn test_value_basic() {
        let mut opts = AgateOptions::default();
        let tmp_dir = tempdir().unwrap();
        opts.value_dir = tmp_dir.path().to_path_buf();
        opts.value_threshold = 32;
        opts.value_log_file_size = 1024;
        let vlog = ValueLog::new(opts.clone()).unwrap().unwrap();

        let val1 = b"sampleval012345678901234567890123";
        let val2 = b"samplevalb012345678901234567890123";

        assert!(val1.len() > opts.value_threshold);

        let mut e1 = Entry::new(
            Bytes::from_static(b"samplekey"),
            Bytes::copy_from_slice(val1),
        );
        e1.meta = VALUE_POINTER;
        let mut e2 = Entry::new(
            Bytes::from_static(b"samplekeyb"),
            Bytes::copy_from_slice(val2),
        );
        e2.meta = VALUE_POINTER;

        let mut reqs = vec![Request {
            entries: vec![e1, e2],
            ptrs: vec![],
            done: None,
        }];

        vlog.write(&mut reqs).unwrap();
        let req = reqs.pop().unwrap();
        assert_eq!(req.ptrs.len(), 2);

        let mut buf1 = vlog.read(req.ptrs[0].clone()).unwrap();
        let mut buf2 = vlog.read(req.ptrs[1].clone()).unwrap();

        let e1 = Wal::decode_entry(&mut buf1).unwrap();
        let e2 = Wal::decode_entry(&mut buf2).unwrap();

        assert_eq!(&e1.key[..], b"samplekey");
        assert_eq!(&e1.value[..], val1);

        assert_eq!(&e2.key[..], b"samplekeyb");
        assert_eq!(&e2.value[..], val2);
    }
}
