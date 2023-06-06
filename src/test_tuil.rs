use std::os::unix::prelude::MetadataExt;

use bytes::Bytes;
use walkdir::WalkDir;

use crate::{entry::Entry, Agate};

pub fn txn_delete(db: &Agate, key: Bytes) {
    let mut txn = db.new_transaction(true);
    txn.delete(key).unwrap();
    txn.commit().unwrap();
}

pub fn txn_set(db: &Agate, key: Bytes, val: Bytes, meta: u8) {
    let mut txn = db.new_transaction(true);
    let mut entry = Entry::new(key, val);
    entry.meta = meta;
    txn.set_entry(entry).unwrap();
    txn.commit().unwrap();
}

pub fn dir_size(path: &str) -> Result<u64> {
    let mut size = 0;
    for entry in WalkDir::new(path) {
        let entry = entry?;
        if entry.file_type().is_file() {
            size += entry.metadata()?.size();
        }
    }
    Ok(size >> 20)
}
