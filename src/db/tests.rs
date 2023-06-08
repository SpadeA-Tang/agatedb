use std::{fs::read_dir, os::unix::prelude::MetadataExt, path::Path};

use bytes::{Bytes, BytesMut};
use rand::distributions::{Alphanumeric, DistString};
use tempdir::TempDir;
use tempfile::tempdir;

use super::*;
use crate::{
    entry::Entry,
    format::{append_ts, key_with_ts},
    test_tuil::{dir_size, txn_delete, txn_set},
    IteratorOptions,
};

#[test]
fn test_open_mem_tables() {
    let mut opts = AgateOptions::default();
    let tmp_dir = tempdir().unwrap();
    opts.dir = tmp_dir.path().to_path_buf();

    let (_imm_tables, next_mem_fid) = Core::open_mem_tables(&opts).unwrap();
    assert_eq!(next_mem_fid, 1);
    let _mt = Core::open_mem_table(&opts, next_mem_fid).unwrap();
}

#[test]
fn test_memtable_persist() {
    let mut opts = AgateOptions::default();
    let tmp_dir = tempdir().unwrap();
    opts.dir = tmp_dir.path().to_path_buf();

    let mt = Core::open_mem_table(&opts, 1).unwrap();

    let mut key = BytesMut::from("key".to_string().as_bytes());
    append_ts(&mut key, 100);
    let key = key.freeze();
    let value = Value::new(key.clone());

    mt.put(key.clone(), value.clone()).unwrap();

    let value_get = mt.skl.get(&key).unwrap();
    assert_eq!(&Bytes::from(value.clone()), value_get);

    mt.mark_save();

    let mt = Core::open_mem_table(&opts, 1).unwrap();
    let value_get = mt.skl.get(&key).unwrap();
    assert_eq!(&Bytes::from(value), value_get);
}

#[test]
fn test_ensure_room_for_write() {
    let mut opts = AgateOptions::default();
    let tmp_dir = tempdir().unwrap();
    opts.dir = tmp_dir.path().to_path_buf();
    opts.value_dir = opts.dir.clone();

    // Wal::zero_next_entry will need MAX_HEADER_SIZE bytes free space.
    // So we should put bytes more than value_log_file_size but less than
    // 2*value_log_file_size - MAX_HEADER_SIZE.
    opts.value_log_file_size = 25;

    let core = Core::new(&opts).unwrap();

    {
        let mts = core.mts.read().unwrap();
        assert_eq!(mts.nums_of_memtable(), 1);

        let mt = mts.mut_table();

        let key = key_with_ts(BytesMut::new(), 1);
        let value = Value::new(Bytes::new());
        // Put once, write_at in wal += 13, so we put twice to make write_at larger
        // than value_log_file_size.
        mt.put(key.clone(), value.clone()).unwrap();
        mt.put(key, value).unwrap();
    }

    core.ensure_room_for_write().unwrap();

    let mts = core.mts.read().unwrap();
    assert_eq!(mts.nums_of_memtable(), 2);
}

pub fn generate_test_agate_options() -> AgateOptions {
    AgateOptions {
        mem_table_size: 1 << 14,
        // Force more compaction.
        base_table_size: 1 << 15,
        // Set base level size small enought to make the compactor flush L0 to L5 and L6.
        base_level_size: 4 << 10,
        value_log_file_size: 4 << 20,
        ..Default::default()
    }
}

pub fn helper_dump_dir(path: &Path) {
    let mut result = vec![];
    for entry in fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_file() {
            result.push(path);
        }
    }
    result.sort();

    for path in result {
        println!("{:?}", path);
    }
}

pub fn with_payload(mut buf: BytesMut, payload: usize, fill_char: u8) -> Bytes {
    let mut payload_buf = vec![];
    payload_buf.resize(payload, fill_char);
    buf.extend_from_slice(&payload_buf);
    buf.freeze()
}

pub fn run_agate_test<F>(opts: Option<AgateOptions>, test_fn: F)
where
    F: FnOnce(Arc<Agate>),
{
    let tmp_dir = TempDir::new("agatedb").unwrap();

    let mut opts = if let Some(opts) = opts {
        opts
    } else {
        AgateOptions::default()
    };

    if !opts.in_memory {
        opts.dir = tmp_dir.as_ref().to_path_buf();
        opts.value_dir = tmp_dir.as_ref().to_path_buf();
    }

    let agate = Arc::new(opts.open().unwrap());

    test_fn(agate);

    helper_dump_dir(tmp_dir.path());
    tmp_dir.close().unwrap();
}

#[test]
fn test_simple_get_put() {
    run_agate_test(None, |agate| {
        let key = key_with_ts(BytesMut::from("2333"), 0);
        let value = Bytes::from("2333333333333333");
        let req = Request {
            entries: vec![Entry::new(key.clone(), value)],
            ptrs: vec![],
            done: None,
        };
        agate.write_to_lsm(req).unwrap();
        let value = agate.get(&key).unwrap();
        assert_eq!(value.value, Bytes::from("2333333333333333"));
    });
}

fn generate_requests(n: usize) -> Vec<Request> {
    (0..n)
        .map(|i| Request {
            entries: vec![Entry::new(
                key_with_ts(BytesMut::from(format!("{:08x}", i).as_str()), 0),
                Bytes::from(i.to_string()),
            )],
            ptrs: vec![],
            done: None,
        })
        .collect()
}

fn verify_requests(n: usize, agate: &Agate) {
    for i in 0..n {
        let value = agate
            .get(&key_with_ts(
                BytesMut::from(format!("{:08x}", i).as_str()),
                0,
            ))
            .unwrap();
        assert_eq!(value.value, i.to_string());
    }
}

#[test]
fn test_flush_memtable() {
    run_agate_test(None, |agate| {
        agate.write_requests(generate_requests(2000)).unwrap();
        verify_requests(2000, &agate);
    });
}

#[test]
fn test_in_memory_agate() {
    run_agate_test(
        Some(AgateOptions {
            in_memory: true,
            ..Default::default()
        }),
        |agate| {
            agate.write_requests(generate_requests(10)).unwrap();
            verify_requests(10, &agate);
        },
    );
}

#[test]
fn test_flush_l1() {
    run_agate_test(None, |agate| {
        let requests = generate_requests(10000);
        for request in requests.chunks(100) {
            agate.write_requests(request.to_vec()).unwrap();
        }
        verify_requests(10000, &agate);
    });
}

#[test]
fn test_value_gc() {
    let dir = tempdir::TempDir::new("gc-test").unwrap();
    let mut opts = AgateOptions::default_for_test(dir.path());
    opts.value_log_file_size = 1 << 20;
    opts.base_table_size = 1 << 15;
    opts.value_threshold = 1 << 10;

    let db = opts.open().unwrap();

    let sz = 32 << 10;
    let mut txn = db.new_transaction(true);
    for i in 0..100 {
        let val = Alphanumeric.sample_string(&mut rand::thread_rng(), sz);
        txn.set_entry(Entry::new(
            Bytes::from(format!("key{:03}", i)),
            Bytes::from(val),
        ))
        .unwrap();
        if i % 20 == 0 {
            txn.commit().unwrap();
            txn = db.new_transaction(true);
        }
    }
    txn.commit().unwrap();

    for i in 0..45 {
        txn_delete(&db, Bytes::from(format!("key{:03}", i)));
    }

    let log_file_path = {
        let log_file = db.core.value_log().get_log_file(1).unwrap();
        let log_file_gaurd = log_file.read().unwrap();
        let path = log_file_gaurd.file_path().to_owned();

        db.core.rewrite(&log_file_gaurd).unwrap();
        path
    };
    assert!(!log_file_path.exists());

    for i in 45..100 {
        let key = Bytes::from(format!("key{:03}", i));
        db.view(|txn| {
            let item = txn.get(&key).unwrap();
            let val = item.value();
            assert_eq!(val.len(), sz);
            Ok(())
        })
        .unwrap();
    }
}

#[test]
fn test_value_gc2() {
    let dir = tempdir::TempDir::new("gc-test").unwrap();
    let mut opts = AgateOptions::default_for_test(dir.path());
    opts.value_log_file_size = 1 << 20;
    opts.base_table_size = 1 << 15;
    opts.value_threshold = 1 << 10;

    let db = opts.open().unwrap();

    let sz = 32 << 10;
    let mut txn = db.new_transaction(true);
    for i in 0..100 {
        let val = Alphanumeric.sample_string(&mut rand::thread_rng(), sz);
        txn.set_entry(Entry::new(
            Bytes::from(format!("key{:03}", i)),
            Bytes::from(val),
        ))
        .unwrap();
        if i % 20 == 0 {
            txn.commit().unwrap();
            txn = db.new_transaction(true);
        }
    }
    txn.commit().unwrap();

    for i in 0..5 {
        txn_delete(&db, Bytes::from(format!("key{:03}", i)));
    }

    for i in 5..10 {
        txn_set(
            &db,
            Bytes::from(format!("key{:03}", i)),
            Bytes::from(format!("value{:03}", i)),
            0,
        );
    }

    let log_file_path = {
        let log_file = db.core.value_log().get_log_file(1).unwrap();
        let log_file_gaurd = log_file.read().unwrap();
        let path = log_file_gaurd.file_path().to_owned();

        db.core.rewrite(&log_file_gaurd).unwrap();
        path
    };
    assert!(!log_file_path.exists());

    for i in 0..5 {
        let key = Bytes::from(format!("key{:03}", i));
        db.view(|txn| {
            match txn.get(&key) {
                Err(Error::KeyNotFound(_)) => {}
                _ => unreachable!(),
            }
            Ok(())
        })
        .unwrap();
    }

    for i in 5..10 {
        let key = Bytes::from(format!("key{:03}", i));
        db.view(|txn| {
            let item = txn.get(&key).unwrap();
            let val = item.value();
            assert_eq!(val, Bytes::from(format!("value{:03}", i)));
            Ok(())
        })
        .unwrap();
    }

    // Moved entries.
    for i in 10..100 {
        let key = Bytes::from(format!("key{:03}", i));
        db.view(|txn| {
            let item = txn.get(&key).unwrap();
            let val = item.value();
            assert_eq!(val.len(), sz);
            Ok(())
        })
        .unwrap();
    }
}

#[test]
fn test_value_gc3() {
    let dir = tempdir::TempDir::new("gc-test").unwrap();
    let mut opts = AgateOptions::default_for_test(dir.path());
    opts.value_log_file_size = 1 << 20;
    opts.base_table_size = 1 << 15;
    opts.value_threshold = 1 << 10;

    let db = opts.open().unwrap();

    let sz = 32 << 10;
    let mut txn = db.new_transaction(true);
    let mut value3 = None;
    for i in 0..100 {
        let val = Alphanumeric.sample_string(&mut rand::thread_rng(), sz);
        if i == 3 {
            value3 = Some(val.clone());
        }
        txn.set_entry(Entry::new(
            Bytes::from(format!("key{:03}", i)),
            Bytes::from(val),
        ))
        .unwrap();
        if i % 20 == 0 {
            txn.commit().unwrap();
            txn = db.new_transaction(true);
        }
    }
    txn.commit().unwrap();

    let it_opts = IteratorOptions {
        prefetch_values: false,
        prefetch_size: 0,
        reverse: false,
        ..Default::default()
    };
    let txn = db.new_transaction(true);
    let mut iter = txn.new_iterator(&it_opts);

    iter.rewind();
    assert!(iter.valid());
    let mut item = iter.item();
    assert_eq!(Bytes::from("key000"), item.key);

    iter.next();
    assert!(iter.valid());
    item = iter.item();
    assert_eq!(Bytes::from("key001"), item.key);

    iter.next();
    assert!(iter.valid());
    item = iter.item();
    assert_eq!(Bytes::from("key002"), item.key);

    // Like other tests, we pull out a logFile to rewrite it directly

    let log_file_path = {
        let log_file = db.core.value_log().get_log_file(1).unwrap();
        let log_file_gaurd = log_file.read().unwrap();
        let path = log_file_gaurd.file_path().to_owned();

        db.core.rewrite(&log_file_gaurd).unwrap();
        path
    };
    // the log file shoud exist as txn iterator is not released
    assert!(log_file_path.exists());

    iter.next();
    assert!(iter.valid());
    item = iter.item();
    assert_eq!(Bytes::from("key003"), item.key);
    let val = item.value();
    assert_eq!(val, value3.unwrap());

    drop(iter);
    // now the log file shoud be deleted
    assert!(!log_file_path.exists());
}

#[test]
fn test_value_gc4() {
    let dir = tempdir::TempDir::new("gc-test").unwrap();
    let mut opts = AgateOptions::default_for_test(dir.path());
    opts.value_log_file_size = 1 << 20;
    opts.base_table_size = 1 << 15;
    opts.value_threshold = 1 << 10;

    let db = opts.open().unwrap();

    let sz = 128 << 10;
    let mut txn = db.new_transaction(true);
    for i in 0..24 {
        let val = Alphanumeric.sample_string(&mut rand::thread_rng(), sz);
        txn.set_entry(Entry::new(
            Bytes::from(format!("key{:03}", i)),
            Bytes::from(val),
        ))
        .unwrap();
        if i % 3 == 0 {
            txn.commit().unwrap();
            txn = db.new_transaction(true);
        }
    }
    txn.commit().unwrap();

    for i in 0..8 {
        txn_delete(&db, Bytes::from(format!("key{:03}", i)));
    }

    for i in 8..16 {
        txn_set(
            &db,
            Bytes::from(format!("key{:03}", i)),
            Bytes::from(format!("value{:03}", i)),
            0,
        );
    }

    let (log_file_path, log_file_path2) = {
        let log_file = db.core.value_log().get_log_file(1).unwrap();
        let log_file_gaurd = log_file.read().unwrap();
        let path = log_file_gaurd.file_path().to_owned();
        db.core.rewrite(&log_file_gaurd).unwrap();

        let log_file = db.core.value_log().get_log_file(2).unwrap();
        let log_file_gaurd = log_file.read().unwrap();
        let path2 = log_file_gaurd.file_path().to_owned();
        db.core.rewrite(&log_file_gaurd).unwrap();

        (path, path2)
    };
    assert!(!log_file_path.exists());
    assert!(!log_file_path2.exists());
    drop(db);

    let db = opts.open().unwrap();
    for i in 0..8 {
        let key = Bytes::from(format!("key{:03}", i));
        db.view(|txn| {
            match txn.get(&key) {
                Err(Error::KeyNotFound(_)) => {}
                _ => unreachable!(),
            }
            Ok(())
        })
        .unwrap();
    }
    for i in 8..16 {
        let key = Bytes::from(format!("key{:03}", i));
        db.view(|txn| {
            let item = txn.get(&key).unwrap();
            let val = item.value();
            assert_eq!(val, Bytes::from(format!("value{:03}", i)));
            Ok(())
        })
        .unwrap();
    }
}

#[test]
fn test_value_gc_managed() {
    let dir = tempdir::TempDir::new("agate-test").unwrap();
    let n = 10000;

    let mut opts = AgateOptions::default_for_test(dir.path());
    opts.value_log_max_entries = n / 10;
    opts.managed_txns = true;
    opts.base_table_size = 1 << 15;
    opts.value_threshold = 1 << 10;
    opts.mem_table_size = 1 << 15;

    let mut db = opts.open().unwrap();
    let mut ts = 0;
    let mut new_ts = || -> u64 {
        ts += 1;
        ts
    };

    let sz = 64 << 10;
    for i in 0..n {
        let value = Bytes::from(Alphanumeric.sample_string(&mut rand::thread_rng(), sz));
        let mut txn = db.new_transaction_at(new_ts(), true);
        txn.set_entry(Entry::new(Bytes::from(format!("key{:03}", i)), value))
            .unwrap();
        txn.commit_at(new_ts()).unwrap();
    }

    for i in 0..n {
        let mut txn = db.new_transaction_at(new_ts(), true);
        txn.delete(Bytes::from(format!("key{:03}", i))).unwrap();
        txn.commit_at(new_ts()).unwrap();
    }

    let mut entries = read_dir(dir.path()).unwrap();
    while let Some(Ok(e)) = entries.next() {
        let meta = e.metadata().unwrap();
        info!(
            "File {:?}. Size {:?}.",
            e.path(),
            human_bytes(meta.size() as f64)
        );
    }

    db.set_discard_ts(u32::MAX as u64);
    db.flatten(3).unwrap();

    info!("After flatten");
    let mut entries = read_dir(dir.path()).unwrap();
    while let Some(Ok(e)) = entries.next() {
        let meta = e.metadata().unwrap();
        info!(
            "File {:?}. Size {:?}.",
            e.path(),
            human_bytes(meta.size() as f64)
        );
    }

    for _ in 0..100 {
        // Try at max 100 times to GC even a single value log file.
        if let Err(_) = db.run_value_log_gc(0.0001) {
            break;
        }
    }

    info!("After gc");
    let mut entries = read_dir(dir.path()).unwrap();
    // now, we should only have one .sst and one .vlog file.
    let mut sst_count = 0;
    let mut vlog_count = 0;
    while let Some(Ok(e)) = entries.next() {
        if e.file_name().to_str().unwrap().ends_with(".sst") {
            sst_count += 1;
        }
        if e.file_name().to_str().unwrap().ends_with(".vlog") {
            vlog_count += 1;
        }
        let meta = e.metadata().unwrap();
        info!(
            "File {:?}. Size {:?}.",
            e.path(),
            human_bytes(meta.size() as f64)
        );
    }

    assert_eq!(sst_count, 1);
    assert_eq!(vlog_count, 1);
}

#[test]
fn test_db_growth() {
    let dir = tempdir::TempDir::new("agate-test").unwrap();
    let path_str = dir.path().to_str().unwrap();

    let mut start = 0;
    let mut last_start = 0;
    let num_keys = 2000;
    let value_size = 1024;
    let value = Bytes::from(Alphanumeric.sample_string(&mut rand::thread_rng(), value_size));

    let discard_ratio = 0.001;
    let max_writes = 200;
    let mut opts = AgateOptions::default_for_test(dir.path());
    opts.value_log_file_size = 64 << 15;
    opts.base_table_size = 4 << 15;
    opts.base_level_size = 16 << 15;
    opts.num_versions_to_keep = 1;
    opts.num_level_zero_tables = 1;
    opts.num_level_zero_tables_stall = 2;
    let mut db = opts.open().unwrap();
    for _ in 0..max_writes {
        let mut txn = db.new_transaction(true);
        if start > 0 {
            for i in last_start..start {
                let key = Bytes::from(format!("{:?}", i));
                match txn.delete(key) {
                    Err(Error::TxnTooBig) => {
                        txn.commit().unwrap();
                        txn = db.new_transaction(true);
                    }
                    Err(_) => unreachable!(),
                    _ => {}
                }
            }
        }

        for i in start..start + num_keys {
            let key = Bytes::from(format!("{:?}", i));
            match txn.set_entry(Entry::new(key, value.clone())) {
                Err(Error::TxnTooBig) => {
                    txn.commit().unwrap();
                    txn = db.new_transaction(true);
                }
                Err(_) => unreachable!(),
                _ => {}
            }
        }
        txn.commit().unwrap();
        db.flatten(1).unwrap();

        loop {
            match db.run_value_log_gc(discard_ratio) {
                Err(Error::ErrNoRewrite) => {
                    break;
                }
                Err(e) => panic!("meet error {:?}", e),
                _ => {}
            }
        }

        let size = dir_size(path_str).unwrap();
        println!("Agate DB Size = {}MB", size);
        last_start = start;
        start += num_keys;
    }

    drop(db);
    let size = dir_size(path_str).unwrap();
    assert!(size < 70);
    println!("Agate DB Size = {}MB", size);
}
