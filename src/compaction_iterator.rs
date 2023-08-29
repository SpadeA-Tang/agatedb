use bytes::Bytes;
use indexing::algorithms::lower_bound;
use log::info;

use crate::{format::user_key, get_ts, iterator::is_deleted_or_expired, AgateIterator, Value};

const MAX_SEQUENCE_NUMBER: u64 = u64::MAX;

pub struct CompactionIterator<I: AgateIterator> {
    valid: bool,
    input: I,

    key: Bytes,
    current_user_key: Option<Bytes>,
    value: Value,

    current_user_key_sequence: u64,
    current_user_key_snapshot: u64,
    snapshots: Vec<u64>,

    iter_stats: CompactionIterationStats,
}

impl<I: AgateIterator> CompactionIterator<I> {
    pub fn new(mut iter: I, exist_snapshots: Vec<u64>) -> Self {
        iter.rewind();
        CompactionIterator {
            valid: false,
            input: iter,
            iter_stats: CompactionIterationStats::default(),
            key: Bytes::default(),
            current_user_key: None,
            value: Value::default(),
            current_user_key_sequence: 0,
            current_user_key_snapshot: 0,
            snapshots: exist_snapshots,
        }
    }

    pub fn valid(&self) -> bool {
        self.valid
    }

    pub fn key(&self) -> &Bytes {
        &self.key
    }

    pub fn value(&self) -> &Value {
        &self.value
    }

    fn next_from_input(&mut self) {
        self.valid = false;
        while !self.valid && self.input.valid() {
            self.key = Bytes::from(self.input.key().to_vec());
            // println!("key: {:?}", self.key);
            self.value = self.input.value();
            if is_deleted_or_expired(self.value.meta, self.value.expires_at) {
                self.iter_stats.num_input_deletion_records += 1;
            }
            self.iter_stats.total_input_raw_key_bytes += self.key.len() as u64;
            self.iter_stats.total_input_raw_value_bytes += self.value.encoded_size() as u64;

            // If need_skip is true, we should seek the input iterator
            // to internal key skip_until and continue from there.
            let _need_skip = false;

            let user_key = user_key(self.key.as_ref());
            // println!("user key: {:?}", String::from_utf8(user_key.to_vec()));
            let mut user_key_equal_without_ts = false;
            if let Some(ref current_user_key) = self.current_user_key {
                user_key_equal_without_ts = current_user_key.as_ref() == user_key
            }

            if self.current_user_key.is_none() || !user_key_equal_without_ts {
                self.current_user_key_sequence = MAX_SEQUENCE_NUMBER;
                self.current_user_key_snapshot = 0;
                self.current_user_key = Some(Bytes::from(user_key.to_vec()));
            }

            let last_sequence = self.current_user_key_sequence;
            self.current_user_key_sequence = get_ts(self.key.as_ref());
            let last_snapshot = self.current_user_key_snapshot;
            let (_prev_snapshot, current_user_key_snaphsot) =
                self.find_earlist_visible_snaphsot(self.current_user_key_sequence);
            self.current_user_key_snapshot = current_user_key_snaphsot;

            if _need_skip {
            } else if last_snapshot == self.current_user_key_snapshot
                || (last_snapshot > 0 && last_snapshot < self.current_user_key_snapshot)
            {
                // If the earliest snapshot is which this key is visible in
                // is the same as the visibility of a previous instance of the
                // same key, then this kv is not visible in any snapshot.
                // Hidden by an newer entry for same user key
                //
                // Note: Dropping this key will not affect TransactionDB write-conflict
                // checking since there has already been a record returned for this key
                // in this snapshot.
                assert!(last_sequence >= self.current_user_key_sequence);
                if last_sequence < self.current_user_key_sequence {
                    panic!("");
                }

                self.iter_stats.num_record_drop_hidden += 1;
                self.input.next();
            } else {
                self.valid = true;
            }
        }
    }

    fn seek_to_first(&mut self) {
        self.next_from_input();
        self.prepare_output();
    }

    fn next(&mut self) {
        self.input.next();
        self.next_from_input();
        self.prepare_output();
    }

    fn prepare_output(&mut self) {
        if self.valid {
            // todo
        }
    }

    fn find_earlist_visible_snaphsot(&self, sequence: u64) -> (u64, u64) {
        if self.snapshots.is_empty() {
            info!("No snapshot left in findEarliestVisibleSnapshot");
        }
        let mut prev_snapshot = 0;
        let i = lower_bound(&self.snapshots, &sequence);
        if i != 0 {
            prev_snapshot = self.snapshots[i - 1];
            assert!(prev_snapshot < sequence);
        }
        if i != self.snapshots.len() {
            (prev_snapshot, self.snapshots[i])
        } else {
            (prev_snapshot, MAX_SEQUENCE_NUMBER)
        }
    }
}

#[derive(Default, Debug)]
pub struct CompactionIterationStats {
    // Compaction statistics

    // Doesn't include records skipped because of
    // CompactionFilter::Decision::kRemoveAndSkipUntil.
    num_record_drop_user: u64,

    num_record_drop_hidden: u64,
    num_record_drop_obsolete: u64,
    num_record_drop_range_del: u64,
    num_range_del_drop_obsolete: u64,
    // Deletions obsoleted before bottom level due to file gap optimization.
    num_optimized_del_drop_obsolete: u64,
    total_filter_time: u64,

    // Input statistics
    // TODO(noetzli): The stats are incomplete. They are lacking everything
    // consumed by MergeHelper.
    num_input_records: u64,
    num_input_deletion_records: u64,
    num_input_corrupt_records: u64,
    total_input_raw_key_bytes: u64,
    total_input_raw_value_bytes: u64,

    // Single-Delete diagnostics for exceptional situations
    num_single_del_fallthru: u64,
    num_single_del_mismatch: u64,

    // Blob related statistics
    num_blobs_read: u64,
    total_blob_bytes_read: u64,
    num_blobs_relocated: u64,
    total_blob_bytes_relocated: u64,
}

#[cfg(test)]
mod test {
    use bytes::{Bytes, BytesMut};
    use skiplist::{FixedLengthSuffixComparator, Skiplist};

    use crate::{iterator::SkiplistIterator, key_with_ts, value::VALUE_DELETE};

    use super::*;

    const ARENA_SIZE: usize = 1 << 20;

    #[test]
    fn test_compaction_iter() {
        let test_iter = |allow_concurrent_write, snapshots, expect_count, delete_others| {
            let comp = FixedLengthSuffixComparator::new(8);
            let list = Skiplist::with_capacity(comp, ARENA_SIZE, allow_concurrent_write);
            for i in 0..10 {
                for t in 1..=5 {
                    let mut key = BytesMut::default();
                    key.extend_from_slice(format!("key{:05}", i * 10 + 5).as_bytes());
                    let key = key_with_ts(key, t);
                    let mut val = Value::default();
                    let value = Bytes::from(format!("{:05}", i));
                    val.value = value;
                    val.version = t;

                    if delete_others && t != 5 {
                        val.meta |= VALUE_DELETE;
                    }

                    let mut enc_val = BytesMut::default();
                    val.encode(&mut enc_val);
                    list.put(key, enc_val);
                }
            }
            let iter = SkiplistIterator::new(list.iter(), false);
            let mut c_iter = CompactionIterator::new(iter, snapshots);

            c_iter.seek_to_first();
            let mut count = 0;
            while c_iter.valid() {
                count += 1;
                c_iter.next();
            }
            assert_eq!(count, expect_count);
            assert_eq!(
                c_iter.iter_stats.num_record_drop_hidden,
                50 - expect_count as u64
            );

            if delete_others {
                assert_eq!(c_iter.iter_stats.num_input_deletion_records, 40);
            }
        };

        test_iter(false, vec![], 10, true);
        test_iter(true, vec![], 10, false);
        test_iter(false, vec![2, 4], 30, true);
        test_iter(false, vec![2, 4], 30, false);
    }
}
