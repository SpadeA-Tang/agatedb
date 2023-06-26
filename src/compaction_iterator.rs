use crate::{format::user_key, iterator::is_deleted_or_expired, AgateIterator, Value};

pub struct CompactionIterator<'a, I: AgateIterator> {
    valid: bool,
    input: I,

    key: &'a [u8],
    current_user_key: Option<&'a [u8]>,
    value: Value,

    current_user_key_sequence: u64,
    current_user_key_snapshot: u64,

    iter_stats: CompactionIterationStats,
}

impl<'a, I: AgateIterator> CompactionIterator<'a, I> {
    fn next_from_input(&'a mut self) {
        while !self.valid {
            self.key = self.input.key();
            self.value = self.input.value();
            if is_deleted_or_expired(self.value.meta, self.value.expires_at) {
                self.iter_stats.num_input_deletion_records += 1;
            }
            self.iter_stats.total_input_raw_key_bytes += self.key.len() as u64;
            self.iter_stats.total_input_raw_value_bytes += self.value.encoded_size() as u64;

            // If need_skip is true, we should seek the input iterator
            // to internal key skip_until and continue from there.
            let mut need_skip = false;

            let user_key = user_key(self.key);
            let mut user_key_equal_without_ts = false;
            if let Some(current_user_key) = self.current_user_key {
                user_key_equal_without_ts = current_user_key == user_key
            }

            if self.current_user_key.is_none() || !user_key_equal_without_ts {
                self.current_user_key_sequence = u64::MAX;
                self.current_user_key_snapshot = 0;
                self.current_user_key = Some(user_key);
            }
        }
    }
}

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
