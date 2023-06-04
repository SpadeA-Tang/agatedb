use bytes::Bytes;

use crate::value::{ValuePointer, VALUE_DELETE};

#[derive(Clone)]
pub struct Entry {
    pub key: Bytes,
    pub value: Bytes,
    pub(crate) meta: u8,
    pub user_meta: u8,
    pub expires_at: u64,
    pub(crate) version: u64,
    pub(crate) offset: u32,
}

pub struct EntryRef<'a> {
    pub key: &'a [u8],
    pub value: &'a [u8],
    pub(crate) meta: u8,
    pub user_meta: u8,
    pub expires_at: u64,
    pub(crate) version: u64,
    pub(crate) offset: u32,
}

impl<'a> EntryRef<'a> {
    pub fn is_zero(&self) -> bool {
        self.key.is_empty()
    }
}

impl Entry {
    pub fn new(key: Bytes, value: Bytes) -> Entry {
        Entry {
            key,
            value,
            meta: 0,
            user_meta: 0,
            expires_at: 0,
            version: 0,
            offset: 0,
        }
    }

    pub fn mark_delete(&mut self) {
        self.meta |= VALUE_DELETE;
    }

    pub fn estimate_size(&self, threshold: usize) -> usize {
        // The estimated size of an entry will be key length + value length +
        // two bytes of metadata.
        const METADATA_SIZE: usize = std::mem::size_of::<u8>() * 2;
        if self.value.len() < threshold {
            // For those values < threshold, key and value will be directly stored in LSM tree.
            self.key.len() + self.value.len() + METADATA_SIZE
        } else {
            // For those values >= threshold, only key will be stored in LSM tree.
            self.key.len() + ValuePointer::encoded_size() + METADATA_SIZE
        }
    }

    // TODO: entry encoding will be done later, as current WAL encodes header and key / value separately
    /*
    pub fn encoded_len(&self) -> usize {
        let kl = self.key.len();
        let vl = self.value.len();
        kl + vl + varint_len(kl) + varint_len(vl) + 1
    }

    pub fn encode(&self, bytes: &mut BytesMut) {
        let encoded_len = self.encoded_len();
        bytes.reserve(encoded_len);
        unsafe {
            let buf = bytes.bytes_mut();
            *(*buf.get_unchecked_mut(0)).as_mut_ptr() = self.meta;
            let mut read = 1;
            read += encode_varint_uncheck(buf.get_unchecked_mut(read..), self.key.len() as u64);
            read += encode_varint_uncheck(buf.get_unchecked_mut(read..), self.value.len() as u64);
            ptr::copy_nonoverlapping(
                self.key.as_ptr(),
                buf.as_mut_ptr().add(read) as _,
                self.key.len(),
            );
            ptr::copy_nonoverlapping(
                self.value.as_ptr(),
                buf.as_mut_ptr().add(read + self.key.len()) as _,
                self.value.len(),
            );
        }
        unsafe {
            bytes.advance_mut(encoded_len);
        }
    }*/
}

#[cfg(test)]
mod test {
    use tempfile::tempdir;

    use crate::{
        value::{Request, VALUE_POINTER},
        value_log::ValueLog,
        AgateOptions,
    };

    use super::*;

    #[test]
    fn test_entry_offset() {
        let mut opts = AgateOptions::default();
        let tmp_dir = tempdir().unwrap();
        opts.value_dir = tmp_dir.path().to_path_buf();
        opts.value_threshold = 32;
        opts.value_log_file_size = 1024;
        let vlog = ValueLog::new(opts.clone()).unwrap().unwrap();

        let val1 = b"sampleval012345678901234567890123";
        let val2 = b"samplevalb012345678901234567890123";
        let val3 = b"samplevalb01234567890123456789012345";
        let val4 = b"samplevalb012345678321123124141241213123";

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
        let mut e3 = Entry::new(
            Bytes::from_static(b"samplekeyc"),
            Bytes::copy_from_slice(val3),
        );
        e3.meta = VALUE_POINTER;
        let mut e4 = Entry::new(
            Bytes::from_static(b"samplekeyd"),
            Bytes::copy_from_slice(val4),
        );
        e4.meta = VALUE_POINTER;

        let mut reqs = vec![Request {
            entries: vec![e1, e2, e3, e4],
            ptrs: vec![],
            done: None,
        }];
        vlog.write(&mut reqs).unwrap();

        let log_file = vlog.get_log_file(1).unwrap();
        let log_file = log_file.read().unwrap();
        let mut iter = log_file.iter().unwrap();

        let mut i = 0;
        while let Ok(Some(e)) = iter.next() {
            assert_eq!(e.offset, reqs[0].ptrs[i].offset);
            i += 1;
        }
    }
}
