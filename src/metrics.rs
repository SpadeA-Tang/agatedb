use lazy_static::lazy_static;
use prometheus::{exponential_buckets, register_histogram, Histogram};

lazy_static! {
    pub static ref DB_WRITE_DURATION_HISTOGRAM: Histogram =
        register_histogram!("xxx", "xxx", exponential_buckets(0.00001, 2.0, 26).unwrap()).unwrap();
}
