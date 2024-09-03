use std::sync::atomic::AtomicU64;
use std::sync::OnceLock;
use sysinfo::System;

use clap::Parser;

// Write once, safe read across threads
pub static ARGS: OnceLock<Args> = OnceLock::new();

// Element counter to track read progress
pub static ELEMENT_COUNTER: AtomicU64 = AtomicU64::new(0);

// Max recommended size of an uncompressed single blob is 16MB, assumes compression ratio of 2:1 or better
pub const DEFAULT_BUF_READER_SIZE: usize = 1024 * 1024 * 8;

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Path to input PBF
    #[arg(short, long)]
    pub input: String,

    /// Path to output directory
    /// S3 URIs and filesystem paths are supported
    #[arg(short, long, default_value = "./parquet")]
    pub output: String,

    /// Zstd compression level, 1-22, 0 for no compression
    #[arg(long, default_value_t = 3)]
    pub compression: u8,

    /// Worker thread count, default CPU count
    #[arg(long)]
    pub worker_threads: Option<usize>,

    /// Override target record batch size, balance this with available memory
    /// default is total memory (MB) / CPU count / 8
    #[arg(long)]
    pub record_batch_target_mb: Option<usize>,

    /// Max feature count per row group
    #[arg(long)]
    pub max_row_group_count: Option<usize>,

    /// Override target parquet file size
    #[arg(long, default_value_t = 500usize)]
    pub file_target_mb: usize,
}

impl Args {
    pub fn new(input: String, output: String, compression: u8) -> Self {
        Args {
            input,
            output,
            compression,
            worker_threads: None,
            record_batch_target_mb: None,
            max_row_group_count: None,
            file_target_mb: 500usize,
        }
    }
}

pub fn default_record_batch_size_mb() -> usize {
    let system = System::new_all();
    // Estimate per thread available memory, leaving overhead for copies and system processes
    return ((system.total_memory() as usize / 1_000_000usize) / system.cpus().len()) / 8usize;
}

pub fn default_worker_thread_count() -> usize {
    let system = System::new_all();
    return system.cpus().len();
}
