use std::sync::OnceLock;
use sysinfo::System;

use clap::Parser;

// Write once, safe read across threads
pub static ARGS: OnceLock<Args> = OnceLock::new();

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Path to input PBF
    #[arg(short, long)]
    pub input: String,

    /// Path to output directory
    #[arg(short, long, default_value = "./parquet")]
    pub output: String,

    /// Zstd compression level, 1-22, 0 for no compression
    #[arg(long, default_value = "3")]
    pub compression: u8,

    /// Override target record batch size, balance this with available memory
    /// default is total memory / CPU count / 4
    #[arg(long)]
    pub record_batch_target_bytes: Option<usize>,

    /// Max feature count per row group
    #[arg(long)]
    pub max_row_group_size: Option<usize>,
}

impl Args {
    pub fn new(input: String, output: String, compression: u8) -> Self {
        Args {
            input,
            output,
            compression,
            record_batch_target_bytes: None,
            max_row_group_size: None,
        }
    }
}

pub fn default_record_batch_size() -> usize {
    let system = System::new_all();
    // Estimate per thread available memory, leaving overhead for copies and system processes
    return (system.total_memory() as usize / system.cpus().len()) / 4usize;
}
