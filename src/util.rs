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

    /// Override target row group est size, balance this with available memory
    /// default is total memory / CPU count / 4
    #[arg(long)]
    pub row_group_target_bytes: Option<usize>,

    /// Max feature count per row group
    #[arg(long)]
    pub row_group_max_feature_count: Option<u64>,
}

pub fn default_row_group_size() -> usize {
    let system = System::new_all();
    // Estimate per thread available memory, leaving overhead for copies and system processes
    return (system.total_memory() as usize / system.cpus().len()) / 4usize;
}
