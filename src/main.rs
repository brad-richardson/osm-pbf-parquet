use std::io;
use tokio;

use clap::Parser;

use osm_pbf_parquet::driver;
use osm_pbf_parquet::util::Args;

// #[tokio::main]
fn main() {
    let args = Args::parse();
    println!("{:?}", args);
    // rt.block_on(
    let _ = driver(args);
}
