use std::io;

use clap::Parser;

use osm_pbf_parquet::{Args, driver};

fn main() -> Result<(), io::Error> {
    let args = Args::parse();
    println!("{:?}", args);
    let _ = driver(args);
    Ok(())
}
