use std::io;

use clap::Parser;

use osm_pbf_parquet::driver;
use osm_pbf_parquet::util::Args;

fn main() -> Result<(), io::Error> {
    let args = Args::parse();
    println!("{:?}", args);
    let _ = driver(args);
    Ok(())
}
