use std::io;
use tokio;

use clap::Parser;

use osm_pbf_parquet::driver;
use osm_pbf_parquet::util::Args;

#[tokio::main]
async fn main() {
    let args = Args::parse();
    println!("{:?}", args);
    driver(args).await.unwrap();
}
