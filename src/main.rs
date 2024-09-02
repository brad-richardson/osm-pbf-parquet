use clap::Parser;

use osm_pbf_parquet::driver;
use osm_pbf_parquet::util::{Args, default_worker_thread_count};

fn main() {
    let args = Args::parse();
    println!("{:?}", args);
    
    let worker_threads = args.worker_threads.unwrap_or(default_worker_thread_count());

    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let _ = driver(args).await;
        });
}
