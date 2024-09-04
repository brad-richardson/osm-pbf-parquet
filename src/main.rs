use clap::Parser;
use env_logger::{Builder, Env};

use osm_pbf_parquet::driver;
use osm_pbf_parquet::util::{default_worker_thread_count, Args};

fn main() {
    Builder::from_env(Env::default().default_filter_or("info")).init();

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
