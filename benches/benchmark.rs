use criterion::{criterion_group, criterion_main, Criterion};
use osm_pbf_parquet::{driver, Args};
use std::fs;

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("benchmark", |b| b.iter(|| {
        let args = Args {
            input: "./test/el-salvador-latest.osm.pbf".to_string(),
            output: "./test/bench-out/".to_string(), // Will just overwrite files on each run
        };
        let _ = driver(args);
    }));
    let _ = fs::remove_dir_all("./test/bench-out/");
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = criterion_benchmark
}
criterion_main!(benches);
