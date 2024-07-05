## About
Transcode OSM PBF file to parquet files with hive-style partitioning by type:
```
planet.osm.pbf
parquet/
  type=node/
    node_00000.parquet
    ...
  type=relation/
    relation_00000.parquet
    ...
  type=way/
    way_00000.parquet
    ...
```
[Reference Arrow/SQL schema](https://github.com/brad-richardson/osm-pbf-parquet/blob/main/src/osm_arrow.rs)


## Download
Download latest version for OS from [releases](https://github.com/brad-richardson/osm-pbf-parquet/releases)


## Usage
Example for x86_64 linux system with pre-compiled binary:
```
curl -L "https://github.com/brad-richardson/osm-pbf-parquet/releases/latest/download/osm-pbf-parquet-x86_64-unknown-linux-gnu.tar.gz" -o "osm-pbf-parquet.tar.gz"
tar -xzf osm-pbf-parquet.tar.gz
chmod +x osm-pbf-parquet
./osm-pbf-parquet --input your.osm.pbf --output ./parquet
```

OR compile and run locally:
```
git clone https://github.com/brad-richardson/osm-pbf-parquet.git
cargo run --release -- --input your.osm.pbf --output ./parquet
```


## Development
1. [Install rust](https://www.rust-lang.org/tools/install)
2. Clone repo `git clone https://github.com/brad-richardson/osm-pbf-parquet.git`
3. Make changes
4. Run against PBF with `cargo run -- --input your.osm.pbf` ([Geofabrik regional PBF extracts here](https://download.geofabrik.de/))
5. Test with `cd test && ./prepare.sh && python3 validate.py`


## License
Distributed under the MIT License. See `LICENSE` for more information.


## Acknowledgments
* [osmpbf](https://github.com/b-r-u/osmpbf) and [osm2gzip](https://github.com/b-r-u/osm2gzip) for reading PBF data
* [osm2orc](https://github.com/mojodna/osm2orc) for schema and processing ideas
