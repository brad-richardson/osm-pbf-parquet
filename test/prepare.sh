#!/bin/sh

# Cleanup old artifacts if exist
rm cook-islands-latest.*
rm -rf ./parquet/
mkdir -p ./parquet/

# Download PBF, convert to OSM XML
curl -L "https://download.geofabrik.de/australia-oceania/cook-islands-latest.osm.pbf" -o "cook-islands-latest.osm.pbf"
osmium cat cook-islands-latest.osm.pbf -o cook-islands-latest.osm

# Also download larger PBF for benchmarking
curl -L "https://download.geofabrik.de/central-america/el-salvador-latest.osm.pbf" -o "el-salvador-latest.osm.pbf"

# Run parquet conversion
cargo run -- --input cook-islands-latest.osm.pbf --output ./parquet/
