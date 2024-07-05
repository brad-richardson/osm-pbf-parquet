#!/bin/sh

# Cleanup old artifacts if exist
rm cook-islands-latest.*
rm -rf ./parquet/
mkdir -p ./parquet/

# Download PBF, convert to OSM XML
curl -L "https://download.geofabrik.de/australia-oceania/cook-islands-latest.osm.pbf" -o "cook-islands-latest.osm.pbf"
osmium cat cook-islands-latest.osm.pbf -o cook-islands-latest.osm

# Run parquet conversion
cargo run --release -- --input cook-islands-latest.osm.pbf --output ./parquet/
