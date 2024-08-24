use std::collections::HashMap;
use std::time::Duration;
use std::{boxed, thread};
use std::io::{self};
use std::sync::{Arc, Mutex};

use futures::{future, StreamExt};
use osmpbf::{BlobDecode, BlobReader, Element, PrimitiveBlock};
use rayon::iter::ParallelBridge;
use rayon::iter::ParallelIterator;
use tokio::io::AsyncBufRead;
use tokio_util::io::SyncIoBridge;
use tokio_stream::{self as stream};

pub mod osm_arrow;
pub mod sink;
pub mod util;
use crate::osm_arrow::OSMType;
use crate::sink::ElementSink;
use crate::util::{Args, ARGS, cpu_count};

pub async fn create_s3_async_reader() -> Result<impl AsyncBufRead, anyhow::Error> {
    let input_url = ARGS.get().unwrap().input.clone();
    let path = input_url.strip_prefix("s3://").expect("Error parsing S3 path");
    let (bucket, key) = path.split_once('/').expect("Error parsing S3 path");
    let sdk_config = aws_config::load_from_env().await;
    let client = aws_sdk_s3::Client::new(&sdk_config);
    let stream = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;
    Ok(stream.body.into_async_read())
}

fn get_sink_from_pool(osm_type: OSMType, sinkpools: Arc<HashMap<OSMType, Arc<Mutex<Vec<ElementSink>>>>>, filenums: Arc<HashMap<OSMType, Arc<Mutex<u64>>>>) -> Result<ElementSink, std::io::Error> {
    {
        let mut pool = sinkpools[&osm_type].lock().unwrap();
        if let Some(sink) = pool.pop() {
            return Ok(sink);
        }
    }
    ElementSink::new(filenums[&osm_type].clone(), osm_type)
}

fn add_sink_to_pool(sink: ElementSink, sinkpools: Arc<HashMap<OSMType, Arc<Mutex<Vec<ElementSink>>>>>) {
    let osm_type = sink.osm_type.clone();
    let mut pool = sinkpools[&osm_type].lock().unwrap();
    pool.push(sink);
}

fn process_block(block: PrimitiveBlock, sinkpools: Arc<HashMap<OSMType, Arc<Mutex<Vec<ElementSink>>>>>, filenums: Arc<HashMap<OSMType, Arc<Mutex<u64>>>>) {
    // let sinkpools = sinkpools.clone();
    // let filenums = filenums.clone();
    let mut node_sink = get_sink_from_pool(OSMType::Node, sinkpools.clone(), filenums.clone()).unwrap();
    let mut way_sink = get_sink_from_pool(OSMType::Way, sinkpools.clone(), filenums.clone()).unwrap();
    let mut rel_sink = get_sink_from_pool(OSMType::Relation, sinkpools.clone(), filenums.clone()).unwrap();
    for element in block.elements() {
        match element {
            Element::Node(ref node) => {
                node_sink.add_node(node);
            }
            Element::DenseNode(ref node) => {
                node_sink.add_dense_node(node);
            }
            Element::Way(ref way) => {
                way_sink.add_way(way);
            }
            Element::Relation(ref rel) => {
                rel_sink.add_relation(rel);
            }
        }
    }
    add_sink_to_pool(node_sink, sinkpools.clone());
    add_sink_to_pool(way_sink, sinkpools.clone());
    add_sink_to_pool(rel_sink, sinkpools.clone());
}

fn s3_read(sinkpools: Arc<HashMap<OSMType, Arc<Mutex<Vec<ElementSink>>>>>, filenums: Arc<HashMap<OSMType, Arc<Mutex<u64>>>>) -> Result<(), osmpbf::Error> {
    // Create sync reader because underlying BlobReader is not async
    // Backed by multi-threaded runtime to allow fetch concurrency
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    let async_reader = rt.block_on(create_s3_async_reader()).unwrap();
    let sync_reader = SyncIoBridge::new_with_handle(async_reader, rt.handle().clone());
    let blob_reader = BlobReader::new(std::io::BufReader::new(sync_reader));

    for blob in blob_reader {
        if let BlobDecode::OsmData(block) = blob.unwrap().decode().unwrap() {
            process_block(block, sinkpools.clone(), filenums.clone());
        }
    }

    Ok(())
}

fn local_read(sinkpools: Arc<HashMap<OSMType, Arc<Mutex<Vec<ElementSink>>>>>, filenums: Arc<HashMap<OSMType, Arc<Mutex<u64>>>>) -> Result<(), osmpbf::Error> {
    let blob_reader = BlobReader::from_path(ARGS.get().unwrap().input.clone()).unwrap();
    blob_reader
        .par_bridge()
        .for_each(|blob| {
            if let BlobDecode::OsmData(block) = blob.unwrap().decode().unwrap() {
                process_block(block, sinkpools.clone(), filenums.clone());
            }
        });
    Ok(())
}

pub fn driver(args: Args) -> Result<(), io::Error> {
    // TODO - validation of args
    // Store value for reading across threads (write-once)
    let _ = ARGS.set(args.clone());

    let sinkpools: Arc<HashMap<OSMType, Arc<Mutex<Vec<ElementSink>>>>> = Arc::new(HashMap::from([
        (OSMType::Node, Arc::new(Mutex::new(vec![]))),
        (OSMType::Way, Arc::new(Mutex::new(vec![]))),
        (OSMType::Relation, Arc::new(Mutex::new(vec![]))),
    ]));

    let filenums: Arc<HashMap<OSMType, Arc<Mutex<u64>>>> = Arc::new(HashMap::from([
        (OSMType::Node, Arc::new(Mutex::new(0))),
        (OSMType::Way, Arc::new(Mutex::new(0))),
        (OSMType::Relation, Arc::new(Mutex::new(0))),
    ]));

    if args.input.starts_with("s3://") {
        s3_read(sinkpools.clone(), filenums.clone());
    } else {
        local_read(sinkpools.clone(), filenums.clone());
    }

    {
        let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        for sinkpool in sinkpools.values() {
            let mut pool = sinkpool.lock().unwrap();
            for mut sink in pool.drain(..) {
                sink.finish();
            }
        }
    }
    Ok(())
}
