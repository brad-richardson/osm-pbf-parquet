use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use osmpbf::{BlobDecode, BlobReader, Element, PrimitiveBlock};
use rayon::iter::ParallelBridge;
use rayon::iter::ParallelIterator;
use tokio::io::AsyncBufRead;
use tokio_util::io::SyncIoBridge;

pub mod osm_arrow;
pub mod sink;
pub mod util;
use crate::osm_arrow::OSMType;
use crate::sink::ElementSink;
use crate::util::{Args, ARGS};

pub async fn create_s3_async_reader() -> Result<impl AsyncBufRead, anyhow::Error> {
    let input_url = ARGS.get().unwrap().input.clone();
    let path = input_url
        .strip_prefix("s3://")
        .expect("Error parsing S3 path");
    let (bucket, key) = path.split_once('/').expect("Error parsing S3 path");
    let sdk_config = aws_config::load_from_env().await;
    let client = aws_sdk_s3::Client::new(&sdk_config);
    let stream = client.get_object().bucket(bucket).key(key).send().await?;
    Ok(stream.body.into_async_read())
}

fn get_sink_from_pool(
    osm_type: OSMType,
    sinkpools: Arc<HashMap<OSMType, Arc<Mutex<Vec<ElementSink>>>>>,
    filenums: Arc<HashMap<OSMType, Arc<Mutex<u64>>>>,
) -> Result<ElementSink, std::io::Error> {
    {
        let mut pool = sinkpools[&osm_type].lock().unwrap();
        if let Some(sink) = pool.pop() {
            return Ok(sink);
        }
    }
    ElementSink::new(filenums[&osm_type].clone(), osm_type)
}

fn add_sink_to_pool(
    sink: ElementSink,
    sinkpools: Arc<HashMap<OSMType, Arc<Mutex<Vec<ElementSink>>>>>,
) {
    let osm_type = sink.osm_type.clone();
    let mut pool = sinkpools[&osm_type].lock().unwrap();
    pool.push(sink);
}

fn process_block(
    block: PrimitiveBlock,
    sinkpools: Arc<HashMap<OSMType, Arc<Mutex<Vec<ElementSink>>>>>,
    filenums: Arc<HashMap<OSMType, Arc<Mutex<u64>>>>,
) {
    let mut node_sink =
        get_sink_from_pool(OSMType::Node, sinkpools.clone(), filenums.clone()).unwrap();
    let mut way_sink =
        get_sink_from_pool(OSMType::Way, sinkpools.clone(), filenums.clone()).unwrap();
    let mut rel_sink =
        get_sink_from_pool(OSMType::Relation, sinkpools.clone(), filenums.clone()).unwrap();
    for element in block.elements() {
        match element {
            Element::Node(ref node) => {
                let _ = node_sink.add_node(node);
            }
            Element::DenseNode(ref node) => {
                let _ = node_sink.add_dense_node(node);
            }
            Element::Way(ref way) => {
                let _ = way_sink.add_way(way);
            }
            Element::Relation(ref rel) => {
                let _ = rel_sink.add_relation(rel);
            }
        }
    }
    add_sink_to_pool(node_sink, sinkpools.clone());
    add_sink_to_pool(way_sink, sinkpools.clone());
    add_sink_to_pool(rel_sink, sinkpools.clone());
}

fn s3_read(
    sinkpools: Arc<HashMap<OSMType, Arc<Mutex<Vec<ElementSink>>>>>,
    filenums: Arc<HashMap<OSMType, Arc<Mutex<u64>>>>,
) -> Result<(), osmpbf::Error> {
    // Create sync reader because underlying BlobReader is not async
    // Backed by multi-threaded runtime to allow fetch concurrency
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let async_reader = rt.block_on(create_s3_async_reader()).unwrap();
    let sync_reader = SyncIoBridge::new_with_handle(async_reader, rt.handle().clone());
    let blob_reader = BlobReader::new(std::io::BufReader::new(sync_reader));

    // TODO - test this with and without on larger files, small files suffer
    // for blob in blob_reader {
    // Using rayon here because SyncIoBridge can't run on tokio-enabled threads
    blob_reader.par_bridge().for_each(|blob| {
        // TODO - the decode + process should work with a separate, non-tokio thread pool
        if let BlobDecode::OsmData(block) = blob.unwrap().decode().unwrap() {
            process_block(block, sinkpools.clone(), filenums.clone());
        }
        // }
    });

    Ok(())
}

fn local_read(
    sinkpools: Arc<HashMap<OSMType, Arc<Mutex<Vec<ElementSink>>>>>,
    filenums: Arc<HashMap<OSMType, Arc<Mutex<u64>>>>,
) -> Result<(), osmpbf::Error> {
    let blob_reader = BlobReader::from_path(ARGS.get().unwrap().input.clone()).unwrap();
    blob_reader.par_bridge().for_each(|blob| {
        if let BlobDecode::OsmData(block) = blob.unwrap().decode().unwrap() {
            process_block(block, sinkpools.clone(), filenums.clone());
        }
    });
    Ok(())
}

pub fn driver(args: Args) -> Result<(), std::io::Error> {
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
        let _ = s3_read(sinkpools.clone(), filenums.clone());
    } else {
        let _ = local_read(sinkpools.clone(), filenums.clone());
    }

    {
        for sinkpool in sinkpools.values() {
            let mut pool = sinkpool.lock().unwrap();
            for mut sink in pool.drain(..) {
                sink.finish();
            }
        }
    }
    Ok(())
}
