use std::collections::HashMap;
use std::fs::File;
use std::sync::{Arc, Mutex};

use futures::StreamExt;
use futures_util::pin_mut;
use object_store::aws::AmazonS3Builder;
use object_store::buffered::BufReader;
use object_store::path::Path;
use object_store::ObjectStore;
use osmpbf::{AsyncBlobReader, BlobDecode, BlobReader, Element, PrimitiveBlock};
use rayon::iter::ParallelBridge;
use rayon::iter::ParallelIterator;
use url::Url;

pub mod osm_arrow;
pub mod sink;
pub mod util;
use crate::osm_arrow::OSMType;
use crate::sink::ElementSink;
use crate::util::{Args, ARGS, DEFAULT_BUF_READER_SIZE};

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

async fn create_s3_buf_reader(url: Url) -> Result<BufReader, anyhow::Error> {
    let s3_store = AmazonS3Builder::from_env()
        .with_url(url.clone())
        .build()
        .unwrap();
    let path = Path::parse(url.path())?;
    let meta = s3_store.head(&path).await?;
    Ok(BufReader::with_capacity(
        Arc::new(s3_store),
        &meta,
        DEFAULT_BUF_READER_SIZE,
    ))
}

async fn s3_read(
    url: Url,
    sinkpools: Arc<HashMap<OSMType, Arc<Mutex<Vec<ElementSink>>>>>,
    filenums: Arc<HashMap<OSMType, Arc<Mutex<u64>>>>,
) -> Result<(), anyhow::Error> {
    let s3_buf_reader = create_s3_buf_reader(url).await?;

    let mut blob_reader = AsyncBlobReader::new(s3_buf_reader);

    let stream = blob_reader.stream();
    pin_mut!(stream);

    let mut futures = vec![];
    while let Some(Ok(blob)) = stream.next().await {
        let sinkpools = sinkpools.clone();
        let filenums = filenums.clone();
        let f = tokio::spawn(async move {
            if let Ok(BlobDecode::OsmData(block)) = blob.decode() {
                process_block(block, sinkpools.clone(), filenums.clone());
            }
        });
        futures.push(f);
    }
    futures::future::join_all(futures).await;
    Ok(())
}

fn local_read(
    path: &str,
    sinkpools: Arc<HashMap<OSMType, Arc<Mutex<Vec<ElementSink>>>>>,
    filenums: Arc<HashMap<OSMType, Arc<Mutex<u64>>>>,
) -> Result<(), osmpbf::Error> {
    let file = File::open(path).unwrap();
    let reader = std::io::BufReader::with_capacity(DEFAULT_BUF_READER_SIZE, file);
    let blob_reader = BlobReader::new(reader);
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

    let full_path = args.input;
    if let Ok(url) = Url::parse(&full_path) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(s3_read(url, sinkpools.clone(), filenums.clone()));
        // TODO - better error handling
        result.unwrap();
    } else {
        let result = local_read(&full_path, sinkpools.clone(), filenums.clone());
        result.unwrap();
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
