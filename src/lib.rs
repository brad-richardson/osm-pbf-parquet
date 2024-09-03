use std::collections::HashMap;
use std::fs::File;
use std::sync::{Arc, Mutex};

use futures::stream::FuturesUnordered;
use futures::StreamExt;
use futures_util::pin_mut;
use log::info;
use object_store::aws::AmazonS3Builder;
use object_store::buffered::BufReader;
use object_store::path::Path;
use object_store::ObjectStore;
use osmpbf::{AsyncBlobReader, BlobDecode, BlobReader, Element, PrimitiveBlock};
use rayon::iter::ParallelBridge;
use rayon::iter::ParallelIterator;
use tokio::runtime::Handle;
use url::Url;

pub mod osm_arrow;
pub mod sink;
pub mod util;
use crate::osm_arrow::OSMType;
use crate::sink::ElementSink;
use crate::util::{Args, ARGS, DEFAULT_BUF_READER_SIZE, ELEMENT_COUNTER};

fn get_sink_from_pool(
    osm_type: OSMType,
    sinkpools: Arc<HashMap<OSMType, Arc<Mutex<Vec<ElementSink>>>>>,
    filenums: Arc<HashMap<OSMType, Arc<Mutex<u64>>>>,
) -> Result<ElementSink, anyhow::Error> {
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

async fn process_block(
    block: PrimitiveBlock,
    sinkpools: Arc<HashMap<OSMType, Arc<Mutex<Vec<ElementSink>>>>>,
    filenums: Arc<HashMap<OSMType, Arc<Mutex<u64>>>>,
) -> Result<u64, anyhow::Error> {
    let mut node_sink = get_sink_from_pool(OSMType::Node, sinkpools.clone(), filenums.clone())?;
    let mut way_sink = get_sink_from_pool(OSMType::Way, sinkpools.clone(), filenums.clone())?;
    let mut rel_sink = get_sink_from_pool(OSMType::Relation, sinkpools.clone(), filenums.clone())?;

    let mut block_counter = 0u64;
    for element in block.elements() {
        block_counter += 1;
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
    ELEMENT_COUNTER.fetch_add(block_counter, std::sync::atomic::Ordering::Relaxed);

    node_sink.increment_and_cycle().await?;
    way_sink.increment_and_cycle().await?;
    rel_sink.increment_and_cycle().await?;
    add_sink_to_pool(node_sink, sinkpools.clone());
    add_sink_to_pool(way_sink, sinkpools.clone());
    add_sink_to_pool(rel_sink, sinkpools.clone());

    return Ok(block_counter);
}

async fn create_s3_buf_reader(url: Url) -> Result<BufReader, anyhow::Error> {
    let s3_store = AmazonS3Builder::from_env().with_url(url.clone()).build()?;
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

    let futures = FuturesUnordered::new();
    while let Some(Ok(blob)) = stream.next().await {
        let sinkpools = sinkpools.clone();
        let filenums = filenums.clone();
        let f = tokio::spawn(async move {
            match blob.decode() {
                Ok(BlobDecode::OsmHeader(_)) => (),
                Ok(BlobDecode::OsmData(block)) => {
                    process_block(block, sinkpools.clone(), filenums.clone())
                        .await
                        .unwrap();
                }
                Ok(BlobDecode::Unknown(unknown)) => {
                    panic!("Unknown blob: {}", unknown);
                }
                Err(error) => {
                    panic!("Error decoding blob: {}", error);
                }
            }
        });
        futures.push(f);
    }
    for future in futures {
        future.await?;
    }
    Ok(())
}

async fn local_read(
    path: &str,
    sinkpools: Arc<HashMap<OSMType, Arc<Mutex<Vec<ElementSink>>>>>,
    filenums: Arc<HashMap<OSMType, Arc<Mutex<u64>>>>,
) -> Result<(), anyhow::Error> {
    let file = File::open(path)?;
    let reader = std::io::BufReader::with_capacity(DEFAULT_BUF_READER_SIZE, file);
    let blob_reader = BlobReader::new(reader);
    let handle = Handle::current();
    blob_reader.par_bridge().for_each(|blob| {
        let sinkpools = sinkpools.clone();
        let filenums = filenums.clone();
        handle.block_on(async {
            if let BlobDecode::OsmData(block) = blob.unwrap().decode().unwrap() {
                process_block(block, sinkpools.clone(), filenums.clone())
                    .await
                    .unwrap();
            }
        });
    });
    Ok(())
}

pub async fn progress_log() {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
    interval.tick().await; // First tick is immediate

    loop {
        interval.tick().await;
        let processed = ELEMENT_COUNTER.load(std::sync::atomic::Ordering::Relaxed);
        let mut processed_str = format!("{}", processed);
        if processed >= 1_000_000_000 {
            processed_str = format!("{:.2}B", (processed as f64) / 1_000_000_000.0);
        } else if processed >= 1_000_000 {
            processed_str = format!("{:.2}M", (processed as f64) / 1_000_000.0);
        }
        info!("Processed {} elements", processed_str);
    }
}

pub async fn driver(args: Args) -> Result<(), anyhow::Error> {
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

    // Verify we're running in a tokio runtime and start separate logging thread
    Handle::current().spawn(async { progress_log().await });

    let full_path = args.input;
    if let Ok(url) = Url::parse(&full_path) {
        s3_read(url, sinkpools.clone(), filenums.clone()).await?;
    } else {
        local_read(&full_path, sinkpools.clone(), filenums.clone()).await?;
    }

    {
        let handle = Handle::current();
        let futures = FuturesUnordered::new();
        for sinkpool in sinkpools.values() {
            let mut pool = sinkpool.lock().unwrap();
            for mut sink in pool.drain(..) {
                let f = handle.spawn(async move {
                    // TODO - handle this
                    sink.finish().await.unwrap();
                });
                futures.push(f);
            }
        }
        for result in futures {
            result.await?;
        }
    }
    Ok(())
}
