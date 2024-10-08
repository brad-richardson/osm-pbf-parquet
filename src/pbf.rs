use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures::StreamExt;
use futures_util::pin_mut;
use log::info;
use object_store::aws::AmazonS3Builder;
use object_store::buffered::BufReader;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::ObjectStore;
use osmpbf::{AsyncBlobReader, BlobDecode, Element, PrimitiveBlock};
use tokio::runtime::Handle;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use url::Url;

use crate::osm_arrow::OSMType;
use crate::sink::ElementSink;
use crate::util::{SinkpoolStore, ARGS, ELEMENT_COUNTER};

pub async fn create_s3_buf_reader(url: Url) -> Result<BufReader, anyhow::Error> {
    let s3_store = AmazonS3Builder::from_env().with_url(url.clone()).build()?;
    let path = Path::parse(url.path())?;
    let meta = s3_store.head(&path).await?;
    Ok(BufReader::with_capacity(
        Arc::new(s3_store),
        &meta,
        ARGS.get().unwrap().get_input_buffer_size_bytes(),
    ))
}

pub async fn create_local_buf_reader(path: &str) -> Result<BufReader, anyhow::Error> {
    let local_store: LocalFileSystem = LocalFileSystem::new();
    let path = std::path::Path::new(path);
    let filesystem_path = object_store::path::Path::from_filesystem_path(path)?;
    let meta = local_store.head(&filesystem_path).await?;
    Ok(BufReader::with_capacity(
        Arc::new(local_store),
        &meta,
        ARGS.get().unwrap().get_input_buffer_size_bytes(),
    ))
}

pub async fn process_blobs(
    buf_reader: BufReader,
    sinkpools: Arc<SinkpoolStore>,
) -> Result<(), anyhow::Error> {
    let mut blob_reader = AsyncBlobReader::new(buf_reader);

    let stream = blob_reader.stream();
    pin_mut!(stream);

    let filenums: Arc<HashMap<OSMType, Arc<Mutex<u64>>>> = Arc::new(HashMap::from([
        (OSMType::Node, Arc::new(Mutex::new(0))),
        (OSMType::Way, Arc::new(Mutex::new(0))),
        (OSMType::Relation, Arc::new(Mutex::new(0))),
    ]));

    // Avoid too many tasks in memory
    let active_tasks = (1.5 * ARGS.get().unwrap().get_worker_threads() as f32) as usize;
    let semaphore = Arc::new(Semaphore::new(active_tasks));

    let mut join_set = JoinSet::new();
    while let Some(Ok(blob)) = stream.next().await {
        let sinkpools = sinkpools.clone();
        let filenums = filenums.clone();
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        join_set.spawn(async move {
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
            drop(permit);
        });
    }
    while let Some(result) = join_set.join_next().await {
        result?;
    }
    Ok(())
}

pub async fn monitor(sinkpools: Arc<SinkpoolStore>) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
    interval.tick().await; // First tick is immediate

    loop {
        interval.tick().await;

        // Run cleanup
        finish_sinks(sinkpools.clone(), false).await.unwrap();

        // Log progress
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

pub async fn finish_sinks(
    sinkpools: Arc<SinkpoolStore>,
    force_finish: bool,
) -> Result<(), anyhow::Error> {
    let handle = Handle::current();
    let mut join_set = JoinSet::new();
    for sinkpool in sinkpools.values() {
        let mut pool = sinkpool.lock().unwrap();
        let sinks = pool.drain(..).collect::<Vec<_>>();
        for mut sink in sinks {
            if force_finish || sink.last_write_cycle.elapsed().as_secs() > 30 {
                // Finish, old or final cleanup run
                join_set.spawn_on(
                    async move {
                        sink.finish().await.unwrap();
                    },
                    &handle,
                );
            } else {
                // Retain, still being written to
                pool.push(sink);
            }
        }
    }
    while let Some(result) = join_set.join_next().await {
        result?;
    }
    Ok(())
}

fn get_sink_from_pool(
    osm_type: OSMType,
    sinkpools: Arc<SinkpoolStore>,
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

fn add_sink_to_pool(sink: ElementSink, sinkpools: Arc<SinkpoolStore>) {
    let osm_type = sink.osm_type.clone();
    let mut pool = sinkpools[&osm_type].lock().unwrap();
    pool.push(sink);
}

async fn process_block(
    block: PrimitiveBlock,
    sinkpools: Arc<SinkpoolStore>,
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

    Ok(block_counter)
}
