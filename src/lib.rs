// use core::sync;
use std::collections::HashMap;
use std::thread;
use std::error;
use std::io::{self};
use std::sync::{Arc, Mutex};
use anyhow::Error;
// use bytes::Bytes;
use futures::StreamExt;
// use futures_core::stream::Stream;
// use futures_util::stream::StreamExt;

use osmpbf::PrimitiveBlock;
use osmpbf::{Blob, BlobDecode, BlobReader, Element};
// use rayon::iter::ParallelBridge;
// use rayon::iter::ParallelIterator;
use tokio::io::AsyncBufRead;
use tokio_util::io::{StreamReader, SyncIoBridge};
use tokio_stream::{self as stream};
// use tokio_util::io::StreamReader;

pub mod osm_arrow;
pub mod sink;
pub mod util;
use crate::osm_arrow::OSMType;
use crate::sink::ElementSink;
use crate::util::{Args, ARGS, cpu_count};

pub async fn create_s3_async_reader() -> Result<impl AsyncBufRead, anyhow::Error> {
    // let args = ARGS.get().unwrap();
// pub async fn create_sync_reader<T>(args: Args) -> Result<BufReader<T>, std::io::Error> {
    // Bucket: omf-transp-public-devo-us-west-2
    // Small: test/pbf/greenland-latest.osm.pbf
    // Large: test/pbf/mexico-latest.osm.pbf
    let sdk_config = aws_config::load_from_env().await;
    let client = aws_sdk_s3::Client::new(&sdk_config);
    let stream = client
        .get_object()
        // TODO - parse these out from args input
        .bucket("omf-transp-public-devo-us-west-2")
        .key("test/pbf/greenland-latest.osm.pbf")
        .send()
        .await?;

    // Convert the stream into a sync reader
    Ok(stream.body.into_async_read())
}

// pub fn async_driver(args: Args) -> Result<(), io::Error> {
//     // TODO - validation of args
//     // Store value for reading across threads (write-once)
//     let _ = ARGS.set(args.clone());



//     let sinkpools = HashMap::from([
//         (OSMType::Node, Arc::new(Mutex::new(vec![]))),
//         (OSMType::Way, Arc::new(Mutex::new(vec![]))),
//         (OSMType::Relation, Arc::new(Mutex::new(vec![]))),
//     ]);

//     let filenums = HashMap::from([
//         (OSMType::Node, Arc::new(Mutex::new(0))),
//         (OSMType::Way, Arc::new(Mutex::new(0))),
//         (OSMType::Relation, Arc::new(Mutex::new(0))),
//     ]);

//     let get_sink_from_pool = |osm_type: OSMType| -> Result<ElementSink, std::io::Error> {
//         {
//             let mut pool = sinkpools[&osm_type].lock().unwrap();
//             if let Some(sink) = pool.pop() {
//                 return Ok(sink);
//             }
//         }
//         ElementSink::new(filenums[&osm_type].clone(), osm_type)
//     };

//     let add_sink_to_pool = |sink: ElementSink| {
//         let osm_type = sink.osm_type.clone();
//         let mut pool = sinkpools[&osm_type].lock().unwrap();
//         pool.push(sink);
//     };

//     // TODO - async blob reader that can use par_bridge
//     reader
//         .par_bridge()
//         .try_for_each(|blob| -> Result<(), io::Error> {
//             if let BlobDecode::OsmData(block) = blob?.decode()? {
//                 let mut node_sink = get_sink_from_pool(OSMType::Node)?;
//                 let mut way_sink = get_sink_from_pool(OSMType::Way)?;
//                 let mut rel_sink = get_sink_from_pool(OSMType::Relation)?;
//                 for elem in block.elements() {
//                     match elem {
//                         Element::Node(ref node) => {
//                             node_sink.add_node(node)?;
//                         }
//                         Element::DenseNode(ref node) => {
//                             node_sink.add_dense_node(node)?;
//                         }
//                         Element::Way(ref way) => {
//                             way_sink.add_way(way)?;
//                         }
//                         Element::Relation(ref rel) => {
//                             rel_sink.add_relation(rel)?;
//                         }
//                     }
//                 }
//                 add_sink_to_pool(node_sink);
//                 add_sink_to_pool(way_sink);
//                 add_sink_to_pool(rel_sink);
//             }
//             Ok(())
//         })?;

//     {
//         for sinkpool in sinkpools.values() {
//             let mut pool = sinkpool.lock().unwrap();
//             for mut sink in pool.drain(..) {
//                 sink.finish();
//             }
//         }
//     }
//     Ok(())
// }

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
    // TODO - no unwraps
    let mut node_sink = get_sink_from_pool(OSMType::Node, sinkpools.clone(), filenums.clone()).unwrap();
    let mut way_sink = get_sink_from_pool(OSMType::Way, sinkpools.clone(), filenums.clone()).unwrap();
    let mut rel_sink = get_sink_from_pool(OSMType::Relation, sinkpools.clone(), filenums.clone()).unwrap();
    for elem in block.elements() {
        match elem {
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

    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    // TODO - probably move this back into the element adds while it is piped
    rt.block_on(node_sink.increment_and_cycle());
    rt.block_on(way_sink.increment_and_cycle());
    rt.block_on(rel_sink.increment_and_cycle());
    
    add_sink_to_pool(node_sink, sinkpools.clone());
    add_sink_to_pool(way_sink, sinkpools.clone());
    add_sink_to_pool(rel_sink, sinkpools.clone());
}

fn s3_read(sinkpools: Arc<HashMap<OSMType, Arc<Mutex<Vec<ElementSink>>>>>, filenums: Arc<HashMap<OSMType, Arc<Mutex<u64>>>>) -> Result<(), osmpbf::Error> {
    // let rt = tokio::runtime::Builder::new_multi_thread()
    // .enable_all()
    // .build()
    // .unwrap();
    // TODO - unwraps
    // tokio::task::spawn_blocking(move || {
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    let async_reader = rt.block_on(create_s3_async_reader()).unwrap();
    // Need tokio runtime, but don't need to run on it otherwise
    let sync_reader = SyncIoBridge::new_with_handle(async_reader, rt.handle().clone());
    // rt.
    let blob_reader = BlobReader::new(std::io::BufReader::new(sync_reader)); // TODO - std::io::BufReader here?
    // let stream = stream::iter(blob_reader);
    // stream.for
    // TODO - maybe block on each blob read?



    // TODO - cleanup
    // TODO - stream element reads
    // TODO - pipe out to other threads


    let futures = blob_reader.filter_map(|blob| {
        match blob.unwrap().decode() {
            Ok(BlobDecode::OsmData(block)) => {
                let sinkpools = sinkpools.clone();
                let filenums = filenums.clone();
                Some(
                    process_block(block, sinkpools, filenums)
                //     rt.spawn(async move {
                //     // process_block(block, sinkpools, filenums).await
                // })
                )
                // Some(1)
            },
            _ => {
                // Ignore other blocks
                None
            }
        }
        // let decoded = blob.unwrap().decode().unwrap();
        // match blob.ok()?.decode() {
        //     Ok(BlobDecode::OsmData(block)) => {
        //         // None
        //         println!("post decode");
        //         let sinkpools = sinkpools.clone();
        //         let filenums = filenums.clone();
        //         Some(
        //             rt.spawn(async move {
        //             // process_block(block, sinkpools, filenums).await
        //         })
        //     )
        //     },
        //     _ => {
        //         // Ignore other blocks
        //         None
        //     }
        // }
        // Some(
        //     rt.spawn(async move {
        //                     // process_block(block, sinkpools, filenums).await
        //                 })
        // )
    });
    println!("{}", futures.count());
    // rt.block_on(async {
    //     tokio::join!(futures::future::join_all(futures))
    // });
    // tokio::join!(futures::future::join_all(futures));
    // rt.block_on(future.await);
    Ok(())
}

// async fn local_read(sinkpools: Arc<HashMap<OSMType, Arc<Mutex<Vec<ElementSink>>>>>, filenums: Arc<HashMap<OSMType, Arc<Mutex<u64>>>>) -> Result<(), osmpbf::Error> {
//     // let rt = tokio::runtime::Builder::new_multi_thread()
//     // .enable_all()
//     // .build()

//     // .unwrap();
//     // let blob_reader = BlobReader::from_path(ARGS.get().unwrap().input.clone()).unwrap();
//     // let stream = stream::iter(blob_reader);
// TODO - keep concurrency here
//     // stream.for_each_concurrent(cpu_count() / 2, |blob| async {
//     //     // TODO - fix unwrap
//     //     process_block(blob.unwrap(), sinkpools.clone(), filenums.clone()).await
//     // }).await;
//     // rt.block_on(future.await);
//     Ok(())
// }

pub fn driver(args: Args) -> Result<(), io::Error> {
    // TODO - validation of args
    // Store value for reading across threads (write-once)
    let _ = ARGS.set(args.clone());

    // if args.input.starts_with("s3://") {
    // } else {
    //     let reader = BlobReader::from_path(args.input).unwrap();
    // }
    // TODO - need async blob reader

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

    // TODO - re-implement for local
    // if args.input.starts_with("s3://") {
    s3_read(sinkpools.clone(), filenums.clone());
    // } else {
    //     local_read(sinkpools.clone(), filenums.clone());
    // }

    // let get_sink_from_pool = |osm_type: OSMType| -> Result<ElementSink, std::io::Error> {

    // };

    // let add_sink_to_pool = 

    // let stream = stream::iter(reader);



    // reader
    //     .par_bridge()
    //     .try_for_each(|blob| -> Result<(), io::Error> {

    // stream::StreamExt::try_for_each(stream, |blob| -> Result<(), io::Error> {
    //     StreamExt::chunks_timeout(self, max_size, duration);
        // rt.enter();
        // TODO - async move?
    // stream.try_for_each_concurrent(cpu_count() / 2, |blob| -> Result<(), io::Error> {
    //     if let BlobDecode::OsmData(block) = blob?.decode()? {
    //         process_block(block, sinkpools, filenums)?;
    //     }
    //     Ok(())
    // })?;

    {
        let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        for sinkpool in sinkpools.values() {
            let mut pool = sinkpool.lock().unwrap();
            for mut sink in pool.drain(..) {
                rt.block_on(sink.finish());
            }
        }
    }
    Ok(())
}
