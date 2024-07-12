use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};

use osmpbf::{BlobDecode, BlobReader, Element};
use rayon::iter::ParallelBridge;
use rayon::iter::ParallelIterator;

use clap::Parser;

mod osm_arrow;
mod sink;
use crate::sink::ElementSink;
use crate::osm_arrow::OSMType;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Path to input PBF
    #[arg(short, long)]
    pub input: String,

    /// Path to output directory
    #[arg(short, long, default_value = "./parquet")]
    pub output: String,
}

pub fn driver(args: Args) -> Result<(), io::Error> {
    // TODO - validation of args
    let reader = BlobReader::from_path(args.input)?;
    let output_dir = args.output;
    
    let sinkpools = HashMap::from([
        (OSMType::Node, Arc::new(Mutex::new(vec![]))),
        (OSMType::Way, Arc::new(Mutex::new(vec![]))),
        (OSMType::Relation, Arc::new(Mutex::new(vec![]))),
    ]);

    let filenums = HashMap::from([
        (OSMType::Node, Arc::new(Mutex::new(0))),
        (OSMType::Way, Arc::new(Mutex::new(0))),
        (OSMType::Relation, Arc::new(Mutex::new(0))),
    ]);

    let get_sink_from_pool = |osm_type: OSMType| -> Result<ElementSink, std::io::Error> {
        {
            let mut pool = sinkpools[&osm_type].lock().unwrap();
            if let Some(sink) = pool.pop() {
                return Ok(sink);
            }
        }
        ElementSink::new(filenums[&osm_type].clone(), output_dir.clone(), osm_type)
    };

    let add_sink_to_pool = |sink: ElementSink| {
        let osm_type = sink.osm_type.clone();
        let mut pool = sinkpools[&osm_type].lock().unwrap();
        pool.push(sink);
    };

    reader
        .par_bridge()
        .try_for_each(|blob| -> Result<(), io::Error> {
            if let BlobDecode::OsmData(block) = blob?.decode()? {
                let mut node_sink = get_sink_from_pool(OSMType::Node)?;
                let mut way_sink = get_sink_from_pool(OSMType::Way)?;
                let mut rel_sink = get_sink_from_pool(OSMType::Relation)?;
                for elem in block.elements() {
                    match elem {
                        Element::Node(ref node) => {
                            node_sink.add_node(node)?;
                        }
                        Element::DenseNode(ref node) => {
                            node_sink.add_dense_node(node)?;
                        }
                        Element::Way(ref way) => {
                            way_sink.add_way(way)?;
                        }
                        Element::Relation(ref rel) => {
                            rel_sink.add_relation(rel)?;
                        }
                    }
                }
                add_sink_to_pool(node_sink);
                add_sink_to_pool(way_sink);
                add_sink_to_pool(rel_sink);
            }
            Ok(())
        })?;

    {
        for sinkpool in sinkpools.values() {
            let mut pool = sinkpool.lock().unwrap();
            for mut sink in pool.drain(..) {
                sink.finish_batch();
            }
        }
    }
    Ok(())
}
