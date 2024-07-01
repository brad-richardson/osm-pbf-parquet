use std::io;
use std::sync::{Arc, Mutex};

use osmpbf::{BlobDecode, BlobReader, Element};
use rayon::iter::ParallelBridge;
use rayon::iter::ParallelIterator;

use clap::Parser;

mod sink;
use crate::sink::ElementSink;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to input PBF
    #[arg(short, long)]
    input: String,

    /// Path to output directory
    #[arg(short, long, default_value = "./")]
    output: String,
}

fn main() -> Result<(), io::Error> {
    let args = Args::parse();
    println!("{:?}", args);

    // TODO - validation of args
    let reader = BlobReader::from_path(args.input)?;

    let sinkpool: Arc<Mutex<Vec<ElementSink>>> = Arc::new(Mutex::new(vec![]));
    let filenum: Arc<Mutex<u64>> = Arc::new(Mutex::new(0));
    let output_dir = args.output;

    let get_sink_from_pool = || -> Result<ElementSink, std::io::Error> {
        {
            let mut pool = sinkpool.lock().unwrap();
            if let Some(sink) = pool.pop() {
                return Ok(sink);
            }
        }
        ElementSink::new(filenum.clone(), output_dir.clone())
    };

    let add_sink_to_pool = |sink| {
        let mut pool = sinkpool.lock().unwrap();
        pool.push(sink);
    };

    reader
        .par_bridge()
        .try_for_each(|blob| -> Result<(), io::Error> {
            if let BlobDecode::OsmData(block) = blob?.decode()? {
                let mut sink = get_sink_from_pool()?;
                for elem in block.elements() {
                    match elem {
                        Element::Node(ref node) => {
                            sink.add_node(node)?;
                        }
                        Element::DenseNode(ref node) => {
                            sink.add_dense_node(node)?;
                        }
                        Element::Way(ref way) => {
                            sink.add_way(way)?;
                        }
                        Element::Relation(ref rel) => {
                            sink.add_relation(rel)?;
                        }
                    }
                }
                add_sink_to_pool(sink);
            }
            Ok(())
        })?;

    {
        let mut pool = sinkpool.lock().unwrap();
        for mut sink in pool.drain(..) {
            sink.finish_batch();
        }
    }
    Ok(())
}
