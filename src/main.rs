use std::fs::File;
use std::io;
use std::sync::{Arc, Mutex};

use arrow::array::builder::{
    ArrayBuilder, BooleanBuilder, Int64Builder, ListBuilder, MapBuilder, StringBuilder,
    StructBuilder,
};
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::record_batch::RecordBatch;
use osmpbf::{
    BlobDecode, BlobReader, DenseNode, Element, Node, RelMemberIter, RelMemberType, Relation,
    TagIter, Way,
};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;

struct ElementSink {
    builders: Vec<Box<dyn ArrayBuilder>>,
    num_elements: u64,
    filenum: Arc<Mutex<u64>>,
}

impl ElementSink {
    const MAX_ELEMENTS_COUNT: u64 = 1_000_000;

    fn new(filenum: Arc<Mutex<u64>>) -> Result<Self, std::io::Error> {
        // `nds` ARRAY<STRUCT<ref: BIGINT>>,
        let nodes_builder = ListBuilder::new(StructBuilder::from_fields(
            vec![Field::new("ref", DataType::Int64, true)],
            0,
        ));

        // `members` ARRAY<STRUCT<type: STRING, ref: BIGINT, role: STRING>>,
        let members_builder = ListBuilder::new(StructBuilder::from_fields(
            vec![
                Field::new("type", DataType::Utf8, true),
                Field::new("ref", DataType::Int64, true),
                Field::new("role", DataType::Utf8, true),
            ],
            0,
        ));

        // `id` BIGINT,
        // `type` STRING,
        // `tags` MAP <STRING, STRING>,
        // `lat` DECIMAL(9, 7),
        // `lon` DECIMAL(10, 7),
        // `nds` ARRAY<STRUCT<ref: BIGINT>>,
        // `members` ARRAY<STRUCT<type: STRING, ref: BIGINT, role: STRING>>,
        // `changeset` BIGINT,
        // `timestamp` TIMESTAMP,
        // `uid` BIGINT,
        // `user` STRING,
        // `version` BIGINT,
        // `visible` BOOLEAN
        let data_builders: Vec<Box<dyn ArrayBuilder>> = vec![
            Box::new(Int64Builder::new()),  // id
            Box::new(StringBuilder::new()), // type
            Box::new(MapBuilder::new(
                None,
                StringBuilder::new(),
                StringBuilder::new(),
            )), // tags
            Box::new(StringBuilder::new()), // lat
            Box::new(StringBuilder::new()), // lon
            Box::new(nodes_builder),        // nds
            Box::new(members_builder),      // members
            Box::new(Int64Builder::new()),  // changeset
            Box::new(StringBuilder::new()), // timestamp TODO
            Box::new(Int64Builder::new()),  // uid
            Box::new(StringBuilder::new()), // user
            Box::new(Int64Builder::new()),  // version
            Box::new(BooleanBuilder::new()), // visible
        ];

        Ok(ElementSink {
            builders: data_builders,
            num_elements: 0,
            filenum,
        })
    }

    fn finish_batch(&mut self) -> () {
        let file = File::create(Self::new_file_path(&self.filenum)).unwrap();

        let array_refs: Vec<ArrayRef> = self
            .builders
            .iter_mut()
            .map(|builder| builder.finish())
            .collect();

        let batch = RecordBatch::try_from_iter(vec![
            ("id", array_refs[0].clone()),
            ("types", array_refs[1].clone()),
            ("tags", array_refs[2].clone()),
            ("members", array_refs[6].clone()),
        ])
        .unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();

        writer.write(&batch).expect("Writing batch");
        writer.close().unwrap();

        self.num_elements = 0;
    }

    fn increment_and_cycle(&mut self) -> Result<(), std::io::Error> {
        self.num_elements += 1;
        if self.num_elements >= Self::MAX_ELEMENTS_COUNT {
            self.finish_batch();
        }
        Ok(())
    }

    fn new_file_path(filenum: &Arc<Mutex<u64>>) -> String {
        let mut num = filenum.lock().unwrap();
        let path = format!("out/elements_{:05}.parquet", num);
        *num += 1;
        path
    }

    fn add_node(&mut self, node: &Node) -> Result<(), std::io::Error> {
        self.increment_and_cycle()
    }

    fn add_dense_node(&mut self, node: &DenseNode) -> Result<(), std::io::Error> {
        self.increment_and_cycle()
    }

    fn add_way(&mut self, way: &Way) -> Result<(), std::io::Error> {
        // self.append_feature_values(way.id(), "way".to_string(), way.tags());
        self.increment_and_cycle()
    }

    fn add_relation(&mut self, relation: &Relation) -> Result<(), std::io::Error> {
        self.append_feature_values(
            relation.id(),
            "relation".to_string(),
            relation.tags(),
            relation.members(),
        );

        self.increment_and_cycle()
    }

    fn append_feature_values<'a>(
        &mut self,
        id: i64,
        feature_type: String,
        tag_iter: TagIter<'a>,
        member_iter: RelMemberIter<'a>,
    ) -> () {
        // Result<(), std::io::Error> {
        let _id_builder = self.builders[0]
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .unwrap()
            .append_value(id);
        let _type_builder = self.builders[1]
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .unwrap()
            .append_value(feature_type);

        let tags_builder = self.builders[2]
            .as_any_mut()
            .downcast_mut::<MapBuilder<StringBuilder, StringBuilder>>()
            .unwrap();
        for (key, value) in tag_iter {
            tags_builder.keys().append_value(key.to_string());
            tags_builder.values().append_value(value.to_string());
        }
        let _ = tags_builder.append(true);

        // Derived from https://docs.rs/arrow/latest/arrow/array/struct.StructBuilder.html
        let members_builder = self.builders[6]
            .as_any_mut()
            .downcast_mut::<ListBuilder<StructBuilder>>()
            .unwrap();

        let struct_builder = members_builder.values();

        for member in member_iter {
            let type_builder = struct_builder.field_builder::<StringBuilder>(0).unwrap();
            match member.member_type {
                RelMemberType::Node => type_builder.append_value("node"),
                RelMemberType::Way => type_builder.append_value("way"),
                RelMemberType::Relation => type_builder.append_value("relation"),
            }

            struct_builder
                .field_builder::<Int64Builder>(1)
                .unwrap()
                .append_value(member.member_id);

            let role_builder = struct_builder.field_builder::<StringBuilder>(2).unwrap();
            match member.role() {
                Ok(role) => role_builder.append_value(role.to_string()),
                Err(_) => role_builder.append_null(),
            }
            struct_builder.append(true);
        }

        members_builder.append(true);
    }
}

fn main() -> Result<(), io::Error> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        println!("Need *.osm.pbf file as first argument.");
        return Ok(());
    }
    let reader = BlobReader::from_path(&args[1])?;

    let sinkpool: Arc<Mutex<Vec<ElementSink>>> = Arc::new(Mutex::new(vec![]));
    let filenum: Arc<Mutex<u64>> = Arc::new(Mutex::new(0));

    let get_sink_from_pool = || -> Result<ElementSink, std::io::Error> {
        {
            let mut pool = sinkpool.lock().unwrap();
            if let Some(sink) = pool.pop() {
                return Ok(sink);
            }
        }
        ElementSink::new(filenum.clone())
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
            // sink.writer.finish()?.flush()?;
            sink.finish_batch();
        }
    }
    Ok(())
}
