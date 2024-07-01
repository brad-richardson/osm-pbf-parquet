use std::fs::File;
use std::sync::{Arc, Mutex};

use arrow::array::builder::{
    ArrayBuilder, BooleanBuilder, Decimal128Builder, Int64Builder, ListBuilder, MapBuilder,
    StringBuilder, StructBuilder,
};
use arrow::array::{ArrayRef, Int32Builder};
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::DECIMAL128_MAX_PRECISION;
use arrow::record_batch::RecordBatch;
use osmpbf::{DenseNode, Node, RelMemberType, Relation, TagIter, Way};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

pub struct ElementSink {
    builders: Vec<Box<dyn ArrayBuilder>>,
    num_elements: u64,
    filenum: Arc<Mutex<u64>>,
}

impl ElementSink {
    const MAX_ELEMENTS_COUNT: u64 = 1_000_000;

    pub fn new(filenum: Arc<Mutex<u64>>) -> Result<Self, std::io::Error> {
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

        let data_builders: Vec<Box<dyn ArrayBuilder>> = vec![
            Box::new(Int64Builder::new()),  // id
            Box::new(StringBuilder::new()), // type
            Box::new(MapBuilder::new(
                None,
                StringBuilder::new(),
                StringBuilder::new(),
            )), // tags
            Box::new(
                Decimal128Builder::new()
                    .with_data_type(DataType::Decimal128(DECIMAL128_MAX_PRECISION, 9)),
            ), // lat
            Box::new(
                Decimal128Builder::new()
                    .with_data_type(DataType::Decimal128(DECIMAL128_MAX_PRECISION, 9)),
            ), // lon
            Box::new(nodes_builder),        // nds
            Box::new(members_builder),      // members
            Box::new(Int64Builder::new()),  // changeset
            Box::new(Int64Builder::new()),  // timestamp
            Box::new(Int32Builder::new()),  // uid
            Box::new(StringBuilder::new()), // user
            Box::new(Int32Builder::new()),  // version
            Box::new(BooleanBuilder::new()), // visible
        ];

        Ok(ElementSink {
            builders: data_builders,
            num_elements: 0,
            filenum,
        })
    }

    pub fn finish_batch(&mut self) -> () {
        let file = File::create(Self::new_file_path(&self.filenum)).unwrap();

        let array_refs: Vec<ArrayRef> = self
            .builders
            .iter_mut()
            .map(|builder| builder.finish())
            .collect();

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
        let batch = RecordBatch::try_from_iter(vec![
            ("id", array_refs[0].clone()),
            ("type", array_refs[1].clone()),
            ("tags", array_refs[2].clone()),
            ("lat", array_refs[3].clone()),
            ("lon", array_refs[4].clone()),
            ("nds", array_refs[5].clone()),
            ("members", array_refs[6].clone()),
            ("changeset", array_refs[7].clone()),
            ("timestamp", array_refs[8].clone()),
            ("uid", array_refs[9].clone()),
            ("user", array_refs[10].clone()),
            ("version", array_refs[11].clone()),
            ("visible", array_refs[12].clone()),
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

    pub fn add_node(&mut self, node: &Node) -> Result<(), std::io::Error> {
        let info = node.info();
        self.append_feature_values(
            node.id(),
            "node".to_string(),
            info.changeset(),
            info.milli_timestamp(),
            info.uid(),
            info.version(),
            info.visible(),
        );

        self.add_node_coords(node.nano_lat(), node.nano_lon());
        self.add_tags(node.tags());

        self.increment_and_cycle()
    }

    pub fn add_dense_node(&mut self, node: &DenseNode) -> Result<(), std::io::Error> {
        if let Some(info) = node.info() {
            self.append_feature_values(
                node.id(),
                "node".to_string(),
                Some(info.changeset()),
                Some(info.milli_timestamp()),
                Some(info.uid()),
                Some(info.version()),
                info.visible(),
            );
        } else {
            self.append_feature_values(node.id(), "node".to_string(), None, None, None, None, true);
        }

        // Tags
        let tags_builder = self.builders[2]
            .as_any_mut()
            .downcast_mut::<MapBuilder<StringBuilder, StringBuilder>>()
            .unwrap();
        for (key, value) in node.tags() {
            tags_builder.keys().append_value(key.to_string());
            tags_builder.values().append_value(value.to_string());
        }
        let _ = tags_builder.append(true);

        self.add_node_coords(node.nano_lat(), node.nano_lon());

        self.increment_and_cycle()
    }

    pub fn add_node_coords(&mut self, lat: i64, lon: i64) {
        self.builders[3]
            .as_any_mut()
            .downcast_mut::<Decimal128Builder>()
            .unwrap()
            .append_value(lat as i128);
        self.builders[4]
            .as_any_mut()
            .downcast_mut::<Decimal128Builder>()
            .unwrap()
            .append_value(lon as i128);
    }

    pub fn add_way(&mut self, way: &Way) -> Result<(), std::io::Error> {
        let info = way.info();
        self.append_feature_values(
            way.id(),
            "way".to_string(),
            info.changeset(),
            info.milli_timestamp(),
            info.uid(),
            info.version(),
            info.visible(),
        );
        self.add_tags(way.tags());

        // Derived from https://docs.rs/arrow/latest/arrow/array/struct.StructBuilder.html
        let nodes_builder = self.builders[5]
            .as_any_mut()
            .downcast_mut::<ListBuilder<StructBuilder>>()
            .unwrap();

        let struct_builder = nodes_builder.values();

        for way_ref in way.refs() {
            struct_builder
                .field_builder::<Int64Builder>(0)
                .unwrap()
                .append_value(way_ref);
            struct_builder.append(true);
        }

        nodes_builder.append(true);

        self.increment_and_cycle()
    }

    pub fn add_relation(&mut self, relation: &Relation) -> Result<(), std::io::Error> {
        let info = relation.info();
        self.append_feature_values(
            relation.id(),
            "relation".to_string(),
            info.changeset(),
            info.milli_timestamp(),
            info.uid(),
            info.version(),
            info.visible(),
        );
        self.add_tags(relation.tags());

        // Derived from https://docs.rs/arrow/latest/arrow/array/struct.StructBuilder.html
        let members_builder = self.builders[6]
            .as_any_mut()
            .downcast_mut::<ListBuilder<StructBuilder>>()
            .unwrap();

        let struct_builder = members_builder.values();

        for member in relation.members() {
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

        self.increment_and_cycle()
    }

    fn append_feature_values<'a>(
        &mut self,
        id: i64,
        feature_type: String,
        changeset: Option<i64>,
        timestamp: Option<i64>,
        uid: Option<i32>,
        version: Option<i32>,
        visible: bool,
    ) -> () {
        // Logic by feature type
        if feature_type != "node" {
            // Lat/lon builders
            self.builders[3]
                .as_any_mut()
                .downcast_mut::<Decimal128Builder>()
                .unwrap()
                .append_null();
            self.builders[4]
                .as_any_mut()
                .downcast_mut::<Decimal128Builder>()
                .unwrap()
                .append_null();
        }
        if feature_type != "way" {
            // Node builder
            self.builders[5]
                .as_any_mut()
                .downcast_mut::<ListBuilder<StructBuilder>>()
                .unwrap()
                .append_null();
        }
        if feature_type != "relation" {
            // Members builder
            self.builders[6]
                .as_any_mut()
                .downcast_mut::<ListBuilder<StructBuilder>>()
                .unwrap()
                .append_null();
        }

        // Simple values
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
        let _changeset_builder = self.builders[7]
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .unwrap()
            .append_option(changeset); // TODO - always 0?
        let _timestamp_builder = self.builders[8]
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .unwrap()
            .append_option(timestamp);
        let _uid_builder = self.builders[9]
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .unwrap()
            .append_option(uid);
        let _user_builder = self.builders[10]
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .unwrap()
            .append_null(); // TODO
        let _version_builder = self.builders[11]
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .unwrap()
            .append_option(version);
        let _type_builder = self.builders[12]
            .as_any_mut()
            .downcast_mut::<BooleanBuilder>()
            .unwrap()
            .append_value(visible);
    }

    fn add_tags<'a>(&mut self, tag_iter: TagIter<'a>) -> () {
        let tags_builder = self.builders[2]
            .as_any_mut()
            .downcast_mut::<MapBuilder<StringBuilder, StringBuilder>>()
            .unwrap();
        for (key, value) in tag_iter {
            tags_builder.keys().append_value(key.to_string());
            tags_builder.values().append_value(value.to_string());
        }
        let _ = tags_builder.append(true);
    }
}
