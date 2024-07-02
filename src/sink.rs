use std::fs::File;
use std::sync::{Arc, Mutex};

use arrow::array::builder::{
    ArrayBuilder, BooleanBuilder, Decimal128Builder, Int64Builder, ListBuilder, MapBuilder,
    StringBuilder, StructBuilder,
};
use arrow::array::{make_builder, ArrayRef, Int32Builder};
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Fields;
use arrow::datatypes::Schema;
use arrow::datatypes::DECIMAL128_MAX_PRECISION;
use arrow::error::ArrowError;
use arrow::ipc::FieldBuilder;
use arrow::record_batch::RecordBatch;
use osmpbf::{DenseNode, Node, RelMemberType, Relation, TagIter, Way};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
// use parquet::data_type::DataType;
use parquet::file::properties::WriterProperties;

use crate::osm_arrow::OSMArrowBuilder;
use crate::osm_arrow::OSMType;

pub struct ElementSink {
    osm_builder: Box<OSMArrowBuilder>,
    num_elements: u64,
    filenum: Arc<Mutex<u64>>,
    output_dir: String,
    pub osm_type: OSMType,
}

impl ElementSink {
    const MAX_ELEMENTS_COUNT: u64 = 1_000_000;

    pub fn new(
        filenum: Arc<Mutex<u64>>,
        output_dir: String,
        osm_type: OSMType,
    ) -> Result<Self, std::io::Error> {
        Ok(ElementSink {
            osm_builder: Box::new(OSMArrowBuilder::new()),
            num_elements: 0,
            filenum,
            output_dir,
            osm_type,
        })
    }

    pub fn finish_batch(&mut self) -> () {
        let file = File::create(self.new_file_path(&self.filenum)).unwrap();

        let batch = self.osm_builder.finish().unwrap();

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

    fn new_file_path(&self, filenum: &Arc<Mutex<u64>>) -> String {
        let mut num = filenum.lock().unwrap();
        let osm_type_str = match self.osm_type {
            OSMType::Node => "node",
            OSMType::Way => "way",
            OSMType::Relation => "relation",
        };
        let path = format!(
            "{}/type={}/{}_{:05}.parquet",
            self.output_dir, osm_type_str, osm_type_str, num
        );
        *num += 1;
        path
    }

    pub fn add_node(&mut self, node: &Node) -> Result<(), std::io::Error> {
        let info = node.info();
        let user = info
            .user()
            .unwrap_or_else(|| Ok(""))
            .unwrap_or("")
            .to_string();

        self.osm_builder.append_row(
            node.id(),
            OSMType::Node,
            Self::process_tags(node.tags()),
            Some(node.nano_lat() as i128),
            Some(node.nano_lon() as i128),
            None,
            None,
            info.changeset(),
            info.milli_timestamp(),
            info.uid(),
            Some(user),
            info.version(),
            Some(info.visible()),
        );

        self.increment_and_cycle()
    }

    pub fn add_dense_node(&mut self, node: &DenseNode) -> Result<(), std::io::Error> {
        let info = node.info();
        let mut user: Option<String> = None;
        if let Some(info) = info {
            user = Some(info.user().unwrap_or("").to_string());
        }

        let mut tags = Vec::new();
        for (key, value) in node.tags() {
            tags.push((key.to_string(), value.to_string()));
        }

        self.osm_builder.append_row(
            node.id(),
            OSMType::Node,
            tags,
            Some(node.nano_lat() as i128),
            Some(node.nano_lon() as i128),
            None,
            None,
            info.map(|info| info.changeset()),
            info.map(|info| info.milli_timestamp()),
            info.map(|info| info.uid()),
            user,
            info.map(|info| info.version()),
            info.map(|info| info.visible()),
        );
        self.increment_and_cycle()
    }

    pub fn add_way(&mut self, way: &Way) -> Result<(), std::io::Error> {
        let info = way.info();
        let user = info
            .user()
            .unwrap_or_else(|| Ok(""))
            .unwrap_or("")
            .to_string();

        let mut nodes = Vec::new();
        for way_ref in way.refs() {
            nodes.push(way_ref);
        }

        self.osm_builder.append_row(
            way.id(),
            OSMType::Way,
            Self::process_tags(way.tags()),
            None,
            None,
            Some(nodes),
            None,
            info.changeset(),
            info.milli_timestamp(),
            info.uid(),
            Some(user),
            info.version(),
            Some(info.visible()),
        );

        self.increment_and_cycle()
    }

    pub fn add_relation(&mut self, relation: &Relation) -> Result<(), std::io::Error> {
        let info = relation.info();
        let user = info
            .user()
            .unwrap_or_else(|| Ok(""))
            .unwrap_or("")
            .to_string();

        let mut members = Vec::new();
        for member in relation.members() {
            let type_ = match member.member_type {
                RelMemberType::Node => OSMType::Node,
                RelMemberType::Way => OSMType::Way,
                RelMemberType::Relation => OSMType::Relation,
            };

            let role = match member.role() {
                Ok(role) => Some(role.to_string()),
                Err(_) => None,
            };
            members.push((type_, member.member_id, role));
        }

        self.osm_builder.append_row(
            relation.id(),
            OSMType::Relation,
            Self::process_tags(relation.tags()),
            None,
            None,
            None,
            Some(members),
            info.changeset(),
            info.milli_timestamp(),
            info.uid(),
            Some(user),
            info.version(),
            Some(info.visible()),
        );

        self.increment_and_cycle()
    }

    fn process_tags<'a>(tag_iter: TagIter<'a>) -> Vec<(String, String)> {
        let mut tags = Vec::new();
        for (key, value) in tag_iter {
            tags.push((key.to_string(), value.to_string()));
        }
        return tags;
    }
}
