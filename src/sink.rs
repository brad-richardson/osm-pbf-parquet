use std::fs::File;
use std::sync::{Arc, Mutex};

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
    estimated_current_size_bytes: usize,
    filenum: Arc<Mutex<u64>>,
    output_dir: String,
    pub osm_type: OSMType,
}

impl ElementSink {
    const MAX_FEATURE_COUNT: u64 = 5_000_000;
    // Balance between memory pressure and parquet block size
    const TARGET_SIZE_BYTES: usize = 250_000_000usize;

    pub fn new(
        filenum: Arc<Mutex<u64>>,
        output_dir: String,
        osm_type: OSMType,
    ) -> Result<Self, std::io::Error> {
        Ok(ElementSink {
            osm_builder: Box::new(OSMArrowBuilder::new()),
            num_elements: 0,
            estimated_current_size_bytes: 0usize,
            filenum,
            output_dir,
            osm_type,
        })
    }

    pub fn finish_batch(&mut self) {
        let path_str = self.new_file_path(&self.filenum);
        let path = std::path::Path::new(&path_str);
        let prefix = path.parent().unwrap();
        std::fs::create_dir_all(prefix).unwrap();
        let file = File::create(path).unwrap();

        let batch = self.osm_builder.finish().unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();

        writer.write(&batch).expect("Writing batch");
        writer.close().unwrap();

        self.num_elements = 0;
        self.estimated_current_size_bytes = 0;
    }

    fn increment_and_cycle(&mut self) -> Result<(), std::io::Error> {
        self.num_elements += 1;
        if self.num_elements >= Self::MAX_FEATURE_COUNT
            || self.estimated_current_size_bytes >= Self::TARGET_SIZE_BYTES
        {
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

        let est_size_bytes = self.osm_builder.append_row(
            node.id(),
            OSMType::Node,
            node.tags()
                .map(|(key, value)| (key.to_string(), value.to_string())),
            Some(node.nano_lat() as i128),
            Some(node.nano_lon() as i128),
            std::iter::empty(),
            std::iter::empty(),
            info.changeset(),
            info.milli_timestamp(),
            info.uid(),
            Some(user),
            info.version(),
            Some(info.visible()),
        );
        self.estimated_current_size_bytes += est_size_bytes;

        self.increment_and_cycle()
    }

    pub fn add_dense_node(&mut self, node: &DenseNode) -> Result<(), std::io::Error> {
        let info = node.info();
        let mut user: Option<String> = None;
        if let Some(info) = info {
            user = Some(info.user().unwrap_or("").to_string());
        }

        let est_size_bytes = self.osm_builder.append_row(
            node.id(),
            OSMType::Node,
            node.tags()
                .map(|(key, value)| (key.to_string(), value.to_string())),
            Some(node.nano_lat() as i128),
            Some(node.nano_lon() as i128),
            std::iter::empty(),
            std::iter::empty(),
            info.map(|info| info.changeset()),
            info.map(|info| info.milli_timestamp()),
            info.map(|info| info.uid()),
            user,
            info.map(|info| info.version()),
            info.map(|info| info.visible()),
        );
        self.estimated_current_size_bytes += est_size_bytes;

        self.increment_and_cycle()
    }

    pub fn add_way(&mut self, way: &Way) -> Result<(), std::io::Error> {
        let info = way.info();
        let user = info
            .user()
            .unwrap_or_else(|| Ok(""))
            .unwrap_or("")
            .to_string();

        let est_size_bytes = self.osm_builder.append_row(
            way.id(),
            OSMType::Way,
            way.tags()
                .map(|(key, value)| (key.to_string(), value.to_string())),
            None,
            None,
            way.refs().map(|id| id),
            std::iter::empty(),
            info.changeset(),
            info.milli_timestamp(),
            info.uid(),
            Some(user),
            info.version(),
            Some(info.visible()),
        );
        self.estimated_current_size_bytes += est_size_bytes;

        self.increment_and_cycle()
    }

    pub fn add_relation(&mut self, relation: &Relation) -> Result<(), std::io::Error> {
        let info = relation.info();
        let user = info
            .user()
            .unwrap_or_else(|| Ok(""))
            .unwrap_or("")
            .to_string();

        // let mut members = Vec::new();
        // for member in relation.members() {
        //     let type_ = match member.member_type {
        //         RelMemberType::Node => OSMType::Node,
        //         RelMemberType::Way => OSMType::Way,
        //         RelMemberType::Relation => OSMType::Relation,
        //     };

        //     let role = match member.role() {
        //         Ok(role) => Some(role.to_string()),
        //         Err(_) => None,
        //     };
        //     members.push((type_, member.member_id, role));
        // }
        let members_iter = relation.members().map(|member| {
            let type_ = match member.member_type {
                RelMemberType::Node => OSMType::Node,
                RelMemberType::Way => OSMType::Way,
                RelMemberType::Relation => OSMType::Relation,
            };

            let role = match member.role() {
                Ok(role) => Some(role.to_string()),
                Err(_) => None,
            };
            return (type_, member.member_id, role);
        });

        let est_size_bytes = self.osm_builder.append_row(
            relation.id(),
            OSMType::Relation,
            relation
                .tags()
                .map(|(key, value)| (key.to_string(), value.to_string())),
            None,
            None,
            std::iter::empty(),
            members_iter,
            info.changeset(),
            info.milli_timestamp(),
            info.uid(),
            Some(user),
            info.version(),
            Some(info.visible()),
        );
        self.estimated_current_size_bytes += est_size_bytes;

        self.increment_and_cycle()
    }
}
