use futures::executor::block_on;
use std::path::absolute;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use osmpbf::{DenseNode, Node, RelMemberType, Relation, Way};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use url::Url;

use crate::osm_arrow::OSMArrowBuilder;
use crate::osm_arrow::OSMType;
use crate::util::{default_record_batch_size, ARGS};

pub struct ElementSink {
    osm_builder: Box<OSMArrowBuilder>,
    num_elements: u64,
    estimated_current_size_bytes: usize,
    filenum: Arc<Mutex<u64>>,
    output_path: String,
    pub osm_type: OSMType,
    target_record_batch_size: usize,
}

impl ElementSink {
    pub fn new(
        filenum: Arc<Mutex<u64>>,
        // output_dir: String,
        osm_type: OSMType,
    ) -> Result<Self, std::io::Error> {
        let args = ARGS.get().unwrap();
        Ok(ElementSink {
            osm_builder: Box::new(OSMArrowBuilder::new()),
            num_elements: 0,
            estimated_current_size_bytes: 0usize,
            filenum,
            output_path: args.output.clone(),
            osm_type,
            target_record_batch_size: args
                .record_batch_target_bytes
                .unwrap_or(default_record_batch_size()),
        })
    }

    pub fn finish_batch(&mut self) {

        let mut props_builder = WriterProperties::builder();
        let args = ARGS.get().unwrap();
        if args.compression == 0 {
            props_builder = props_builder.set_compression(Compression::UNCOMPRESSED);
        } else {
            props_builder = props_builder.set_compression(Compression::ZSTD(
                ZstdLevel::try_new(args.compression as i32).unwrap(),
            ));
        }
        if let Some(max_row_group_size) = args.max_row_group_size {
            props_builder = props_builder.set_max_row_group_size(max_row_group_size);
        }
        let props = props_builder.build();

        let trailing_path = self.new_trailing_path(&self.filenum, args.compression != 0);
        // Remove trailing `/`s to avoid empty path segment
        let full_path = format!(
            "{0}{trailing_path}",
            &self.output_path.trim_end_matches('/')
        );

        let batch = self.osm_builder.finish().unwrap();

        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).expect("Writing batch");
        writer.close().unwrap();

        let payload = PutPayload::from_bytes(Bytes::from(buffer));

        // TODO - create this when sink is created for reuse
        if let Ok(url) = Url::parse(&full_path) {
            let object_store = AmazonS3Builder::from_env()
                .with_url(&full_path)
                .build()
                .unwrap();

            let path = Path::parse(url.path()).unwrap();

            // S3 object store put needs to in a tokio runtime context
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async { object_store.put(&path, payload).await })
                .unwrap_or_else(|_| panic!("Failed to write to path {0}", &path));
        } else {
            let absolute_path = absolute(&full_path).unwrap();
            let store_path = Path::from_absolute_path(&absolute_path).unwrap();
            let object_store = LocalFileSystem::new();

            block_on(object_store.put(&store_path, payload)).unwrap_or_else(|_| panic!("Failed to write to path {0}",
                &absolute_path.display()));
        }

        self.num_elements = 0;
        self.estimated_current_size_bytes = 0;
    }

    fn increment_and_cycle(&mut self) -> Result<(), std::io::Error> {
        self.num_elements += 1;
        if self.estimated_current_size_bytes >= self.target_record_batch_size {
            self.finish_batch();
        }
        Ok(())
    }

    fn new_trailing_path(&self, filenum: &Arc<Mutex<u64>>, is_zstd_compression: bool) -> String {
        let mut num = filenum.lock().unwrap();
        let compression_stem = if is_zstd_compression { ".zstd" } else { "" };
        let path = format!(
            "/type={}/{}_{:04}{}.parquet",
            self.osm_type,
            self.osm_type,
            num,
            compression_stem
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
            Some(node.lat()),
            Some(node.lon()),
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
            Some(node.lat()),
            Some(node.lon()),
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
            way.refs(),
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
            (type_, member.member_id, role)
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
