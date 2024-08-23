use object_store::buffered::BufWriter;
use std::path::absolute;
use std::sync::{Arc, Mutex};

use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use osmpbf::{DenseNode, Node, RelMemberType, Relation, Way};
use parquet::arrow::async_writer::AsyncArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use tokio::runtime::Runtime;
use url::Url;

use crate::osm_arrow::osm_arrow_schema;
use crate::osm_arrow::OSMArrowBuilder;
use crate::osm_arrow::OSMType;
use crate::util::{default_record_batch_size_mb, ARGS};

pub struct ElementSink {
    // Config for writing file
    pub osm_type: OSMType,
    filenum: Arc<Mutex<u64>>,

    // Arrow wrappers
    osm_builder: Box<OSMArrowBuilder>,
    writer: Option<AsyncArrowWriter<BufWriter>>,

    // State tracking for batching
    estimated_record_batch_bytes: usize,
    estimated_file_bytes: usize,
    target_record_batch_bytes: usize,
    target_file_bytes: usize,
    // tokio_runtime: Arc<Runtime>,
}

impl ElementSink {
    pub fn new(filenum: Arc<Mutex<u64>>, osm_type: OSMType) -> Result<Self, std::io::Error> {
        let args = ARGS.get().unwrap();

        let full_path = Self::create_full_path(&args.output, &osm_type, &filenum, args.compression);
        let buf_writer = Self::create_buf_writer(&full_path);
        let writer = Self::create_writer(buf_writer, args.compression, args.max_row_group_count);

        let target_record_batch_bytes = args
            .record_batch_target_mb
            .unwrap_or(default_record_batch_size_mb())
            * 1_000_000usize;

        Ok(ElementSink {
            osm_type,
            filenum,

            osm_builder: Box::new(OSMArrowBuilder::new()),
            writer: Some(writer),

            estimated_record_batch_bytes: 0usize,
            estimated_file_bytes: 0usize,
            target_record_batch_bytes,
            target_file_bytes: args.file_target_mb * 1_000_000usize,

            // Underlying object store writer (cloud/s3) needs to run in a tokio runtime context
            // tokio_runtime: Arc::new(
            //     tokio::runtime::Builder::new_multi_thread()
            //         .enable_all()
            //         .build()
            //         .unwrap(),
            // ),
        })
    }

    pub async fn finish(&mut self) {
        self.finish_batch().await;
        // let _ = self
        //     .tokio_runtime
        //     .block_on(self.writer.take().unwrap().close());
        self.writer.take().unwrap().close().await;
    }

    async fn finish_batch(&mut self) {
        if self.estimated_record_batch_bytes == 0 {
            // Nothing to write
            return;
        }
        let batch = self.osm_builder.finish().unwrap();
        // let _ = self
        //     .tokio_runtime
        //     .block_on(self.writer.as_mut().unwrap().write(&batch));
        self.writer.as_mut().unwrap().write(&batch).await;

        // Reset writer to new path if needed
        self.estimated_file_bytes += self.estimated_record_batch_bytes;
        if self.estimated_file_bytes >= self.target_file_bytes {
            // let _ = self
            //     .tokio_runtime
            //     .block_on(self.writer.take().unwrap().close());
            self.writer.take().unwrap().close().await;

            // Create new writer and output
            let args = ARGS.get().unwrap();
            let full_path = Self::create_full_path(
                &args.output,
                &self.osm_type,
                &self.filenum,
                args.compression,
            );
            let buf_writer = Self::create_buf_writer(&full_path);
            self.writer = Some(Self::create_writer(
                buf_writer,
                args.compression,
                args.max_row_group_count,
            ));
            self.estimated_file_bytes = 0;
        }

        self.estimated_record_batch_bytes = 0;
    }

    pub async fn increment_and_cycle(&mut self) -> Result<(), std::io::Error> {
        if self.estimated_record_batch_bytes >= self.target_record_batch_bytes {
            self.finish_batch().await;
        }
        Ok(())
    }

    fn create_buf_writer(full_path: &str) -> BufWriter {
        // TODO - better validation of URL/paths here and error handling
        if let Ok(url) = Url::parse(full_path) {
            let s3_store = AmazonS3Builder::from_env()
                .with_url(url.clone())
                .build()
                .unwrap();
            let path = Path::parse(url.path()).unwrap();

            BufWriter::new(Arc::new(s3_store), path)
        } else {
            let object_store = LocalFileSystem::new();
            let absolute_path = absolute(full_path).unwrap();
            let store_path = Path::from_absolute_path(absolute_path).unwrap();

            BufWriter::new(Arc::new(object_store), store_path)
        }
    }

    fn create_writer(
        buffer: BufWriter,
        compression: u8,
        max_row_group_rows: Option<usize>,
    ) -> AsyncArrowWriter<BufWriter> {
        let mut props_builder = WriterProperties::builder();
        if compression == 0 {
            props_builder = props_builder.set_compression(Compression::UNCOMPRESSED);
        } else {
            props_builder = props_builder.set_compression(Compression::ZSTD(
                ZstdLevel::try_new(compression as i32).unwrap(),
            ));
        }
        if let Some(max_rows) = max_row_group_rows {
            props_builder = props_builder.set_max_row_group_size(max_rows);
        }
        let props = props_builder.build();

        AsyncArrowWriter::try_new(buffer, Arc::new(osm_arrow_schema()), Some(props)).unwrap()
    }

    fn create_full_path(
        output_path: &str,
        osm_type: &OSMType,
        filenum: &Arc<Mutex<u64>>,
        compression: u8,
    ) -> String {
        let trailing_path = Self::new_trailing_path(osm_type, filenum, compression != 0);
        // Remove trailing `/`s to avoid empty path segment
        format!("{0}{trailing_path}", &output_path.trim_end_matches('/'))
    }

    fn new_trailing_path(
        osm_type: &OSMType,
        filenum: &Arc<Mutex<u64>>,
        is_zstd_compression: bool,
    ) -> String {
        let mut num = filenum.lock().unwrap();
        let compression_stem = if is_zstd_compression { ".zstd" } else { "" };
        let path = format!(
            "/type={}/{}_{:04}{}.parquet",
            osm_type, osm_type, num, compression_stem
        );
        *num += 1;
        path
    }

    pub fn add_node(&mut self, node: &Node) { //-> Result<(), std::io::Error> {
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
        self.estimated_record_batch_bytes += est_size_bytes;

        // self.increment_and_cycle().await
    }

    pub fn add_dense_node(&mut self, node: &DenseNode) { //-> Result<(), std::io::Error> {
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
        self.estimated_record_batch_bytes += est_size_bytes;

        // self.increment_and_cycle().await
    }

    pub fn add_way(&mut self, way: &Way) { // -> Result<(), std::io::Error> {
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
        self.estimated_record_batch_bytes += est_size_bytes;

        // self.increment_and_cycle().await
    }

    pub fn add_relation(&mut self, relation: &Relation) { // -> Result<(), std::io::Error> {
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
        self.estimated_record_batch_bytes += est_size_bytes;

        // self.increment_and_cycle().await
    }
}
