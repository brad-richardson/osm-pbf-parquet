use futures::executor::block_on;
use object_store::buffered::BufWriter;
use std::borrow::BorrowMut;
use std::cell::{RefCell, RefMut};
use std::path::absolute;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{ObjectStore, WriteMultipart};
use osmpbf::{DenseNode, Node, RelMemberType, Relation, Way};
use parquet::arrow::ArrowWriter;
use parquet::arrow::async_writer::{AsyncArrowWriter, AsyncFileWriter};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use url::Url;

use crate::osm_arrow::OSMArrowBuilder;
use crate::osm_arrow::OSMType;
use crate::osm_arrow::osm_arrow_schema;
use crate::util::{default_record_batch_size, ARGS};

pub struct ElementSink {
    // Config for writing file
    pub osm_type: OSMType,
    filenum: Arc<Mutex<u64>>,
    output_path: String,
    
    // Arrow wrappers
    osm_builder: Box<OSMArrowBuilder>,
    // writer: ArrowWriter<&'a mut Vec<u8>>,
    writer: AsyncArrowWriter<BufWriter>,

    // S3/local writer
    // write: WriteMultipart,
    // buffer: BufWriter,

    // State tracking for batching
    // buffer: Arc<Vec<u8>>,
    num_elements: u64,
    estimated_record_batch_bytes: usize,
    target_record_batch_size: usize,
}

impl ElementSink {
    pub fn new(
        filenum: Arc<Mutex<u64>>,
        osm_type: OSMType,
    ) -> Result<Self, std::io::Error> {
        let args = ARGS.get().unwrap();

        let target_record_batch_size = args
            .record_batch_target_bytes
            .unwrap_or(default_record_batch_size());

        let full_path = Self::create_full_path(&args.output, &osm_type, &filenum, args.compression);

        // TODO - better validation of URL/paths here and error handling
        // let write: WriteMultipart;
        let buffer: BufWriter;
        if let Ok(url) = Url::parse(&full_path) {
            let s3_store = AmazonS3Builder::from_env()
                .with_url(url.clone())
                .build()
                .unwrap();
            let path = Path::parse(url.path()).unwrap();

            // let upload = block_on(
            //     s3_store.put_multipart(&path)
            // ).unwrap();
            buffer = BufWriter::new(Arc::new(s3_store), path);
            // write = WriteMultipart::new_with_chunk_size(upload, target_record_batch_size);
        } else {
            let object_store = LocalFileSystem::new();
            let absolute_path = absolute(&full_path).unwrap();
            let store_path = Path::from_absolute_path(&absolute_path).unwrap();

            // let upload = block_on(
            //     object_store.put_multipart(&store_path)
            // ).unwrap();
            buffer = BufWriter::new(Arc::new(object_store), store_path);
            // write = WriteMultipart::new_with_chunk_size(upload, target_record_batch_size);
        }


        // let wrapped_buffer = Arc::new(Vec::<u8>::with_capacity(target_record_batch_size));
        // let mut unwrapped = ;
        // let test2 = wrapped_buffer.clone().borrow_mut();
        // // let mut test = Rc::as_ref(&wrapped_buffer);
        // // let test2 = wrapped_buffer.borrow_mut();
        // let test3 = Rc::try_unwrap(wrapped_buffer).unwrap();
        // let buffer = BufWriter
        let writer = Self::create_writer(buffer, args.compression, target_record_batch_size);

        Ok(ElementSink {
            osm_type,
            filenum,
            output_path: full_path,

            osm_builder: Box::new(OSMArrowBuilder::new()),
            writer,

            // write,

            // buffer: buffer,
            num_elements: 0,
            estimated_record_batch_bytes: 0usize,
            target_record_batch_size,
        })
    }

    pub fn finish(mut self) {
        self.finish_batch();
        self.writer.close();
        // self.buffer.complete();
        // TODO - re-init writer, buffer
    }

    fn finish_batch(&mut self) {
        if self.num_elements == 0 {
            // Nothing to write
            return
        }
        let batch = self.osm_builder.finish().unwrap();
        // .expect("Failed writing batch");
        
        // TODO - need this?
        // self.writer.flush();

        // S3 object store put needs to in a tokio runtime context
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async { self.writer.write(&batch).await } )
            .unwrap_or_else(|_| panic!("Failed to write batch"));
        // self.write.write(self.buffer);
        // TODO - is this the right call
        // self.write.put(Bytes::from(self.buffer.clone()));

        // TODO - if file is large enough, get a new path

        // self.buffer.clear();
        // self.buffer.flush();
        self.num_elements = 0;
        self.estimated_record_batch_bytes = 0;
    }

    fn create_writer(buffer: BufWriter, compression: u8, max_row_group_size: usize) -> AsyncArrowWriter<BufWriter> {
        let mut props_builder = WriterProperties::builder().set_max_row_group_size(max_row_group_size);
        if compression == 0 {
            props_builder = props_builder.set_compression(Compression::UNCOMPRESSED);
        } else {
            props_builder = props_builder.set_compression(Compression::ZSTD(
                ZstdLevel::try_new(compression as i32).unwrap(),
            ));
        }
        let props = props_builder.build();

        AsyncArrowWriter::try_new(buffer, Arc::new(osm_arrow_schema()), Some(props)).unwrap()
    }

    fn increment_and_cycle(&mut self) -> Result<(), std::io::Error> {
        self.num_elements += 1;
        if self.estimated_record_batch_bytes >= self.target_record_batch_size {
            self.finish_batch();
        }
        Ok(())
    }

    fn create_full_path(output_path: &str, osm_type: &OSMType, filenum: &Arc<Mutex<u64>>, compression: u8) -> String {
        let trailing_path = Self::new_trailing_path(osm_type, filenum, compression != 0);
        // Remove trailing `/`s to avoid empty path segment
        format!(
            "{0}{trailing_path}",
            &output_path.trim_end_matches('/')
        )
    }

    fn new_trailing_path(osm_type: &OSMType, filenum: &Arc<Mutex<u64>>, is_zstd_compression: bool) -> String {
        let mut num = filenum.lock().unwrap();
        let compression_stem = if is_zstd_compression { ".zstd" } else { "" };
        let path = format!(
            "/type={}/{}_{:04}{}.parquet",
            osm_type, osm_type, num, compression_stem
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
        self.estimated_record_batch_bytes += est_size_bytes;

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
        self.estimated_record_batch_bytes += est_size_bytes;

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
        self.estimated_record_batch_bytes += est_size_bytes;

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
        self.estimated_record_batch_bytes += est_size_bytes;

        self.increment_and_cycle()
    }
}
