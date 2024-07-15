use std::fmt;
use std::sync::Arc;

use arrow::array::builder::{
    ArrayBuilder, BooleanBuilder, Int64Builder, ListBuilder, MapBuilder, StringBuilder,
    StructBuilder,
};
use arrow::array::{make_builder, Float64Builder, Int32Builder};
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Fields;
use arrow::datatypes::Schema;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum OSMType {
    Node,
    Way,
    Relation,
}

impl fmt::Display for OSMType {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "{}", format!("{:?}", self).to_lowercase())
    }
}

pub fn osm_arrow_schema() -> Schema {
    // Derived from this schema:
    // `id` BIGINT,
    // `tags` MAP <STRING, STRING>,
    // `lat` DOUBLE,
    // `lon` DOUBLE,
    // `nds` ARRAY<STRUCT<ref: BIGINT>>,
    // `members` ARRAY<STRUCT<type: STRING, ref: BIGINT, role: STRING>>,
    // `changeset` BIGINT,
    // `timestamp` TIMESTAMP,
    // `uid` BIGINT,
    // `user` STRING,
    // `version` BIGINT,
    // `visible` BOOLEAN

    // TODO - add type field when not writing with partitions
    // `type` STRING
    // Field::new("type", DataType::Utf8, false)

    Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "tags",
            DataType::Map(Arc::new(Field::new("entries", 
                DataType::Struct(Fields::from(vec![
                    Field::new("keys", DataType::Utf8, false),
                    Field::new("values", DataType::Utf8, true)
                ])),
                false
            )), false),
            // DataType::Dictionary(Box::new(DataType::Utf8), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new("lat", DataType::Float64, true),
        Field::new("lon", DataType::Float64, true),
        Field::new(
            "nds",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(Fields::from(vec![Field::new("ref", DataType::Int64, true)])),
                true,
            ))),
            true,
        ),
        Field::new(
            "members",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(Fields::from(vec![
                    Field::new("type", DataType::Utf8, true),
                    Field::new("ref", DataType::Int64, true),
                    Field::new("role", DataType::Utf8, true),
                ])),
                true,
            ))),
            true,
        ),
        Field::new("changeset", DataType::Int64, true),
        Field::new("timestamp", DataType::Int64, true),
        Field::new("uid", DataType::Int32, true),
        Field::new("user", DataType::Utf8, true),
        Field::new("version", DataType::Int32, true),
        Field::new("visible", DataType::Boolean, true),
    ])
}

pub struct OSMArrowBuilder {
    builders: Vec<Box<dyn ArrayBuilder>>,
    schema: Arc<Schema>,
}

impl Default for OSMArrowBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl OSMArrowBuilder {
    pub fn new() -> Self {
        let schema = osm_arrow_schema();

        let mut builders: Vec<Box<dyn ArrayBuilder>> = Vec::new();
        for field in schema.fields() {
            // Custom builders for `tags`, `nodes`, and `members` as `make_builder` creates a more complex builder structure or doesn't support the type
            if field.name() == "tags" {
                builders.push(Box::new(MapBuilder::new(
                    None,
                    StringBuilder::new(),
                    StringBuilder::new(),
                )));
            } else if field.name() == "nds" {
                builders.push(Box::new(ListBuilder::new(StructBuilder::from_fields(
                    vec![Field::new("ref", DataType::Int64, true)],
                    0,
                ))));
            } else if field.name() == "members" {
                builders.push(Box::new(ListBuilder::new(StructBuilder::from_fields(
                    vec![
                        Field::new("type", DataType::Utf8, true),
                        Field::new("ref", DataType::Int64, true),
                        Field::new("role", DataType::Utf8, true),
                    ],
                    0,
                ))));
            } else {
                builders.push(make_builder(field.data_type(), 0));
            }
        }

        OSMArrowBuilder { builders, schema: Arc::new(schema) }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn append_row<T, N, M>(
        &mut self,
        id: i64,
        _type_: OSMType,
        tags_iter: T,
        lat: Option<f64>,
        lon: Option<f64>,
        nodes_iter: N,
        members_iter: M,
        changeset: Option<i64>,
        timestamp_ms: Option<i64>,
        uid: Option<i32>,
        user: Option<String>,
        version: Option<i32>,
        visible: Option<bool>,
    ) -> usize
    where
        T: IntoIterator<Item = (String, String)>,
        N: IntoIterator<Item = i64>,
        M: IntoIterator<Item = (OSMType, i64, Option<String>)>,
    {
        // Track approximate size of inserted data, starting with known constant sizes
        let mut est_size_bytes = 64usize;

        self.builders[0]
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .unwrap()
            .append_value(id);

        let tags_builder = self.builders[1]
            .as_any_mut()
            .downcast_mut::<MapBuilder<StringBuilder, StringBuilder>>()
            .unwrap();
        for (key, value) in tags_iter {
            est_size_bytes += key.len() + value.len();
            tags_builder.keys().append_value(key);
            tags_builder.values().append_value(value);
        }
        let _ = tags_builder.append(true);

        self.builders[2]
            .as_any_mut()
            .downcast_mut::<Float64Builder>()
            .unwrap()
            .append_option(lat);
        self.builders[3]
            .as_any_mut()
            .downcast_mut::<Float64Builder>()
            .unwrap()
            .append_option(lon);

        // Derived from https://docs.rs/arrow/latest/arrow/array/struct.StructBuilder.html
        let nodes_builder = self.builders[4]
            .as_any_mut()
            .downcast_mut::<ListBuilder<StructBuilder>>()
            .unwrap();

        let struct_builder = nodes_builder.values();

        for node_id in nodes_iter {
            est_size_bytes += 8usize;
            struct_builder
                .field_builder::<Int64Builder>(0)
                .unwrap()
                .append_value(node_id);
            struct_builder.append(true);
        }

        nodes_builder.append(true);

        // Derived from https://docs.rs/arrow/latest/arrow/array/struct.StructBuilder.html
        let members_builder = self.builders[5]
            .as_any_mut()
            .downcast_mut::<ListBuilder<StructBuilder>>()
            .unwrap();

        let members_struct_builder = members_builder.values();

        for (osm_type, ref_, role) in members_iter {
            // Rough size to avoid unwrapping, role should be fairly short.
            est_size_bytes += 10usize;

            members_struct_builder
                .field_builder::<StringBuilder>(0)
                .unwrap()
                .append_value(osm_type.to_string());

            members_struct_builder
                .field_builder::<Int64Builder>(1)
                .unwrap()
                .append_value(ref_);

            members_struct_builder
                .field_builder::<StringBuilder>(2)
                .unwrap()
                .append_option(role);

            members_struct_builder.append(true);
        }

        members_builder.append(true);

        self.builders[6]
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .unwrap()
            .append_option(changeset);
        self.builders[7]
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .unwrap()
            .append_option(timestamp_ms);
        self.builders[8]
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .unwrap()
            .append_option(uid);
        self.builders[9]
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .unwrap()
            .append_option(user);
        self.builders[10]
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .unwrap()
            .append_option(version);
        self.builders[11]
            .as_any_mut()
            .downcast_mut::<BooleanBuilder>()
            .unwrap()
            .append_option(visible);

        // // TODO - write this if not writing with partitions
        // self.builders[12]
        //     .as_any_mut()
        //     .downcast_mut::<StringBuilder>()
        //     .unwrap()
        //     .append_value(type_.to_string());
        est_size_bytes
    }

    pub fn finish(&mut self) -> Result<RecordBatch, ArrowError> {
        let field_arrays_iter = self
            .schema
            .fields()
            .iter()
            .zip(self.builders.iter_mut())
            .map(|(field, builder)| (field.name(), builder.finish()));

        RecordBatch::try_from_iter(field_arrays_iter)
    }
}
