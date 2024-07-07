use std::sync::Arc;

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
use arrow::record_batch::RecordBatch;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum OSMType {
    Node,
    Way,
    Relation,
}

pub fn osm_arrow_schema(lat_decimal_scale: i8, lon_decimal_scale: i8) -> Schema {
    // Derived from this schema:
    // `id` BIGINT,
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

    // TODO - add type field when not writing with partitions
    // `type` STRING
    // Field::new("type", DataType::Utf8, false)

    Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "tags",
            DataType::Dictionary(Box::new(DataType::Utf8), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new(
            "lat",
            DataType::Decimal128(DECIMAL128_MAX_PRECISION, lat_decimal_scale),
            true,
        ),
        Field::new(
            "lon",
            DataType::Decimal128(DECIMAL128_MAX_PRECISION, lon_decimal_scale),
            true,
        ),
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
}

impl OSMArrowBuilder {
    const DEFAULT_DECIMAL_SCALE: i8 = 9;

    pub fn new() -> Self {
        Self::new_params(Self::DEFAULT_DECIMAL_SCALE, Self::DEFAULT_DECIMAL_SCALE)
    }

    pub fn new_params(lat_decimal_scale: i8, lon_decimal_scale: i8) -> Self {
        let arrow_schema = osm_arrow_schema(lat_decimal_scale, lon_decimal_scale);

        let mut builders: Vec<Box<dyn ArrayBuilder>> = Vec::new();
        for field in arrow_schema.fields() {
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

        OSMArrowBuilder { builders }
    }

    pub fn append_row<T, N, M>(
        &mut self,
        id: i64,
        _type_: OSMType,
        tags_iter: T,
        lat: Option<i128>,
        lon: Option<i128>,
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
            .downcast_mut::<Decimal128Builder>()
            .unwrap()
            .append_option(lat);
        self.builders[3]
            .as_any_mut()
            .downcast_mut::<Decimal128Builder>()
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

            let type_builder = members_struct_builder
                .field_builder::<StringBuilder>(0)
                .unwrap();
            match osm_type {
                OSMType::Node => type_builder.append_value("node"),
                OSMType::Way => type_builder.append_value("way"),
                OSMType::Relation => type_builder.append_value("relation"),
            }

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

        // let feature_type = match type_ {
        //     OSMType::Node => "node",
        //     OSMType::Way => "way",
        //     OSMType::Relation => "relation",
        // };
        // // TODO - write this if not writing with partitions
        // self.builders[12]
        //     .as_any_mut()
        //     .downcast_mut::<StringBuilder>()
        //     .unwrap()
        //     .append_value(feature_type);
        return est_size_bytes;
    }

    pub fn finish(&mut self) -> Result<RecordBatch, ArrowError> {
        let array_refs: Vec<ArrayRef> = self
            .builders
            .iter_mut()
            .map(|builder| builder.finish())
            .collect();

        let schema = osm_arrow_schema(Self::DEFAULT_DECIMAL_SCALE, Self::DEFAULT_DECIMAL_SCALE);

        let field_arrays_iter = schema
            .fields()
            .iter()
            .zip(array_refs.iter())
            .map(|(field, array)| (field.name(), array.clone()));

        RecordBatch::try_from_iter(field_arrays_iter)
    }
}
