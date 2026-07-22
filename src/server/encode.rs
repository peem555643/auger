//! Arrow to PostgreSQL wire encoding.
//!
//! Two responsibilities: describe the result columns as PostgreSQL types so a
//! client knows what it is receiving, and turn each Arrow row into a `DataRow`.
//! Encoding is done per batch and streamed, so `SELECT` over a large collection
//! never materialises the whole result in the gateway.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, BooleanArray, Decimal128Array, StringArray,
    StructArray,
};
use datafusion::arrow::datatypes::{
    DataType, Field, Int32Type, Int64Type, Float64Type, Schema, TimeUnit,
    TimestampMillisecondType,
};
use datafusion::arrow::record_batch::RecordBatch;
use pgwire::api::Type;
use pgwire::api::portal::Format;
use pgwire::api::results::{DataRowEncoder, FieldInfo};
use pgwire::error::PgWireResult;
use pgwire::messages::data::DataRow;

/// Map an Arrow type onto the PostgreSQL type a client should see.
///
/// Composite Arrow types (structs, lists) are reported as `TEXT` holding JSON.
/// Reporting them as PostgreSQL composite or array types would require every
/// nested element to share one type, which is exactly the assumption BSON
/// documents violate.
pub fn pg_type_of(dt: &DataType) -> Type {
    match dt {
        DataType::Boolean => Type::BOOL,
        DataType::Int8 | DataType::Int16 => Type::INT2,
        DataType::Int32 | DataType::UInt8 | DataType::UInt16 => Type::INT4,
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => Type::INT8,
        DataType::Float16 | DataType::Float32 => Type::FLOAT4,
        DataType::Float64 => Type::FLOAT8,
        DataType::Decimal128(..) | DataType::Decimal256(..) => Type::NUMERIC,
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => Type::BYTEA,
        DataType::Date32 | DataType::Date64 => Type::DATE,
        DataType::Timestamp(_, Some(_)) => Type::TIMESTAMPTZ,
        DataType::Timestamp(_, None) => Type::TIMESTAMP,
        _ => Type::TEXT,
    }
}

/// Build the row description for a result schema.
pub fn field_infos(schema: &Schema, format: &Format) -> Vec<FieldInfo> {
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, f)| {
            FieldInfo::new(
                f.name().clone(),
                None,
                None,
                pg_type_of(f.data_type()),
                format.format_for(idx),
            )
        })
        .collect()
}

/// Encode every row of `batch` into wire rows.
pub fn encode_batch(
    batch: &RecordBatch,
    fields: &Arc<Vec<FieldInfo>>,
) -> PgWireResult<Vec<DataRow>> {
    let mut rows = Vec::with_capacity(batch.num_rows());
    let columns = batch.columns();

    for row in 0..batch.num_rows() {
        let mut encoder = DataRowEncoder::new(Arc::clone(fields));
        for column in columns {
            encode_value(&mut encoder, column, row)?;
        }
        rows.push(encoder.take_row());
    }
    Ok(rows)
}

fn encode_value(encoder: &mut DataRowEncoder, array: &ArrayRef, row: usize) -> PgWireResult<()> {
    if array.is_null(row) {
        // The concrete type is irrelevant for a null; only the -1 length is
        // written to the wire.
        return encoder.encode_field(&None::<&str>);
    }

    match array.data_type() {
        DataType::Boolean => {
            let a = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            encoder.encode_field(&a.value(row))
        }
        DataType::Int32 => encoder.encode_field(&array.as_primitive::<Int32Type>().value(row)),
        DataType::Int64 => encoder.encode_field(&array.as_primitive::<Int64Type>().value(row)),
        DataType::Float64 => encoder.encode_field(&array.as_primitive::<Float64Type>().value(row)),

        DataType::Utf8 => {
            let a = array.as_any().downcast_ref::<StringArray>().unwrap();
            encoder.encode_field(&a.value(row))
        }
        DataType::Binary => {
            let a = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            encoder.encode_field(&a.value(row))
        }

        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let millis = array.as_primitive::<TimestampMillisecondType>().value(row);
            match chrono::DateTime::from_timestamp_millis(millis) {
                Some(dt) => encoder.encode_field(&dt),
                // Outside chrono's range: send the raw epoch value as text
                // rather than dropping the row.
                None => encoder.encode_field(&millis.to_string()),
            }
        }

        DataType::Decimal128(_, scale) => {
            let a = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let raw = a.value(row);
            // `rust_decimal` holds 96 bits of mantissa; a Decimal128 value that
            // does not fit is sent as its exact text form instead of being
            // rounded silently.
            match rust_decimal::Decimal::try_from_i128_with_scale(raw, (*scale).max(0) as u32) {
                Ok(d) => encoder.encode_field(&d),
                Err(_) => encoder.encode_field(&a.value_as_string(row)),
            }
        }

        // Everything else — structs, lists, and any type the planner produced
        // that has no PostgreSQL equivalent — goes out as JSON text.
        _ => {
            let json = value_to_json(array, row);
            encoder.encode_field(&json)
        }
    }
}

/// Render one Arrow value as JSON.
///
/// Used for the composite columns that a document database naturally produces.
/// Written by hand rather than pulled from `arrow-json` so that the output
/// matches what [`crate::mongo::convert`] puts in a JSON-typed column — a query
/// that mixes the two should not produce two different renderings of a value.
pub fn value_to_json(array: &ArrayRef, row: usize) -> String {
    if array.is_null(row) {
        return "null".to_string();
    }
    match array.data_type() {
        DataType::Boolean => array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(row)
            .to_string(),
        DataType::Int32 => array.as_primitive::<Int32Type>().value(row).to_string(),
        DataType::Int64 => array.as_primitive::<Int64Type>().value(row).to_string(),
        DataType::Float64 => {
            let v = array.as_primitive::<Float64Type>().value(row);
            // JSON has no NaN or Infinity.
            if v.is_finite() { v.to_string() } else { "null".to_string() }
        }
        DataType::Utf8 => {
            let a = array.as_any().downcast_ref::<StringArray>().unwrap();
            quote_json(a.value(row))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let millis = array.as_primitive::<TimestampMillisecondType>().value(row);
            match chrono::DateTime::from_timestamp_millis(millis) {
                Some(dt) => quote_json(&dt.to_rfc3339()),
                None => millis.to_string(),
            }
        }
        DataType::Decimal128(..) => array
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .value_as_string(row),
        DataType::Binary => {
            let a = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            quote_json(&hex(a.value(row)))
        }
        DataType::Struct(fields) => {
            let s = array.as_any().downcast_ref::<StructArray>().unwrap();
            let parts: Vec<String> = fields
                .iter()
                .enumerate()
                .map(|(i, f): (usize, &Arc<Field>)| {
                    format!("{}:{}", quote_json(f.name()), value_to_json(s.column(i), row))
                })
                .collect();
            format!("{{{}}}", parts.join(","))
        }
        DataType::List(_) => {
            let l = array.as_list::<i32>();
            let values = l.value(row);
            let parts: Vec<String> =
                (0..values.len()).map(|i| value_to_json(&values, i)).collect();
            format!("[{}]", parts.join(","))
        }
        // A type we do not model explicitly still has to produce something
        // legible rather than an error mid-result.
        _ => quote_json(&format!("{:?}", array.slice(row, 1))),
    }
}

fn quote_json(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, ListArray};
    use datafusion::arrow::buffer::{OffsetBuffer, ScalarBuffer};

    #[test]
    fn arrow_types_map_to_sensible_pg_types() {
        assert_eq!(pg_type_of(&DataType::Int64), Type::INT8);
        assert_eq!(pg_type_of(&DataType::Utf8), Type::TEXT);
        assert_eq!(
            pg_type_of(&DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))),
            Type::TIMESTAMPTZ
        );
        // Composite types are text-encoded JSON: a document database cannot
        // promise a single element type for an array column.
        assert_eq!(
            pg_type_of(&DataType::Struct(vec![Field::new("a", DataType::Int32, true)].into())),
            Type::TEXT
        );
    }

    #[test]
    fn json_strings_are_escaped() {
        assert_eq!(quote_json("a\"b"), "\"a\\\"b\"");
        assert_eq!(quote_json("line\nbreak"), "\"line\\nbreak\"");
        assert_eq!(quote_json("tab\there"), "\"tab\\there\"");
        assert_eq!(quote_json("back\\slash"), "\"back\\\\slash\"");

        // A raw control character makes the whole document unparseable, so it
        // has to come out as a \u escape rather than pass through.
        let escaped = quote_json(&char::from(1u8).to_string());
        assert_eq!(escaped, "\"\\u0001\"");
    }

    #[test]
    fn struct_values_render_as_json_objects() {
        let inner = Arc::new(Int32Array::from(vec![Some(1), None])) as ArrayRef;
        let s = StructArray::from(vec![(
            Arc::new(Field::new("n", DataType::Int32, true)),
            inner,
        )]);
        let array = Arc::new(s) as ArrayRef;
        assert_eq!(value_to_json(&array, 0), r#"{"n":1}"#);
        assert_eq!(value_to_json(&array, 1), r#"{"n":null}"#);
    }

    #[test]
    fn list_values_render_as_json_arrays() {
        let values = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let list = ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 3])),
            values,
            None,
        );
        let array = Arc::new(list) as ArrayRef;
        assert_eq!(value_to_json(&array, 0), "[1,2]");
        assert_eq!(value_to_json(&array, 1), "[3]");
    }

    #[test]
    fn non_finite_floats_become_json_null() {
        use datafusion::arrow::array::Float64Array;
        let a = Arc::new(Float64Array::from(vec![f64::NAN, 1.5])) as ArrayRef;
        assert_eq!(value_to_json(&a, 0), "null");
        assert_eq!(value_to_json(&a, 1), "1.5");
    }

    #[test]
    fn hex_encoding_is_lowercase_and_padded() {
        assert_eq!(hex(&[0x00, 0x0f, 0xff]), "000fff");
    }
}
