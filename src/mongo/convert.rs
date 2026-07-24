//! BSON to Arrow conversion.
//!
//! Conversion is driven entirely by the *frozen* schema from the catalog, never
//! by the documents themselves. That inversion is what removes Drill's
//! mid-scan `SchemaChangeException`: a document whose field types disagree with
//! the schema is coerced or nulled per column, and the scan keeps going.
//!
//! Columns are built one at a time rather than row-by-row into a `StructBuilder`
//! — nested types stay readable that way, and the per-column loop is what Arrow
//! wants anyway.

use std::sync::Arc;

use bson::{Bson, Document};
use datafusion::arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Decimal128Array, Float64Array, Int32Array, Int64Array,
    ListArray, StringArray, StructArray, TimestampMillisecondArray,
};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};

use crate::catalog::infer::BsonTag;

/// Build one `RecordBatch` from a slice of documents, following `schema`.
pub fn documents_to_batch(schema: &SchemaRef, docs: &[Document]) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    // Reused across columns to avoid one allocation per field per batch.
    let mut slots: Vec<Option<&Bson>> = Vec::with_capacity(docs.len());

    for field in schema.fields() {
        slots.clear();
        // A projected schema uses the leaf name, but the value lives at the
        // recorded path, so walk it rather than doing a flat `get`.
        let path = crate::catalog::infer::field_path(field);
        slots.extend(docs.iter().map(|d| lookup_path(d, &path)));
        columns.push(build_array(field, &slots)?);
    }

    let options = datafusion::arrow::record_batch::RecordBatchOptions::new()
        .with_row_count(Some(docs.len()));
    RecordBatch::try_new_with_options(Arc::clone(schema), columns, &options)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

/// Follow a dotted path into a document. Returns `None` for a missing field,
/// which the caller renders as SQL NULL.
fn lookup_path<'a>(doc: &'a Document, path: &str) -> Option<&'a Bson> {
    let mut parts = path.split('.');
    let first = parts.next()?;
    let mut current = doc.get(first)?;
    for part in parts {
        match current {
            Bson::Document(inner) => current = inner.get(part)?,
            _ => return None,
        }
    }
    Some(current)
}

/// True for values that must become SQL NULL regardless of the target type.
fn is_null(value: Option<&Bson>) -> bool {
    matches!(value, None | Some(Bson::Null) | Some(Bson::Undefined))
}

pub fn build_array(field: &Field, values: &[Option<&Bson>]) -> Result<ArrayRef> {
    let n = values.len();
    Ok(match field.data_type() {
        DataType::Boolean => {
            let it = values.iter().map(|v| match v {
                Some(Bson::Boolean(b)) => Some(*b),
                _ => None,
            });
            Arc::new(it.collect::<BooleanArray>())
        }

        DataType::Int32 => {
            let it = values.iter().map(|v| as_i64(*v).and_then(|i| i32::try_from(i).ok()));
            Arc::new(it.collect::<Int32Array>())
        }
        DataType::Int64 => {
            let it = values.iter().map(|v| as_i64(*v));
            Arc::new(it.collect::<Int64Array>())
        }
        DataType::Float64 => {
            let it = values.iter().map(|v| as_f64(*v));
            Arc::new(it.collect::<Float64Array>())
        }

        DataType::Utf8 => {
            let tag = BsonTag::of(field);
            let mut out: Vec<Option<String>> = Vec::with_capacity(n);
            for v in values {
                out.push(if is_null(*v) { None } else { render_text(v.unwrap(), tag) });
            }
            Arc::new(StringArray::from(out))
        }

        DataType::Binary => {
            let mut out: Vec<Option<&[u8]>> = Vec::with_capacity(n);
            for v in values {
                out.push(match v {
                    Some(Bson::Binary(b)) => Some(b.bytes.as_slice()),
                    _ => None,
                });
            }
            Arc::new(BinaryArray::from(out))
        }

        DataType::Timestamp(TimeUnit::Millisecond, tz) => {
            let it = values.iter().map(|v| as_millis(*v));
            let array: TimestampMillisecondArray = it.collect();
            match tz {
                Some(zone) => Arc::new(array.with_timezone(zone.clone())),
                None => Arc::new(array),
            }
        }

        DataType::Decimal128(precision, scale) => {
            let it = values.iter().map(|v| as_decimal(*v, *scale));
            let array = it.collect::<Decimal128Array>().with_precision_and_scale(*precision, *scale)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
            Arc::new(array)
        }

        DataType::Struct(children) => {
            let mut child_arrays: Vec<ArrayRef> = Vec::with_capacity(children.len());
            let mut slots: Vec<Option<&Bson>> = Vec::with_capacity(n);
            for child in children {
                slots.clear();
                for v in values {
                    slots.push(match v {
                        Some(Bson::Document(d)) => d.get(child.name()),
                        _ => None,
                    });
                }
                child_arrays.push(build_array(child, &slots)?);
            }
            // A row is null when the whole embedded document is absent, which
            // is distinct from an embedded document whose fields are all null.
            let validity: NullBuffer =
                values.iter().map(|v| matches!(v, Some(Bson::Document(_)))).collect();

            if children.is_empty() {
                // Arrow cannot represent a zero-field struct array with a row
                // count, so such columns are inferred as JSON text instead.
                return Err(DataFusionError::Internal(format!(
                    "struct column {} has no fields",
                    field.name()
                )));
            }
            Arc::new(StructArray::new(children.clone(), child_arrays, Some(validity)))
        }

        DataType::List(item) => {
            let mut flat: Vec<Option<&Bson>> = Vec::with_capacity(n);
            let mut offsets: Vec<i32> = Vec::with_capacity(n + 1);
            let mut valid: Vec<bool> = Vec::with_capacity(n);
            offsets.push(0);

            for v in values {
                match v {
                    Some(Bson::Array(items)) => {
                        flat.extend(items.iter().map(Some));
                        valid.push(true);
                    }
                    // Mongo models a lone scalar as a one-element array for
                    // matching purposes; inference agrees, so honour it here.
                    Some(other) if !is_null(*v) => {
                        flat.push(Some(other));
                        valid.push(true);
                    }
                    _ => valid.push(false),
                }
                offsets.push(i32::try_from(flat.len()).map_err(|_| {
                    DataFusionError::Execution(
                        "array column exceeds 2^31 elements in a single batch".into(),
                    )
                })?);
            }

            let child = build_array(item, &flat)?;
            Arc::new(ListArray::new(
                Arc::clone(item),
                OffsetBuffer::new(ScalarBuffer::from(offsets)),
                child,
                Some(NullBuffer::from(valid)),
            ))
        }

        other => {
            return Err(DataFusionError::NotImplemented(format!(
                "no BSON decoder for Arrow type {other}"
            )));
        }
    })
}

// ---------------------------------------------------------------------------
// Scalar coercions
// ---------------------------------------------------------------------------

fn as_i64(value: Option<&Bson>) -> Option<i64> {
    match value? {
        Bson::Int32(v) => Some(*v as i64),
        Bson::Int64(v) => Some(*v),
        // Only exact integers convert; 2.5 in an integer column is a genuine
        // type error for that row and becomes NULL rather than silently 2.
        Bson::Double(v) if v.fract() == 0.0 && v.is_finite() => Some(*v as i64),
        Bson::Boolean(b) => Some(*b as i64),
        _ => None,
    }
}

fn as_f64(value: Option<&Bson>) -> Option<f64> {
    match value? {
        Bson::Double(v) => Some(*v),
        Bson::Int32(v) => Some(*v as f64),
        Bson::Int64(v) => Some(*v as f64),
        Bson::Decimal128(d) => d.to_string().parse().ok(),
        _ => None,
    }
}

fn as_millis(value: Option<&Bson>) -> Option<i64> {
    match value? {
        Bson::DateTime(dt) => Some(dt.timestamp_millis()),
        // A BSON internal timestamp counts whole seconds since the epoch in its
        // high 32 bits; the low bits are an intra-second ordinal, not a time.
        Bson::Timestamp(ts) => (ts.time as i64).checked_mul(1_000),
        Bson::Int64(v) => Some(*v),
        _ => None,
    }
}

fn as_decimal(value: Option<&Bson>, scale: i8) -> Option<i128> {
    let text = match value? {
        Bson::Decimal128(d) => d.to_string(),
        Bson::Int32(v) => return scale_int(*v as i128, scale),
        Bson::Int64(v) => return scale_int(*v as i128, scale),
        Bson::Double(v) => format!("{v}"),
        Bson::String(s) => s.clone(),
        _ => return None,
    };
    parse_decimal(&text, scale)
}

fn scale_int(v: i128, scale: i8) -> Option<i128> {
    let factor = 10i128.checked_pow(u32::try_from(scale.max(0)).ok()?)?;
    v.checked_mul(factor)
}

/// Parse a decimal string into an integer scaled by `10^scale`.
///
/// Handles the plain and scientific forms that `Decimal128`'s display and
/// `f64` formatting can produce (`-12.5`, `1.23E+7`, `NaN`, `Infinity`).
fn parse_decimal(text: &str, scale: i8) -> Option<i128> {
    let text = text.trim();
    if text.is_empty() || text.eq_ignore_ascii_case("nan") {
        return None;
    }

    let (mantissa, exponent) = match text.find(['e', 'E']) {
        Some(pos) => (&text[..pos], text[pos + 1..].parse::<i32>().ok()?),
        None => (text, 0),
    };

    let (negative, digits) = match mantissa.as_bytes().first() {
        Some(b'-') => (true, &mantissa[1..]),
        Some(b'+') => (false, &mantissa[1..]),
        _ => (false, mantissa),
    };
    if digits.eq_ignore_ascii_case("infinity") || digits.eq_ignore_ascii_case("inf") {
        return None;
    }

    let (int_part, frac_part) = match digits.split_once('.') {
        Some((i, f)) => (i, f),
        None => (digits, ""),
    };
    if !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }

    let mut value: i128 = 0;
    for b in int_part.bytes().chain(frac_part.bytes()) {
        value = value.checked_mul(10)?.checked_add((b - b'0') as i128)?;
    }

    // Digits consumed put us at 10^-frac_len; the literal exponent and the
    // target scale move us the rest of the way.
    let shift = scale as i32 + exponent - frac_part.len() as i32;
    value = if shift >= 0 {
        value.checked_mul(10i128.checked_pow(u32::try_from(shift).ok()?)?)?
    } else {
        // Truncation, not rounding: a value with more precision than the column
        // can hold loses its tail rather than being reported as null.
        value / 10i128.checked_pow(u32::try_from(-shift).ok()?)?
    };

    Some(if negative { -value } else { value })
}

/// Render any BSON value as text for a `Utf8` column.
///
/// The `tag` decides the *intent*: an `ObjectId` column renders the canonical
/// 24-character hex, whereas a `json` column (a field whose types conflicted)
/// renders relaxed extended JSON so nothing is lost.
fn render_text(value: &Bson, tag: BsonTag) -> Option<String> {
    Some(match (value, tag) {
        (Bson::String(s), _) | (Bson::Symbol(s), _) => s.clone(),
        (Bson::ObjectId(oid), _) => oid.to_hex(),
        (Bson::Boolean(b), _) => b.to_string(),
        (Bson::Int32(v), _) => v.to_string(),
        (Bson::Int64(v), _) => v.to_string(),
        (Bson::Double(v), _) => v.to_string(),
        (Bson::Decimal128(d), _) => d.to_string(),
        (Bson::DateTime(dt), _) => dt
            .try_to_rfc3339_string()
            .unwrap_or_else(|_| dt.timestamp_millis().to_string()),
        // UUID binary subtypes read back as the canonical 8-4-4-4-12 string —
        // matched regardless of the column tag, so even a mixed column that
        // degraded to JSON still shows a readable UUID rather than raw bytes.
        (Bson::Binary(b), _)
            if b.bytes.len() == 16
                && matches!(
                    b.subtype,
                    bson::spec::BinarySubtype::Uuid | bson::spec::BinarySubtype::UuidOld
                ) =>
        {
            uuid_hyphenated(&b.bytes)
        }
        (Bson::Binary(b), _) => {
            use base64_min::encode;
            encode(&b.bytes)
        }
        // Documents, arrays and the exotic BSON types keep their full fidelity
        // as extended JSON, which is also what the JSON SQL functions expect.
        (other, _) => other.clone().into_relaxed_extjson().to_string(),
    })
}

/// Format 16 raw bytes as a canonical hyphenated UUID (8-4-4-4-12 lowercase
/// hex). The caller guarantees the length.
fn uuid_hyphenated(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(36);
    for (i, &byte) in bytes.iter().enumerate() {
        if matches!(i, 4 | 6 | 8 | 10) {
            s.push('-');
        }
        s.push(HEX[(byte >> 4) as usize] as char);
        s.push(HEX[(byte & 0x0f) as usize] as char);
    }
    s
}

/// Minimal base64 encoder — pulling a dependency for eleven lines of table
/// lookup is not worth the supply-chain surface.
mod base64_min {
    const ALPHABET: &[u8; 64] =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    pub fn encode(input: &[u8]) -> String {
        let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
        for chunk in input.chunks(3) {
            let b = [chunk[0], *chunk.get(1).unwrap_or(&0), *chunk.get(2).unwrap_or(&0)];
            let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | b[2] as u32;
            out.push(ALPHABET[(n >> 18) as usize & 63] as char);
            out.push(ALPHABET[(n >> 12) as usize & 63] as char);
            out.push(if chunk.len() > 1 { ALPHABET[(n >> 6) as usize & 63] as char } else { '=' });
            out.push(if chunk.len() > 2 { ALPHABET[n as usize & 63] as char } else { '=' });
        }
        out
    }
}

/// Build a schema containing only the projected columns, preserving metadata.
pub fn project_schema(schema: &SchemaRef, projection: Option<&Vec<usize>>) -> Result<SchemaRef> {
    match projection {
        None => Ok(Arc::clone(schema)),
        Some(indices) => {
            let fields: Vec<Arc<Field>> = indices
                .iter()
                .map(|i| {
                    schema.fields().get(*i).cloned().ok_or_else(|| {
                        DataFusionError::Internal(format!("projection index {i} out of range"))
                    })
                })
                .collect::<Result<_>>()?;
            Ok(Arc::new(
                Schema::new(fields).with_metadata(schema.metadata().clone()),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::infer::Sampler;
    use bson::doc;
    use datafusion::arrow::array::{Array, AsArray};
    use datafusion::arrow::datatypes::Int32Type;

    fn batch_of(docs: Vec<Document>) -> RecordBatch {
        let mut s = Sampler::new();
        for d in &docs {
            s.observe(d);
        }
        let schema: SchemaRef = Arc::new(s.finish(4));
        documents_to_batch(&schema, &docs).unwrap()
    }

    #[test]
    fn basic_scalars_round_trip() {
        let b = batch_of(vec![doc! {"n": 1i32, "s": "x", "f": 1.5f64, "t": true}]);
        assert_eq!(b.num_rows(), 1);
        assert_eq!(b.column_by_name("n").unwrap().as_primitive::<Int32Type>().value(0), 1);
        assert_eq!(b.column_by_name("s").unwrap().as_string::<i32>().value(0), "x");
    }

    #[test]
    fn missing_field_becomes_null_not_an_error() {
        let b = batch_of(vec![doc! {"a": 1i32, "b": 2i32}, doc! {"a": 3i32}]);
        let col = b.column_by_name("b").unwrap();
        assert!(!col.is_null(0));
        assert!(col.is_null(1));
    }

    #[test]
    fn type_conflict_renders_as_extended_json() {
        // The scenario that kills a Drill query mid-scan.
        let b = batch_of(vec![doc! {"v": "text"}, doc! {"v": doc! {"a": 1i32}}]);
        let col = b.column_by_name("v").unwrap().as_string::<i32>();
        assert_eq!(col.value(0), "text");
        assert_eq!(col.value(1), r#"{"a":1}"#);
    }

    #[test]
    fn objectid_renders_as_hex() {
        let oid = bson::oid::ObjectId::new();
        let b = batch_of(vec![doc! {"_id": oid}]);
        assert_eq!(
            b.column_by_name("_id").unwrap().as_string::<i32>().value(0),
            oid.to_hex()
        );
    }

    #[test]
    fn nested_documents_become_struct_columns() {
        let b = batch_of(vec![
            doc! {"u": doc! {"name": "a", "age": 30i32}},
            doc! {"u": doc! {"name": "b"}},
            doc! {"other": 1i32},
        ]);
        let s = b.column_by_name("u").unwrap().as_struct();
        assert_eq!(s.column_by_name("name").unwrap().as_string::<i32>().value(0), "a");
        assert!(s.column_by_name("age").unwrap().is_null(1), "absent nested field is null");
        assert!(s.is_null(2), "absent embedded document is a null row");
    }

    #[test]
    fn arrays_become_lists_and_scalars_are_wrapped() {
        let b = batch_of(vec![doc! {"tags": ["a", "b"]}, doc! {"tags": "c"}, doc! {"x": 1i32}]);
        let l = b.column_by_name("tags").unwrap().as_list::<i32>();
        assert_eq!(l.value_length(0), 2);
        assert_eq!(l.value_length(1), 1, "a lone scalar is a one-element list");
        assert!(l.is_null(2));
    }

    #[test]
    fn uuid_binary_renders_as_a_hyphenated_string() {
        let bin = bson::Binary {
            subtype: bson::spec::BinarySubtype::Uuid,
            bytes: (0u8..16).collect(),
        };
        let b = batch_of(vec![doc! {"ref": bin}]);
        assert_eq!(
            b.column_by_name("ref").unwrap().as_string::<i32>().value(0),
            "00010203-0405-0607-0809-0a0b0c0d0e0f"
        );
    }

    #[test]
    fn array_of_uuid_binary_renders_each_element_as_a_string() {
        let mk = |n: u8| Bson::Binary(bson::Binary {
            subtype: bson::spec::BinarySubtype::Uuid,
            bytes: vec![n; 16],
        });
        let b = batch_of(vec![doc! {"ids": vec![mk(0), mk(0xff)]}]);
        let list = b.column_by_name("ids").unwrap().as_list::<i32>();
        let inner = list.value(0);
        let strs = inner.as_string::<i32>();
        assert_eq!(strs.value(0), "00000000-0000-0000-0000-000000000000");
        assert_eq!(strs.value(1), "ffffffff-ffff-ffff-ffff-ffffffffffff");
    }

    #[test]
    fn dates_convert_to_millisecond_timestamps() {
        let dt = bson::DateTime::from_millis(1_700_000_000_000);
        let b = batch_of(vec![doc! {"at": dt}]);
        let col = b.column_by_name("at").unwrap();
        assert!(matches!(
            col.data_type(),
            DataType::Timestamp(TimeUnit::Millisecond, Some(_))
        ));
    }

    #[test]
    fn non_integral_double_in_an_int_column_is_null_not_truncated() {
        let f = Field::new("n", DataType::Int64, true);
        let values = [Some(&Bson::Double(2.5)), Some(&Bson::Double(4.0))];
        let arr = build_array(&f, &values).unwrap();
        let arr = arr.as_primitive::<datafusion::arrow::datatypes::Int64Type>();
        assert!(arr.is_null(0), "2.5 must not silently become 2");
        assert_eq!(arr.value(1), 4);
    }

    #[test]
    fn decimal_strings_parse_with_scale() {
        assert_eq!(parse_decimal("1.5", 2), Some(150));
        assert_eq!(parse_decimal("-0.25", 4), Some(-2500));
        assert_eq!(parse_decimal("1.23E+3", 0), Some(1230));
        assert_eq!(parse_decimal("12", 3), Some(12000));
        // More precision than the column holds is truncated, not rejected.
        assert_eq!(parse_decimal("1.2345", 2), Some(123));
        assert_eq!(parse_decimal("NaN", 2), None);
        assert_eq!(parse_decimal("Infinity", 2), None);
        assert_eq!(parse_decimal("abc", 2), None);
    }

    #[test]
    fn base64_matches_the_reference_vectors() {
        assert_eq!(base64_min::encode(b""), "");
        assert_eq!(base64_min::encode(b"f"), "Zg==");
        assert_eq!(base64_min::encode(b"fo"), "Zm8=");
        assert_eq!(base64_min::encode(b"foo"), "Zm9v");
        assert_eq!(base64_min::encode(b"foobar"), "Zm9vYmFy");
    }

    #[test]
    fn projection_preserves_field_metadata() {
        let mut s = Sampler::new();
        s.observe(&doc! {"a": 1i32, "b": "x"});
        let schema: SchemaRef = Arc::new(s.finish(4));
        let projected = project_schema(&schema, Some(&vec![1])).unwrap();
        assert_eq!(projected.fields().len(), 1);
        assert!(!projected.field(0).metadata().is_empty());
    }

    #[test]
    fn dotted_path_lookup_walks_embedded_documents() {
        let d = doc! {"a": doc! {"b": doc! {"c": 7i32}}};
        assert_eq!(lookup_path(&d, "a.b.c"), Some(&Bson::Int32(7)));
        assert_eq!(lookup_path(&d, "a.missing"), None);
        assert_eq!(lookup_path(&d, "a.b.c.d"), None);
    }

    #[test]
    fn unused_scale_helper_is_exercised() {
        assert_eq!(scale_int(5, 2), Some(500));
    }
}
