//! Sampling-based schema inference for heterogeneous BSON collections.
//!
//! The design goal here is the thing Apache Drill gets wrong. Drill infers a
//! schema per *record batch* while the query is already running, so a document
//! that introduces a new field or changes a field's type mid-scan aborts the
//! query with a `SchemaChangeException`. Auger instead:
//!
//! 1. samples up front, combining a uniform `$sample` with a recency-biased
//!    tail (new fields appear in new documents first, and `$sample` alone
//!    routinely misses them),
//! 2. records a *type histogram* per field rather than "last type wins",
//! 3. resolves conflicts through a total lattice ([`unify`]) that always has an
//!    answer — worst case a field degrades to canonical extended JSON text
//!    instead of failing the query,
//! 4. freezes the result in a persistent catalog so the schema a query planned
//!    against is the schema it executes against.
//!
//! Every inferred [`Field`] carries metadata describing the BSON type it came
//! from. That is what lets filter pushdown build a *typed* literal later on —
//! comparing an `ObjectId` column against `'65f...'` has to emit
//! `ObjectId("65f...")`, not the string, or Mongo silently matches nothing.

use std::collections::BTreeMap;

use bson::{Bson, Document};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};

/// Metadata key: the BSON type this column was inferred from.
pub const META_BSON: &str = "auger.bson";
/// Metadata key: dotted path to the value inside the Mongo document.
pub const META_PATH: &str = "auger.path";
/// Metadata key: fraction of sampled documents that contained this field.
pub const META_FREQ: &str = "auger.freq";
/// Metadata key: `"true"` when values of incompatible types were coerced.
/// Filters on such a column can never be pushed down as `Exact`.
pub const META_MIXED: &str = "auger.mixed";

/// Precision/scale used when a collection stores `Decimal128`.
pub const DECIMAL_PRECISION: u8 = 38;
pub const DECIMAL_SCALE: i8 = 10;

/// The BSON provenance of an Arrow column, preserved so that pushdown can
/// rebuild literals in the representation Mongo actually stores.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BsonTag {
    Bool,
    Int32,
    Int64,
    Double,
    Decimal,
    String,
    ObjectId,
    DateTime,
    Timestamp,
    Binary,
    Document,
    Array,
    /// Heterogeneous or unrepresentable — stored as extended JSON text.
    Json,
    /// Only nulls were observed; the column exists but has no known type.
    Unknown,
}

impl BsonTag {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Bool => "bool",
            Self::Int32 => "int32",
            Self::Int64 => "int64",
            Self::Double => "double",
            Self::Decimal => "decimal",
            Self::String => "string",
            Self::ObjectId => "objectId",
            Self::DateTime => "date",
            Self::Timestamp => "timestamp",
            Self::Binary => "binary",
            Self::Document => "document",
            Self::Array => "array",
            Self::Json => "json",
            Self::Unknown => "unknown",
        }
    }

    pub fn parse(s: &str) -> Self {
        match s {
            "bool" => Self::Bool,
            "int32" => Self::Int32,
            "int64" => Self::Int64,
            "double" => Self::Double,
            "decimal" => Self::Decimal,
            "string" => Self::String,
            "objectId" => Self::ObjectId,
            "date" => Self::DateTime,
            "timestamp" => Self::Timestamp,
            "binary" => Self::Binary,
            "document" => Self::Document,
            "array" => Self::Array,
            "json" => Self::Json,
            _ => Self::Unknown,
        }
    }

    /// Read the tag back off an Arrow field.
    pub fn of(field: &Field) -> Self {
        field
            .metadata()
            .get(META_BSON)
            .map(|s| Self::parse(s))
            .unwrap_or(Self::Unknown)
    }
}

// ---------------------------------------------------------------------------
// Shape lattice
// ---------------------------------------------------------------------------

/// An inferred type. Every pair of shapes has a join, so inference never fails.
#[derive(Debug, Clone, PartialEq)]
pub enum Shape {
    /// Nothing but null/missing observed so far — the lattice bottom.
    Unknown,
    Bool,
    Int32,
    Int64,
    Double,
    Decimal,
    Utf8,
    ObjectId,
    DateTime,
    BsonTimestamp,
    Binary,
    Document(BTreeMap<String, FieldShape>),
    Array(Box<Shape>),
    /// The lattice top: anything that could not be represented more precisely.
    Json,
}

/// A field's shape plus the occurrence counters that drive nullability and
/// the rare-field heuristic.
#[derive(Debug, Clone, PartialEq)]
pub struct FieldShape {
    pub shape: Shape,
    /// Documents in which the key was present at all.
    pub present: u64,
    /// Documents in which the key was present but explicitly null/undefined.
    pub nulls: u64,
    /// Whether reaching this shape required coercing across type families.
    pub mixed: bool,
}

impl FieldShape {
    fn empty() -> Self {
        Self { shape: Shape::Unknown, present: 0, nulls: 0, mixed: false }
    }
}

/// Join two shapes. Commutative and associative; `Json` absorbs everything.
pub fn unify(a: Shape, b: Shape, mixed: &mut bool) -> Shape {
    use Shape::*;

    if a == b {
        return a;
    }
    match (a, b) {
        (Unknown, other) | (other, Unknown) => other,
        (Json, _) | (_, Json) => {
            *mixed = true;
            Json
        }

        // Numeric widening: safe, keeps the column arithmetic-capable.
        (Int32, Int64) | (Int64, Int32) => Int64,
        (Int32, Double) | (Double, Int32) | (Int64, Double) | (Double, Int64) => Double,
        (Decimal, Int32 | Int64 | Double) | (Int32 | Int64 | Double, Decimal) => Decimal,

        // ObjectId and its hex rendering interchange cleanly; strings win
        // because that is what a SQL user can actually type in a predicate.
        (ObjectId, Utf8) | (Utf8, ObjectId) => {
            *mixed = true;
            Utf8
        }

        // The two BSON time types both land on a millisecond timestamp.
        (DateTime, BsonTimestamp) | (BsonTimestamp, DateTime) => DateTime,

        // Two arrays merge element-wise. This case MUST come before the
        // scalar-absorption arm below: that arm binds one array as the "other"
        // (scalar) operand and wraps it, so `unify(Array(a), Array(b))` would
        // become `Array(Array(unify(a, b)))` — a spurious extra level on every
        // merge. A field seen as an array in N sampled documents would then nest
        // N deep and, past ~128, make the persisted schema unreadable.
        (Array(a), Array(b)) => Array(Box::new(unify(*a, *b, mixed))),

        // A bare scalar where an array is expected is how Mongo itself models
        // single-element arrays, so absorb the scalar into the element type
        // rather than giving up on the column.
        (Array(inner), other) | (other, Array(inner)) => {
            let joined = unify(*inner, other, mixed);
            Array(Box::new(joined))
        }

        // Structural merge: union of keys, per-key join, counters summed.
        (Document(mut left), Document(right)) => {
            for (k, rv) in right {
                match left.get_mut(&k) {
                    Some(lv) => {
                        let mut m = lv.mixed || rv.mixed;
                        let joined = unify(lv.shape.clone(), rv.shape, &mut m);
                        lv.shape = joined;
                        lv.present += rv.present;
                        lv.nulls += rv.nulls;
                        lv.mixed = m;
                    }
                    None => {
                        left.insert(k, rv);
                    }
                }
            }
            Document(left)
        }

        // Everything else is a genuine conflict. Degrade to JSON text rather
        // than aborting the query the way Drill does.
        _ => {
            *mixed = true;
            Json
        }
    }
}

/// Shape of a single value, with all counters set to "seen once".
fn shape_of(value: &Bson) -> Shape {
    match value {
        Bson::Boolean(_) => Shape::Bool,
        Bson::Int32(_) => Shape::Int32,
        Bson::Int64(_) => Shape::Int64,
        Bson::Double(_) => Shape::Double,
        Bson::Decimal128(_) => Shape::Decimal,
        Bson::String(_) | Bson::Symbol(_) => Shape::Utf8,
        Bson::ObjectId(_) => Shape::ObjectId,
        Bson::DateTime(_) => Shape::DateTime,
        Bson::Timestamp(_) => Shape::BsonTimestamp,
        Bson::Binary(_) => Shape::Binary,
        Bson::Null | Bson::Undefined => Shape::Unknown,
        Bson::Document(doc) => {
            let mut fields = BTreeMap::new();
            for (k, v) in doc {
                let mut fs = FieldShape::empty();
                observe_value(&mut fs, v);
                fields.insert(k.clone(), fs);
            }
            Shape::Document(fields)
        }
        Bson::Array(items) => {
            let mut elem = Shape::Unknown;
            let mut mixed = false;
            for item in items {
                elem = unify(elem, shape_of(item), &mut mixed);
            }
            Shape::Array(Box::new(elem))
        }
        // Regexes, JS code, DBPointer, min/max key: no useful SQL type.
        _ => Shape::Json,
    }
}

fn observe_value(slot: &mut FieldShape, value: &Bson) {
    slot.present += 1;
    if matches!(value, Bson::Null | Bson::Undefined) {
        slot.nulls += 1;
        return;
    }
    let mut mixed = slot.mixed;
    slot.shape = unify(std::mem::replace(&mut slot.shape, Shape::Unknown), shape_of(value), &mut mixed);
    slot.mixed = mixed;
}

// ---------------------------------------------------------------------------
// Sampler
// ---------------------------------------------------------------------------

/// Accumulates shapes across a stream of sampled documents.
#[derive(Debug, Default)]
pub struct Sampler {
    root: BTreeMap<String, FieldShape>,
    docs_seen: u64,
}

impl Sampler {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn observe(&mut self, doc: &Document) {
        self.docs_seen += 1;
        for (k, v) in doc {
            let slot = self.root.entry(k.clone()).or_insert_with(FieldShape::empty);
            observe_value(slot, v);
        }
    }

    pub fn docs_seen(&self) -> u64 {
        self.docs_seen
    }

    pub fn is_empty(&self) -> bool {
        self.root.is_empty()
    }

    /// Materialise the accumulated shapes as an Arrow schema.
    ///
    /// `max_depth` bounds struct nesting; anything deeper collapses into a
    /// single JSON text column so that a pathological document (say, a linked
    /// list encoded as nested objects) cannot blow up the schema.
    pub fn finish(&self, max_depth: usize) -> Schema {
        let mut fields: Vec<Field> = Vec::with_capacity(self.root.len());
        // `_id` is always present and is the natural primary key, so surface it
        // first regardless of alphabetical order.
        let mut names: Vec<&String> = self.root.keys().collect();
        names.sort_by_key(|n| (n.as_str() != "_id", n.as_str()));

        for name in names {
            let fs = &self.root[name];
            fields.push(build_field(name, name, fs, self.docs_seen, 0, max_depth));
        }
        Schema::new(fields)
    }
}

fn build_field(
    name: &str,
    path: &str,
    fs: &FieldShape,
    parent_count: u64,
    depth: usize,
    max_depth: usize,
) -> Field {
    let (data_type, tag) = arrow_type(&fs.shape, path, depth, max_depth);
    // Sampling can prove a field is sometimes null; it cannot prove it is never
    // null. A field present in every sampled document may still be absent from
    // one that was not sampled, and declaring it non-nullable then aborts the
    // scan with "declared as non-nullable but contains null values" the moment
    // such a document is read. Only `_id`, which MongoDB writes on every
    // document, is safe to mark non-nullable — and only at the root, since a
    // nested `_id` carries no such guarantee.
    let nullable = path != "_id";
    let freq = if parent_count == 0 {
        0.0
    } else {
        fs.present as f64 / parent_count as f64
    };

    let mut meta = std::collections::HashMap::with_capacity(4);
    meta.insert(META_BSON.to_string(), tag.as_str().to_string());
    meta.insert(META_PATH.to_string(), path.to_string());
    meta.insert(META_FREQ.to_string(), format!("{freq:.6}"));
    if fs.mixed {
        meta.insert(META_MIXED.to_string(), "true".to_string());
    }

    Field::new(name, data_type, nullable).with_metadata(meta)
}

fn arrow_type(shape: &Shape, path: &str, depth: usize, max_depth: usize) -> (DataType, BsonTag) {
    match shape {
        // A column we only ever saw as null still has to exist and still has to
        // round-trip through the wire protocol; Arrow's Null type is awkward
        // downstream, so use nullable Utf8.
        Shape::Unknown => (DataType::Utf8, BsonTag::Unknown),
        Shape::Bool => (DataType::Boolean, BsonTag::Bool),
        Shape::Int32 => (DataType::Int32, BsonTag::Int32),
        Shape::Int64 => (DataType::Int64, BsonTag::Int64),
        Shape::Double => (DataType::Float64, BsonTag::Double),
        Shape::Decimal => (
            DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
            BsonTag::Decimal,
        ),
        Shape::Utf8 => (DataType::Utf8, BsonTag::String),
        Shape::ObjectId => (DataType::Utf8, BsonTag::ObjectId),
        Shape::DateTime => (
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            BsonTag::DateTime,
        ),
        Shape::BsonTimestamp => (
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            BsonTag::Timestamp,
        ),
        Shape::Binary => (DataType::Binary, BsonTag::Binary),
        Shape::Json => (DataType::Utf8, BsonTag::Json),

        Shape::Document(children) if depth < max_depth && !children.is_empty() => {
            let total: u64 = children.values().map(|c| c.present).max().unwrap_or(0);
            let inner: Vec<Field> = children
                .iter()
                .map(|(k, v)| {
                    let child_path = format!("{path}.{k}");
                    build_field(k, &child_path, v, total, depth + 1, max_depth)
                })
                .collect();
            (DataType::Struct(Fields::from(inner)), BsonTag::Document)
        }
        // Too deep, or an object with no observed keys: keep the data, drop the
        // structure. The column is still queryable with JSON functions.
        Shape::Document(_) => (DataType::Utf8, BsonTag::Json),

        Shape::Array(inner) if depth < max_depth => {
            let (item_type, item_tag) = arrow_type(inner, path, depth + 1, max_depth);
            let mut item_meta = std::collections::HashMap::with_capacity(1);
            item_meta.insert(META_BSON.to_string(), item_tag.as_str().to_string());
            let item = Field::new("item", item_type, true).with_metadata(item_meta);
            (DataType::List(std::sync::Arc::new(item)), BsonTag::Array)
        }
        // Too deeply nested to model as a typed list — same treatment as an
        // over-deep document: keep the values, drop the structure.
        Shape::Array(_) => (DataType::Utf8, BsonTag::Json),
    }
}

/// Whether a field's values were coerced across type families. Predicates on
/// such columns must not be pushed down as `Exact`: Mongo compares by BSON
/// type order, which does not agree with the coerced SQL type.
pub fn field_is_mixed(field: &Field) -> bool {
    field.metadata().get(META_MIXED).map(|s| s == "true").unwrap_or(false)
}

/// The dotted Mongo path a column reads from.
pub fn field_path(field: &Field) -> String {
    field
        .metadata()
        .get(META_PATH)
        .cloned()
        .unwrap_or_else(|| field.name().clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    fn infer(docs: &[Document]) -> Schema {
        let mut s = Sampler::new();
        for d in docs {
            s.observe(d);
        }
        s.finish(4)
    }

    #[test]
    fn widens_int32_to_int64() {
        let schema = infer(&[doc! {"n": 1i32}, doc! {"n": 2i64}]);
        assert_eq!(schema.field_with_name("n").unwrap().data_type(), &DataType::Int64);
        assert!(!field_is_mixed(schema.field_with_name("n").unwrap()));
    }

    #[test]
    fn widens_int_to_double() {
        let schema = infer(&[doc! {"n": 1i32}, doc! {"n": 2.5f64}]);
        assert_eq!(schema.field_with_name("n").unwrap().data_type(), &DataType::Float64);
    }

    #[test]
    fn incompatible_types_degrade_to_json_not_an_error() {
        // Drill would raise SchemaChangeException partway through the scan.
        let schema = infer(&[doc! {"v": "text"}, doc! {"v": doc!{"a": 1}}]);
        let f = schema.field_with_name("v").unwrap();
        assert_eq!(f.data_type(), &DataType::Utf8);
        assert_eq!(BsonTag::of(f), BsonTag::Json);
        assert!(field_is_mixed(f));
    }

    #[test]
    fn only_root_id_is_non_nullable() {
        // A field seen in every sampled document is still nullable: the sample
        // cannot prove an unsampled document does not omit it. `_id` is the one
        // field MongoDB guarantees, so it alone is non-nullable.
        let schema = infer(&[doc! {"_id": 1i32, "a": 1i32, "b": 2i32}, doc! {"_id": 2i32, "a": 3i32}]);
        assert!(!schema.field_with_name("_id").unwrap().is_nullable());
        assert!(schema.field_with_name("a").unwrap().is_nullable());
        assert!(schema.field_with_name("b").unwrap().is_nullable());
    }

    #[test]
    fn nested_documents_become_structs() {
        let schema = infer(&[doc! {"u": doc!{"name": "a", "age": 3i32}}]);
        match schema.field_with_name("u").unwrap().data_type() {
            DataType::Struct(fields) => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields.find("age").unwrap().1.data_type(), &DataType::Int32);
            }
            other => panic!("expected struct, got {other:?}"),
        }
    }

    #[test]
    fn nesting_beyond_max_depth_collapses_to_json() {
        let mut s = Sampler::new();
        s.observe(&doc! {"a": doc!{"b": doc!{"c": doc!{"d": 1i32}}}});
        let schema = s.finish(2);
        // a -> struct, a.b -> struct, a.b.c -> json text
        let a = schema.field_with_name("a").unwrap();
        let DataType::Struct(l1) = a.data_type() else { panic!("a should be a struct") };
        let DataType::Struct(l2) = l1.find("b").unwrap().1.data_type() else {
            panic!("a.b should be a struct")
        };
        assert_eq!(l2.find("c").unwrap().1.data_type(), &DataType::Utf8);
    }

    #[test]
    fn unifying_two_arrays_stays_one_level_deep() {
        // Regression: `unify(Array, Array)` used to fall through to the
        // scalar-absorption arm, wrapping one operand and adding a level of
        // nesting on every merge.
        let mut mixed = false;
        let joined = unify(
            Shape::Array(Box::new(Shape::Int32)),
            Shape::Array(Box::new(Shape::Int64)),
            &mut mixed,
        );
        assert_eq!(joined, Shape::Array(Box::new(Shape::Int64)));
    }

    #[test]
    fn repeated_array_observations_do_not_deepen_the_schema() {
        // The bug that made the persisted catalog unreadable: an array field
        // seen across many documents nested itself once per merge, so after
        // ~128 documents serde_json could no longer parse the cached schema.
        let mut s = Sampler::new();
        for _ in 0..200 {
            s.observe(&doc! {"tags": ["a", "b"]});
        }
        let schema = s.finish(4);
        match schema.field_with_name("tags").unwrap().data_type() {
            DataType::List(item) => assert_eq!(item.data_type(), &DataType::Utf8),
            other => panic!("expected a single-level List(Utf8), got {other:?}"),
        }
    }

    #[test]
    fn scalar_merges_into_array_element_type() {
        // Mongo treats `{tags: "x"}` as matching `tags = "x"` on an array field,
        // so the column stays a list rather than collapsing to JSON.
        let schema = infer(&[doc! {"tags": ["a", "b"]}, doc! {"tags": "c"}]);
        match schema.field_with_name("tags").unwrap().data_type() {
            DataType::List(item) => assert_eq!(item.data_type(), &DataType::Utf8),
            other => panic!("expected list, got {other:?}"),
        }
    }

    #[test]
    fn objectid_and_string_coexist_as_text() {
        let oid = bson::oid::ObjectId::new();
        let schema = infer(&[doc! {"ref": oid}, doc! {"ref": "manual-key"}]);
        let f = schema.field_with_name("ref").unwrap();
        assert_eq!(f.data_type(), &DataType::Utf8);
        assert!(field_is_mixed(f), "coerced column must block Exact pushdown");
    }

    #[test]
    fn id_column_is_ordered_first() {
        let schema = infer(&[doc! {"zeta": 1i32, "_id": 1i32, "alpha": 2i32}]);
        assert_eq!(schema.field(0).name(), "_id");
        assert_eq!(schema.field(1).name(), "alpha");
    }

    #[test]
    fn null_only_column_survives_as_nullable_text() {
        let schema = infer(&[doc! {"maybe": Bson::Null}]);
        let f = schema.field_with_name("maybe").unwrap();
        assert!(f.is_nullable());
        assert_eq!(BsonTag::of(f), BsonTag::Unknown);
    }
}
