//! Translation of DataFusion predicates into MongoDB `$match` documents.
//!
//! Two rules govern everything in this module.
//!
//! **Rule 1 — never change the answer.** A translation is reported as `Exact`
//! only when the `$match` accepts precisely the rows SQL would. Anything else is
//! `Inexact`: still sent to Mongo (so the server does the bulk of the filtering
//! against its indexes) but re-evaluated by DataFusion afterwards. Refusing to
//! push down is always safe; claiming `Exact` wrongly silently corrupts results.
//!
//! **Rule 2 — respect the two places Mongo disagrees with SQL.**
//!
//! * *Negation and missing fields.* `{x: {$ne: 5}}` matches documents that have
//!   no `x` at all, but SQL evaluates `NULL <> 5` to NULL, which `WHERE`
//!   discards. Every negated predicate is therefore conjoined with
//!   `{x: {$ne: null}}` — see [`exclude_null`].
//! * *Type-ordered comparison.* Mongo orders values across BSON types, so
//!   `{x: {$gt: 5}}` will not match the string `"9"`. When inference had to
//!   coerce a column across type families the comparison is no longer faithful,
//!   so such columns are never `Exact`.

use bson::{Bson, Document, doc};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{Between, BinaryExpr, Expr, Like, Operator};

use crate::catalog::infer::{BsonTag, field_is_mixed, field_path};

/// A predicate translated into a Mongo filter document.
#[derive(Debug, Clone, PartialEq)]
pub struct Translated {
    pub filter: Document,
    /// `true` when the filter is exactly equivalent to the SQL predicate and
    /// DataFusion may drop its own copy of it.
    pub exact: bool,
}

impl Translated {
    fn exact(filter: Document) -> Self {
        Self { filter, exact: true }
    }
    fn with_exactness(self, exact: bool) -> Self {
        Self { filter: self.filter, exact: self.exact && exact }
    }
}

/// A column reference resolved against the inferred schema.
#[derive(Debug, Clone)]
struct ColumnRef {
    /// Dotted path as Mongo knows it, e.g. `profile.address.city`.
    path: String,
    tag: BsonTag,
    data_type: DataType,
    /// Inference had to coerce values of different BSON types into this column.
    mixed: bool,
}

/// Translate a single predicate. `None` means "cannot be expressed", which is
/// always a legal answer.
pub fn translate(expr: &Expr, schema: &Schema) -> Option<Translated> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => translate_binary(left, *op, right, schema),

        Expr::Not(inner) => {
            let t = translate(inner, schema)?;
            // `$nor` is Mongo's only true negation, and like `$ne` it admits
            // documents where the referenced fields are missing. Only negate
            // when we can also assert the columns are non-null.
            let guards = null_guards(inner, schema)?;
            let mut clauses = vec![Bson::Document(doc! { "$nor": [t.filter] })];
            clauses.extend(guards.into_iter().map(Bson::Document));
            Some(Translated { filter: doc! { "$and": clauses }, exact: t.exact })
        }

        Expr::IsNull(inner) => {
            let c = resolve_column(inner, schema)?;
            // `{path: null}` matches explicit nulls *and* absent fields, which
            // is exactly how the schema models a SQL NULL.
            Some(Translated::exact(doc! { c.path: Bson::Null }))
        }
        Expr::IsNotNull(inner) => {
            let c = resolve_column(inner, schema)?;
            Some(Translated::exact(doc! { c.path: { "$ne": Bson::Null } }))
        }

        Expr::IsTrue(inner) => bool_test(inner, schema, true),
        Expr::IsFalse(inner) => bool_test(inner, schema, false),
        Expr::IsNotTrue(inner) => {
            // NOT TRUE is true for both FALSE and NULL.
            let c = resolve_column(inner, schema)?;
            require_bool(&c)?;
            Some(Translated::exact(
                doc! { "$or": [ { &c.path: false }, { &c.path: Bson::Null } ] },
            ))
        }
        Expr::IsNotFalse(inner) => {
            let c = resolve_column(inner, schema)?;
            require_bool(&c)?;
            Some(Translated::exact(
                doc! { "$or": [ { &c.path: true }, { &c.path: Bson::Null } ] },
            ))
        }

        Expr::Between(Between { expr, negated, low, high }) => {
            let c = resolve_column(expr, schema)?;
            let lo = literal_of(low, &c)?;
            let hi = literal_of(high, &c)?;
            let filter = if *negated {
                exclude_null(
                    &c.path,
                    doc! { "$or": [ { &c.path: { "$lt": lo } }, { &c.path: { "$gt": hi } } ] },
                )
            } else {
                doc! { &c.path: { "$gte": lo, "$lte": hi } }
            };
            Some(Translated { filter, exact: !c.mixed })
        }

        Expr::InList(list) => {
            let c = resolve_column(&list.expr, schema)?;
            let values: Option<Vec<Bson>> =
                list.list.iter().map(|e| literal_of(e, &c)).collect();
            let values = values?;
            let filter = if list.negated {
                exclude_null(&c.path, doc! { &c.path: { "$nin": values } })
            } else {
                doc! { &c.path: { "$in": values } }
            };
            Some(Translated { filter, exact: !c.mixed })
        }

        Expr::Like(like) => translate_like(like, schema, false),
        Expr::SimilarTo(like) => translate_like(like, schema, true),

        // A bare boolean column used as a predicate: `WHERE is_active`.
        Expr::Column(_) => bool_test(expr, schema, true),

        Expr::Alias(alias) => translate(&alias.expr, schema),

        _ => None,
    }
}

/// Combine per-filter translations into a single `$match` document, reporting
/// whether *every* component was exact.
pub fn combine(translations: &[Translated]) -> Option<Document> {
    let clauses: Vec<Bson> = translations
        .iter()
        .map(|t| Bson::Document(t.filter.clone()))
        .collect();
    match clauses.len() {
        0 => None,
        1 => translations.first().map(|t| t.filter.clone()),
        _ => Some(doc! { "$and": clauses }),
    }
}

// ---------------------------------------------------------------------------
// Binary operators
// ---------------------------------------------------------------------------

fn translate_binary(
    left: &Expr,
    op: Operator,
    right: &Expr,
    schema: &Schema,
) -> Option<Translated> {
    match op {
        Operator::And => {
            // A conjunction is worth pushing even if only one side translates:
            // the other half stays with DataFusion and the result is `Inexact`.
            let l = translate(left, schema);
            let r = translate(right, schema);
            match (l, r) {
                (Some(a), Some(b)) => Some(Translated {
                    filter: doc! { "$and": [a.filter, b.filter] },
                    exact: a.exact && b.exact,
                }),
                (Some(a), None) | (None, Some(a)) => Some(a.with_exactness(false)),
                (None, None) => None,
            }
        }
        Operator::Or => {
            // A disjunction, by contrast, is all-or-nothing: dropping one arm
            // of an OR would wrongly narrow the result set.
            let a = translate(left, schema)?;
            let b = translate(right, schema)?;
            Some(Translated {
                filter: doc! { "$or": [a.filter, b.filter] },
                exact: a.exact && b.exact,
            })
        }

        Operator::Eq
        | Operator::NotEq
        | Operator::Lt
        | Operator::LtEq
        | Operator::Gt
        | Operator::GtEq => {
            // Normalise to `column <op> literal`, flipping the operator if the
            // planner left the literal on the left-hand side.
            let (column_expr, literal_expr, op) = match (is_literal(left), is_literal(right)) {
                (false, true) => (left, right, op),
                (true, false) => (right, left, flip(op)),
                // literal-vs-literal is constant-folded upstream; column-vs-column
                // has no `$match` form (it needs `$expr`, which cannot use indexes).
                _ => return None,
            };

            let c = resolve_column(column_expr, schema)?;
            let value = literal_of(literal_expr, &c)?;

            let mongo_op = match op {
                Operator::Eq => "$eq",
                Operator::NotEq => "$ne",
                Operator::Lt => "$lt",
                Operator::LtEq => "$lte",
                Operator::Gt => "$gt",
                Operator::GtEq => "$gte",
                _ => unreachable!("guarded by the outer match"),
            };

            let base = doc! { &c.path: { mongo_op: value } };
            let filter = if op == Operator::NotEq { exclude_null(&c.path, base) } else { base };
            Some(Translated { filter, exact: !c.mixed })
        }

        _ => None,
    }
}

fn flip(op: Operator) -> Operator {
    match op {
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        other => other,
    }
}

fn is_literal(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(..))
}

/// Wrap a negated predicate so that documents missing the field are excluded,
/// matching SQL's three-valued logic.
fn exclude_null(path: &str, inner: Document) -> Document {
    doc! { "$and": [ inner, { path: { "$ne": Bson::Null } } ] }
}

/// `{path: {$ne: null}}` for every column referenced by `expr`, used to make
/// `$nor` safe. `None` if any leaf is not a resolvable column.
fn null_guards(expr: &Expr, schema: &Schema) -> Option<Vec<Document>> {
    let mut out = Vec::new();
    collect_guards(expr, schema, &mut out)?;
    Some(out)
}

fn collect_guards(expr: &Expr, schema: &Schema, out: &mut Vec<Document>) -> Option<()> {
    match expr {
        Expr::Column(_) | Expr::ScalarFunction(_) => {
            let c = resolve_column(expr, schema)?;
            out.push(doc! { c.path: { "$ne": Bson::Null } });
            Some(())
        }
        Expr::Literal(..) => Some(()),
        Expr::BinaryExpr(BinaryExpr { left, right, .. }) => {
            collect_guards(left, schema, out)?;
            collect_guards(right, schema, out)
        }
        Expr::Not(i)
        | Expr::IsNull(i)
        | Expr::IsNotNull(i)
        | Expr::IsTrue(i)
        | Expr::IsFalse(i)
        | Expr::IsNotTrue(i)
        | Expr::IsNotFalse(i) => collect_guards(i, schema, out),
        Expr::Between(Between { expr, low, high, .. }) => {
            collect_guards(expr, schema, out)?;
            collect_guards(low, schema, out)?;
            collect_guards(high, schema, out)
        }
        Expr::InList(l) => collect_guards(&l.expr, schema, out),
        Expr::Like(l) => collect_guards(&l.expr, schema, out),
        Expr::Alias(a) => collect_guards(&a.expr, schema, out),
        _ => None,
    }
}

fn require_bool(c: &ColumnRef) -> Option<()> {
    matches!(c.data_type, DataType::Boolean).then_some(())
}

fn bool_test(expr: &Expr, schema: &Schema, want: bool) -> Option<Translated> {
    let c = resolve_column(expr, schema)?;
    require_bool(&c)?;
    Some(Translated { filter: doc! { &c.path: want }, exact: !c.mixed })
}

// ---------------------------------------------------------------------------
// LIKE
// ---------------------------------------------------------------------------

fn translate_like(like: &Like, schema: &Schema, is_similar_to: bool) -> Option<Translated> {
    // SIMILAR TO uses POSIX regex semantics that do not map cleanly onto PCRE,
    // so it is left to DataFusion.
    if is_similar_to {
        return None;
    }
    let c = resolve_column(&like.expr, schema)?;
    // Regex matching against a non-string column would compare against Mongo's
    // own rendering, not the SQL value.
    if !matches!(c.data_type, DataType::Utf8 | DataType::LargeUtf8) {
        return None;
    }
    let Expr::Literal(ScalarValue::Utf8(Some(pattern)) | ScalarValue::LargeUtf8(Some(pattern)), _) =
        &*like.pattern
    else {
        return None;
    };

    let regex = like_to_regex(pattern, like.escape_char)?;
    let options = if like.case_insensitive { "is" } else { "s" };
    let matcher = doc! { "$regex": regex, "$options": options };

    let filter = if like.negated {
        exclude_null(&c.path, doc! { &c.path: { "$not": { "$regex": matcher.get_str("$regex").ok()?, "$options": options } } })
    } else {
        doc! { &c.path: matcher }
    };
    Some(Translated { filter, exact: !c.mixed })
}

/// Convert a SQL `LIKE` pattern into an anchored PCRE pattern.
///
/// `%` becomes `.*`, `_` becomes `.`, and every regex metacharacter in between
/// is escaped so that a literal `.` in the pattern cannot match any character.
pub fn like_to_regex(pattern: &str, escape: Option<char>) -> Option<String> {
    let mut out = String::with_capacity(pattern.len() + 4);
    out.push('^');
    let mut chars = pattern.chars();
    while let Some(ch) = chars.next() {
        if Some(ch) == escape {
            // The escaped character is matched literally, whatever it is.
            let next = chars.next()?;
            push_escaped(&mut out, next);
            continue;
        }
        match ch {
            '%' => out.push_str(".*"),
            '_' => out.push('.'),
            other => push_escaped(&mut out, other),
        }
    }
    out.push('$');
    Some(out)
}

fn push_escaped(out: &mut String, ch: char) {
    if matches!(
        ch,
        '.' | '^' | '$' | '*' | '+' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '|' | '\\' | '/'
    ) {
        out.push('\\');
    }
    out.push(ch);
}

// ---------------------------------------------------------------------------
// Column and literal resolution
// ---------------------------------------------------------------------------

/// Resolve an expression to a Mongo field path, following struct field access
/// so that `profile['city']` becomes the path `profile.city`.
fn resolve_column(expr: &Expr, schema: &Schema) -> Option<ColumnRef> {
    match expr {
        Expr::Column(col) => {
            let field = schema.field_with_name(&col.name).ok()?;
            Some(column_ref_of(field))
        }
        Expr::Alias(a) => resolve_column(&a.expr, schema),
        Expr::Cast(cast) => {
            // A cast that does not change the value is transparent; anything
            // else changes comparison semantics and blocks pushdown.
            let inner = resolve_column(&cast.expr, schema)?;
            (*cast.field.data_type() == inner.data_type).then_some(inner)
        }
        Expr::ScalarFunction(func) if func.name() == "get_field" => {
            let [base, Expr::Literal(ScalarValue::Utf8(Some(key)), _)] = &func.args[..] else {
                return None;
            };
            let parent = resolve_column(base, schema)?;
            let DataType::Struct(children) = &parent.data_type else { return None };
            let (_, child) = children.find(key)?;
            let mut c = column_ref_of(child);
            // Nested fields carry their own recorded path, but fall back to
            // composing one if the metadata is absent.
            if c.path == *child.name() {
                c.path = format!("{}.{}", parent.path, child.name());
            }
            c.mixed |= parent.mixed;
            Some(c)
        }
        _ => None,
    }
}

fn column_ref_of(field: &Field) -> ColumnRef {
    ColumnRef {
        path: field_path(field),
        tag: BsonTag::of(field),
        data_type: field.data_type().clone(),
        mixed: field_is_mixed(field),
    }
}

/// Convert a literal expression into the BSON representation the *column*
/// uses. Getting this wrong is silent: comparing an `ObjectId` field against a
/// plain string matches zero documents rather than raising an error.
fn literal_of(expr: &Expr, column: &ColumnRef) -> Option<Bson> {
    let Expr::Literal(scalar, _) = expr else { return None };
    scalar_to_bson(scalar, column.tag)
}

pub(crate) fn scalar_to_bson(scalar: &ScalarValue, tag: BsonTag) -> Option<Bson> {
    // A NULL literal in a comparison yields NULL, i.e. no rows; let DataFusion
    // handle that rather than encoding it as a Mongo filter.
    if scalar.is_null() {
        return None;
    }
    Some(match (scalar, tag) {
        // Strings against an ObjectId column must become real ObjectIds.
        (ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)), BsonTag::ObjectId) => {
            Bson::ObjectId(s.parse().ok()?)
        }
        (ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)), _) => {
            Bson::String(s.clone())
        }

        (ScalarValue::Boolean(Some(b)), _) => Bson::Boolean(*b),

        (ScalarValue::Int8(Some(v)), _) => int_literal(*v as i64, tag),
        (ScalarValue::Int16(Some(v)), _) => int_literal(*v as i64, tag),
        (ScalarValue::Int32(Some(v)), _) => int_literal(*v as i64, tag),
        (ScalarValue::Int64(Some(v)), _) => int_literal(*v, tag),
        (ScalarValue::UInt8(Some(v)), _) => int_literal(*v as i64, tag),
        (ScalarValue::UInt16(Some(v)), _) => int_literal(*v as i64, tag),
        (ScalarValue::UInt32(Some(v)), _) => int_literal(*v as i64, tag),
        (ScalarValue::UInt64(Some(v)), _) => int_literal(i64::try_from(*v).ok()?, tag),

        (ScalarValue::Float32(Some(v)), _) => Bson::Double(*v as f64),
        (ScalarValue::Float64(Some(v)), _) => Bson::Double(*v),

        (ScalarValue::TimestampMillisecond(Some(ms), _), _) => {
            Bson::DateTime(bson::DateTime::from_millis(*ms))
        }
        (ScalarValue::TimestampMicrosecond(Some(us), _), _) => {
            Bson::DateTime(bson::DateTime::from_millis(us.div_euclid(1_000)))
        }
        (ScalarValue::TimestampNanosecond(Some(ns), _), _) => {
            Bson::DateTime(bson::DateTime::from_millis(ns.div_euclid(1_000_000)))
        }
        (ScalarValue::TimestampSecond(Some(s), _), _) => {
            Bson::DateTime(bson::DateTime::from_millis(s.checked_mul(1_000)?))
        }
        (ScalarValue::Date32(Some(days)), _) => {
            Bson::DateTime(bson::DateTime::from_millis((*days as i64).checked_mul(86_400_000)?))
        }
        (ScalarValue::Date64(Some(ms)), _) => Bson::DateTime(bson::DateTime::from_millis(*ms)),

        // bson 3 exposes no constructor for Decimal128 from a numeric value, so
        // a decimal comparison cannot be expressed faithfully. Refusing keeps
        // the result correct; DataFusion evaluates it after the scan.
        (ScalarValue::Decimal128(..), _) | (ScalarValue::Decimal256(..), _) => return None,

        _ => return None,
    })
}

/// Emit an integer in the width the column actually stores. Mongo compares
/// numerics across widths correctly, but keeping the width means an index on
/// the field is used without a conversion.
fn int_literal(v: i64, tag: BsonTag) -> Bson {
    match tag {
        BsonTag::Int32 => match i32::try_from(v) {
            Ok(small) => Bson::Int32(small),
            Err(_) => Bson::Int64(v),
        },
        BsonTag::Double => Bson::Double(v as f64),
        _ => Bson::Int64(v),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::infer::{META_BSON, META_MIXED, META_PATH};
    use datafusion::functions::core::expr_ext::FieldAccessor;
    use datafusion::logical_expr::{col, lit};
    use std::collections::HashMap;

    fn field(name: &str, dt: DataType, tag: &str, mixed: bool) -> Field {
        let mut meta = HashMap::new();
        meta.insert(META_BSON.to_string(), tag.to_string());
        meta.insert(META_PATH.to_string(), name.to_string());
        if mixed {
            meta.insert(META_MIXED.to_string(), "true".to_string());
        }
        Field::new(name, dt, true).with_metadata(meta)
    }

    fn test_schema() -> Schema {
        Schema::new(vec![
            field("_id", DataType::Utf8, "objectId", false),
            field("age", DataType::Int32, "int32", false),
            field("name", DataType::Utf8, "string", false),
            field("active", DataType::Boolean, "bool", false),
            field("score", DataType::Utf8, "json", true),
            Field::new(
                "profile",
                DataType::Struct(vec![field("city", DataType::Utf8, "string", false)].into()),
                true,
            )
            .with_metadata(HashMap::from([
                (META_BSON.to_string(), "document".to_string()),
                (META_PATH.to_string(), "profile".to_string()),
            ])),
        ])
    }

    #[test]
    fn simple_equality() {
        let t = translate(&col("age").eq(lit(30i32)), &test_schema()).unwrap();
        assert!(t.exact);
        assert_eq!(t.filter, doc! { "age": { "$eq": 30i32 } });
    }

    #[test]
    fn literal_on_the_left_flips_the_operator() {
        let t = translate(&lit(30i32).lt(col("age")), &test_schema()).unwrap();
        assert_eq!(t.filter, doc! { "age": { "$gt": 30i32 } });
    }

    #[test]
    fn not_equal_excludes_missing_documents() {
        // The whole point: Mongo's `$ne` matches documents with no `age` field,
        // but SQL's `age <> 30` must not.
        let t = translate(&col("age").not_eq(lit(30i32)), &test_schema()).unwrap();
        assert_eq!(
            t.filter,
            doc! { "$and": [ { "age": { "$ne": 30i32 } }, { "age": { "$ne": Bson::Null } } ] }
        );
        assert!(t.exact);
    }

    #[test]
    fn objectid_columns_get_typed_literals() {
        let oid = bson::oid::ObjectId::new();
        let t = translate(&col("_id").eq(lit(oid.to_hex())), &test_schema()).unwrap();
        assert_eq!(t.filter, doc! { "_id": { "$eq": Bson::ObjectId(oid) } });
    }

    #[test]
    fn malformed_objectid_literal_is_not_pushed() {
        assert!(translate(&col("_id").eq(lit("not-an-oid")), &test_schema()).is_none());
    }

    #[test]
    fn coerced_columns_are_never_exact() {
        let t = translate(&col("score").eq(lit("7")), &test_schema()).unwrap();
        assert!(!t.exact, "a column whose types were coerced cannot be filtered faithfully");
    }

    #[test]
    fn conjunction_keeps_the_translatable_half() {
        let schema = test_schema();
        // `starts_with` has no `$match` equivalent here, but the age predicate does.
        let unsupported = col("name").eq(col("profile"));
        let expr = col("age").gt(lit(18i32)).and(unsupported);
        let t = translate(&expr, &schema).unwrap();
        assert_eq!(t.filter, doc! { "age": { "$gt": 18i32 } });
        assert!(!t.exact, "the dropped conjunct must force re-evaluation");
    }

    #[test]
    fn disjunction_is_all_or_nothing() {
        let schema = test_schema();
        let expr = col("age").gt(lit(18i32)).or(col("name").eq(col("profile")));
        assert!(
            translate(&expr, &schema).is_none(),
            "dropping one arm of an OR would lose rows"
        );
    }

    #[test]
    fn is_null_covers_absent_fields() {
        let t = translate(&col("age").is_null(), &test_schema()).unwrap();
        assert_eq!(t.filter, doc! { "age": Bson::Null });
        assert!(t.exact);
    }

    #[test]
    fn in_list_and_negated_in_list() {
        let schema = test_schema();
        let t = translate(&col("age").in_list(vec![lit(1i32), lit(2i32)], false), &schema).unwrap();
        assert_eq!(t.filter, doc! { "age": { "$in": [1i32, 2i32] } });

        let t = translate(&col("age").in_list(vec![lit(1i32)], true), &schema).unwrap();
        assert_eq!(
            t.filter,
            doc! { "$and": [ { "age": { "$nin": [1i32] } }, { "age": { "$ne": Bson::Null } } ] }
        );
    }

    #[test]
    fn between_becomes_a_range() {
        let t = translate(
            &col("age").between(lit(18i32), lit(65i32)),
            &test_schema(),
        )
        .unwrap();
        assert_eq!(t.filter, doc! { "age": { "$gte": 18i32, "$lte": 65i32 } });
    }

    #[test]
    fn like_pattern_is_anchored_and_escaped() {
        assert_eq!(like_to_regex("a%", None).unwrap(), "^a.*$");
        assert_eq!(like_to_regex("_b", None).unwrap(), "^.b$");
        // A literal dot in the pattern must not become "any character".
        assert_eq!(like_to_regex("a.b", None).unwrap(), r"^a\.b$");
        assert_eq!(like_to_regex(r"100\%", Some('\\')).unwrap(), "^100%$");
    }

    #[test]
    fn like_on_non_text_column_is_refused() {
        let schema = test_schema();
        let expr = Expr::Like(Like::new(false, Box::new(col("age")), Box::new(lit("1%")), None, false));
        assert!(translate(&expr, &schema).is_none());
    }

    #[test]
    fn struct_field_access_becomes_a_dotted_path() {
        let schema = test_schema();
        let expr = col("profile").field("city").eq(lit("Bangkok"));
        let t = translate(&expr, &schema).expect("nested field should translate");
        assert_eq!(t.filter, doc! { "profile.city": { "$eq": "Bangkok" } });
    }

    #[test]
    fn column_to_column_comparison_is_refused() {
        // Expressible only via `$expr`, which cannot use an index; leaving it
        // to DataFusion is faster than a collection scan on the server.
        let schema = test_schema();
        assert!(translate(&col("age").eq(col("age")), &schema).is_none());
    }
}
