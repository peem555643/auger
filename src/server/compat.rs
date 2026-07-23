//! PostgreSQL compatibility surface.
//!
//! Speaking the wire protocol gets a client connected; it does not get it
//! working. Real clients immediately issue `SET`, `BEGIN`, `SELECT version()`
//! and a handful of `pg_catalog` queries, none of which a query engine answers
//! on its own. This module supplies them:
//!
//! * [`intercept`] answers session statements that have no meaning here but
//!   must not be errors,
//! * [`register`] installs the `pg_catalog` relations as views over the real
//!   `information_schema`, so introspection reflects the live Mongo catalog
//!   rather than a snapshot taken at startup.

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int32Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
use datafusion::prelude::SessionContext;

/// Version string reported to clients.
///
/// Clients gate features on the major version they parse out of this, so it
/// names a real PostgreSQL release and puts the true identity after it. It has
/// to agree with the `server_version` sent during startup, which is why both
/// live here.
pub const SERVER_VERSION: &str = "16.6";
pub const VERSION_STRING: &str =
    "PostgreSQL 16.6 (Auger 0.1.0, Apache DataFusion 54, MongoDB backend)";

/// The outcome of intercepting a session statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Intercepted {
    /// Statement acknowledged with the given command tag; no rows.
    Tag(String),
    /// Statement should be executed normally, after replacing it with this SQL.
    Rewritten(String),
}

/// Handle statements that a query engine has no business executing.
///
/// Returning `None` means "this is a real query"; the caller runs it.
pub fn intercept(sql: &str) -> Option<Intercepted> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if trimmed.is_empty() {
        return Some(Intercepted::Tag("EMPTY".into()));
    }

    let upper = trimmed.to_ascii_uppercase();
    let first = upper.split_whitespace().next().unwrap_or("");

    match first {
        // Session GUCs: accepted and ignored. The alternative — erroring — makes
        // psql and every JDBC driver fail before the first query.
        "SET" => Some(Intercepted::Tag("SET".into())),
        "RESET" => Some(Intercepted::Tag("RESET".into())),
        "DISCARD" => Some(Intercepted::Tag("DISCARD ALL".into())),
        "LISTEN" => Some(Intercepted::Tag("LISTEN".into())),
        "UNLISTEN" => Some(Intercepted::Tag("UNLISTEN".into())),

        // A read-only gateway has nothing to roll back, but clients that wrap
        // every statement in a transaction must still see it succeed.
        "BEGIN" | "START" => Some(Intercepted::Tag("BEGIN".into())),
        "COMMIT" | "END" => Some(Intercepted::Tag("COMMIT".into())),
        "ROLLBACK" | "ABORT" => Some(Intercepted::Tag("ROLLBACK".into())),
        "SAVEPOINT" => Some(Intercepted::Tag("SAVEPOINT".into())),
        "RELEASE" => Some(Intercepted::Tag("RELEASE".into())),

        // Mutations are refused explicitly, with a message that says why,
        // rather than failing later with a parser error.
        "INSERT" | "UPDATE" | "DELETE" | "MERGE" | "TRUNCATE" | "DROP" | "ALTER" | "GRANT"
        | "REVOKE" => None,

        _ => {
            // Nothing below touches a statement that never mentions a catalog
            // relation or function, which is almost every real query.
            if !upper.contains("PG_") {
                return None;
            }
            // `SELECT pg_catalog.version()` and friends carry a schema
            // qualifier that DataFusion resolves as a table reference.
            let mut rewritten = if upper.contains("PG_CATALOG.") {
                strip_pg_catalog_prefix(trimmed)
            } else {
                trimmed.to_string()
            };
            // A driver writes `FROM pg_type` and means `pg_catalog.pg_type`,
            // because PostgreSQL keeps pg_catalog on the search path. DataFusion
            // has none, so the qualifier has to be put back or the query fails
            // with "table not found". SQLAlchemy reflection — and therefore
            // Superset — depends on this.
            rewritten = qualify_bare_pg_catalog(&rewritten);
            if rewritten != trimmed {
                return Some(Intercepted::Rewritten(rewritten));
            }
            None
        }
    }
}

/// System-catalog relations Auger emulates as views in the `pg_catalog` schema.
const PG_CATALOG_RELATIONS: &[&str] =
    &["pg_namespace", "pg_class", "pg_attribute", "pg_type", "pg_index", "pg_roles"];

/// Qualify bare references to the emulated `pg_catalog` relations.
///
/// A name is rewritten only where it stands alone as an identifier. One
/// preceded by `.` — a column such as `t.pg_type`, or an already-qualified
/// `pg_catalog.pg_type` — is left alone, as is any occurrence inside a string
/// literal. The scan works on bytes and copies non-ASCII (UTF-8 inside a
/// literal) through untouched.
fn qualify_bare_pg_catalog(sql: &str) -> String {
    let b = sql.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(sql.len());
    let mut i = 0;
    let mut in_quote = false;
    // Last non-whitespace byte emitted; decides whether an identifier is bare.
    let mut prev = b' ';

    while i < b.len() {
        let c = b[i];

        if in_quote {
            out.push(c);
            if c == b'\'' {
                in_quote = false;
            }
            i += 1;
            continue;
        }
        if c == b'\'' {
            in_quote = true;
            out.push(c);
            prev = c;
            i += 1;
            continue;
        }

        let starts_ident = c.is_ascii_alphabetic() || c == b'_';
        let attached = prev == b'.' || prev.is_ascii_alphanumeric() || prev == b'_';
        if starts_ident && !attached {
            let start = i;
            while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_') {
                i += 1;
            }
            let ident = &sql[start..i];
            if PG_CATALOG_RELATIONS.iter().any(|r| r.eq_ignore_ascii_case(ident)) {
                out.extend_from_slice(b"pg_catalog.");
            }
            out.extend_from_slice(ident.as_bytes());
            prev = b[i - 1];
            continue;
        }

        out.push(c);
        if !c.is_ascii_whitespace() {
            prev = c;
        }
        i += 1;
    }

    String::from_utf8(out).unwrap_or_else(|_| sql.to_string())
}

/// Remove `pg_catalog.` qualifiers that precede a function call.
///
/// `pg_catalog.version()` is a schema-qualified *function*, but `pg_catalog.pg_class`
/// is a table that really does live in that schema, so only qualifiers followed
/// by an open parenthesis are stripped.
fn strip_pg_catalog_prefix(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let lower = sql.to_ascii_lowercase();
    let needle = "pg_catalog.";
    let mut cursor = 0;

    while let Some(found) = lower[cursor..].find(needle) {
        let start = cursor + found;
        let after = start + needle.len();
        // Look ahead over the identifier to see whether a `(` follows.
        let ident_end = lower[after..]
            .find(|c: char| !(c.is_alphanumeric() || c == '_'))
            .map(|i| after + i)
            .unwrap_or(lower.len());
        let is_call = lower[ident_end..].trim_start().starts_with('(');

        out.push_str(&sql[cursor..start]);
        if !is_call {
            out.push_str(&sql[start..after]);
        }
        cursor = after;
    }
    out.push_str(&sql[cursor..]);
    out
}

/// Install compatibility functions and the `pg_catalog` schema.
pub async fn register(ctx: &SessionContext, catalog: &str, default_schema: &str) -> anyhow::Result<()> {
    register_functions(ctx, catalog, default_schema);
    register_pg_catalog(ctx).await
}

fn register_functions(ctx: &SessionContext, catalog: &str, default_schema: &str) {
    let constant = |name: &str, value: String| {
        let value = Arc::new(value);
        create_udf(
            name,
            vec![],
            DataType::Utf8,
            Volatility::Stable,
            Arc::new(move |_args: &[ColumnarValue]| {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some((*value).clone()))))
            }),
        )
    };

    // DataFusion ships its own `version()`; overriding it is deliberate, since
    // clients parse the leading "PostgreSQL <major>" to decide what they may use.
    ctx.register_udf(constant("version", VERSION_STRING.to_string()));
    ctx.register_udf(constant("current_schema", default_schema.to_string()));
    ctx.register_udf(constant("current_database", catalog.to_string()));
    ctx.register_udf(constant("current_catalog", catalog.to_string()));

    ctx.register_udf(create_udf(
        "pg_backend_pid",
        vec![],
        DataType::Int32,
        Volatility::Stable,
        Arc::new(|_| Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(std::process::id() as i32))))),
    ));

    // `pg_class.oid` and `pg_attribute.attrelid` have to agree for a join
    // between them to work, so both are derived from this one function.
    ctx.register_udf(create_udf(
        "auger_oid",
        vec![DataType::Utf8],
        DataType::Int32,
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| {
            Ok(match &args[0] {
                ColumnarValue::Scalar(ScalarValue::Utf8(v)) => {
                    ColumnarValue::Scalar(ScalarValue::Int32(v.as_deref().map(fnv_oid)))
                }
                ColumnarValue::Array(array) => {
                    let strings = array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            datafusion::error::DataFusionError::Execution(
                                "auger_oid expects text".into(),
                            )
                        })?;
                    let out: Int32Array =
                        strings.iter().map(|s| s.map(fnv_oid)).collect();
                    ColumnarValue::Array(Arc::new(out) as ArrayRef)
                }
                other => other.clone(),
            })
        }),
    ));
}

/// FNV-1a, folded into the positive half of `int4`.
///
/// PostgreSQL OIDs are unsigned, but every client reads them into a signed
/// integer, so staying positive avoids clients that reject a negative OID.
fn fnv_oid(name: &str) -> i32 {
    let mut hash: u32 = 0x811c_9dc5;
    for byte in name.as_bytes() {
        hash ^= *byte as u32;
        hash = hash.wrapping_mul(0x0100_0193);
    }
    (hash & 0x7fff_ffff) as i32
}

/// Define `pg_catalog` as views over `information_schema`.
///
/// Views rather than materialised tables: the Mongo catalog is discovered
/// lazily, so a snapshot taken at startup would miss every collection created
/// afterwards — and would force schema inference on all of them up front.
async fn register_pg_catalog(ctx: &SessionContext) -> anyhow::Result<()> {
    let statements = [
        // Schemas.
        r#"CREATE OR REPLACE VIEW pg_catalog.pg_namespace AS
           SELECT auger_oid(schema_name)         AS oid,
                  schema_name                    AS nspname,
                  10                             AS nspowner,
                  CAST(NULL AS VARCHAR)          AS nspacl
           FROM information_schema.schemata"#,
        // Relations. Every Mongo collection is reported as an ordinary table
        // ('r'); views defined in SQL come back as 'v'.
        r#"CREATE OR REPLACE VIEW pg_catalog.pg_class AS
           SELECT auger_oid(table_schema || '.' || table_name) AS oid,
                  table_name                                   AS relname,
                  auger_oid(table_schema)                      AS relnamespace,
                  0                                            AS reltype,
                  10                                           AS relowner,
                  CASE WHEN table_type = 'VIEW' THEN 'v' ELSE 'r' END AS relkind,
                  CAST(-1 AS BIGINT)                           AS reltuples,
                  0                                            AS relpages,
                  false                                        AS relhasindex,
                  false                                        AS relisshared,
                  false                                        AS relhasrules,
                  false                                        AS relhastriggers,
                  false                                        AS relrowsecurity,
                  CAST(NULL AS VARCHAR)                        AS relacl
           FROM information_schema.tables"#,
        // Columns. Querying this is what triggers schema inference for a
        // collection, which is why it is a view and not a table.
        r#"CREATE OR REPLACE VIEW pg_catalog.pg_attribute AS
           SELECT auger_oid(table_schema || '.' || table_name) AS attrelid,
                  column_name                                  AS attname,
                  0                                            AS atttypid,
                  CAST(ordinal_position AS INT)                AS attnum,
                  CAST(-1 AS INT)                              AS atttypmod,
                  is_nullable = 'NO'                           AS attnotnull,
                  false                                        AS attisdropped,
                  false                                        AS atthasdef,
                  0                                            AS attndims,
                  data_type                                    AS auger_type
           FROM information_schema.columns"#,
        // Enough of pg_type for a client to label the types it will actually
        // receive; see `server::encode::pg_type_of` for the mapping.
        //
        // typnamespace and typarray exist so psycopg2's hstore probe — the
        // first thing SQLAlchemy runs on connect, `SELECT t.oid, typarray FROM
        // pg_type t JOIN pg_namespace ns ON typnamespace = ns.oid WHERE typname
        // = 'hstore'` — plans and returns nothing rather than failing on a
        // missing column. Every emulated type lives in pg_catalog, and none is
        // an array element type, so the values are constant.
        r#"CREATE OR REPLACE VIEW pg_catalog.pg_type AS
           SELECT oid, typname, typtype, typlen,
                  auger_oid('pg_catalog') AS typnamespace,
                  0                        AS typarray
           FROM (VALUES
               (16,   'bool',        'b', 1),
               (17,   'bytea',       'b', 1),
               (20,   'int8',        'b', 8),
               (21,   'int2',        'b', 2),
               (23,   'int4',        'b', 4),
               (25,   'text',        'b', 1),
               (700,  'float4',      'b', 4),
               (701,  'float8',      'b', 8),
               (1082, 'date',        'b', 4),
               (1114, 'timestamp',   'b', 8),
               (1184, 'timestamptz', 'b', 8),
               (1700, 'numeric',     'b', 1)
           ) AS t(oid, typname, typtype, typlen)"#,
        // psql consults this before `\d`; an empty relation is a truthful answer.
        r#"CREATE OR REPLACE VIEW pg_catalog.pg_index AS
           SELECT CAST(0 AS INT) AS indexrelid,
                  CAST(0 AS INT) AS indrelid,
                  false          AS indisprimary,
                  false          AS indisunique
           WHERE false"#,
        r#"CREATE OR REPLACE VIEW pg_catalog.pg_roles AS
           SELECT 10 AS oid, 'auger' AS rolname, true AS rolsuper, true AS rolcanlogin"#,
    ];

    for sql in statements {
        ctx.sql(sql)
            .await
            .map_err(|e| anyhow::anyhow!("registering pg_catalog: {e}\n  in: {sql}"))?
            .collect()
            .await
            .map_err(|e| anyhow::anyhow!("registering pg_catalog: {e}"))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_statements_are_acknowledged_not_executed() {
        assert_eq!(intercept("SET client_encoding = 'UTF8'"), Some(Intercepted::Tag("SET".into())));
        assert_eq!(intercept("BEGIN"), Some(Intercepted::Tag("BEGIN".into())));
        assert_eq!(intercept("  commit ; "), Some(Intercepted::Tag("COMMIT".into())));
        assert_eq!(intercept(""), Some(Intercepted::Tag("EMPTY".into())));
    }

    #[test]
    fn real_queries_are_passed_through() {
        assert_eq!(intercept("SELECT * FROM users"), None);
        assert_eq!(intercept("INSERT INTO t VALUES (1)"), None);
    }

    #[test]
    fn function_qualifiers_are_stripped_but_table_qualifiers_are_kept() {
        assert_eq!(
            intercept("SELECT pg_catalog.version()"),
            Some(Intercepted::Rewritten("SELECT version()".into()))
        );
        // `pg_catalog.pg_class` is a genuine relation and must survive.
        assert_eq!(intercept("SELECT * FROM pg_catalog.pg_class"), None);
    }

    #[test]
    fn qualifier_stripping_handles_several_occurrences() {
        let sql = "SELECT pg_catalog.version(), pg_catalog.current_schema() FROM pg_catalog.pg_type";
        assert_eq!(
            strip_pg_catalog_prefix(sql),
            "SELECT version(), current_schema() FROM pg_catalog.pg_type"
        );
    }

    #[test]
    fn bare_catalog_relations_are_qualified() {
        // The psycopg2 hstore probe SQLAlchemy runs on connect: both relations
        // are bare, the aliases and columns must be left alone, and the string
        // literal must not be touched.
        assert_eq!(
            qualify_bare_pg_catalog(
                "SELECT t.oid, typarray FROM pg_type t \
                 JOIN pg_namespace ns ON typnamespace = ns.oid WHERE typname = 'hstore'"
            ),
            "SELECT t.oid, typarray FROM pg_catalog.pg_type t \
             JOIN pg_catalog.pg_namespace ns ON typnamespace = ns.oid WHERE typname = 'hstore'"
        );
    }

    #[test]
    fn qualification_reaches_intercept() {
        assert_eq!(
            intercept("SELECT nspname FROM pg_namespace WHERE nspname NOT LIKE 'pg_%'"),
            Some(Intercepted::Rewritten(
                "SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname NOT LIKE 'pg_%'".into()
            ))
        );
    }

    #[test]
    fn already_qualified_and_data_relations_are_left_alone() {
        // Preceded by `.`, so not bare — unchanged, and intercept returns None.
        assert_eq!(qualify_bare_pg_catalog("SELECT * FROM pg_catalog.pg_class"),
                   "SELECT * FROM pg_catalog.pg_class");
        assert_eq!(intercept("SELECT * FROM pg_catalog.pg_class"), None);
        // A collection that merely contains the substring is not a catalog name.
        assert_eq!(qualify_bare_pg_catalog("SELECT * FROM pg_type_log"),
                   "SELECT * FROM pg_type_log");
        // A column reference with a matching name is not the relation.
        assert_eq!(qualify_bare_pg_catalog("SELECT t.pg_type FROM things t"),
                   "SELECT t.pg_type FROM things t");
    }

    #[test]
    fn oids_are_deterministic_and_positive() {
        assert_eq!(fnv_oid("mydb.users"), fnv_oid("mydb.users"));
        assert_ne!(fnv_oid("mydb.users"), fnv_oid("mydb.orders"));
        for name in ["", "a", "very.long.qualified.name", "\u{1f600}"] {
            assert!(fnv_oid(name) >= 0, "{name} produced a negative oid");
        }
    }

    #[tokio::test]
    async fn pg_catalog_views_answer_queries() {
        let ctx = SessionContext::new_with_config(
            datafusion::prelude::SessionConfig::new().with_information_schema(true),
        );
        // A memory schema to hold the views.
        ctx.catalog("datafusion")
            .unwrap()
            .register_schema(
                "pg_catalog",
                Arc::new(datafusion::catalog::MemorySchemaProvider::new()),
            )
            .unwrap();

        register(&ctx, "datafusion", "public").await.unwrap();

        let rows = ctx.sql("SELECT version()").await.unwrap().collect().await.unwrap();
        let text = datafusion::arrow::util::pretty::pretty_format_batches(&rows).unwrap().to_string();
        assert!(text.contains("PostgreSQL 16.6"), "got: {text}");

        // The views must be queryable, not merely creatable.
        for relation in ["pg_namespace", "pg_class", "pg_type", "pg_index", "pg_roles"] {
            let sql = format!("SELECT * FROM pg_catalog.{relation}");
            ctx.sql(&sql).await.unwrap().collect().await.unwrap_or_else(|e| {
                panic!("querying {relation} failed: {e}");
            });
        }
    }
}
