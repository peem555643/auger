//! The PostgreSQL wire-protocol front end.
//!
//! Choosing this protocol over a bespoke JDBC/ODBC driver is the design
//! decision that makes the gateway usable: psql, DBeaver, Tableau, Power BI,
//! Metabase, Grafana and every PostgreSQL client library connect with no driver
//! to install. Drill's equivalent surface is its own JDBC jar, which is why
//! anything outside the JVM has a bad time with it.
//!
//! Results are streamed. A `RecordBatch` is encoded and flushed as soon as the
//! cursor produces it, so peak memory is one batch rather than one result set.

pub mod compat;
pub mod encode;

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use futures::{Stream, StreamExt};
use pgwire::api::auth::md5pass::{Md5PasswordAuthStartupHandler, hash_md5_password};
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::auth::{
    AuthSource, DefaultServerParameterProvider, LoginInfo, Password, StartupHandler,
};
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DescribePortalResponse, DescribeStatementResponse, FieldInfo, QueryResponse, Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::{ClientInfo, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use pgwire::tokio::process_socket;
use tokio::net::TcpListener;

use crate::config::{AuthMode, Config};
use crate::server::compat::Intercepted;
use crate::server::encode::{encode_batch, field_infos};

/// Shared query-execution state behind every connection.
pub struct AugerHandler {
    ctx: Arc<SessionContext>,
    config: Arc<Config>,
    query_parser: Arc<NoopQueryParser>,
}

impl AugerHandler {
    pub fn new(ctx: Arc<SessionContext>, config: Arc<Config>) -> Self {
        Self { ctx, config, query_parser: Arc::new(NoopQueryParser::new()) }
    }

    /// Plan and run one statement, returning a streaming response.
    async fn run(&self, sql: &str, format: &Format) -> PgWireResult<Response> {
        let sql = match compat::intercept(sql) {
            Some(Intercepted::Tag(tag)) => return Ok(Response::Execution(Tag::new(&tag))),
            Some(Intercepted::Rewritten(rewritten)) => rewritten,
            None => sql.to_string(),
        };

        let df = self.ctx.sql(&sql).await.map_err(planning_error)?;
        let schema = Arc::new(df.schema().as_arrow().clone());
        let fields = Arc::new(field_infos(&schema, format));

        let stream = df.execute_stream().await.map_err(execution_error)?;
        let rows = self.to_row_stream(stream, Arc::clone(&fields));

        Ok(Response::Query(QueryResponse::new(fields, rows)))
    }

    /// Adapt a stream of Arrow batches into a stream of wire rows.
    fn to_row_stream(
        &self,
        batches: datafusion::execution::SendableRecordBatchStream,
        fields: Arc<Vec<FieldInfo>>,
    ) -> impl Stream<Item = PgWireResult<DataRow>> + Send + 'static {
        let deadline = (self.config.server.statement_timeout_secs > 0).then(|| {
            Instant::now() + Duration::from_secs(self.config.server.statement_timeout_secs)
        });

        batches
            .map(move |result| {
                // The timeout is checked per batch rather than enforced by a
                // timer, so a query is cut off between batches and never in the
                // middle of encoding a row.
                if let Some(deadline) = deadline {
                    if Instant::now() > deadline {
                        return futures::stream::iter(vec![Err(PgWireError::UserError(
                            Box::new(ErrorInfo::new(
                                "ERROR".into(),
                                // 57014: query_canceled
                                "57014".into(),
                                "canceled: statement timeout exceeded".into(),
                            )),
                        ))]);
                    }
                }

                let encoded = match result {
                    Ok(batch) => match encode_batch(&batch, &fields) {
                        Ok(rows) => rows.into_iter().map(Ok).collect(),
                        Err(e) => vec![Err(e)],
                    },
                    Err(e) => vec![Err(execution_error(e))],
                };
                futures::stream::iter(encoded)
            })
            .flatten()
    }

    /// Describe a statement's result columns without running it.
    async fn describe(&self, sql: &str, format: &Format) -> PgWireResult<Vec<FieldInfo>> {
        // Session statements produce no rows, so there is nothing to describe.
        if matches!(compat::intercept(sql), Some(Intercepted::Tag(_))) {
            return Ok(Vec::new());
        }
        let sql = match compat::intercept(sql) {
            Some(Intercepted::Rewritten(r)) => r,
            _ => sql.to_string(),
        };

        let plan = self
            .ctx
            .state()
            .create_logical_plan(&sql)
            .await
            .map_err(planning_error)?;
        let schema = plan.schema().as_arrow().clone();
        Ok(field_infos(&schema, format))
    }
}

#[async_trait]
impl SimpleQueryHandler for AugerHandler {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let mut responses = Vec::new();
        for statement in split_statements(query) {
            responses.push(self.run(&statement, &Format::UnifiedText).await?);
        }
        if responses.is_empty() {
            responses.push(Response::EmptyQuery);
        }
        Ok(responses)
    }
}

#[async_trait]
impl ExtendedQueryHandler for AugerHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::clone(&self.query_parser)
    }

    async fn do_query<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        // Bound parameters are not substituted yet; a portal carrying them is
        // refused rather than silently executed with the placeholders intact.
        if portal.parameter_len() > 0 {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".into(),
                "0A000".into(),
                "bound parameters are not supported yet; send the statement with literals".into(),
            ))));
        }
        self.run(&portal.statement.statement, &portal.result_column_format).await
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        stmt: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let param_types: Vec<Type> = stmt
            .parameter_types
            .iter()
            .map(|t| t.clone().unwrap_or(Type::UNKNOWN))
            .collect();
        let fields = self.describe(&stmt.statement, &Format::UnifiedBinary).await?;
        Ok(DescribeStatementResponse::new(param_types, fields))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let fields = self
            .describe(&portal.statement.statement, &portal.result_column_format)
            .await?;
        Ok(DescribePortalResponse::new(fields))
    }
}

/// Trust mode: any user is accepted, no password requested.
impl NoopStartupHandler for AugerHandler {}

/// Password lookup for the `md5` and `scram` auth modes.
#[derive(Debug)]
struct ConfiguredAuth {
    users: std::collections::HashMap<String, String>,
}

#[async_trait]
impl AuthSource for ConfiguredAuth {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        let user = login.user().map(str::to_string).unwrap_or_default();
        let Some(password) = self.users.get(&user) else {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "FATAL".into(),
                // 28P01: invalid_password — the same code as a wrong password,
                // so probing cannot distinguish an unknown user from a bad one.
                "28P01".into(),
                format!("password authentication failed for user \"{user}\""),
            ))));
        };
        let salt = vec![0u8; 4];
        let hashed = hash_md5_password(&user, password, &salt);
        Ok(Password::new(Some(salt), hashed.into_bytes()))
    }
}

/// Connection factory handed to pgwire.
pub struct AugerFactory {
    handler: Arc<AugerHandler>,
    config: Arc<Config>,
}

impl PgWireServerHandlers for AugerFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::clone(&self.handler)
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        Arc::clone(&self.handler)
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        match self.config.server.auth {
            AuthMode::Trust => StartupVariant::Trust(Arc::clone(&self.handler)),
            AuthMode::Md5 | AuthMode::Scram => {
                let mut params = DefaultServerParameterProvider::default();
                params.server_version = compat::SERVER_VERSION.to_string();
                StartupVariant::Md5(Box::new(Md5PasswordAuthStartupHandler::new(
                    Arc::new(ConfiguredAuth { users: self.config.server.users.clone() }),
                    Arc::new(params),
                )))
            }
        }
        .into_arc()
    }
}

/// `PgWireServerHandlers::startup_handler` returns one concrete type, so the
/// two authentication paths are unified behind this enum.
enum StartupVariant {
    Trust(Arc<AugerHandler>),
    Md5(Box<Md5PasswordAuthStartupHandler<ConfiguredAuth, DefaultServerParameterProvider>>),
}

impl StartupVariant {
    fn into_arc(self) -> Arc<Self> {
        Arc::new(self)
    }
}

#[async_trait]
impl StartupHandler for StartupVariant {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: pgwire::messages::PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo
            + futures::Sink<pgwire::messages::PgWireBackendMessage>
            + Unpin
            + Send
            + Sync,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as futures::Sink<pgwire::messages::PgWireBackendMessage>>::Error>,
    {
        match self {
            Self::Trust(h) => h.on_startup(client, message).await,
            Self::Md5(h) => h.on_startup(client, message).await,
        }
    }
}

/// Bind the listener and serve until the process is stopped.
pub async fn serve(ctx: Arc<SessionContext>, config: Arc<Config>) -> anyhow::Result<()> {
    let listener = TcpListener::bind(&config.server.listen).await.map_err(|e| {
        anyhow::anyhow!("cannot bind {}: {e}", config.server.listen)
    })?;
    tracing::info!(
        address = %config.server.listen,
        auth = ?config.server.auth,
        "accepting PostgreSQL connections"
    );

    let factory = Arc::new(AugerFactory {
        handler: Arc::new(AugerHandler::new(ctx, Arc::clone(&config))),
        config,
    });

    loop {
        let (socket, peer) = match listener.accept().await {
            Ok(pair) => pair,
            Err(e) => {
                // A failed accept (fd exhaustion, a client vanishing mid
                // handshake) must not take the whole server down.
                tracing::warn!(error = %e, "accept failed");
                continue;
            }
        };
        let factory = Arc::clone(&factory);
        tokio::spawn(async move {
            tracing::debug!(%peer, "client connected");
            if let Err(e) = process_socket(socket, None, factory).await {
                tracing::debug!(%peer, error = %e, "connection closed with an error");
            }
        });
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Split a simple-query payload into individual statements.
///
/// The simple query protocol allows several statements in one message, and
/// clients use it — psql sends `SET ...; SELECT ...;` on connect. Splitting has
/// to respect quoting or a semicolon inside a string literal truncates the
/// query.
fn split_statements(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut in_single = false;
    let mut in_double = false;
    let mut chars = sql.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '\'' if !in_double => {
                // Two adjacent quotes are an escaped quote, not a close.
                if in_single && chars.peek() == Some(&'\'') {
                    current.push(ch);
                    current.push(chars.next().unwrap());
                    continue;
                }
                in_single = !in_single;
                current.push(ch);
            }
            '"' if !in_single => {
                in_double = !in_double;
                current.push(ch);
            }
            ';' if !in_single && !in_double => {
                if !current.trim().is_empty() {
                    out.push(current.trim().to_string());
                }
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    if !current.trim().is_empty() {
        out.push(current.trim().to_string());
    }
    out
}

fn planning_error(e: datafusion::error::DataFusionError) -> PgWireError {
    // 42601 is syntax_error; 42P01 is undefined_table. Distinguishing them lets
    // a client tell a typo from a missing collection.
    let message = e.to_string();
    let code = if message.contains("table") && message.contains("not found") {
        "42P01"
    } else {
        "42601"
    };
    PgWireError::UserError(Box::new(ErrorInfo::new("ERROR".into(), code.into(), message)))
}

fn execution_error(e: datafusion::error::DataFusionError) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".into(),
        // 58000: system_error — the statement was valid but could not be run.
        "58000".into(),
        e.to_string(),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_multiple_statements() {
        assert_eq!(
            split_statements("SET a = 1; SELECT 1"),
            vec!["SET a = 1".to_string(), "SELECT 1".to_string()]
        );
    }

    #[test]
    fn semicolon_inside_a_string_does_not_split() {
        assert_eq!(
            split_statements("SELECT 'a;b' FROM t"),
            vec!["SELECT 'a;b' FROM t".to_string()]
        );
    }

    #[test]
    fn escaped_quotes_are_handled() {
        assert_eq!(
            split_statements("SELECT 'it''s; fine'"),
            vec!["SELECT 'it''s; fine'".to_string()]
        );
    }

    #[test]
    fn quoted_identifiers_protect_semicolons() {
        assert_eq!(
            split_statements(r#"SELECT "od;d" FROM t"#),
            vec![r#"SELECT "od;d" FROM t"#.to_string()]
        );
    }

    #[test]
    fn trailing_and_empty_statements_are_dropped() {
        assert_eq!(split_statements("SELECT 1;;  ;"), vec!["SELECT 1".to_string()]);
        assert!(split_statements("   ").is_empty());
    }
}
