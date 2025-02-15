import gleam/dynamic
import gleam/dynamic/decode
import gleam/erlang/charlist.{type Charlist, from_string}
import gleam/list
import gleam/option
import gleam/result

// TODO: SSL
// TODO: Connection pooling
// TODO: Better Error Handling
// TODO: Datetime Decoder

/// The configuration of a connection.
pub opaque type Config {
  Config(
    /// (default: locahost) Hostname of the MySQL database.
    host: Charlist,
    /// (default: 3306) Port the server is listing on,
    port: Int,
    /// Username to connect to the database as.
    user: Charlist,
    /// Name of the database to use.
    database: Charlist,
    /// password of the user.
    password: Charlist,
    /// The maximum time to spend on connect.
    connection_timeout: Int,
    /// (default: false) Whether to fetch warnings and log them using error_logger.
    log_warnings: Bool,
    /// (default: false) Whether to log slow queries using error_logger.
    /// Queries are flagged as slow by the server if their execution time exceeds
    /// the value in the `long_query_time' variable.
    log_slow_queries: Bool,
    /// Send ping when unused for a certain time.
    keep_alive: Bool,
    /// The default time to wait for a response when executing a query.
    query_timeout: Int,
    /// The minimum number of milliseconds to cache prepared statements used
    /// for parametrized queries with query.
    query_cache_time: Int,
  )
}

/// The default configuration for a connection.
///
pub fn default_config() {
  Config(
    host: from_string("localhost"),
    port: 3306,
    user: from_string("mysql"),
    database: from_string("default"),
    password: from_string("mysql"),
    connection_timeout: 3000,
    log_warnings: False,
    log_slow_queries: False,
    keep_alive: True,
    query_timeout: 5000,
    query_cache_time: 1000,
  )
}

/// Database server hostname.
///
/// (default: localhost)
pub fn host(config: Config, host: String) -> Config {
  Config(..config, host: from_string(host))
}

/// Port the server is listing on.
///
/// (default: 3306)
pub fn port(config: Config, port: Int) -> Config {
  Config(..config, port:)
}

/// Username to connect to database as.
pub fn user(config: Config, user: String) -> Config {
  Config(..config, user: from_string(user))
}

/// Name of the database to use.
pub fn database(config: Config, database: String) -> Config {
  Config(..config, database: from_string(database))
}

/// Password for the user.
pub fn password(config: Config, password: String) -> Config {
  Config(..config, password: from_string(password))
}

/// The maximum time to spend on connect.
///
/// (default: 3000)
pub fn connection_timeout(config: Config, query_timeout: Int) -> Config {
  Config(..config, query_timeout:)
}

/// Whether to fetch warnings and log them using error_logger.
///
/// (default: False)
pub fn log_warnings(config: Config, log_warnings: Bool) -> Config {
  Config(..config, log_warnings:)
}

/// Whether to log queries that got as slow query from the server.
///
/// (default: False)
pub fn log_slow_queries(config: Config, log_slow_queries: Bool) -> Config {
  Config(..config, log_slow_queries:)
}

/// Whether to send keep alive messages for used connections.
///
/// (default: False)
pub fn keep_alive(config: Config, keep_alive: Bool) -> Config {
  Config(..config, keep_alive:)
}

/// Default time to wait for a query to execute.
///
/// (default: 5000)
pub fn query_timeout(config: Config, query_timeout: Int) -> Config {
  Config(..config, query_timeout:)
}

/// The minimum number of milliseconds to cache prepared statements used
/// for parametrized queries with query.
///
/// (default: 1000)
pub fn query_cache_time(config: Config, query_cache_time: Int) -> Config {
  Config(..config, query_cache_time:)
}

/// A connection to a database against which queries can be made.
///
/// Created using the `connect` function and shutdown using the `disconnect`
// function

pub opaque type Connection {
  Connection(raw: RawConnection, disconnect: fn(Connection) -> Nil)
}

/// internal raw connection type
/// 
/// The reason for this indirection is that the disconnect behaviour might
/// be different for pooled connections 
type RawConnection

@external(erlang, "shork_ffi", "connect")
fn raw_connect(a: Config) -> RawConnection

pub fn make_pooled(
  connection: Connection,
  disconnect: fn(Connection) -> Nil,
) -> Connection {
  Connection(..connection, disconnect: disconnect)
}

/// Starts a database connection.
pub fn connect(config: Config) -> Connection {
  Connection(raw_connect(config), simple_disconnect)
}

fn simple_disconnect(c: Connection) -> Nil {
  raw_disconnect(c.raw)
}

@external(erlang, "shork_ffi", "disconnect")
fn raw_disconnect(a: RawConnection) -> Nil

/// Stops a database connection.
pub fn disconnect(connection: Connection) -> Nil {
  connection.disconnect(connection)
}

/// A value that can be sent to MySQL as one of the arguments to a
/// parameterised SQL query.
pub type Value

@external(erlang, "shork_ffi", "null")
pub fn null() -> Value

@external(erlang, "shork_ffi", "coerce")
pub fn bool(a: Bool) -> Value

@external(erlang, "shork_ffi", "coerce")
pub fn int(a: Int) -> Value

@external(erlang, "shork_ffi", "coerce")
pub fn float(a: Float) -> Value

@external(erlang, "shork_ffi", "coerce")
pub fn text(a: String) -> Value

pub opaque type Query(row_type) {
  Query(
    sql: String,
    parameters: List(Value),
    row_decoder: decode.Decoder(row_type),
    timeout: option.Option(Int),
  )
}

/// Create a new query to use with the `execute`, `returning` and `parameters`
/// function.
///
pub fn returning(query: Query(t1), decoder: decode.Decoder(t2)) -> Query(t2) {
  let Query(sql:, parameters:, row_decoder: _, timeout:) = query
  Query(sql:, parameters:, row_decoder: decoder, timeout:)
}

pub type QueryError {
  /// The rows returned by the database could not be decoded using
  /// the supplied dynamic decoder.
  UnexpectedResultType(List(decode.DecodeError))
  /// The server returned a error during processing the query.
  ServerError(Int, String)
}

/// Set the decoder to use for the type of row returned by executing this query.
///
/// If the decoder is unable to decode the row value then query will return an
/// error.
pub fn query(sql: String) -> Query(Nil) {
  Query(
    sql:,
    parameters: [],
    row_decoder: decode.success(Nil),
    timeout: option.None,
  )
}

/// Push a new parameter value for the query.
pub fn parameter(query: Query(t), parameter: Value) -> Query(t) {
  Query(..query, parameters: [parameter, ..query.parameters])
}

/// Use a custom timeout for the query.
/// The timeout give is given in ms.
pub fn timeout(query: Query(t), timeout: Int) -> Query(t) {
  Query(..query, timeout: option.Some(timeout))
}

pub type TransactionError {
  /// One query inside a query returned a error.
  TransactionQueryError(QueryError)
  /// The transaction rolled back as an result of an
  /// error inside the transaction.
  TransactionRolledBack(String)
}

@external(erlang, "shork_ffi", "query")
fn run_query(
  a: RawConnection,
  b: String,
  c: List(Value),
  d: option.Option(Int),
) -> Result(#(List(String), List(dynamic.Dynamic)), QueryError)

@external(erlang, "shork_ffi", "transaction")
fn raw_transaction(
  connection: RawConnection,
  callback cb: fn(RawConnection) -> Result(t, e),
) -> Result(t, TransactionError)

pub fn transaction(
  connection: Connection,
  callback cb: fn(Connection) -> Result(t, e),
) -> Result(t, TransactionError) {
  raw_transaction(connection.raw, fn(raw: RawConnection) {
    cb(Connection(..connection, raw: raw))
  })
}

/// The names of the column names and the  rows returned
/// by the query.
pub type Returned(t) {
  Returned(column_names: List(String), rows: List(t))
}

/// Run a query against a MySQL/MariaDB database.
pub fn execute(
  query query: Query(t),
  on connection: Connection,
) -> Result(Returned(t), QueryError) {
  let parameters = list.reverse(query.parameters)

  use #(column_names, rows) <- result.try(run_query(
    connection.raw,
    query.sql,
    parameters,
    query.timeout,
  ))
  use rows <- result.then(
    rows
    |> list.try_map(decode.run(_, query.row_decoder))
    |> result.map_error(UnexpectedResultType),
  )

  Ok(Returned(column_names, rows))
}
