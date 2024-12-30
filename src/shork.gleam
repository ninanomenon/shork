import gleam/dynamic
import gleam/dynamic/decode
import gleam/erlang/charlist.{type Charlist, from_string}
import gleam/list
import gleam/result

// TODO: Connection pooling 
// TODO: Transactions
// TODO: Better Error Handling
// TODO: Documentation
// TODO: Datetime Decoder
// TODO: Tests

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

pub fn host(config: Config, host: String) -> Config {
  Config(..config, host: from_string(host))
}

pub fn port(config: Config, port: Int) -> Config {
  Config(..config, port:)
}

pub fn user(config: Config, user: String) -> Config {
  Config(..config, user: from_string(user))
}

pub fn database(config: Config, database: String) -> Config {
  Config(..config, database: from_string(database))
}

pub fn password(config: Config, password: String) -> Config {
  Config(..config, password: from_string(password))
}

pub fn connection_timeout(config: Config, query_timeout: Int) -> Config {
  Config(..config, query_timeout:)
}

pub fn log_warnings(config: Config, log_warnings: Bool) -> Config {
  Config(..config, log_warnings:)
}

pub fn log_slow_queries(config: Config, log_slow_queries: Bool) -> Config {
  Config(..config, log_slow_queries:)
}

pub fn keep_alive(config: Config, keep_alive: Bool) -> Config {
  Config(..config, keep_alive:)
}

pub fn query_timeout(config: Config, query_timeout: Int) -> Config {
  Config(..config, query_timeout:)
}

pub fn query_cache_time(config: Config, query_cache_time: Int) -> Config {
  Config(..config, query_cache_time:)
}

pub type Connection

@external(erlang, "shork_ffi", "connect")
pub fn connect(a: Config) -> Connection

@external(erlang, "shork_ffi", "disconnect")
pub fn disconnect(a: Connection) -> Nil

pub type Value

@external(erlang, "shork_ffi", "intimidate")
pub fn bool(a: Bool) -> Value

@external(erlang, "shork_ffi", "intimidate")
pub fn int(a: Int) -> Value

@external(erlang, "shork_ffi", "intimidate")
pub fn float(a: Float) -> Value

@external(erlang, "shork_ffi", "intimidate")
pub fn text(a: String) -> Value

pub opaque type Query(row_type) {
  Query(
    sql: String,
    parameters: List(Value),
    row_decoder: decode.Decoder(row_type),
  )
}

pub fn returning(query: Query(t1), decoder: decode.Decoder(t2)) -> Query(t2) {
  let Query(sql:, parameters:, row_decoder: _) = query
  Query(sql:, parameters:, row_decoder: decoder)
}

pub type QueryError {
  UnexpectedResultType(List(decode.DecodeError))
}

pub fn query(sql: String) -> Query(Nil) {
  Query(sql:, parameters: [], row_decoder: decode.success(Nil))
}

@external(erlang, "shork_ffi", "query")
pub fn run_query(
  a: Connection,
  b: String,
  c: List(Value),
) -> Result(#(List(String), List(dynamic.Dynamic)), QueryError)

pub type Returend(t) {
  Returend(column_names: List(String), rows: List(t))
}

pub fn parameter(query: Query(t1), parameter: Value) -> Query(t1) {
  Query(..query, parameters: [parameter, ..query.parameters])
}

pub fn execute(
  query: Query(t),
  connection: Connection,
) -> Result(Returend(t), QueryError) {
  let parameters = list.reverse(query.parameters)

  use #(column_names, rows) <- result.try(run_query(
    connection,
    query.sql,
    parameters,
  ))
  use rows <- result.then(
    rows
    |> list.try_map(decode.run(_, query.row_decoder))
    |> result.map_error(UnexpectedResultType),
  )
  Ok(Returend(column_names, rows))
}
