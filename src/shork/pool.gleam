import gleam/erlang/process.{type Subject}
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor.{type Next}
import gleam/result
import shork

/// Config used to create connection pools.
pub opaque type PoolConfig {
  PoolConfig(
    /// The config to create new connections with
    connection_config: shork.Config,
    /// How many connections to hold in the pool
    /// The pool will also create more if needed, but the additional connections won't be 
    /// persisted in the pool.
    pool_size: Int,
    /// Maximal age of connections in the pool in milliseconds
    max_age: Int,
    /// Interval for housekeeping tasks like reopening connections, etc
    tick_interval: Int,
    /// Timeout of aquireing connections from the pool
    connection_timeout: Int,
  )
}

/// Internal representation of a connection in the pool
type PoolConnection {
  PoolConnection(
    /// The underlying shork connection
    /// `None` in case it is not yet established
    connection: Option(shork.Connection),
    /// Whether the connection is currently in use
    in_use: Bool,
    /// Whether the connection is outside of the pool
    /// This is used for connections that exceed the pool size.
    /// Example: Let's say the `pool_size` is 5 and there are currently 5 connections in use. 
    /// The next call to `connect()` will create a new connection that's not part of the 
    /// pool (i.e. `is_outside_of_pool` = True). When that connection is disconnected, instead
    /// of it being marked for reuse, it's completely disconnected. That way `connect()` will
    /// always be able to return a connection without the pool growing over time.
    is_outside_of_pool: Bool,
    /// The age of the connection in milliseconds
    age: Int,
  )
}

/// Internal representation of the pool state
type PoolState {
  PoolState(
    /// The config of the pool
    config: PoolConfig,
    /// The list of connections in the pool
    /// /// Notably, the connections list also contains temporary connections that are outside
    /// of the pool. See `PoolConnection.is_outside_of_pool`.
    connections: List(PoolConnection),
  )
}

/// This structure is used to interact with a connection pool
pub opaque type ConnectionPool {
  ConnectionPool(
    /// The actor subject to talk to the pool
    actor: Subject(PoolActorMessage),
    /// The config of the pool
    /// We need this copy of the config, because some properties (namely, the
    /// `connection_timeout`) are accessed from outside the actor.
    config: PoolConfig,
  )
}

/// The default config for a pool
pub fn default_config(connection_config: shork.Config) -> PoolConfig {
  PoolConfig(
    connection_config: connection_config,
    pool_size: 10,
    max_age: 20_000,
    tick_interval: 250,
    connection_timeout: 1000,
  )
}

/// Builder-pattern-style setter for the pool size
pub fn pool_size(config: PoolConfig, pool_size: Int) -> PoolConfig {
  PoolConfig(..config, pool_size: pool_size)
}

/// Builder-pattern-style setter for the maximal pool age
pub fn max_age(config: PoolConfig, max_age: Int) -> PoolConfig {
  PoolConfig(..config, max_age: max_age)
}

/// Create a connection pool
/// 
/// An Ok result contains the initized connection pool that can directly be used.
/// Note however, the connections in the pool are only established after `tick_interval`
/// milliseconds. Put another way, all connections created by the pool within that time
/// frame are temporary and will not be reused.
/// 
/// An Error result means the pool could not be started.
pub fn create(config: PoolConfig) -> Result(ConnectionPool, actor.StartError) {
  let initial_state =
    PoolState(
      config: config,
      connections: list.range(1, config.pool_size)
        |> list.map(fn(_) {
          PoolConnection(
            connection: None,
            in_use: False,
            age: 0,
            is_outside_of_pool: False,
          )
        }),
    )

  let pool_actor_result =
    actor.new(initial_state)
    |> actor.on_message(pool_actor_handler)
    |> actor.start

  use pool_actor <- result.try(pool_actor_result)
  let pool_actor_subject = pool_actor.data
  actor.send(pool_actor_subject, Tick(pool_actor_subject))

  Ok(ConnectionPool(pool_actor_subject, config))
}

/// The message type for new connections
/// 
/// This is used for exposing a single connection from the pool actor to the calling actor.
type ConnectionActorMessage {
  ConnectionEstablished(connection: PoolConnection)
}

/// The message type for the pool actor
type PoolActorMessage {
  /// Tick message for housekeeping
  Tick(Subject(PoolActorMessage))
  /// Message to create a new connection
  /// The parameter subject is the caller.
  Connect(Subject(ConnectionActorMessage))
  /// Message to (virtually) disconnect a connection
  Disconnect(PoolConnection)
}

/// Get a connection from the pool
/// 
/// If there is no connection available a new (temporary) connection will be returned.
pub fn connect(pool: ConnectionPool) -> shork.Connection {
  let response =
    process.call(
      pool.actor,
      pool.config.connection_timeout,
      fn(connection_actor) { Connect(connection_actor) },
    )
  let pool_connection = response.connection
  // This assert is justified because `find_unused_and_mark_as_used()` will only return 
  // connections that are active.
  let assert Some(shork_connection) = pool_connection.connection
  shork.make_pooled(shork_connection, fn(_) {
    pool_disconnect(pool, pool_connection)
  })
}

/// Internal disconnect function
fn pool_disconnect(pool: ConnectionPool, connection: PoolConnection) {
  actor.send(pool.actor, Disconnect(connection))
}

/// Handler function for the pool actor
/// 
fn pool_actor_handler(
  state: PoolState,
  msg: PoolActorMessage,
) -> Next(PoolState, PoolActorMessage) {
  case msg {
    Tick(actor) -> pool_actor_tick(state, actor)
    Connect(connection_actor) -> pool_actor_connect(state, connection_actor)
    Disconnect(connection) -> pool_actor_disconnect(state, connection)
  }
}

/// Handler function for new connections
/// 
/// This function will send a connection to the `connection_actor`.
/// 
/// If there is a free & connected connection in the pool, that one will be used.
/// If there is not, a temporary connection outside of the pool will be created.
fn pool_actor_connect(
  state: PoolState,
  connection_actor: Subject(ConnectionActorMessage),
) -> Next(PoolState, PoolActorMessage) {
  let #(reuseable_connection, new_state) = find_unused_and_mark_as_used(state)

  case reuseable_connection {
    Some(connection) -> connection
    None ->
      // no connection available -> create new temporary connection outside of pool
      PoolConnection(
        connection: Some(shork.connect(state.config.connection_config)),
        age: 0,
        in_use: True,
        is_outside_of_pool: True,
      )
  }
  |> ConnectionEstablished
  |> actor.send(connection_actor, _)

  actor.continue(new_state)
}

/// Handler function for disconnecting a connection
/// 
/// If the connection is inside of the pool (i.e. `is_outside_of_pool` = False), it will
/// be marked as free. Otherwise it is disconnected.
fn pool_actor_disconnect(
  state: PoolState,
  connection: PoolConnection,
) -> Next(PoolState, PoolActorMessage) {
  case connection.is_outside_of_pool {
    True -> {
      let assert Some(shork_connection) = connection.connection
      shork.disconnect(shork_connection)
      state
    }
    False ->
      PoolState(
        ..state,
        connections: state.connections
          |> list.map(fn(pool_connection) {
            case pool_connection == connection {
              True -> PoolConnection(..pool_connection, in_use: False)
              False -> pool_connection
            }
          }),
      )
  }
  |> actor.continue
}

/// Find an unused (& active) connection in the pool
/// 
/// If such a connection is found, it is marked as `in_use` and returned.
/// Otherwise `None` is returned and the PoolState is not modified.
fn find_unused_and_mark_as_used(
  state: PoolState,
) -> #(Option(PoolConnection), PoolState) {
  let #(connection_candidate, connections) =
    state.connections
    |> list.map_fold(None, fn(candidate, pool_connection) {
      case candidate, pool_connection {
        Some(_), _ -> #(candidate, pool_connection)
        None, PoolConnection(in_use: False, connection: Some(_), ..) -> #(
          Some(pool_connection),
          PoolConnection(..pool_connection, in_use: True),
        )
        None, _ -> #(candidate, pool_connection)
      }
    })

  #(connection_candidate, PoolState(..state, connections: connections))
}

/// Handler function for housekeeping
/// 
/// This will refresh the pool, and reschedule itself.
fn pool_actor_tick(
  state: PoolState,
  pool_actor: Subject(PoolActorMessage),
) -> Next(PoolState, PoolActorMessage) {
  process.send_after(pool_actor, state.config.tick_interval, Tick(pool_actor))

  state
  |> refresh_connections
  |> actor.continue
}

/// Create a new connection for the pool
fn create_new_pool_connection(state: PoolState) -> PoolConnection {
  PoolConnection(
    connection: Some(shork.connect(state.config.connection_config)),
    in_use: False,
    age: 0,
    is_outside_of_pool: False,
  )
}

/// Disconnected a connection and connects it again
fn close_and_recreate_pool_connection(
  connection: shork.Connection,
  state: PoolState,
) {
  shork.disconnect(connection)
  create_new_pool_connection(state)
}

/// Looks for old connections that are not `in_use` and reconnects them
/// 
/// Also incremens `age` counter of connections.
fn refresh_connections(state: PoolState) -> PoolState {
  PoolState(
    ..state,
    connections: state.connections
      |> list.map(fn(connection: PoolConnection) -> PoolConnection {
        case connection {
          PoolConnection(in_use: False, connection: None, ..) ->
            create_new_pool_connection(state)
          PoolConnection(
            in_use: False,
            connection: Some(shork_conenction),
            age: age,
            ..,
          )
            if age > state.config.max_age
          -> close_and_recreate_pool_connection(shork_conenction, state)
          PoolConnection(age: age, ..) ->
            PoolConnection(..connection, age: age + state.config.tick_interval)
        }
      }),
  )
}
