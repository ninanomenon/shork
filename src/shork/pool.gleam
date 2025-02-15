import gleam/erlang/process.{type Subject}
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor.{type Next}
import gleam/result
import shork

pub opaque type PoolConfig {
  PoolConfig(
    connection_config: shork.Config,
    pool_size: Int,
    max_age: Int,
    tick_interval: Int,
    connection_timeout: Int,
  )
}

type PoolConnection {
  PoolConnection(
    connection: Option(shork.Connection),
    in_use: Bool,
    is_outside_of_pool: Bool,
    age: Int,
  )
}

type PoolState {
  PoolState(config: PoolConfig, connections: List(PoolConnection))
}

pub opaque type ConnectionPool {
  ConnectionPool(actor: Subject(PoolActorMessage), config: PoolConfig)
}

pub fn default_config(connection_config: shork.Config) -> PoolConfig {
  PoolConfig(
    connection_config: connection_config,
    pool_size: 10,
    max_age: 20_000,
    tick_interval: 250,
    connection_timeout: 1000,
  )
}

pub fn pool_size(config: PoolConfig, pool_size: Int) -> PoolConfig {
  PoolConfig(..config, pool_size: pool_size)
}

pub fn max_age(config: PoolConfig, max_age: Int) -> PoolConfig {
  PoolConfig(..config, max_age: max_age)
}

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

  use pool_actor <- result.try(actor.start(initial_state, pool_actor_handler))
  actor.send(pool_actor, Tick(pool_actor))

  Ok(ConnectionPool(pool_actor, config))
}

type ConnectionActorMessage {
  ConnectionEstablished(connection: PoolConnection)
}

type PoolActorMessage {
  Tick(Subject(PoolActorMessage))
  Connect(Subject(ConnectionActorMessage))
  Disconnect(PoolConnection)
}

pub fn connect(pool: ConnectionPool) -> Result(shork.Connection, Nil) {
  process.try_call(
    pool.actor,
    fn(connection_actor) { Connect(connection_actor) },
    pool.config.connection_timeout,
  )
  |> result.map(fn(response: ConnectionActorMessage) {
    let pool_connection = response.connection
    let assert Some(shork_connection) = pool_connection.connection
    shork.make_pooled(shork_connection, fn(_) {
      pool_disconnect(pool, pool_connection)
    })
  })
  |> result.map_error(fn(_) { Nil })
}

fn pool_disconnect(pool: ConnectionPool, connection: PoolConnection) {
  actor.send(pool.actor, Disconnect(connection))
}

fn pool_actor_handler(
  msg: PoolActorMessage,
  state: PoolState,
) -> Next(PoolActorMessage, PoolState) {
  case msg {
    Tick(actor) -> pool_actor_tick(state, actor)
    Connect(connection_actor) -> pool_actor_connect(state, connection_actor)
    Disconnect(connection) -> pool_actor_disconnect(state, connection)
  }
}

fn pool_actor_connect(
  state: PoolState,
  connection_actor: Subject(ConnectionActorMessage),
) -> Next(PoolActorMessage, PoolState) {
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

fn pool_actor_disconnect(
  state: PoolState,
  connection: PoolConnection,
) -> Next(PoolActorMessage, PoolState) {
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

fn find_unused_and_mark_as_used(
  state: PoolState,
) -> #(Option(PoolConnection), PoolState) {
  let #(connection_candidate, connections) =
    state.connections
    |> list.map_fold(None, fn(candidate, pool_connection) {
      case candidate, pool_connection {
        Some(_), _ -> #(candidate, pool_connection)
        None, PoolConnection(in_use: True, ..) -> #(candidate, pool_connection)
        None, PoolConnection(in_use: False, ..) -> #(
          Some(pool_connection),
          PoolConnection(..pool_connection, in_use: True),
        )
      }
    })

  #(connection_candidate, PoolState(..state, connections: connections))
}

fn pool_actor_tick(
  state: PoolState,
  pool_actor: Subject(PoolActorMessage),
) -> Next(PoolActorMessage, PoolState) {
  process.send_after(pool_actor, state.config.tick_interval, Tick(pool_actor))

  state
  |> refresh_connections
  |> actor.continue
}

fn create_new_pool_connection(state: PoolState) -> PoolConnection {
  PoolConnection(
    connection: Some(shork.connect(state.config.connection_config)),
    in_use: False,
    age: 0,
    is_outside_of_pool: False,
  )
}

fn close_and_recreate_pool_connection(
  connection: shork.Connection,
  state: PoolState,
) {
  shork.disconnect(connection)
  create_new_pool_connection(state)
}

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
