import gleam/io
import gleam/dynamic/decode
import gleam/list
import gleeunit
import gleeunit/should
import shork

pub fn main() {
  gleeunit.main()
}

fn start_default() -> shork.Connection {
  shork.default_config()
  |> shork.user("root")
  |> shork.password("strong_password")
  |> shork.database("shork_test")
  |> shork.connect
}

pub fn insert_new_shorks_test() {
  let connection = start_default()
  let sql =
    "
    insert into shorks (name, age, length, species)
    values ('Tony Shark', 54, 150, 'blue shark')
    "

  let assert Ok(returned) =
    shork.query(sql)
    |> shork.returning({
      use x0 <- decode.field(0, decode.int)
      use x1 <- decode.field(1, decode.int)
      use x2 <- decode.field(2, decode.int)

      decode.success(#(x0, x1, x2))
    })
    |> shork.execute(connection)

  let assert Ok(first_item) = list.first(returned.rows)
  should.equal(first_item.1, 1)
  should.equal(first_item.2, 0)

  let sql =
    "
    select id, name, age, length, species 
    from shorks where id = ?
    "

  let assert Ok(returned) =
    shork.query(sql)
    |> shork.parameter(shork.int(first_item.0))
    |> shork.returning({
      use x0 <- decode.field(0, decode.int)
      use x1 <- decode.field(1, decode.string)
      use x2 <- decode.field(2, decode.int)
      use x3 <- decode.field(3, decode.int)
      use x4 <- decode.field(4, decode.string)

      decode.success(#(x0, x1, x2, x3, x4))
    })
    |> shork.execute(connection)

  let assert Ok(first_item) = list.first(returned.rows)
  should.equal(first_item.1, "Tony Shark")
  should.equal(first_item.2, 54)
  should.equal(first_item.3, 150)
  should.equal(first_item.4, "blue shark")
}

pub fn simple_select_test() {
  let connection = start_default()

  let assert Ok(a) =
    shork.query("select id, name, age from shorks where id = ?")
    |> shork.parameter(shork.int(1))
    |> shork.returning({
      use x0 <- decode.field(0, decode.int)
      use x1 <- decode.field(1, decode.string)
      use x2 <- decode.field(2, decode.int)

      decode.success(#(x0, x1, x2))
    })
    |> shork.execute(connection)

  should.equal(a.rows, [#(1, "Tony Shark", 54)])
}

pub fn no_table_test() {
  let connection =
    shork.default_config()
    |> shork.user("root")
    |> shork.password("strong_password")
    |> shork.database("poke")
    |> shork.connect

  let assert Error(res) =
    shork.query("select * from no_table") |> shork.execute(connection)

  should.equal(
    shork.ServerError(1146, "Table 'poke.no_table' doesn't exist"),
    res,
  )
}

pub fn transaction_test() {
  let connection = start_default()
  let insert = fn(connection, name) {
    let sql =
      "
      insert into shorks (name, age, length, species)
      values (?, 54, 150, 'blue shark')
      "

    let assert Ok(res) =
      shork.query(sql)
      |> shork.parameter(shork.text(name))
      |> shork.returning({
        use x0 <- decode.field(0, decode.int)
        use x1 <- decode.field(1, decode.int)
        use x2 <- decode.field(2, decode.int)

        decode.success(#(x0, x1, x2))
      })
      |> shork.execute(connection)
    res.rows
  }

  let assert Ok(#(id1, id2)) = shork.transaction(connection, fn(connection){
    let res1 = insert(connection, "Kleinrich")
    let res2 = insert(connection, "Kleinelore")

    let assert Ok(res1) = list.first(res1)
    let assert Ok(res2) = list.first(res2)
    Ok(#(res1.0, res2.0))
  })

  io.debug(id1)
  io.debug(id2)
}

pub fn failed_transaction_test() {
  let connection = start_default()
  let insert = fn(connection, name) {
    let sql =
      "
      insert into shorks (name, age, length, species)
      values (?, 54, 150, 'blue shark')
      "

    let assert Ok(res) =
      shork.query(sql)
      |> shork.parameter(shork.text(name))
      |> shork.returning({
        use x0 <- decode.field(0, decode.int)
        use x1 <- decode.field(1, decode.int)
        use x2 <- decode.field(2, decode.int)

        decode.success(#(x0, x1, x2))
      })
      |> shork.execute(connection)
    res.rows
  }

  let assert Error(err) = shork.transaction(connection, fn(connection) {
    let _res1 = insert(connection, "Kleinrich 2")
    let _res2 = insert(connection, "Kleinelore 2")

    Error("nope")
  })

  io.debug(err)
}
