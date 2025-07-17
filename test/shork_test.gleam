import common
import gleam/dynamic/decode
import gleam/list
import gleeunit
import shork

pub fn main() {
  let connection = start_default()
  common.create_table(connection)
  common.truncate(connection)
  common.insert_default_data(connection)

  gleeunit.main()

  common.truncate(connection)
  shork.disconnect(connection)
}

fn start_default() -> shork.Connection {
  shork.default_config()
  |> shork.user("root")
  |> shork.password("root")
  |> shork.database("shork_test")
  |> shork.connect
}

pub fn insert_new_shork_test() {
  let connection = start_default()

  let sql =
    "
    insert into shorks (name, size)
    values ('Clark the shark', 75.9)
    "

  let assert Ok(returned) =
    shork.query(sql)
    |> shork.returning({
      use id <- decode.field(0, decode.int)
      use affected_rows <- decode.field(1, decode.int)
      use warning_count <- decode.field(2, decode.int)

      decode.success(#(id, affected_rows, warning_count))
    })
    |> shork.execute(connection)

  assert returned.column_names
    == ["last_insert_id", "affected_rows", "warning_count"]
  assert returned.rows == [#(5, 1, 0)]

  common.rest_database(connection)
  shork.disconnect(connection)
}

pub fn selecting_rows_test() {
  let connection = start_default()

  let sql = "select * from shorks"

  let assert Ok(returned) =
    shork.query(sql)
    |> shork.returning({
      use id <- decode.field(0, decode.int)
      use name <- decode.field(1, decode.string)
      use size <- decode.field(2, decode.float)

      decode.success(#(id, name, size))
    })
    |> shork.execute(connection)

  assert returned.column_names == ["id", "name", "size"]
  assert list.length(returned.rows) == 4

  assert returned.rows
    == [
      #(1, "Hainelore", 90.0),
      #(2, "Tony Shark", 70.5),
      #(3, "Kleinelore", 45.3),
      #(4, "Kleinrich", 20.0),
    ]

  shork.disconnect(connection)
}

pub fn select_single_row_test() {
  let connection = start_default()

  let sql = "select * from shorks where id = ?"

  let assert Ok(returned) =
    shork.query(sql)
    |> shork.parameter(shork.int(1))
    |> shork.returning({
      use id <- decode.field(0, decode.int)
      use name <- decode.field(1, decode.string)
      use size <- decode.field(2, decode.float)

      decode.success(#(id, name, size))
    })
    |> shork.execute(connection)

  assert returned.column_names == ["id", "name", "size"]
  assert list.length(returned.rows) == 1

  assert returned.rows == [#(1, "Hainelore", 90.0)]

  shork.disconnect(connection)
}

pub fn invalid_sql_test() {
  let connection = start_default()

  let sql = "insert update select *!!"

  let assert Error(shork.ServerError(code, message)) =
    shork.query(sql) |> shork.execute(connection)

  assert code == 1064

  assert message
    == "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'update select *!!' at line 1"

  shork.disconnect(connection)
}

pub fn insert_constraint_error_test() {
  let connection = start_default()

  let sql = shork.query("insert into shorks (id, name, size) values (?, ?, ?)")

  let assert Error(shork.ServerError(code, message)) =
    sql
    |> shork.parameter(shork.int(1))
    |> shork.parameter(shork.text("Hainelore"))
    |> shork.parameter(shork.float(90.0))
    |> shork.execute(connection)

  assert code == 1062

  assert message == "Duplicate entry '1' for key 'shorks.PRIMARY'"

  common.rest_database(connection)
  shork.disconnect(connection)
}

pub fn select_from_unknown_table_test() {
  let connection = start_default()

  let sql = "select * from unknown"

  let assert Error(shork.ServerError(code, message)) =
    shork.query(sql)
    |> shork.execute(connection)

  assert code == 1146

  assert message == "Table 'shork_test.unknown' doesn't exist"

  shork.disconnect(connection)
}

pub fn insert_with_incorrect_types_test() {
  let connection = start_default()
  let sql =
    "
    insert into shorks (name, size) into (true, true)
    "

  let assert Error(shork.ServerError(code, message)) =
    shork.query(sql) |> shork.execute(connection)

  assert code == 1064

  assert message
    == "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'into (true, true)' at line 1"

  common.rest_database(connection)
  shork.disconnect(connection)
}

pub fn transaction_commit_test() {
  let connection = start_default()

  let insert = fn(connection, name, size) -> Int {
    let sql =
      "
      insert into shorks (name, size)
      values (?, ?)
      "

    let assert Ok(id) =
      shork.query(sql)
      |> shork.parameter(shork.text(name))
      |> shork.parameter(shork.float(size))
      |> shork.returning({
        use id <- decode.field(0, decode.int)
        decode.success(id)
      })
      |> shork.execute(connection)

    let assert Ok(id) = list.first(id.rows)
    id
  }

  let insert_friend_to_group = fn(connection, friend_group_id, shork_id) {
    let sql =
      "insert into friend_group_member (group_fk, shork_fk) values (?, ?)"
    let assert Ok(id) =
      shork.query(sql)
      |> shork.parameter(shork.int(friend_group_id))
      |> shork.parameter(shork.int(shork_id))
      |> shork.returning({
        use id <- decode.field(0, decode.int)
        decode.success(id)
      })
      |> shork.execute(connection)

    let assert Ok(id) = list.first(id.rows)
    id
  }

  let assert Ok(#(id1, id2)) =
    shork.transaction(connection, fn(connection) {
      let id1 = insert(connection, "Trans Shork 1", 100.4)
      let id2 = insert(connection, "Trans Shork 2", 100.5)

      let _ = insert_friend_to_group(connection, 1, id1)
      let _ = insert_friend_to_group(connection, 1, id2)

      Ok(#(id1, id2))
    })

  assert id1 == 5
  assert id2 == 6

  let assert Error(shork.TransactionRolledBack(message)) =
    shork.transaction(connection, fn(connection) {
      let _ = insert_friend_to_group(connection, 2, id1)
      let _ = insert_friend_to_group(connection, 2, id2)

      Error("Social Anxiety")
    })

  assert message == "Social Anxiety"

  let sql =
    "
    select g.id, s1.name as s1_name, s2.name as s2_name from friend_group_member m1
    inner join friend_groups g on g.id = m1.group_fk
    inner join friend_group_member m2 on g.id = m2.group_fk
    inner join shorks s1 on s1.id = m1.shork_fk
    inner join shorks s2 on s2.id = m2.shork_fk
    where s1.name = 'Trans Shork 1' or s2.name = 'Trans Shork 2'
    "

  let assert Ok(returned) =
    shork.query(sql)
    |> shork.returning({
      use group_id <- decode.field(0, decode.int)
      use s1_name <- decode.field(1, decode.string)
      use s2_name <- decode.field(2, decode.string)

      decode.success(#(group_id, s1_name, s2_name))
    })
    |> shork.execute(connection)

  assert returned.column_names == ["id", "s1_name", "s2_name"]

  list.each(returned.rows, fn(row) {
    assert row.0 == 1
  })

  common.rest_database(connection)
  shork.disconnect(connection)
}

pub fn query_with_custom_timeout_test() {
  let connection = start_default()

  let sql = "select * from shorks"

  let assert Ok(returned) =
    shork.query(sql)
    |> shork.timeout(1000)
    |> shork.returning({
      use id <- decode.field(0, decode.int)
      use name <- decode.field(1, decode.string)
      use size <- decode.field(2, decode.float)

      decode.success(#(id, name, size))
    })
    |> shork.execute(connection)

  assert returned.column_names == ["id", "name", "size"]
  assert list.length(returned.rows) == 4

  assert returned.rows
    == [
      #(1, "Hainelore", 90.0),
      #(2, "Tony Shark", 70.5),
      #(3, "Kleinelore", 45.3),
      #(4, "Kleinrich", 20.0),
    ]

  shork.disconnect(connection)
}

pub fn query_with_too_short_custom_timeout_test() {
  let connection = start_default()

  let sql = "select sleep(4000)"

  let assert Ok(result) =
    shork.query(sql)
    |> shork.timeout(200)
    |> shork.returning({
      use x0 <- decode.field(0, decode.int)
      decode.success(x0)
    })
    |> shork.execute(connection)

  let assert Ok(value) = list.first(result.rows)
  assert value == 1

  shork.disconnect(connection)
}

pub fn null_test() {
  let connection = start_default()

  let sql = shork.query("insert into table_for_nulls (id, name) values (?, ?)")

  let assert Ok(_) =
    sql
    |> shork.parameter(shork.int(1))
    |> shork.parameter(shork.null())
    |> shork.execute(connection)

  shork.disconnect(connection)
}
