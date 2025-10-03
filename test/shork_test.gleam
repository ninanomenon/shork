import gleam/dynamic/decode
import gleam/list
import gleeunit
import shork

pub fn main() {
  let connection = start_default()
  create_table(connection)
  truncate(connection)
  insert_default_data(connection)

  gleeunit.main()
}

fn start_default() -> shork.Connection {
  shork.default_config()
  |> shork.user("root")
  |> shork.password("root")
  |> shork.database("shork_test")
  |> shork.connect
}

fn create_table(connection: shork.Connection) {
  let shork_sql =
    "
    create table if not exists shorks (
      id      int          not null auto_increment,
      name    varchar(255) not null,
      size    float        not null,

      primary key (id)
    )
    "
  let friend_groups_sql =
    "
    create table if not exists friend_groups (
      id   int          not null auto_increment,

      primary key (id)
    )
    "

  let friend_group_member_sql =
    "
    create table if not exists friend_group_member (
      group_fk int not null,
      shork_fk int not null,

      primary key (group_fk, shork_fk),

      foreign key (group_fk) references friend_groups (id),
      foreign key (shork_fk) references shorks (id)
    )
    "

  [
    shork.query(shork_sql),
    shork.query(friend_groups_sql),
    shork.query(friend_group_member_sql),
  ]
  |> list.each(fn(query) {
    let assert Ok(_) =
      query
      |> shork.execute(connection)
  })
}

fn insert_default_data(connection: shork.Connection) {
  let insert_shork_sql =
    shork.query("insert into shorks (name, size) values (?, ?)")
  let insert_friend_group_sql =
    shork.query("insert into friend_groups (id) values (?)")
  let insert_friend_group_member_sql =
    shork.query(
      "insert into friend_group_member (group_fk, shork_fk) values (?, ?)",
    )

  [
    insert_shork_sql
      |> shork.parameter(shork.text("Hainelore"))
      |> shork.parameter(shork.float(90.0)),
    insert_shork_sql
      |> shork.parameter(shork.text("Tony Shark"))
      |> shork.parameter(shork.float(70.5)),
    insert_shork_sql
      |> shork.parameter(shork.text("Kleinelore"))
      |> shork.parameter(shork.float(45.3)),
    insert_shork_sql
      |> shork.parameter(shork.text("Kleinrich"))
      |> shork.parameter(shork.float(20.0)),
  ]
  |> list.each(fn(query) {
    let assert Ok(_) = shork.execute(query, connection)
  })

  [
    insert_friend_group_sql
      |> shork.parameter(shork.int(1)),
    insert_friend_group_sql
      |> shork.parameter(shork.int(2)),
  ]
  |> list.each(fn(query) {
    let assert Ok(_) = shork.execute(query, connection)
  })

  [
    insert_friend_group_member_sql
      |> shork.parameter(shork.int(1))
      |> shork.parameter(shork.int(1)),
    insert_friend_group_member_sql
      |> shork.parameter(shork.int(1))
      |> shork.parameter(shork.int(2)),
    insert_friend_group_member_sql
      |> shork.parameter(shork.int(2))
      |> shork.parameter(shork.int(3)),
    insert_friend_group_member_sql
      |> shork.parameter(shork.int(2))
      |> shork.parameter(shork.int(4)),
  ]
  |> list.each(fn(query) {
    let assert Ok(_) = shork.execute(query, connection)
  })
}

fn truncate(connection: shork.Connection) {
  [
    shork.query("SET FOREIGN_KEY_CHECKS = 0"),
    shork.query("truncate friend_group_member"),
    shork.query("truncate friend_groups"),
    shork.query("truncate shorks"),
    shork.query("SET FOREIGN_KEY_CHECKS = 1;"),
  ]
  |> list.each(fn(query) {
    let assert Ok(_) = shork.execute(query, connection)
  })
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
  assert list.length(returned.rows) == 5

  assert returned.rows
    == [
      #(1, "Hainelore", 90.0),
      #(2, "Tony Shark", 70.5),
      #(3, "Kleinelore", 45.3),
      #(4, "Kleinrich", 20.0),
      #(5, "Clark the shark", 75.9),
    ]
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

  returned.rows == [#(1, "Hainelore", 90.0)]
}

pub fn invalid_sql_test() {
  let connection = start_default()

  let sql = "insert update select *!!"

  let assert Error(shork.ServerError(code, message)) =
    shork.query(sql) |> shork.execute(connection)

  assert code == 1064

  message
  == "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'update select *!!' at line 1"
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
}

pub fn select_from_unknown_table_test() {
  let connection = start_default()

  let sql = "select * from unknown"

  let assert Error(shork.ServerError(code, message)) =
    shork.query(sql)
    |> shork.execute(connection)

  assert code == 1146

  assert message == "Table 'shork_test.unknown' doesn't exist"
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

  assert id1 == 6
  assert id2 == 7

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
  assert list.length(returned.rows) == 7

  assert returned.rows
    == [
      #(1, "Hainelore", 90.0),
      #(2, "Tony Shark", 70.5),
      #(3, "Kleinelore", 45.3),
      #(4, "Kleinrich", 20.0),
      #(5, "Clark the shark", 75.9),
      #(6, "Trans Shork 1", 100.4),
      #(7, "Trans Shork 2", 100.5),
    ]
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
}

pub fn null_test() {
  let connection = start_default()

  let _ = shork.query("truncate table_for_nulls") |> shork.execute(connection)

  let create_table_sql =
    "
    create table if not exists table_for_nulls (
      id      int          not null auto_increment,
      name    varchar(255),

      primary key (id)
    )
    "

  let assert Ok(_) =
    create_table_sql |> shork.query |> shork.execute(connection)

  let sql = shork.query("insert into table_for_nulls (id, name) values (?, ?)")

  let assert Ok(_) =
    sql
    |> shork.parameter(shork.int(1))
    |> shork.parameter(shork.null())
    |> shork.execute(connection)

  shork.query("truncate table_for_nulls") |> shork.execute(connection)
}
