import gleam/dynamic/decode
import gleam/io
import gleam/list
import gleeunit
import gleeunit/should
import shork

pub fn main() {
  let connection = start_default()
  create_table(connection)
  truncate(connection)
  insert_defult_data(connection)

  gleeunit.main()
}

fn start_default() -> shork.Connection {
  shork.default_config()
  |> shork.user("root")
  |> shork.password("strong_password")
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

fn insert_defult_data(connection: shork.Connection) {
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

  should.equal(returned.column_names, [
    "last_insert_id", "affected_rows", "warning_count",
  ])
  should.equal(returned.rows, [#(5, 1, 0)])

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

  should.equal(returned.column_names, ["id", "name", "size"])
  list.length(returned.rows) |> should.equal(5)

  returned.rows
  |> should.equal([
    #(1, "Hainelore", 90.0),
    #(2, "Tony Shark", 70.5),
    #(3, "Kleinelore", 45.3),
    #(4, "Kleinrich", 20.0),
    #(5, "Clark the shark", 75.9),
  ])
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

  should.equal(returned.column_names, ["id", "name", "size"])
  list.length(returned.rows) |> should.equal(1)

  returned.rows
  |> should.equal([#(1, "Hainelore", 90.0)])
}

pub fn invalid_sql_test() {
  let connection = start_default()

  let sql = "insert update select *!!"

  let assert Error(shork.ServerError(code, message)) =
    shork.query(sql) |> shork.execute(connection)

  should.equal(code, 1064)

  message
  |> should.equal(
    "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'update select *!!' at line 1",
  )
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

  should.equal(code, 1062)

  message |> should.equal("Duplicate entry '1' for key 'shorks.PRIMARY'")
}

pub fn select_from_unknown_table_test() {
  let connection = start_default()

  let sql = "select * from unknown"

  let assert Error(shork.ServerError(code, message)) =
    shork.query(sql)
    |> shork.execute(connection)

  should.equal(code, 1146)

  message |> should.equal("Table 'shork_test.unknown' doesn't exist")
}
