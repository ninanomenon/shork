import gleam/list
import shork

pub fn create_table(connection: shork.Connection) {
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

  let null_table_sql =
    "
    create table if not exists table_for_nulls (
      id      int          not null auto_increment,
      name    varchar(255),

      primary key (id)
    )
    "

  [
    shork.query(shork_sql),
    shork.query(friend_groups_sql),
    shork.query(friend_group_member_sql),
    shork.query(null_table_sql),
  ]
  |> list.each(fn(query) {
    let assert Ok(_) =
      query
      |> shork.execute(connection)
  })
}

pub fn insert_default_data(connection: shork.Connection) {
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

pub fn truncate(connection: shork.Connection) {
  [
    shork.query("SET FOREIGN_KEY_CHECKS = 0"),
    shork.query("truncate friend_group_member"),
    shork.query("truncate friend_groups"),
    shork.query("truncate shorks"),
    shork.query("truncate table_for_nulls"),
    shork.query("SET FOREIGN_KEY_CHECKS = 1;"),
  ]
  |> list.each(fn(query) {
    let assert Ok(_) = shork.execute(query, connection)
  })
}

pub fn rest_database(connection: shork.Connection) {
  truncate(connection)
  insert_default_data(connection)
}
