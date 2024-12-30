import gleam/dynamic/decode
import gleam/io
import gleeunit
import gleeunit/should
import shork

pub fn main() {
  gleeunit.main()
}

pub fn simple_select_test() {
  let connection =
    shork.default_config()
    |> shork.user("root")
    |> shork.password("strong_password")
    |> shork.database("poke")
    |> shork.connect

  let assert Ok(a) =
    shork.query("select pok_id, b_def from base_stats where pok_id = 1")
    |> shork.returning({
      use x0 <- decode.field(0, decode.int)
      use x1 <- decode.field(1, decode.int)

      decode.success(#(x0, x1))
    })
    |> shork.execute(connection)

  should.equal(a.rows, [#(1, 49)])
}

pub fn simple_select_2_test() {
  let connection =
    shork.default_config()
    |> shork.user("root")
    |> shork.password("strong_password")
    |> shork.database("poke")
    |> shork.connect

  let assert Ok(a) =
    shork.query(
      "select pok_id, b_hp, b_atk, b_def from base_stats where pok_id = 1",
    )
    |> shork.returning({
      use x0 <- decode.field(0, decode.int)
      use x1 <- decode.field(1, decode.int)
      use x2 <- decode.field(2, decode.int)
      use x3 <- decode.field(3, decode.int)

      decode.success(#(x0, x1, x2, x3))
    })
    |> shork.execute(connection)

  should.equal(a.rows, [#(1, 45, 49, 49)])
}

pub fn simple_select_3_test() {
  let connection =
    shork.default_config()
    |> shork.user("root")
    |> shork.password("strong_password")
    |> shork.database("poke")
    |> shork.connect

  let assert Ok(res) =
    shork.query("select * from pokemon where pok_name = ?")
    |> shork.parameter(shork.text("bulbasaur"))
    |> shork.returning({
      use x0 <- decode.field(0, decode.int)
      use x1 <- decode.field(1, decode.string)
      use x2 <- decode.field(2, decode.int)

      decode.success(#(x0, x1, x2))
    })
    |> shork.execute(connection)

  should.equal(res.rows, [#(1, "bulbasaur", 7)])
}

pub fn no_table_test() {
  let connection =
    shork.default_config()
    |> shork.user("root")
    |> shork.password("strong_password")
    |> shork.database("poke")
    |> shork.connect

  let res = shork.query("select * from no_table") |> shork.execute(connection)

  io.debug(res)
}
