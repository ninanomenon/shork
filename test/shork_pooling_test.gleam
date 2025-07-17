import gleam/dynamic/decode
import gleam/list
import gleam/otp/actor
import shork
import shork/pool

fn start_pooled() -> Result(pool.ConnectionPool, actor.StartError) {
  shork.default_config()
  |> shork.user("root")
  |> shork.password("root")
  |> shork.database("shork_test")
  |> pool.default_config()
  |> pool.connect
}

pub fn pooled_connection_query_test() {
  let assert Ok(connection) = start_pooled()

  let sql = "select * from shorks"

  let assert Ok(returned) =
    shork.query(sql)
    |> shork.returning({
      use id <- decode.field(0, decode.int)
      use name <- decode.field(1, decode.string)
      use size <- decode.field(2, decode.float)

      decode.success(#(id, name, size))
    })
    |> shork.execute(pool.get_connection(connection))

  assert returned.column_names == ["id", "name", "size"]
  assert list.length(returned.rows) == 4

  assert returned.rows
    == [
      #(1, "Hainelore", 90.0),
      #(2, "Tony Shark", 70.5),
      #(3, "Kleinelore", 45.3),
      #(4, "Kleinrich", 20.0),
    ]
}
