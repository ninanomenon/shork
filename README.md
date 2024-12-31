# shork

A MySQL / MariaDB database client for Gleam, based on [MySQL-OTP](https://github.com/mysql-otp/mysql-otp). Heavily inspired by [POG](https://github.com/lpil/pog)

```gleam
import shork

pub fn main() {  
  // Start database connection
  let connection =
    shork.default_config()
    |> shork.user("root")
    |> shork.password("strong_password")
    |> shork.database("poke")
    |> shork.connect

  //  A SQL query with one parameter
  let query = "select name, hp from pokemon where id = ?"

  //  The  decoder for the returned data
  let row_decoder = {
    use name <- decode.field(0, decode.name)
    use hp <- decode.field(1, decode.int)

    decode.success(#(name, hp))
  }

  // Runs the query
  // The int 1 is given as parameter
  let assert Ok(response) = 
    shork.query(query)
    |> shork.parameter(shork.int(1))
    |> shork.returning(row_decoder)
    |> shork.execute(connection)

  response.rows
  |> should.equal([
    #("bulbasaur", 45)
  ])
}
```

Further documentation can be found at <https://hexdocs.pm/shork>.

## Development

```sh
gleam run   # Run the project
gleam test  # Run the tests
```

## Acknowlement

This project is kindly supported by [binary butterfly](https://github.com/binary-butterfly).