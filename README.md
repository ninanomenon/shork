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

To run tests, a mysql instance must be running with the following parameters:

* host: localhost
* port: 3306
* user: root
* password: root
* database: short_test

You can start it using docker like this:

```bash
docker run -d --rm -p 3306:3306 -e MYSQL_ROOT_PASSWORD="root" -e MYSQL_DATABASE=shork_test mysql
```

## Acknowlement

This project is kindly supported by [binary butterfly](https://github.com/binary-butterfly).

## License

GNU Lesser General Public License (LGPL) version 3 or any later version.
Since the LGPL is a set of additional permissions on top of the GPL, both
license texts are included in the files LICENSE and LICENSE.LESSER respectively.

In short, LGPL allows you to use the library in non-free software and software
with incompatible licenses.
