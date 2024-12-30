-module(shork_ffi).

-export([connect/1, disconnect/1, query/3, intimidate/1]).

-record(shork_connection, {pid}).
-record(config,
        {host,
         port,
         user,
         database,
         password,
         connection_timeout,
         log_warnings,
         log_slow_queries,
         keep_alive,
         query_timeout,
         query_cache_time}).

intimidate(Value) ->
  Value.

connect(#config{host = Host,
                port = Port,
                user = User,
                database = Database,
                password = Password,
                connection_timeout = ConnectionTimeout,
                log_warnings = LogWarnings,
                log_slow_queries = LogSlowQueries,
                keep_alive = KeepAlive,
                query_timeout = QueryTimeout,
                query_cache_time = QueryCacheTime}) ->
  {ok, Pid} =
    mysql:start_link([{host, Host},
                      {port, Port},
                      {user, User},
                      {database, Database},
                      {password, Password},
                      {connection_timeout, ConnectionTimeout},
                      {log_warnings, LogWarnings},
                      {log_slow_queries, LogSlowQueries},
                      {keep_alive, KeepAlive},
                      {query_timeout, QueryTimeout},
                      {query_cache_time, QueryCacheTime}]),
  #shork_connection{pid = Pid}.

disconnect(#shork_connection{pid = Pid}) ->
  mysql:stop(Pid).

query(#shork_connection{pid = Pid}, Sql, Arguments) ->
  Res = mysql:query(Pid, Sql, Arguments),
  case Res of
    {ok, ColumnNames, Rows} ->
      {ok, {ColumnNames, lists:map(fun(X) -> list_to_tuple(X) end, Rows)}};
    {error, Error} ->
      {error, Error}
  end.
