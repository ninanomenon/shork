-module(shork_ffi).

-export([connect/1, disconnect/1, query/4, null/0, coerce/1, transaction/2, null/0]).

-record(shork_connection, {pid, default_timeout}).
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

null() ->
    null.

coerce(Value) ->
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
  #shork_connection{pid = Pid, default_timeout = QueryTimeout}.

disconnect(#shork_connection{pid = Pid}) ->
  mysql:stop(Pid).

transaction(#shork_connection{pid = Pid} = Conn, Callback) ->
  Fun =
    fun() ->
       case Callback(Conn) of
         {ok, T} -> {ok, T};
         {error, Reason} -> throw({shork_rollback_transaction, Reason})
       end
    end,
  case mysql:transaction(Pid, Fun) of
    {atomic, {ok, T}} ->
      {ok, T};
    {aborted, {throw, {shork_rollback_transaction, Reason}}} ->
      {error, {transaction_rolled_back, Reason}}
  end.

query(#shork_connection{pid = Pid, default_timeout = DefaultTimeout}, Sql, Arguments, OptionalTimeout) ->
  Timeout =
    case OptionalTimeout of
      none ->
        DefaultTimeout;
      {some, QueryTimeout} ->
        QueryTimeout
    end,
  case mysql:query(Pid, Sql, Arguments, Timeout) of
    ok ->
      build_return(Pid);
    {ok, ok} ->
      build_return(Pid);
    {ok, ColumnNames, Rows} ->
      {ok, {ColumnNames, lists:map(fun(X) -> list_to_tuple(X) end, Rows)}};
    {error, {Code, _, Message}} ->
      {error, {server_error, Code, Message}};
    {error, Error} ->
      {error, {unknown_error, Error}}
  end.

build_return(Pid) ->
  LastInsertId = mysql:insert_id(Pid),
  AffectedRows = mysql:affected_rows(Pid),
  WarningCount = mysql:warning_count(Pid),

  {ok,
   {[<<"last_insert_id"/utf8>>, <<"affected_rows"/utf8>>, <<"warning_count"/utf8>>],
    [{LastInsertId, AffectedRows, WarningCount}]}}.
