-module(waterlily_db_SUITE).

-compile(export_all).
-include_lib("common_test/include/ct.hrl").



all() ->
  [ t_create_db
  , t_insert_row
  , t_update_row
  , t_delete_row
  , t_prepare_statement
  , t_basic_uuid_date_time
  , t_prepared_uuid_date_time
  ].

init_per_testcase(t_create_db, Config) ->
    application:load(waterlily),
    {ok, Pid} = waterlily:connect(),
    Query = <<"SELECT name FROM tables WHERE name='waterlily_test';">>,
    {_,_,R} = wait_for_query(Pid, 5, Query),
    ok = case R of
        [] ->
            ok;
        _ ->
            waterlily:query(Pid, <<"DROP TABLE waterlily_test;">>)
    end,
    [{pid, Pid} | Config];

init_per_testcase(t_basic_uuid_date_time, Config) ->
    Q = <<"CREATE TABLE waterlily_test
           (id UUID,
            wl_date DATE,
            wl_time TIME,
            wl_timetz TIME WITH TIME ZONE,
            wl_timestamp TIMESTAMP,
            wl_timestamptz TIMESTAMP WITH TIME ZONE);">>,
    Pid = init_db(Q),
    [{pid, Pid} | Config];

init_per_testcase(t_prepared_uuid_date_time, Config) ->
    Q = <<"CREATE TABLE waterlily_test
           (id UUID,
            wl_date DATE,
            wl_time TIME,
            wl_timetz TIME WITH TIME ZONE,
            wl_timestamp TIMESTAMP,
            wl_timestamptz TIMESTAMP WITH TIME ZONE);">>,
    Pid = init_db(Q),
    [{pid, Pid} | Config];

init_per_testcase(_, Config) ->
    Pid = init_db(<<"CREATE TABLE waterlily_test (id INT, description
                VARCHAR(255));">>),
    [{pid, Pid} | Config].

end_per_testcase(_, Config) ->
    Pid = ?config(pid, Config),
    waterlily:query(Pid, <<"DROP TABLE waterlily_test;">>).


t_create_db(Config) ->
    Pid = ?config(pid, Config),
    ok = waterlily:query(Pid,
        <<"CREATE TABLE waterlily_test (id INT, description VARCHAR(255));">>),
    {_, [Col1, Col2], []} 
        = waterlily:query(Pid, <<"SELECT * FROM waterlily_test;">>),
    <<"int">> = waterlily:col_type(Col1),
    <<"varchar">> = waterlily:col_type(Col2).

t_insert_row(Config) ->
    Pid = ?config(pid, Config),
    {ok, {update, RowsCount, _LastId}} = waterlily:query(Pid, 
        <<"INSERT INTO waterlily_test(id, description) VALUES(1, 'baddog');">>),
    {ok, {update, RowsCount1, _LastId}} = waterlily:query(Pid, 
        <<"INSERT INTO waterlily_test(id, description) VALUES(2, 'gooddog');">>),
    1 = RowsCount1 = RowsCount,
    {_, _, [[1, "baddog"],[2, "gooddog"]]} 
        = waterlily:query(Pid, <<"SELECT * FROM waterlily_test;">>).

t_update_row(Config) ->
    Pid = ?config(pid, Config),
    {ok, {update, RowsCount, _LastId}} = waterlily:query(Pid, 
        <<"INSERT INTO waterlily_test(id, description) VALUES(1, 'baddog');">>),
    {ok, {update, RowsCount1, _LastId}} = waterlily:query(Pid, 
        <<"UPDATE waterlily_test SET description='gooddog' WHERE id=1;">>),
    1 = RowsCount1 = RowsCount,
    {_, _, [[1, "gooddog"]]} 
        = waterlily:query(Pid, <<"SELECT * FROM waterlily_test;">>).

t_delete_row(Config) ->
    Pid = ?config(pid, Config),
    {ok, {update, RowsCount, _LastId}} = waterlily:query(Pid, 
        <<"INSERT INTO waterlily_test(id, description) VALUES(1, 'baddog');">>),
    {ok, {update, RowsCount1, _LastId}} = waterlily:query(Pid, 
        <<"DELETE FROM waterlily_test WHERE id=1;">>),
    1 = RowsCount1 = RowsCount,
    {_, _, []} 
        = waterlily:query(Pid, <<"SELECT * FROM waterlily_test;">>).

t_prepare_statement(Config) ->
    Pid = ?config(pid, Config),
    Response = waterlily:prepare(Pid, 
        <<"INSERT INTO waterlily_test(id, description) VALUES (?, ?);">>),
    QueryId = waterlily:query_id(Response),
    L = [waterlily:execute(Pid, QueryId, [N, "test"]) || N <- lists:seq(1,10)],
    true = lists:all(fun(E) -> 
                         {ok, {update, 1, -1}} == E 
                     end, L),
    {_, _, [[10]]} 
        = waterlily:query(Pid, <<"SELECT COUNT(*) FROM waterlily_test;">>).

t_basic_uuid_date_time(Config) ->
    Pid = ?config(pid, Config),
    {ok, _} = waterlily:query(Pid,
        <<"INSERT INTO waterlily_test VALUES(uuid(), current_date(),
        current_time(), current_time(), current_timestamp(),
        current_timestamp());">>),
    {_,_,[Row]} = waterlily:query(Pid, <<"SELECT * FROM waterlily_test;">>),
    [{uuid, _}
    ,{date, _}
    ,{time, _}
    ,{timetz, _}
    ,{timestamp, _}
    ,{timestamptz, _}] = Row.

t_prepared_uuid_date_time(Config) ->
    Pid = ?config(pid, Config),
    Response = waterlily:prepare(Pid,
        <<"INSERT INTO waterlily_test VALUES(?,?,?,?,?,?);">>),
    QueryId = waterlily:query_id(Response),
    UUID = "c450f9f6-e50c-4e57-9116-4e591512a1b1",
    Date = "2016-05-04",
    Time = "22:12:55",
    Timetz = "22:12:54+00:00",
    Timestamp = "2016-05-04 22:11:53.123000",
    Timestamptz = "2016-05-03 21:10:52.012000+00:00",
    {ok,_} = waterlily:execute(Pid, QueryId,
                               [{uuid, UUID}
                               ,{date, Date}
                               ,{time, Time}
                               ,{timetz, Timetz}
                               ,{timestamp, Timestamp}
                               ,{timestamptz, Timestamptz}]),
    {_,_,[Row]} = waterlily:query(Pid, <<"SELECT * FROM waterlily_test;">>),
    [{uuid, UUID}
    ,{date, Date}
    ,{time, Time}
    ,{timetz, Timetz}
    ,{timestamp, Timestamp}
    ,{timestamptz, Timestamptz}] = Row.
    

% helpers

init_db(Q) ->
    application:load(waterlily),
    {ok, Pid} = waterlily:connect(),
    Query = <<"SELECT name FROM tables WHERE name='waterlily_test';">>,
    {_,_,R} = wait_for_query(Pid, 5, Query),
    ok = case R of
        [] ->
            waterlily:query(Pid, Q);
        _ ->
            ok
    end,
    Pid.

wait_for_query(_Pid, 0, _Q) ->
    error;
wait_for_query(Pid, N, Q) ->
    case waterlily:query(Pid, Q) of
        {error, _} ->
            timer:sleep(200),
            wait_for_query(Pid, N - 1, Q);
         Response ->
            Response
    end.
