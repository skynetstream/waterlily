-module(waterlily_db_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").



all() ->
  [ t_create_db
  , t_insert_row
  , t_update_row
  , t_delete_row
  , t_prepare_statement
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

init_per_testcase(_, Config) ->
    application:load(waterlily),
    {ok, Pid} = waterlily:connect(),
    Query = <<"SELECT name FROM tables WHERE name='waterlily_test';">>,
    {_,_,R} = wait_for_query(Pid, 5, Query),
    ok = case R of
        [] ->

            waterlily:query(Pid,
                <<"CREATE TABLE waterlily_test (id INT, description
                  VARCHAR(255));">>);
        _ ->
            ok
    end,
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

% helpers
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
