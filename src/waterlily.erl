-module('waterlily').

%% API exports
-export([ connect/0
        , send/2
        , pragma/2
        , query/2
        , prepare/2
        , execute/3
        ]).

-export([ get_env/1
        ]).

-export([ query_id/1
        , rows/1
        , col_type/1
        ]).

%%====================================================================
%% API functions
%%====================================================================

connect() ->
    waterlily_client:start_link().

send(Pid, Message) ->
    waterlily_client:send(Pid, Message).

pragma(Pid, Message) ->
    waterlily_client:pragma(Pid, Message).

query(Pid, Message) ->
    waterlily_client:query(Pid, Message).

prepare(Pid, Message) ->
    waterlily_client:prepare(Pid, Message).

execute(Pid, QueryId, Params) ->
    waterlily_client:execute(Pid, QueryId, Params).

%%====================================================================
%% Response helpers
%%====================================================================

query_id({#{id:=Id},_,_} = _Response) ->
    Id.

rows({_,_,Rows}) ->
    Rows.

col_type(#{<<"type">> := Type}) ->
    Type.

%%====================================================================
%% Application helpers
%%====================================================================

get_env(Var) ->
    {ok, Value} = application:get_env(waterlily, Var),
    Value.
