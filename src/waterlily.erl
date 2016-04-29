-module('waterlily').

%% API exports
-export([ connect/0
        , connect/1
        , register_handler/2
        , send/2
        , pragma/2
        , query/2
        , prepare/2
        , execute/3
        , get_env/1
        , query_id/1
        ]).

%%====================================================================
%% API functions
%%====================================================================

connect() ->
    waterlily_client:start_link().
connect(Handler) ->
    waterlily_client:start_link(Handler).

register_handler(Pid, Handler) ->
    waterlily_client:register_handler(Pid, Handler).

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
%% Helpers
%%====================================================================

get_env(Var) ->
    {ok, Value} = application:get_env(waterlily, Var),
    Value.

query_id({#{id:=Id},_,_} = _Response) ->
    Id.
