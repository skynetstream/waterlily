-module('waterlily').

%% API exports
-export([ connect/1
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

connect(Fun) ->
    waterlily_client:start_link(Fun).

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

query_id({#{id:=Id},_,_} = Response) ->
    Id.


%%====================================================================
%% Internal functions
%%====================================================================
