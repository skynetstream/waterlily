-module('waterlily').

%% API exports
-export([ connect/0
        , send/2
        , get_env/1
        ]).

%%====================================================================
%% API functions
%%====================================================================

connect() ->
    waterlily_client:start_link(fun print_response/1).

send(Pid, Message) ->
    waterlily_client:send(Pid, Message).

print_response(Response) ->
    io:format("Response: ~n~p~n", [Response]).

get_env(Var) ->
    {ok, Value} = application:get_env(waterlily, Var),
    Value.


%%====================================================================
%% Internal functions
%%====================================================================
