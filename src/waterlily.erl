-module('waterlily').

%% API exports
-export([ connect/0
        , send/1
        ]).

%%====================================================================
%% API functions
%%====================================================================

connect() ->
    waterlily_client:start_link(fun print_response/1).

send(Message) ->
    waterlily_client:send(Message).

print_response(Response) ->
    io:format("Response: ~n~p~n", [Response]).

%%====================================================================
%% Internal functions
%%====================================================================
