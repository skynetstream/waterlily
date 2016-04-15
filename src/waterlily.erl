-module('waterlily').

%% API exports
-export([ connect/0
        , send/1
        ]).

%%====================================================================
%% API functions
%%====================================================================

connect() ->
    waterlily_client:start_link().

send(Message) ->
    waterlily_client:send(Message).

%%====================================================================
%% Internal functions
%%====================================================================
