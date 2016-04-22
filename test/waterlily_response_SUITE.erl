-module(waterlily_response_SUITE).

-compile(export_all).

-include("waterlily.hrl").

-include_lib("common_test/include/ct.hrl").


all() ->
    [ t_decode_redirect
    , t_decode_error
    , t_decode_prompt
    , t_decode_query
    , t_decode_schema_header
    , t_decode_async_reply
    , t_decode_unknown
    ].


t_decode_redirect(_Config) ->
    Redirect = rand(),
    {redirect, Redirect} = waterlily_response:decode(redirect(Redirect)).

t_decode_error(_Config) ->
    Error = rand(),
    {error, Error} = waterlily_response:decode(err(Error)).

t_decode_prompt(_Config) ->
    prompt = waterlily_response:decode(prompt()).

t_decode_query(_Config) ->
    {result, {Header, [CI1, CI2], [[42,76]]}} = waterlily_response:decode(query()),
    #{id := 1, rows := 1, cols := 2, index := 1} = Header,
    #{<<"tablename">> := <<"gooddog">>, <<"col_name">> := <<"id">>,
        <<"type">> := <<"int">>, <<"length">> := <<"2">>} = CI1 = CI2.

t_decode_schema_header(_Config) ->
    S = rand(20),
    N = rand(10),
    SH = schema_header(N, S),
    {schema, N,[S,S]} = waterlily_response:decode(SH).

t_decode_async_reply(_Config) ->
    AR = rand(),
    {async_reply, AR} = waterlily_response:decode(async_reply(AR)).

t_decode_unknown(_Config) ->
    U = unknown(),
    {unknown, U} = waterlily_response:decode(U).

% helpers

rand() ->
    rand(100).
rand(Num) ->
    crypto:rand_bytes(Num).

redirect(Rand) ->
    <<"^", Rand/binary>>.

err(Rand) ->
    <<"!", Rand/binary>>.

prompt() ->
    <<"">>.

query() ->
    Header = header(),
    TN = schema_header(<<"tablename">>, <<"gooddog">>),
    CN = schema_header(<<"col_name">>, <<"id">>),
    CT = schema_header(<<"type">>, <<"int">>),
    CL = schema_header(<<"length">>, <<"2">>),
    Line = <<"[ 42,\t76\t]">>,
    list_to_binary([Header, $\n, TN, $\n, CN, $\n, CT, $\n, CL, $\n, Line,
                    $\n]).

header() ->
    <<"&1 1 1 2 1">>.

schema_header(Name, S) ->
    Size = size(S),
    Schema = <<S:Size/binary, ",\t", S:Size/binary>>,
    <<"% ", Schema/binary, " # ", Name/binary>>.

async_reply(AR) ->
    <<"_", AR/binary>>.

unknown() ->
    U = rand(),
    <<0, U/binary>>.
