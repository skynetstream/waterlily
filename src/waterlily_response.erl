-module(waterlily_response).

-export([decode/1]).

-include("waterlily.hrl").

-define(REDIRECT,       "^").
-define(ERR,          "!").
-define(PROMPT,         "").
-define(QUERY,          "&").
-define(SCHEMA_HEADER,  "%").
-define(ASYNC_REPLY,    "_").

-define(TABLE,          1).
-define(UPDATE,         2).
-define(CREATE,         3).
-define(TRANSACTION,    4).
-define(PREPARE,        5).
-define(BLOCK,          6).

-type id() :: non_neg_integer().
-type header() :: map().
-type col_info() :: map().
-type query_result() :: {result, {header(), [col_info()], [any()]}}.
-type query_response() :: query_result()
                        | {update, id()}
                        | {create, id()}
                        | transaction
                        | {block, id(), binary()}.

-type response() :: {redirect, binary()}
                  | {error, binary()}
                  | prompt
                  | {schema, binary(), [binary()]}
                  | {async_reply, binary()}
                  | {unknown, binary()}
                  | query_response().


-spec decode(binary()) -> response().
decode(<<?REDIRECT, Redirect/binary>>) ->
    {redirect, Redirect};
decode(<<?ERR, Error/binary>>) ->
    {error, Error};
decode(<<?PROMPT>>) ->
    prompt;
decode(<<?QUERY, _/binary>> = Response) ->
    [Header | Lines] = binary:split(Response, [<<$\n>>], [global]),
    Header1 = decode_header(Header),
    decode_query(Header1, Lines);
decode(<<?SCHEMA_HEADER, Rest/binary>>) ->
    [_First, Content, <<"#">>, Name | _Ignore] = binary:split(Rest, [<<" ">>], [global]),
    Schema = binary:split(Content, [<<",\t">>], [global]),
    {schema, Name, Schema};
decode(<<?ASYNC_REPLY, Rest/binary>>) ->
    {async_reply, Rest};
decode(Unknown) ->
    {unknown, Unknown}.


-spec decode_header(binary()) -> [non_neg_integer()].
decode_header(<<?QUERY, Rest/binary>>) ->
    Values = binary:split(Rest, [<<" ">>], [global]),
    [binary_to_integer(V) || V <- Values].


-spec decode_query([non_neg_integer()], [binary()]) -> query_response().
decode_query([?TABLE|_]=Header, Lines) ->
    decode_table(Header, Lines);
decode_query([?UPDATE, Id, _Rows, _Cols, _Index], _Lines) ->
    {update, Id};
decode_query([?CREATE, Id, _Rows, _Cols, _Index], _Lines) ->
    {create, Id};
decode_query([?TRANSACTION|_], _Lines) ->
    transaction;
decode_query([?PREPARE|_]=Header, Lines) ->
    decode_table(Header, Lines);
decode_query([?BLOCK, Id, _Rows, _Cols, _Index], Lines) ->
    {block, Id, Lines}.


-spec decode_table([non_neg_integer()], [binary()]) -> query_result().
decode_table([_, Id, NRows, NCols, Index], Lines) ->
    [TableNames, ColumnNames, ColumnTypes, ColumnLengths | Rows] = Lines,
    ColInfos = columns_info(TableNames, ColumnNames, ColumnTypes,
                            ColumnLengths),
    Header = #{id => Id, rows => NRows, cols => NCols, index => Index},
    {schema, <<"type">>, ColumnTypes1} = decode(ColumnTypes),
    AtomColumnTypes = [to_atom(C) || C <- ColumnTypes1],
    Rows1 = decode_rows(lists:droplast(Rows)),
    Rows2 = type_rows(Rows1, AtomColumnTypes),
    {result, {Header, ColInfos, Rows2}}.

columns_info(TableNames, ColumnNames, ColumnTypes, ColumnLengths) ->
    {TN_Info, TNs} = schema_info(TableNames),
    ?INFO("TableNames ~p~n", [TNs]),
    {CN_Info, CNs} = schema_info(ColumnNames),
    ?INFO("ColumnNames ~p~n", [CNs]),
    {CT_Info, CTs} = schema_info(ColumnTypes),
    ?INFO("ColumnTypes ~p~n", [CTs]),
    {CL_Info, CLs} = schema_info(ColumnLengths),
    ?INFO("ColumnLengths ~p~n", [CLs]),
    columns_info({TN_Info, CN_Info, CT_Info, CL_Info}, TNs, CNs, CTs, CLs, []).

columns_info(_, [], [], [], [], Result) ->
    lists:reverse(Result);
columns_info({TName, CName, CType, CLength}=Keys, 
              [TN|TNs], [CN|CNs], [CT|CTs], [CL|CLs], Res) ->
    ColumnInfo = #{TName => TN, CName => CN, CType => CT, CLength => CL},
    columns_info(Keys, TNs, CNs, CTs, CLs, [ColumnInfo | Res]).

-spec to_atom(binary()) -> atom().
to_atom(<<"boolean">>) -> boolean;
to_atom(<<"tinyint">>) -> int;
to_atom(<<"smallint">>) -> int;
to_atom(<<"int">>) -> int;
to_atom(<<"wrd">>) -> int;
to_atom(<<"bigint">>) -> int;
to_atom(<<"real">>) -> float;
to_atom(<<"double">>) -> float;
to_atom(<<"decimal">>) -> float;
to_atom(_) -> binary.


-spec decode_rows([binary()]) -> [[string()]].
decode_rows(Rows) ->
    decode_rows(Rows, []).

-spec decode_rows([binary()], [[any()]]) -> [[string()]].
decode_rows([],Decoded) ->
    lists:reverse(Decoded);
decode_rows([<<"[ ", Row/binary>> |Rows], Decoded) ->
    RS = size(Row) - 1,
    <<Row1:RS/binary, _Rest/binary>> = Row,
    Row2 = parse_row(Row1),
    decode_rows(Rows, [Row2 | Decoded]).


-spec parse_row(binary()) -> [string()].
parse_row(Row) ->
    parse_row(Row, [], [], in_crap).

-spec parse_row(binary(), string(), [string()], atom()) -> [string()].
parse_row(<<>>, _Token, Result, _State) ->
    lists:reverse(Result);
parse_row(<<"\t", Rest/binary>>, Token, Result, in_crap) ->
    parse_row(Rest, Token, Result, in_crap);
parse_row(<<",", Rest/binary>>, Token, Result, in_crap) ->
    parse_row(Rest, Token, Result, in_crap);
parse_row(<<" ", Rest/binary>>, Token, Result, in_crap) ->
    parse_row(Rest, Token, Result, in_crap);
parse_row(<<"\"", Rest/binary>>, Token, Result, in_crap) ->
    parse_row(Rest, Token, Result, in_quotes);
parse_row(<<C:8, Rest/binary>>, _Token, Result, in_crap) ->
    parse_row(Rest, [C], Result, in_token);

parse_row(<<",", Rest/binary>>, Token, Result, in_token) ->
    parse_row(Rest, [], [lists:reverse(Token)|Result], in_crap);
parse_row(<<_C:8>>, Token, Result, in_token) ->
    parse_row(<<>>, [], [lists:reverse(Token)|Result], in_crap);
parse_row(<<C:8, Rest/binary>>, Token, Result, in_token) ->
    parse_row(Rest, [C|Token], Result, in_token);

parse_row(<<$", Rest/binary>>, Token, Result, in_quotes) ->
    parse_row(Rest, Token, Result, in_token);
parse_row(<<$\\, Rest/binary>>, Token, Result, in_quotes) ->
    parse_row(Rest, Token, Result, escaped);
parse_row(<<C:8, Rest/binary>>, Token, Result, in_quotes) ->
    parse_row(Rest, [C | Token], Result, in_quotes);

parse_row(<<"t", Rest/binary>>, Token, Result, escaped) ->
    parse_row(Rest, [$\t | Token], Result, in_quotes);
parse_row(<<"n", Rest/binary>>, Token, Result, escaped) ->
    parse_row(Rest, [$\n | Token], Result, in_quotes);
parse_row(<<"r", Rest/binary>>, Token, Result, escaped) ->
    parse_row(Rest, [$\r | Token], Result, in_quotes);
parse_row(<<C:8, Rest/binary>>, Token, Result, escaped) ->
    parse_row(Rest, [C | Token], Result, in_quotes).


type_rows(Rows, ColumnTypes) ->
    type_rows(Rows, ColumnTypes, []).

type_rows([], _ColumnTypes, Normalized) ->
    lists:reverse(Normalized);
type_rows([Row|Rows], ColumnTypes, Normalized) ->
    Row1 = type_row(Row, ColumnTypes),
    type_rows(Rows, ColumnTypes, [Row1 | Normalized]).

type_row(Row, ColumnTypes) ->
    type_row(Row, ColumnTypes, []).

type_row([], [], Normalized) ->
    lists:reverse(Normalized);
type_row([R | Row] , [CT | ColumnTypes] , Normalized) ->
    type_row(Row, ColumnTypes, [type(R,CT) | Normalized]).

type("NULL", _) ->
    null;
type(R, boolean) ->
    R =:= "true";
type(R, int) ->
    list_to_integer(R);
type(R, float) ->
    list_to_float(R);
type(R, binary) ->
    R.

schema_info(Schema) ->
    {schema, Name, Info} = decode(Schema),
    {Name, Info}.
