-module(waterlily_response).

-export([decode/1]).

-include("waterlily.hrl").

-define(REDIRECT,       <<"^">>).
-define(ERROR,          <<"!">>).
-define(PROMPT,         <<"">>).
-define(QUERY,          <<"&">>).
-define(SCHEMA_HEADER,  <<"%">>).
-define(ASYNC_REPLY,    <<"_">>).

-define(TABLE,          1).
-define(UPDATE,         2).
-define(CREATE,         3).
-define(TRANSACTION,    4).
-define(PREPARE,        5).
-define(BLOCK,          6).


% all of this needs to be handled correctly (and tested)
% Redirect - ignore
% error, log
% prompt, ok, empty response
% query, parse resposne
% schema header, decide later
% async_reply, TODO find out what is it
%
% header
% TABLE or PREPARE - parse response
% UPDATE - comes empty
% CREATE -comes empty
% TRANSACTION - as update
% BLOCK - continuation for prev query
decode(<<"&">>) ->
    <<>>;
decode(<<"&", _/binary>> = Response) ->
    [Header, TableNames, ColumnNames, ColumnTypes, ColumnLengts | Rows] =
        binary:split(Response, [<<$\n>>], [global]),
        ?DEBUG("ColumnTypes ~p~n", [ColumnTypes]),
        ?DEBUG("Rows ~p~n", [Rows]),
        ColumnTypes1 = column_types(ColumnTypes),
        Rows1 = decode_rows(lists:droplast(Rows)),
        Rows2 = type_rows(Rows1, ColumnTypes1),
        Rows2;

decode(Response) ->
    Response.


column_types(ColumnTypes) ->
    [_, ColumnTypes1 | _] = binary:split(ColumnTypes, [<<" ">>], [global]),
    CT = binary:split(ColumnTypes1, [<<",\t">>], [global]),
    [to_atom(C) || C <- CT].


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

decode_rows(Rows) ->
    decode_rows(Rows, []).

decode_rows([],Decoded) ->
    lists:reverse(Decoded);
decode_rows([<<"[ ", Row/binary>> |Rows], Decoded) ->
    RS = size(Row) - 1,
    <<Row1:RS/binary, _Rest/binary>> = Row,
    ?DEBUG("Going for ~p~n", [Row1]),
    Row2 = parse_row(Row1),
    ?DEBUG("Haz ~p~n", [Row2]),
    decode_rows(Rows, [Row2 | Decoded]).

parse_row(Row) ->
    parse_row(Row, [], [], in_crap).

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

% <<"sSELECT * FROM todo;">>
% type, query_id, rows, cols, index
% <<"&1 5 1 3 1\n% sys.todo,\tsys.todo,\tsys.todo # table_name\n% id,\tdescription,\tdone # name\n% int,\tvarchar,\tboolean # type\n% 1,\t15,\t5 # length\n[ 1,\t\"zrobic porzadek\",\tfalse\t]\n">>



% <<"sSELECT * FROM todo;">>

% <<"&1 6 1 3 1\n% sys.todo,\tsys.todo,\tsys.todo # table_name\n% id,\tdescription,\tdone # name\n% int,\tvarchar,\tboolean # type\n% 1,\t15,\t5 # length\n[ 1,\t\"zrobic porzadek\",\tfalse\t]\n">>



% <<"sSELECT * FROM todo;">>

% <<"&1 7 1 3 1\n% sys.todo,\tsys.todo,\tsys.todo # table_name\n% id,\tdescription,\tdone # name\n% int,\tvarchar,\tboolean # type\n% 1,\t15,\t5 # length\n[ 1,\t\"zrobic porzadek\",\tfalse\t]\n">>

