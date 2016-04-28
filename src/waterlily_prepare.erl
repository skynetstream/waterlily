-module(waterlily_prepare).

-export([ quote/1
        , prepare/1]).


quote(List) ->
    quote(List, []).

quote([], Quoted) ->
    [$', lists:reverse(Quoted), $'];

quote([0  | Rest], Quoted) ->
    quote(Rest, [$0, $\\ | Quoted]);
quote([$\n | Rest], Quoted) ->
    quote(Rest, [$\n, $\\ | Quoted]);
quote([$\r | Rest], Quoted) ->
    quote(Rest, [$\r, $\\ | Quoted]);
quote([$\\ | Rest], Quoted) ->
    quote(Rest, [$\\, $\\ | Quoted]);
quote([$' | Rest], Quoted) ->
    quote(Rest, [$', $\\ | Quoted]);
quote([$" | Rest], Quoted) ->
    quote(Rest, [$", $\\ | Quoted]);
quote([Char | Rest], Quoted) ->
    quote(Rest, [Char | Quoted]).


prepare(Params) ->
    Params1 = [to_string(P) || P <- Params],
    string:join(Params1, ",").

to_string(true) ->
    "true";
to_string(false) ->
    "false";
to_string(Integer) when is_integer(Integer) ->
    integer_to_list(Integer);
to_string(Float) when is_float(Float) ->
    float_to_list(Float);
to_string(Bin) when is_binary(Bin) ->
    [$', binary_to_list(quote(Bin)), $'];
to_string(List) when is_list(List) ->
    [$'| quote(List)] ++ "'";
to_string({json, JSON}) when is_binary(JSON)->
    "json '" ++ quote(binary_to_list(JSON)) ++ "'".
