-module(waterlily_codec_SUITE).

-compile(export_all).

-include("waterlily.hrl").

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").

-define(PROPTEST(T), proper:quickcheck(T(), [ {numtests, 100}
                                            , {to_file, user}
                                            , {max_size, 20000}
                                            , noshrink
                                            ])).

all() ->
    [ t_check_specs
    , t_single_message_codec
    , t_multi_message_codec
    , t_part_message].

t_check_specs(_Config) ->
    [] = proper:check_specs(waterlily_codec).

t_single_message_codec(_Config) ->
    true = ?PROPTEST(prop_single).

prop_single() ->
    ?FORALL(Message, bin(),
            begin
                Encoded = waterlily_codec:pack(Message),
                BinEncoded = list_to_binary(Encoded),
                {final, Decoded, <<>>} = waterlily_codec:unpack(BinEncoded),
                Decoded =:= Message
            end).

t_multi_message_codec(_Config) ->
    true = ?PROPTEST(prop_multi).

prop_multi() ->
    ?FORALL(Messages, small_list(),
            begin
                Encoded = [waterlily_codec:pack(M) || M <- Messages],
                BinEncoded = list_to_binary(Encoded),
                {final, Decoded1, Rest1} = waterlily_codec:unpack(BinEncoded),
                {final, Decoded2, Rest2} = waterlily_codec:unpack(Rest1),
                {final, Decoded3, <<>>} = waterlily_codec:unpack(Rest2),
                Decoded = <<Decoded1/binary, Decoded2/binary, Decoded3/binary>>,
                Decoded = list_to_binary(Messages), 
                Messages =:= [Decoded1, Decoded2, Decoded3]
            end).

t_part_message(_Config) ->
    true = ?PROPTEST(prop_part_message).

prop_part_message() ->
    ?FORALL({Message, CutOff}, {binary(100), integer(10, 90)} ,
            begin
                Encoded = waterlily_codec:pack(Message),
                BinEncoded = list_to_binary(Encoded),
                <<M1:CutOff/binary, M2/binary>> = BinEncoded,
                {wait, M} = waterlily_codec:unpack(M1),
                {final, Decoded, <<>>} = waterlily_codec:unpack(M2, M),
                Message =:= Decoded
            end).

% generators

bin() ->
    non_empty(binary()).

small_list() ->
    [bin(), bin(), bin()].
