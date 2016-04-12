-module(waterlily_codec).

-export([ encode/1
        , decode/1
        , decode/2
        ]).

-include("waterlily.hrl").

-define(MAX_CONTENT_LENGTH, 8190).

-type message() :: #message{}.
-type packet() :: <<_:16, _:_*8>>.


-spec encode(binary()) -> [binary()].
encode(Message) when is_binary(Message) ->
    encode_chunks(Message, []).

-spec encode_chunks(binary(), list()) -> list().
encode_chunks(<<>>, Chunks) ->
    lists:reverse(Chunks);

encode_chunks(Content, Chunks) when size(Content) =< ?MAX_CONTENT_LENGTH ->
    Size = size(Content),
    <<Header:16>> = <<Size:15, 1:1>>,
    encode_chunks(<<>>, [<<Header:16/little, Content/binary>> | Chunks]);

encode_chunks(<<Content:?MAX_CONTENT_LENGTH/binary, Rest/binary>>, Chunks) ->
    <<Header:16>> = <<?MAX_CONTENT_LENGTH:15, 0:1>>,
    encode_chunks(Rest, [<<Header:16/little, Content/binary>> | Chunks]).


-spec decode(packet(), message()) ->
    {final, binary(), binary()} | {wait, message()}.
decode(NewData, M) when is_binary(NewData) ->
    decode(M#message{data=NewData}).

-spec decode(packet() | message()) ->
    {final, binary(), binary()} | {wait, message()}.
decode(Data) when is_binary(Data) ->
    decode(#message{data=Data});

% last chunk read, buffer is full, return message
decode(#message{final=true, to_read=0, data=Data, buffer=Buffer}) ->
    {final, list_to_binary(lists:reverse(Buffer)), Data};

% not last chunk, add to buffer, read again
decode(#message{to_read=0, data=Data}=M) when size(Data) < 2 ->
    {wait, M};
decode(#message{final=false, to_read=0, data=Data, buffer=Buffer}) ->
    M = decode_header(Data),
    M1 = decode_chunk(M#message{buffer=Buffer}),
    decode(M1);

% not enough data - we wait for more
decode(#message{data= <<>>}=M) ->
    {wait, M};

% got new data to read
decode(M) ->
    decode(decode_chunk(M)).

-spec decode_header(packet()) -> message().
decode_header(<<Header:16/little, Data/binary>>) ->
    ToRead = Header bsr 1,
    Final = (Header band 1) =:= 1,
    #message{final=Final, to_read=ToRead, data=Data}.

-spec decode_chunk(message()) -> message().
decode_chunk(#message{ to_read=ToRead
                     , buffer=Buffer
                     , data=Data} = M) ->
    TR = min(ToRead, size(Data)),
    <<Part:TR/binary, Rest/binary>> = Data,
    M#message{ to_read = ToRead - TR
             , buffer = [Part | Buffer]
             , data = Rest}.
