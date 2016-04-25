-module(waterlily_codec).

-export([ pack/1
        , unpack/1
        , unpack/2
        ]).

-include("waterlily.hrl").

-define(MAX_CONTENT_LENGTH, 8190).

-type message() :: #message{}.
-type packet() :: <<_:16, _:_*8>>.


-spec pack(binary()) -> [binary()].
pack(Message) when is_binary(Message) ->
    pack_chunks(Message, []).

-spec pack_chunks(binary(), list()) -> list().
pack_chunks(<<>>, Chunks) ->
    lists:reverse(Chunks);

pack_chunks(Content, Chunks) when size(Content) =< ?MAX_CONTENT_LENGTH ->
    Size = size(Content),
    <<Header:16>> = <<Size:15, 1:1>>,
    pack_chunks(<<>>, [<<Header:16/little, Content/binary>> | Chunks]);

pack_chunks(<<Content:?MAX_CONTENT_LENGTH/binary, Rest/binary>>, Chunks) ->
    <<Header:16>> = <<?MAX_CONTENT_LENGTH:15, 0:1>>,
    pack_chunks(Rest, [<<Header:16/little, Content/binary>> | Chunks]).


-spec unpack(packet(), message()) ->
    {final, binary(), binary()} | {wait, message()}.
unpack(NewData, #message{data=Data} = M) when is_binary(NewData) ->
    unpack(M#message{data = <<Data/binary, NewData/binary>>}).

-spec unpack(packet() | message()) ->
    {final, binary(), binary()} | {wait, message()}.
unpack(Data) when is_binary(Data) ->
    unpack(#message{data=Data});

% last chunk read, buffer is full, return message
unpack(#message{final=true, to_read=0, data=Data, buffer=Buffer}) ->
    {final, list_to_binary(lists:reverse(Buffer)), Data};

% not last chunk, add to buffer, read again
unpack(#message{to_read=0, data=Data}=M) when size(Data) < 2 ->
    {wait, M};
unpack(#message{final=false, to_read=0, data=Data, buffer=Buffer}) ->
    M = unpack_header(Data),
    M1 = unpack_chunk(M#message{buffer=Buffer}),
    unpack(M1);

% not enough data - we wait for more
unpack(#message{data= <<>>}=M) ->
    {wait, M};

% got new data to read
unpack(M) ->
    unpack(unpack_chunk(M)).

-spec unpack_header(packet()) -> message().
unpack_header(<<Header:16/little, Data/binary>>) ->
    ToRead = Header bsr 1,
    Final = (Header band 1) =:= 1,
    #message{final=Final, to_read=ToRead, data=Data}.

-spec unpack_chunk(message()) -> message().
unpack_chunk(#message{ to_read=ToRead
                     , buffer=Buffer
                     , data=Data} = M) ->
    TR = min(ToRead, size(Data)),
    <<Part:TR/binary, Rest/binary>> = Data,
    M#message{ to_read = ToRead - TR
             , buffer = [Part | Buffer]
             , data = Rest}.
