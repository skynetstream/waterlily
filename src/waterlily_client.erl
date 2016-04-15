-module(waterlily_client).

-behaviour(gen_fsm).

%% API
-export([ start_link/0
        , send/1
        ]).

%% gen_fsm callbacks
-export([ init/1
        , disconnected/2
        , disconnected/3
        , connected/2
        , connected/3
        , ready/2
        , ready/3
        , handle_event/3
        , handle_sync_event/4
        , handle_info/3
        , terminate/3
        , code_change/4
        ]).

-include("waterlily.hrl").

-record(state,
            { host = "localhost" :: inet:host()
            , port = 50000       :: inet:port()
            , socket             :: undefined | inet:socket()
            , keepalive = true   :: boolean()
            , message            :: #message{}
            , data               :: binary()
            }).

-define(TCP_OPTS, [binary, {active, true}, {packet, raw}]).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_fsm:start_link({local, ?MODULE}, ?MODULE, [], []).

send(Message) ->
    gen_fsm:send_event(?MODULE, {send, Message}).


%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

init(_Opts) ->
    State = #state{host=Host, port=Port, keepalive=Keepalive} = #state{},
    ?DEBUG("Trying to connect to ~p:~p", [Host, Port]),
    case gen_tcp:connect(Host, Port, [{keepalive, Keepalive} | ?TCP_OPTS]) of
        {ok, Socket} ->
            ?INFO("Connected to ~p:~p", [Host, Port]),
            State2 = State#state{socket = Socket},
            {ok, connected, State2};
        {error, Reason} = Error ->
            ?ERROR("Cannot connect to ~p:~p: ~s", [Host, Port, Reason]),
            {stop, Error}
    end.


disconnected(_Event, State) ->
    {next_state, disconnected, State}.

disconnected(_Event, _From, State) ->
    Reply = ok,
    {reply, Reply, disconnected, State}.

connected(_Event, State) ->
    {next_state, connected, State}.

connected(_Event, _From, State) ->
    Reply = ok,
    {reply, Reply, connected, State}.

ready({send, Message}, #state{socket=Socket} = State) ->
    send_message(Socket, Message),
    {next_state, ready, State};

ready(_Event, State) ->
    {next_state, ready, State}.

ready(_Event, _From, State) ->
    Reply = ok,
    {reply, Reply, ready, State}.

handle_event(Event, StateName, State) ->
    ?DEBUG("Got some event ~p~n", [Event]),
    {next_state, StateName, State}.

handle_sync_event(Event, _From, StateName, State) ->
    ?DEBUG("Got some event ~p~n", [Event]),
    {reply, ok, StateName, State}.

handle_info({tcp_closed, _Port}, _StateName, State) ->
    {next_state, disconnected, State};

handle_info({tcp, _Port, Info}, connected, #state{socket=Socket}=State) ->
    ?DEBUG("~p~n", [Info]),
    {Next, S1} = case waterlily_codec:decode(Info) of
        {final, <<"!", _Error/binary>> = Data, Rest} ->
            ?ERROR("Got some error ~p~n", [Data]),
            {disconnected, State#state{data=Rest}};
        {final, <<"^", _Redirect/binary>> = Data, Rest} ->
            ?DEBUG("Got some redirect ~p~n", [Data]),
            {connected, State#state{data=Rest}};
        {final, <<"">>, Rest} ->
            ?INFO("Let's roll~n"),
            send_message(Socket, <<"Xreply_size -1">>),
            send_message(Socket, <<"Xauto_commit 1">>),
            send_message(Socket, <<"sSELECT 42;">>),
            {ready, State#state{data=Rest}};
        {final, Data, Rest} ->
            % dbname = merovingian
            [Salt, DBname | _Other] = binary:split( Data, [<<":">>], [global]),

            ?DEBUG("Got some dbname ~p~n", [DBname]),
            ?DEBUG("Got some salt ~p~n", [Salt]),

            Password = "monetdb",
            Hash = crypto:hash(sha512, [bin_to_hex(crypto:hash(sha512, Password)), Salt]),
            BinHexHash = bin_to_hex(Hash),
            % Format:
            % LIT
            % user
            % {SHA512}hexhash
            % language (sql)
            % dbname (todo)
            Response = <<"LIT:monetdb:{SHA512}", BinHexHash/binary, ":sql:todo:">>,
            send_message(Socket, Response),
            {connected, State#state{data=Rest}};
        {wait, M} ->
            {connected, State#state{message=M}}
    end,
    {next_state, Next, S1};

handle_info({tcp, _Port, Info}, StateName, State) ->
    S1 = case waterlily_codec:decode(Info) of
        {final, Data, Rest} ->
            D = waterlily_response:decode(Data),
            ?DEBUG("Got some info: ~n~p~n", [D]),
            % response & = charAt(0)
            State#state{data=Rest};
        {wait, M} ->
            State#state{message=M}
    end,
    {next_state, StateName, S1}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

send_message(Socket, Message) ->
    Encoded = waterlily_codec:encode(Message),
    gen_tcp:send(Socket, Encoded).

bin_to_hex(Bin) ->
    B = [io_lib:format("~2.16.0b", [N]) || N <- binary_to_list(Bin)],
    list_to_binary(B).
