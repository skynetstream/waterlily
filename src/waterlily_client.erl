-module(waterlily_client).

-behaviour(gen_fsm).

%% API
-export([ start_link/0
        , send/2
        , pragma/2
        , query/2
        , prepare/2
        , execute/3
        , connect/1
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
            { host                     :: inet:host()
            , port                     :: inet:port()
            , database                 :: string()
            , socket                   :: undefined | inet:socket()
            , keepalive = true         :: boolean()
            , reconnect                :: boolean()
            , message                  :: #message{}
            , data                     :: binary()
            , handler                  :: {reference(), pid()}
            , throttle_min = 0         :: non_neg_integer()
            , throttle_now = 0         :: non_neg_integer()
            , throttle_by = pow2       :: atom()
            , throttle_max = 120       :: non_neg_integer() | infinity
            }).

-define(TCP_OPTS, [binary, {active, false}, {packet, raw}]).
-define(TIMEOUT, 10000).

%%%===================================================================
%%% API
%%%===================================================================
start_link() ->
    gen_fsm:start_link(?MODULE, [], []).

send(Pid, Message) ->
    send_and_wait(Pid, {send, Message}).

pragma(Pid, Message) ->
    send_and_wait(Pid, {pragma, Message}).

query(Pid, Message) ->
    send_and_wait(Pid, {query, Message}).

prepare(Pid, Query) ->
    send_and_wait(Pid, {prepare, Query}).

execute(Pid, QueryId, Params) ->
    send_and_wait(Pid, {execute, QueryId, Params}).

connect(Pid) ->
    gen_fsm:send_event(Pid, reconnect).

%%%===================================================================
%%% Helpers
%%%===================================================================
send_and_wait(Pid, Query) ->
    Ref = make_ref(),
    case gen_fsm:sync_send_event(Pid, {Ref, Query}) of
        ok ->
            receive
                {Ref, Response} ->
                    Response
            after ?TIMEOUT ->
                timeout
            end;
        {error, _Reason} = Error ->
            Error
    end.


%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================
init([]) ->
    Host      = waterlily:get_env(host),
    Port      = waterlily:get_env(port),
    Database  = waterlily:get_env(database),
    Reconnect = waterlily:get_env(reconnect),
    Keepalive = waterlily:get_env(keepalive),
    State = #state{host=Host, port=Port, database=Database, message=#message{},
                   reconnect=Reconnect, keepalive=Keepalive},
    case Reconnect of
        true ->
            try_connect(State);
        _ ->
            {ok, disconnected, State}
    end.

try_connect(#state{host=Host, port=Port, keepalive=Keepalive,
                   reconnect=Reconnect, throttle_now=TN, throttle_by=TF,
                   throttle_min=TMIN, throttle_max=TMAX} = State) ->
    case gen_tcp:connect(Host, Port, [{keepalive, Keepalive} | ?TCP_OPTS]) of
      {ok, Socket} ->
          ?INFO("Connected to ~p:~p", [Host, Port]),
          ok = inet:setopts(Socket, [{active, once}]),
          State2 = State#state{socket = Socket, throttle_now=TMIN},
          {ok, connected, State2};
      {error, Reason} ->
          ?ERROR("Cannot connect to ~p:~p: ~s", [Host, Port, Reason]),
          State1 = case Reconnect of
              true ->
                  CalculatedThrottle = waterlily_throttle:TF(TN),
                  NewThrottle = case CalculatedThrottle > TMAX of
                      true -> TMAX;
                      _ -> CalculatedThrottle
                  end,
                  ?DEBUG("Reconnecting after ~p~n", [NewThrottle]),
                  gen_fsm:send_event_after(NewThrottle * 1000, reconnect),
                  State#state{throttle_now=NewThrottle};
              false ->
                  State
          end,
          {ok, disconnected, State1}
    end.


disconnected(reconnect, State) ->
    {ok, NextState, State2} = try_connect(State),
    {next_state, NextState, State2};
disconnected(Event, State) ->
    {stop, {no_handler, Event}, State}.

disconnected(_Event, _From, State) ->
    {reply, not_connected, disconnected, State}.


connected({error, Error}, State) ->
    ?ERROR(binary_to_list(Error)),
    {next_state, disconnected, State};
connected({redirect, Redirect}, State) ->
    ?INFO("Redirect ~p, do nothing~n", [Redirect]),
    {next_state, connected, State};
connected({unknown, Auth}, #state{socket=Socket, database=Database} = State) ->
    [Salt, DBname | _Other] =  binary:split( Auth, [<<":">>], [global]),

    ?DEBUG("Salt ~p~n", [Salt]),
    ?DEBUG("Dbname ~p~n", [DBname]),
    Username = waterlily:get_env(username),
    Password = waterlily:get_env(password),
    Hash = crypto:hash(sha512,
        [bin_to_hex(crypto:hash(sha512, Password)), Salt]),
    HexHash = bin_to_hex(Hash),
    % Format:
    % LIT (endianess)
    % user
    % {SHA512}hexhash
    % language (sql)
    % dbname (voc)
    Response = ["LIT:", Username, ":{SHA512}", HexHash, ":sql:", Database,":"],
    pack_and_send(Socket, {send, list_to_binary(Response)}),
    {next_state, connected, State};
connected(prompt, #state{socket=Socket} = State) ->
    % reply size set to unlimited
    pack_and_send(Socket, {pragma, <<"reply_size -1">>}),
    pack_and_send(Socket, {pragma, <<"auto_commit 1">>}),
    {next_state, ready, State};

connected(Event, State) ->
    {stop, {no_handler, Event}, State}.

connected(Event, _From, State) ->
    {reply, {error, {no_handler, Event}}, connected, State}.

ready(create, #state{handler=Handler} = State) ->
    handle(ok, Handler),
    {next_state, ready, State};
ready({update, _RowsCount, _LastId} = Result, #state{handler=Handler} = State) ->
    handle({ok, Result}, Handler),
    {next_state, ready, State};
ready({result, Result}, #state{handler=Handler} = State) ->
    handle(Result, Handler),
    {next_state, ready, State};
ready({error, Reason} = Error, #state{handler=Handler} = State) ->
    ?ERROR(binary_to_list(Reason)),
    handle(Error, Handler),
    {next_state, ready, State};
ready(_Event, State) ->
    {next_state, ready, State}.

ready({Ref, Query}, {From, _}, #state{socket=Socket} = State) ->
    PS = pack_and_send(Socket, Query),
    {reply, PS, ready, State#state{handler={Ref, From}}};

ready(Event, _From, State) ->
    {stop, {error, {malformed_event, Event}}, State}.


handle_event({register, Handler}, StateName, State) ->
    ?INFO("New handler registered"),
    {next_state, StateName, State#state{handler=Handler}};
handle_event(Event, StateName, State) ->
    ?DEBUG("Got some event ~p~n", [Event]),
    {next_state, StateName, State}.

handle_sync_event(Event, _From, StateName, State) ->
    ?DEBUG("Got some sync event ~p~n", [Event]),
    {reply, ok, StateName, State}.

handle_info({tcp_closed, Socket}, _StateName, #state{socket=Socket} = State) ->
    ?DEBUG("Got disconnected"),
    gen_fsm:send_event_after(0, reconnect),
    {next_state, disconnected, State};

handle_info({tcp_error, Socket, Reason}, _StateName, #state{socket=Socket} =
            State) ->
    {stop, {tcp_error, Reason}, State};

handle_info({tcp, Socket, Data}, StateName, #state{socket=Socket} = State) ->
    {State1, Messages} = unpack(Data, State),
    Messages1 = [waterlily_response:decode(M) || M <- Messages],
    [gen_fsm:send_event(self(), M) || M <- Messages1],
    ok = inet:setopts(Socket, [{active, once}]),
    {next_state, StateName, State1};

handle_info(Info, StateName, State) ->
    ?ERROR("Unknown info: ~p in state ~s~n", [Info, StateName]),
    {stop, StateName, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

handle(Result, {Ref, Pid}) ->
    Pid ! {Ref,Result}.

unpack(Data, State) ->
    unpack(Data, State, []).

unpack(<<>>, State, Messages) ->
    {State, lists:reverse(Messages)};
unpack(Data, #state{message=Message} = State, Messages) ->
    {Data1, Messages1, State1} = case waterlily_codec:unpack(Data, Message) of
        {final, Unpacked, Rest} ->
            {Rest, [Unpacked|Messages], State#state{message=#message{}}};
        {wait, M} ->
            {<<>>, Messages, State#state{message=M}}
    end,
    unpack(Data1, State1, Messages1).

pack({send, Message}) ->
    Message;
pack({pragma, Message}) ->
    <<"X", Message/binary>>;
pack({query, Message}) ->
    <<"s", Message/binary>>;
pack({prepare, Message}) ->
    <<"sPREPARE ", Message/binary>>;
pack({execute, QueryId, Params}) ->
    Params1 = waterlily_prepare:prepare(Params),
    Q = ["sEXEC ", integer_to_list(QueryId), "(", Params1, ");"],
    list_to_binary(Q).


pack_and_send(Socket, Message) ->
    Encoded = waterlily_codec:pack(pack(Message)),
    gen_tcp:send(Socket, Encoded).

bin_to_hex(Bin) ->
    B = [io_lib:format("~2.16.0b", [N]) || N <- binary_to_list(Bin)],
    list_to_binary(B).
