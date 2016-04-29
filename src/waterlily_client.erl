-module(waterlily_client).

-behaviour(gen_fsm).

%% API
-export([ start_link/0
        , start_link/1
        , send/2
        , pragma/2
        , query/2
        , prepare/2
        , execute/3
        , connect/1
        , register_handler/2
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
            , handler                  :: atom()
            , throttle_min = 0         :: non_neg_integer()
            , throttle_now = 0         :: non_neg_integer()
            , throttle_by = pow2       :: atom()
            , throttle_max = 120       :: non_neg_integer() | infinity
            }).

-define(TCP_OPTS, [binary, {active, false}, {packet, raw}]).

%%%===================================================================
%%% API
%%%===================================================================
start_link() ->
    gen_fsm:start_link(?MODULE, [], []).
start_link(ResponseHandler) ->
    gen_fsm:start_link(?MODULE, [ResponseHandler], []).

send(Pid, Message) ->
    gen_fsm:send_event(Pid, {send, Message}).

pragma(Pid, Message) ->
    gen_fsm:send_event(Pid, {pragma, Message}).

query(Pid, Message) ->
    gen_fsm:send_event(Pid, {query, Message}).

prepare(Pid, Query) ->
    gen_fsm:send_event(Pid, {prepare, Query}).

execute(Pid, QueryId, Params) ->
    gen_fsm:send_event(Pid, {execute, QueryId, Params}).

connect(Pid) ->
    gen_fsm:send_event(Pid, reconnect).

register_handler(Pid, Handler) ->
    gen_fsm:send_all_state_event(Pid, {register, Handler}).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================
init([]) ->
    init([undefined]);
init([ResponseHandler]) ->
    Host      = waterlily:get_env(host),
    Port      = waterlily:get_env(port),
    Database  = waterlily:get_env(database),
    Reconnect = waterlily:get_env(reconnect),
    Keepalive = waterlily:get_env(keepalive),
    State = #state{host=Host, port=Port, database=Database, message=#message{},
                   reconnect=Reconnect, keepalive=Keepalive,
                   handler=ResponseHandler},
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
disconnected(_Event, State) ->
    {next_state, disconnected, State}.

disconnected(_Event, _From, State) ->
    Reply = ok,
    {reply, Reply, disconnected, State}.

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
    % LIT
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

connected(_Event, State) ->
    {next_state, connected, State}.

connected(_Event, _From, State) ->
    Reply = ok,
    {reply, Reply, connected, State}.

ready({send, _}=Message, #state{socket=Socket} = State) ->
    pack_and_send(Socket, Message),
    {next_state, ready, State};
ready({pragma, _}=Message, #state{socket=Socket} = State) ->
    pack_and_send(Socket, Message),
    {next_state, ready, State};
ready({query, _}=Message, #state{socket=Socket} = State) ->
    pack_and_send(Socket, Message),
    {next_state, ready, State};
ready({prepare, _}=Message, #state{socket=Socket} = State) ->
    pack_and_send(Socket, Message),
    {next_state, ready, State};
ready({execute, _, _}=Message, #state{socket=Socket} = State) ->
    pack_and_send(Socket, Message),
    {next_state, ready, State};
ready({result, Result}, #state{handler=Handler} = State) ->
    handle(Result, Handler),
    {next_state, ready, State};
ready({error, Error}, State) ->
    ?ERROR(binary_to_list(Error)),
    {next_state, ready, State};
ready(_Event, State) ->
    {next_state, ready, State}.

ready(_Event, _From, State) ->
    Reply = ok,
    {reply, Reply, ready, State}.


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

handle(_Result, undefined) ->
    ?DEBUG("No handler registered"),
    ok;
handle(Result, Handler) when is_function(Handler) ->
    Handler(Result);
handle(Result, {Mod, Fun}) when is_atom(Mod), is_atom(Fun) ->
    Mod:Fun(Result).

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
