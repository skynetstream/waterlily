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
            , caller             :: pid()
            }).

-define(TCP_OPTS, [binary, {active, false}, {packet, raw}]).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_fsm:start_link({local, ?MODULE}, ?MODULE, [], []).

send(Message) ->
    Self = self(),
    gen_fsm:send_event(?MODULE, {send, Self, Message}).


%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

init(_Opts) ->
    State = #state{host=Host, port=Port, keepalive=Keepalive} = #state{},
    ?DEBUG("Trying to connect to ~p:~p", [Host, Port]),
    case gen_tcp:connect(Host, Port, [{keepalive, Keepalive} | ?TCP_OPTS]) of
        {ok, Socket} ->
            ?INFO("Connected to ~p:~p", [Host, Port]),
            ok = inet:setopts(Socket, [{active, once}]),
            State2 = State#state{socket = Socket, caller=self()},
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

ready({send, From, Message}, #state{socket=Socket} = State) ->
    send_message(Socket, Message),
    {next_state, ready, State#state{caller=From}};

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

handle_info({tcp_closed, Socket}, _StateName, #state{socket=Socket} = State) ->
    {next_state, disconnected, State};

handle_info({tcp_error, Socket, Reason}, _StateName, #state{socket=Socket} =
            State) ->
    {stop, {tcp_error, Reason}, State};

handle_info({tcp, Socket, Data}, connected, #state{socket=Socket}=State) ->
    ok = inet:setopts(Socket, [{active, once}]),
    {Next, S1} = case waterlily_codec:unpack(Data) of
        {final, Unpacked, Rest} ->
            case waterlily_response:decode(Unpacked) of
                {error, _Error} ->
                    {disconnected, State#state{data=Rest}};
                {redirect, Redirect} ->
                    ?INFO("Redirect ~p, do nothing~n", [Redirect]),
                    {connected, State#state{data=Rest}};
                prompt ->
                    % reply size set to unlimited
                    send_message(Socket, <<"Xreply_size -1">>),
                    send_message(Socket, <<"Xauto_commit 1">>),
                    send_message(Socket, <<"sSELECT 42;">>),
                    {ready, State#state{data=Rest}};
                {unknown, Auth} ->
                    [Salt, DBname | _Other] = 
                        binary:split( Auth, [<<":">>], [global]),

                    ?DEBUG("Got some dbname ~p~n", [DBname]),
                    ?DEBUG("Got some salt ~p~n", [Salt]),

                    Password = "monetdb",
                    Hash = crypto:hash(sha512,
                        [bin_to_hex(crypto:hash(sha512, Password)), Salt]),
                    BinHexHash = bin_to_hex(Hash),
                    % Format:
                    % LIT
                    % user
                    % {SHA512}hexhash
                    % language (sql)
                    % dbname (todo)
                    Response = <<"LIT:monetdb:{SHA512}",
                                 BinHexHash/binary, ":sql:voc:">>,
                    send_message(Socket, Response),
                    {connected, State#state{data=Rest}}
            end;
        {wait, M} ->
            {connected, State#state{message=M}}
    end,
    {next_state, Next, S1};

handle_info({tcp, Socket, Data}, ready,
            #state{socket=Socket, caller=From} = State) ->
    ok = inet:setopts(Socket, [{active, once}]),
    S1 = case waterlily_codec:unpack(Data) of
        {final, Unpacked, Rest} ->
            D = waterlily_response:decode(Unpacked),
            From ! D,
            State#state{data=Rest};
        {wait, M} ->
            State#state{message=M}
    end,
    {next_state, ready, S1};

handle_info({response, Response}, ready, State) ->
    ?INFO("Got response ~p~n", [Response]),
    {next_state, ready, State};
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

send_message(Socket, Message) ->
    Encoded = waterlily_codec:pack(Message),
    gen_tcp:send(Socket, Encoded).

bin_to_hex(Bin) ->
    B = [io_lib:format("~2.16.0b", [N]) || N <- binary_to_list(Bin)],
    list_to_binary(B).
