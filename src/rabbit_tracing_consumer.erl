%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ Federation.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_tracing_consumer).

-behaviour(gen_server).

-include_lib("amqp_client/include/amqp_client.hrl").

-import(rabbit_misc, [pget/2, pget/3, table_lookup/2]).

-record(state, {conn, ch, vhost, queue, file, filename, format}).

-define(X, <<"amq.rabbitmq.trace">>).

-export([start_link/1, info_all/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

info_all(Pid) ->
    gen_server:call(Pid, info_all, infinity).

%%----------------------------------------------------------------------------

init(Args) ->
    process_flag(trap_exit, true),
    Name = pget(name, Args),
    VHost = pget(vhost, Args),
    {ok, Username0} = application:get_env(rabbitmq_tracing, username),
    Username = case is_binary(Username0) of
                   true -> Username0;
                   false -> list_to_binary(Username0)
               end,
    P = [{<<"suppress-tracing">>, bool, true}],
    {ok, Conn} = amqp_connection:start(
                   #amqp_params_direct{username          = Username,
                                       virtual_host      = VHost,
                                       client_properties = P}),
    link(Conn),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    link(Ch),
    #'queue.declare_ok'{queue = Q} =
        amqp_channel:call(Ch, #'queue.declare'{durable   = false,
                                               exclusive = true}),
    #'queue.bind_ok'{} =
    amqp_channel:call(
      Ch, #'queue.bind'{exchange = ?X, queue = Q,
                        routing_key = pget(pattern, Args)}),
    #'basic.qos_ok'{} =
        amqp_channel:call(Ch, #'basic.qos'{prefetch_count = 10}),
    #'basic.consume_ok'{} =
        amqp_channel:subscribe(Ch, #'basic.consume'{queue  = Q,
                                                    no_ack = false}, self()),
    {ok, Dir} = application:get_env(directory),
    Filename = Dir ++ "/" ++ binary_to_list(Name) ++ ".log",
    case filelib:ensure_dir(Filename) of
        ok ->
            case file:open(Filename, [append]) of
                {ok, F} ->
                    rabbit_tracing_traces:announce(VHost, Name, self()),
                    Format = list_to_atom(binary_to_list(pget(format, Args))),
                    rabbit_log:info("Tracer opened log file ~p with "
                                    "format ~p~n", [Filename, Format]),
                    {ok, #state{conn = Conn, ch = Ch, vhost = VHost, queue = Q,
                                file = F, filename = Filename,
                                format = Format}};
                {error, E} ->
                    {stop, {could_not_open, Filename, E}}
            end;
        {error, E} ->
            {stop, {could_not_create_dir, Dir, E}}
    end.

handle_call(info_all, _From, State = #state{vhost = V, queue = Q}) ->
    [QInfo] = rabbit_mgmt_db:augment_queues(
                [rabbit_mgmt_wm_queue:queue(V, Q)], basic),
    {reply, [{queue, rabbit_mgmt_format:strip_pids(QInfo)}], State};

handle_call(_Req, _From, State) ->
    {reply, unknown_request, State}.

handle_cast(_C, State) ->
    {noreply, State}.

handle_info({#'basic.deliver'{routing_key  = RKey,
                              delivery_tag = Seq}, Msg = #amqp_msg{}},
            State = #state{ch = Ch, file = F, format = Format}) ->
    Print = fun(Fmt, Args) -> io:format(F, Fmt, Args) end,
    Event = unpack(RKey, Msg),
    log(Format, pget(supertype, Event), Print, Event),
    amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = Seq}),
    {noreply, State};

handle_info(_I, State) ->
    {noreply, State}.

terminate(shutdown, #state{conn = Conn, ch = Ch,
                           file = F, filename = Filename}) ->
    catch amqp_channel:close(Ch),
    catch amqp_connection:close(Conn),
    catch file:close(F),
    rabbit_log:info("Tracer closed log file ~p~n", [Filename]),
    ok;

terminate(_Reason, _State) ->
    ok.

code_change(_, State, _) -> {ok, State}.

%%----------------------------------------------------------------------------

unpack(<<"publish.", _/binary>>, Msg) -> unpack_msg(published, none, Msg);
unpack(<<"deliver.", Q/binary>>, Msg) -> unpack_msg(received, Q, Msg);

unpack(<<"method.in.",  Extra/binary>>, Msg) -> unpack_method(in,  Extra, Msg);
unpack(<<"method.out.", Extra/binary>>, Msg) -> unpack_method(out, Extra, Msg).

unpack_msg(Type, Q, #amqp_msg{props   = #'P_basic'{headers = H},
                              payload = Payload}) ->
    {longstr, Node} = table_lookup(H, <<"node">>),
    {longstr, X}    = table_lookup(H, <<"exchange_name">>),
    {array, Keys}   = table_lookup(H, <<"routing_keys">>),
    {table, Props}  = table_lookup(H, <<"properties">>),
    [{timestamp,    rabbit_mgmt_format:timestamp(os:timestamp())},
     {supertype,    message},
     {type,         Type},
     {exchange,     X},
     {queue,        Q},
     {node,         Node},
     {routing_keys, [K || {_, K} <- Keys]},
     {properties,   Props},
     {payload,      Payload}].

unpack_method(Type, Extra,
              #amqp_msg{props = #'P_basic'{headers = H}}) ->
    Channel = list_to_binary(
                string:join(
                  tl(tl(string:tokens(binary_to_list(Extra), "."))), ".")),
    {longstr, Method} = table_lookup(H, <<"method">>),
    {table, Params}   = table_lookup(H, <<"parameters">>),
    [{timestamp,  rabbit_mgmt_format:timestamp(os:timestamp())},
     {supertype,  method},
     {type,       Type},
     {method,     Method},
     {channel,    Channel},
     {parameters, Params}].

log(text, message, P, Event) ->
    P("~n~s~n", [string:copies("=", 80)]),
    P("~s: ", [pget(timestamp, Event)]),
    case pget(type, Event) of
        published -> P("Message published~n~n", []);
        received  -> P("Message received~n~n", [])
    end,
    P("Node:         ~s~n", [pget(node, Event)]),
    P("Exchange:     ~s~n", [pget(exchange, Event)]),
    case pget(queue, Event) of
        none -> ok;
        Q    -> P("Queue:        ~s~n", [Q])
    end,
    P("Routing keys: ~p~n", [pget(routing_keys, Event)]),
    P("Properties:   ~p~n", [pget(properties, Event)]),
    P("Payload: ~n~s~n",    [pget(payload, Event)]);

log(text, method, P, Event) ->
    P("~s ~s ~s ~s(~s)~n",
      [pget(timestamp, Event),
       pget(channel, Event),
       case pget(type, Event) of in -> "->"; out -> "<-" end,
       pget(method, Event),
       fmt_params(pget(parameters, Event))]);

log(json, message, P, Event) ->
    Event1 = rabbit_mgmt_format:format(
               Event, [{fun rabbit_mgmt_format:amqp_table/1, [properties]},
                       {fun base64:encode/1,                 [payload]}]),
    P("~s~n", [mochijson2:encode(Event1)]);

log(json, method, P, Event) ->
    Event1 = rabbit_mgmt_format:format(
               Event, [{fun rabbit_mgmt_format:amqp_table/1, [params]}]),
    P("~s~n", [mochijson2:encode(Event1)]).

fmt_params(Params) ->
    string:join([fmt_param(K, T, V) || {K, T, V} <- Params], ", ").

fmt_param(K, signedint, V) -> io_lib:format("~s=~p", [K, V]);
fmt_param(K, _T, V) ->        io_lib:format("~s=~s", [K, V]).
