-module(rabbit_amqp10_shovel).

-behaviour(rabbit_shovel_behaviour).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_shovel.hrl").

-export([
         parse/2,
         source_uri/1,
         dest_uri/1,
         connect_source/1,
         init_source/1,
         connect_dest/1,
         init_dest/1,
         handle_source/2,
         handle_dest/2,
         close_source/1,
         close_dest/1,
         ack/3,
         nack/3,
         forward/4
        ]).

-define(INFO(Text, Args), error_logger:info_msg(Text, Args)).

-type state() :: rabbit_shovel_behaviour:state().
-type uri() :: rabbit_shovel_behaviour:uri().
-type tag() :: rabbit_shovel_behaviour:tag().
-type endpoint_config() :: rabbit_shovel_behaviour:source_config()
                           | rabbit_shovel_behaviour:dest_config().

-spec parse(binary(), {source | destination, proplists:proplist()}) ->
    endpoint_config().
parse(_Name, {destination, Conf}) ->
    Uris = pget(uris, Conf),
    #{module => ?MODULE,
      uris => Uris,
      unacked => #{},
      target_address => pget(target_address, Conf),
      delivery_annotations => maps:from_list(pget(delivery_annotations, Conf, [])),
      message_annotations => maps:from_list(pget(message_annotations, Conf, [])),
      properties => maps:from_list(pget(properties, Conf, [])),
      application_properties => maps:from_list(pget(application_properties, Conf, [])),
      add_forward_headers => pget(add_forward_headers, Conf, false),
      add_timestamp_header => pget(add_timestamp_header, Conf, false)
     };
parse(_Name, {source, Conf}) ->
    Uris = pget(uris, Conf),
    #{module => ?MODULE,
      uris => Uris,
      prefetch_count => pget(prefetch_count, Conf, 1000),
      delete_after => pget(delete_after, Conf, never),
      source_address => pget(source_address, Conf)}.

-spec connect_source(state()) -> state().
connect_source(State = #{name := Name,
                         ack_mode := AckMode,
                         source := #{uris := [Uri | _],
                                     source_address := Addr} = Src}) ->
    {ok, Config} = amqp10_client:parse_uri(Uri),
    {ok, Conn} = amqp10_client:open_connection(Config),
    link(Conn),
    {ok, Sess} = amqp10_client:begin_session(Conn),
    LinkName = begin
                   LinkName0 = gen_unique_name(Name, "-receiver"),
                   rabbit_data_coercion:to_binary(LinkName0)
               end,
    % mixed settlement mode covers all the ack_modes
    SettlementMode = case AckMode of
                         no_ack -> settled;
                         _ -> unsettled
                     end,
    {ok, LinkRef} = amqp10_client:attach_receiver_link(Sess, LinkName, Addr,
                                                       SettlementMode),
    State#{source => Src#{current => #{conn => Conn,
                                       session => Sess,
                                       link => LinkRef,
                                       uri => Uri}}}.

-spec connect_dest(state()) -> state().
connect_dest(State = #{name := Name,
                       ack_mode := AckMode,
                       dest := #{uris := [Uri | _],
                                 target_address := Addr} = Dst}) ->
    {ok, Config} = amqp10_client:parse_uri(Uri),
    {ok, Conn} = amqp10_client:open_connection(Config),
    link(Conn),
    {ok, Sess} = amqp10_client:begin_session(Conn),
    LinkName = begin
                   LinkName0 = gen_unique_name(Name, "-sender"),
                   rabbit_data_coercion:to_binary(LinkName0)
               end,
    SettlementMode = case AckMode of
                         no_ack -> settled;
                         _ -> unsettled
                     end,
    % needs to be sync, i.e. awaits the 'attach' event as
    % else we may try to use the link before it is ready
    {ok, LinkRef} = amqp10_client:attach_sender_link_sync(Sess, LinkName, Addr,
                                                          SettlementMode),
    State#{dest => Dst#{current => #{conn => Conn,
                                     session => Sess,
                                     link => LinkRef,
                                     uri => Uri}}}.

-spec init_source(state()) -> state().
init_source(State = #{name := Name,
                      source := #{current := #{link := Link},
                                  prefetch_count := Prefetch} = Src}) ->
    {Credit, RenewAfter} = case Src of
                               #{delete_after := R} when is_integer(R) ->
                                   {R, never};
                               #{prefetch_count := Pre} ->
                                   {Pre, round(Prefetch/10)}
                           end,
    ok = amqp10_client:flow_link_credit(Link, Credit, RenewAfter),
    Remaining = case Src of
                    #{delete_after := never} -> unlimited;
                    #{delete_after := Rem} -> Rem;
                    _ -> unlimited
                end,
    State#{source => Src#{remaining => Remaining,
                          remaining_unacked => Remaining}}.

-spec init_dest(state()) -> state().
init_dest(State) -> State.

-spec source_uri(state()) -> uri().
source_uri(#{source := #{current := #{uri := Uri}}}) -> Uri.

-spec dest_uri(state()) -> uri().
dest_uri(#{dest := #{current := #{uri := Uri}}}) -> Uri.

-spec handle_source(Msg :: any(), state()) -> not_handled | state().
handle_source({amqp10_msg, LinkRef, Msg}, State) ->
    ?INFO("handling msg ~p link_ref ~p~n", [Msg, LinkRef]),
    Tag = amqp10_msg:delivery_id(Msg),
    [Payload] = amqp10_msg:body(Msg),
    rabbit_shovel_behaviour:forward(Tag, #{}, Payload, State);
handle_source({amqp10_event, {connection, Conn, opened}},
              State = #{source := #{current := #{conn := Conn}}}) ->
    ?INFO("Source connection opened.", []),
    State;
handle_source({amqp10_event, {connection, Conn, {closed, Why}}},
              #{source := #{current := #{conn := Conn}}}) ->
    ?INFO("Source connection closed. Reason: ~p~n", [Why]),
    {stop, {inbound_conn_closed, Why}};
handle_source({amqp10_event, {session, Sess, begun}},
              State = #{source := #{current := #{session := Sess}}}) ->
    ?INFO("Source session begun", []),
    State;
handle_source({amqp10_event, {session, Sess, {ended, Why}}},
              #{source := #{current := #{session := Sess}}}) ->
    ?INFO("Source session ended. Reason: ~p~n", [Why]),
    {stop, {inbound_session_ended, Why}};
handle_source({amqp10_event, {link, Link, {detached, Why}}},
              #{source := #{current := #{link := Link}}}) ->
    ?INFO("Source link detached. Reason: ~p~n", [Why]),
    {stop, {inbound_link_detached, Why}};
handle_source({amqp10_event, {link, Link, Evt}},
              State= #{source := #{current := #{link := Link}}}) ->
    ?INFO("Source link event: ~p~n", [Evt]),
    State;
handle_source({'EXIT', Conn, Reason},
              #{source := #{current := #{conn := Conn}}}) ->
    {stop, {outbound_conn_died, Reason}};
handle_source(_Msg, _State) ->
    not_handled.

-spec handle_dest(Msg :: any(), state()) -> not_handled | state().
handle_dest({amqp10_disposition, {Result, Tag}},
            State0 = #{ack_mode := on_confirm,
                       dest := #{unacked := Unacked} = Dst}) ->
    State = State0#{dest => Dst#{unacked => maps:remove(Tag, Unacked)}},
    rabbit_shovel_behaviour:decr_remaining(
      1,
      case {Unacked, Result} of
          {#{Tag := IncomingTag}, accepted} ->
              rabbit_shovel_behaviour:ack(IncomingTag, false, State);
          {#{Tag := IncomingTag}, rejected} ->
              rabbit_shovel_behaviour:nack(IncomingTag, false, State);
          _ -> % not found - this should ideally not happen
              error_logger:warning_msg("amqp10 destination disposition tag not"
                                       "found: ~p~n", [Tag]),
              State
      end);
handle_dest({amqp10_event, {connection, Conn, opened}},
            State = #{dest := #{current := #{conn := Conn}}}) ->
    ?INFO("Destination connection opened.", []),
    State;
handle_dest({amqp10_event, {connection, Conn, {closed, Why}}},
            #{dest := #{current := #{conn := Conn}}}) ->
    ?INFO("Destination connection closed. Reason: ~p~n", [Why]),
    {stop, {outbound_conn_died, Why}};
handle_dest({amqp10_event, {session, Sess, begun}},
            State = #{dest := #{current := #{session := Sess}}}) ->
    ?INFO("Destination session begun", []),
    State;
handle_dest({amqp10_event, {session, Sess, {ended, Why}}},
            #{dest := #{current := #{session := Sess}}}) ->
    ?INFO("Destination session ended. Reason: ~p~n", [Why]),
    {stop, {outbound_conn_died, Why}};
handle_dest({amqp10_event, {link, Link, {detached, Why}}},
            #{dest := #{current := #{link := Link}}}) ->
    ?INFO("Destination link detached. Reason: ~p~n", [Why]),
    {stop, {outbound_link_detached, Why}};
handle_dest({amqp10_event, {link, Link, Evt}},
            State= #{dest := #{current := #{link := Link}}}) ->
    ?INFO("Destination link event: ~p~n", [Evt]),
    State;
handle_dest({'EXIT', Conn, Reason},
            #{dest := #{current := #{conn := Conn}}}) ->
    {stop, {outbound_conn_died, Reason}};
handle_dest(_Msg, _State) ->
    not_handled.

close_source(#{source := #{current := #{conn := Conn}}}) ->
    _ = amqp10_client:close_connection(Conn),
    ok;
close_source(_Config) -> ok.

close_dest(#{dest := #{current := #{conn := Conn}}}) ->
    _ = amqp10_client:close_connection(Conn),
    ok;
close_dest(_Config) -> ok.

-spec ack(Tag :: tag(), Multi :: boolean(), state()) -> state().
% TODO support multiple by keeping track of last ack
ack(Tag, false, State = #{source := #{current := #{session := Session}}}) ->
    % TODO: the tag should be the deliveryid
    ok = amqp10_client_session:disposition(Session, receiver, Tag,
                                           Tag, true, accepted),
    State.

-spec nack(Tag :: tag(), Multi :: boolean(), state()) -> state().
nack(Tag, false, State = #{source := #{current := #{session := Session}}}) ->
    % TODO: the tag should be the deliveryid
    ok = amqp10_client_session:disposition(Session, receiver, Tag,
                                           Tag, false, rejected),
    State.

-spec forward(Tag :: tag(), Props :: #{atom() => any()},
              Payload :: binary(), state()) -> state().
forward(Tag, _Props, _Payload,
        #{source := #{remaining_unacked := 0}} = State) ->
    error_logger:info_msg("dropping ~p~n", [Tag]),
    State;
forward(Tag, Props, Payload,
        #{dest := #{current := #{link := Link},
                    unacked := Unacked} = Dst,
          ack_mode := AckMode} = State) ->
    OutTag = rabbit_data_coercion:to_binary(Tag),
    Msg0 = new_message(OutTag, Payload, State),
    Msg = add_timestamp_header(
            State, set_message_properties(
                     Props, add_forward_headers(State, Msg0))),
    error_logger:info_msg("forwarding ~p ", [Msg]),
    ok = amqp10_client:send_msg(Link, Msg),
    rabbit_shovel_behaviour:decr_remaining_unacked(
      case AckMode of
          no_ack ->
              rabbit_shovel_behaviour:decr_remaining(1, State);
          on_confirm ->
              State#{dest => Dst#{unacked => Unacked#{OutTag => Tag}}};
          on_publish ->
              State1 = rabbit_shovel_behaviour:ack(Tag, false, State),
              rabbit_shovel_behaviour:decr_remaining(1, State1)
      end).

new_message(Tag, Payload, #{ack_mode := AckMode,
                            dest := #{properties := Props,
                                      application_properties := AppProps,
                                      message_annotations := MsgAnns}}) ->
    Msg0 = amqp10_msg:new(Tag, Payload, AckMode =/= on_confirm),
    Msg1 = amqp10_msg:set_properties(Props, Msg0),
    Msg = amqp10_msg:set_message_annotations(MsgAnns, Msg1),
    amqp10_msg:set_application_properties(AppProps, Msg).

add_timestamp_header(#{dest := #{add_timestamp_header := true}}, Msg) ->
    P =#{creation_time => os:system_time(millisecond)},
    amqp10_msg:set_properties(P, Msg);
add_timestamp_header(_, Msg) -> Msg.

add_forward_headers(#{name := Name,
                      shovel_type := Type,
                      dest := #{add_forward_headers := true}}, Msg) ->
      Props = #{<<"shovelled-by">> => rabbit_nodes:cluster_name(),
                <<"shovel-type">> => rabbit_data_coercion:to_binary(Type),
                <<"shovel-name">> => rabbit_data_coercion:to_binary(Name)},
      amqp10_msg:set_application_properties(Props, Msg);
add_forward_headers(_, Msg) -> Msg.

set_message_properties(Props, Msg) ->
    maps:fold(fun(delivery_mode, 2, M) ->
                      amqp10_msg:set_headers(#{durable => true}, M);
                 (content_type, Ct, M) ->
                      amqp10_msg:set_properties(
                        #{content_type => rabbit_data_coercion:to_binary(Ct)}, M);
                 (_, _, M) -> M
              end, Msg, Props).

pget(K, PList) ->
    {K, V} = proplists:lookup(K, PList),
    V.

pget(K, PList, Default) ->
    proplists:get_value(K, PList, Default).

gen_unique_name(Pre0, Post0) ->
    Pre = rabbit_data_coercion:to_binary(Pre0),
    Post = rabbit_data_coercion:to_binary(Post0),
    Id = bin_to_hex(crypto:strong_rand_bytes(8)),
    <<Pre/binary, <<"_">>/binary, Id/binary, <<"_">>/binary, Post/binary>>.

bin_to_hex(Bin) ->
    <<<<if N >= 10 -> N -10 + $a;
           true  -> N + $0 end>>
      || <<N:4>> <= Bin>>.
