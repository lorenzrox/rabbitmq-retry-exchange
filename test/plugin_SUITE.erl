-module(plugin_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

all() ->
    [retry_flow_requeues_message_with_retry_headers,
     retry_flow_respects_configured_delay,
     binding_validation_rejects_missing_dead_letter_exchange,
     binding_validation_rejects_mismatched_dead_letter_exchange,
     binding_validation_accepts_matching_dead_letter_exchange].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [{rmq_nodename_suffix, ?MODULE}]),
    rabbit_ct_helpers:run_setup_steps(Config1,
                                      rabbit_ct_broker_helpers:setup_steps() ++
                                          rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
                                         rabbit_ct_client_helpers:teardown_steps() ++
                                             rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    TestCaseName = rabbit_ct_helpers:config_to_testcase_name(Config, Testcase),
    BaseName = re:replace(TestCaseName, "/", "-", [global, {return, list}]),
    Config1 = rabbit_ct_helpers:set_config(Config, {test_resource_name, BaseName}),
    rabbit_ct_helpers:testcase_started(Config1, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

retry_flow_requeues_message_with_retry_headers(Config) ->
    Chan = rabbit_ct_client_helpers:open_channel(Config),

    RetryEx = make_exchange_name(Config, "retry"),
    PublishEx = make_exchange_name(Config, "publish"),
    Queue = make_queue_name(Config, "work"),
    RoutingKey = <<"work">>,
    Payload = <<"payload-retry">>,

    declare_retry_exchange(Chan, RetryEx,
                           [{<<"x-retry-delay">>, long, 100},
                            {<<"x-retry-max-attempts">>, long, 3},
                            {<<"x-retry-delay-strategy">>, longstr, <<"fixed">>}]),
    declare_direct_exchange(Chan, PublishEx),
    declare_queue(Chan, Queue, [{<<"x-dead-letter-exchange">>, longstr, RetryEx}]),
    bind_queue(Chan, Queue, PublishEx, RoutingKey, []),
    bind_queue(Chan, Queue, RetryEx, Queue, []),

    publish(Chan, PublishEx, RoutingKey, Payload),

    {Tag1, _Msg1} = wait_for_message(Chan, Queue, 2000, false),
    reject(Chan, Tag1),

    {_Tag2, Msg2} = wait_for_message(Chan, Queue, 4000, true),
    ?assertEqual(Payload, Msg2#amqp_msg.payload),
    Headers = headers(Msg2),
    ?assertEqual(undefined, rabbit_basic:header(<<"x-death">>, Headers)),
    ?assertMatch({<<"x-retry-count">>, long, 1}, lists:keyfind(<<"x-retry-count">>, 1, Headers)),
    ?assertMatch({<<"x-retry-delay">>, long, 100}, lists:keyfind(<<"x-retry-delay">>, 1, Headers)),
    ?assertMatch({<<"x-retry-last-death-reason">>, longstr, <<"rejected">>},
                 lists:keyfind(<<"x-retry-last-death-reason">>, 1, Headers)),
    ?assertMatch({<<"x-retry-last-death-queue">>, longstr, Queue},
                 lists:keyfind(<<"x-retry-last-death-queue">>, 1, Headers)),
    ?assertMatch({<<"x-retry-routing-key">>, longstr, RoutingKey},
                 lists:keyfind(<<"x-retry-routing-key">>, 1, Headers)),

    cleanup_test_resources(Chan, [PublishEx, RetryEx], [Queue]),
    rabbit_ct_client_helpers:close_channel(Chan),
    ok.

retry_flow_respects_configured_delay(Config) ->
    Chan = rabbit_ct_client_helpers:open_channel(Config),

    RetryEx = make_exchange_name(Config, "retry"),
    PublishEx = make_exchange_name(Config, "publish"),
    Queue = make_queue_name(Config, "work"),
    RoutingKey = <<"work">>,

    declare_retry_exchange(Chan, RetryEx,
                           [{<<"x-retry-delay">>, long, 5000},
                            {<<"x-retry-max-attempts">>, long, 2},
                            {<<"x-retry-delay-strategy">>, longstr, <<"fixed">>}]),
    declare_direct_exchange(Chan, PublishEx),
    declare_queue(Chan, Queue, [{<<"x-dead-letter-exchange">>, longstr, RetryEx}]),
    bind_queue(Chan, Queue, PublishEx, RoutingKey, []),
    bind_queue(Chan, Queue, RetryEx, Queue, []),

    publish(Chan, PublishEx, RoutingKey, <<"payload-delay">>),
    {Tag1, _Msg1} = wait_for_message(Chan, Queue, 2000, false),
    reject(Chan, Tag1),

    {_Tag2, _Msg2} = wait_for_message(Chan, Queue, 10000, true),

    cleanup_test_resources(Chan, [PublishEx, RetryEx], [Queue]),
    rabbit_ct_client_helpers:close_channel(Chan),
    ok.

binding_validation_rejects_missing_dead_letter_exchange(Config) ->
    Chan = rabbit_ct_client_helpers:open_channel(Config),

    RetryEx = make_exchange_name(Config, "retry"),
    Queue = make_queue_name(Config, "work"),

    declare_retry_exchange(Chan, RetryEx,
                           [{<<"x-retry-delay">>, long, 100},
                            {<<"x-retry-max-attempts">>, long, 3}]),
    declare_queue(Chan, Queue, []),

    case catch amqp_channel:call(Chan,
                                 #'queue.bind'{queue = Queue,
                                               exchange = RetryEx,
                                               routing_key = Queue,
                                               arguments = []}) of
        #'queue.bind_ok'{} ->
            ?assert(false);
        _ ->
            ok
    end,

    rabbit_ct_client_helpers:close_channel(Chan),
    ok.

binding_validation_rejects_mismatched_dead_letter_exchange(Config) ->
    Chan = rabbit_ct_client_helpers:open_channel(Config),

    RetryEx = make_exchange_name(Config, "retry"),
    OtherEx = make_exchange_name(Config, "other"),
    Queue = make_queue_name(Config, "work"),

    declare_retry_exchange(Chan, RetryEx,
                           [{<<"x-retry-delay">>, long, 100},
                            {<<"x-retry-max-attempts">>, long, 3}]),
    declare_direct_exchange(Chan, OtherEx),
    declare_queue(Chan, Queue, [{<<"x-dead-letter-exchange">>, longstr, OtherEx}]),

    case catch amqp_channel:call(Chan,
                                 #'queue.bind'{queue = Queue,
                                               exchange = RetryEx,
                                               routing_key = Queue,
                                               arguments = []}) of
        #'queue.bind_ok'{} ->
            ?assert(false);
        _ ->
            ok
    end,

    rabbit_ct_client_helpers:close_channel(Chan),
    ok.

binding_validation_accepts_matching_dead_letter_exchange(Config) ->
    Chan = rabbit_ct_client_helpers:open_channel(Config),

    RetryEx = make_exchange_name(Config, "retry"),
    Queue = make_queue_name(Config, "work"),

    declare_retry_exchange(Chan, RetryEx,
                           [{<<"x-retry-delay">>, long, 100},
                            {<<"x-retry-max-attempts">>, long, 3}]),
    declare_queue(Chan, Queue, [{<<"x-dead-letter-exchange">>, longstr, RetryEx}]),

    #'queue.bind_ok'{} =
        amqp_channel:call(Chan,
                          #'queue.bind'{queue = Queue,
                                        exchange = RetryEx,
                                        routing_key = Queue,
                                        arguments = []}),

    cleanup_test_resources(Chan, [RetryEx], [Queue]),
    rabbit_ct_client_helpers:close_channel(Chan),
    ok.

declare_retry_exchange(Chan, Exchange, Args) ->
    #'exchange.declare_ok'{} =
        amqp_channel:call(Chan,
                          #'exchange.declare'{exchange = Exchange,
                                              type = <<"x-retry">>,
                                              durable = false,
                                              auto_delete = true,
                                              arguments = Args}).

declare_direct_exchange(Chan, Exchange) ->
    #'exchange.declare_ok'{} =
        amqp_channel:call(Chan,
                          #'exchange.declare'{exchange = Exchange,
                                              type = <<"direct">>,
                                              durable = false,
                                              auto_delete = true}).

declare_queue(Chan, Queue, Args) ->
    #'queue.declare_ok'{queue = Queue} =
        amqp_channel:call(Chan,
                          #'queue.declare'{queue = Queue,
                                           durable = false,
                                           exclusive = true,
                                           auto_delete = true,
                                           arguments = Args}).

bind_queue(Chan, Queue, Exchange, RoutingKey, Args) ->
    #'queue.bind_ok'{} =
        amqp_channel:call(Chan,
                          #'queue.bind'{queue = Queue,
                                        exchange = Exchange,
                                        routing_key = RoutingKey,
                                        arguments = Args}).

publish(Chan, Exchange, RoutingKey, Payload) ->
    ok = amqp_channel:cast(Chan,
                           #'basic.publish'{exchange = Exchange, routing_key = RoutingKey},
                           #amqp_msg{payload = Payload}).

publish_with_headers(Chan, Exchange, RoutingKey, Payload, Headers) ->
    ok = amqp_channel:cast(Chan,
                           #'basic.publish'{exchange = Exchange, routing_key = RoutingKey},
                           #amqp_msg{props = #'P_basic'{headers = Headers}, payload = Payload}).

x_death_header(Queue, RoutingKey, Count) ->
    Exchange = <<"source-exchange">>,
    {<<"x-death">>,
     array,
     [{table,
       [{<<"count">>, long, Count},
        {<<"reason">>, longstr, <<"rejected">>},
        {<<"queue">>, longstr, Queue},
        {<<"exchange">>, longstr, Exchange},
        {<<"time">>, timestamp, erlang:system_time(second)},
        {<<"routing-keys">>, array, [{longstr, RoutingKey}]}]}]}.

reject(Chan, Tag) ->
    ok = amqp_channel:cast(Chan, #'basic.reject'{delivery_tag = Tag, requeue = false}).

wait_for_message(Chan, Queue, Timeout, NoAck) ->
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    wait_for_message_loop(Chan, Queue, Deadline, NoAck).

wait_for_message_loop(Chan, Queue, Deadline, NoAck) ->
    case get_message(Chan, Queue, NoAck) of
        not_found ->
            case erlang:monotonic_time(millisecond) >= Deadline of
                true ->
                    not_found;
                false ->
                    timer:sleep(50),
                    wait_for_message_loop(Chan, Queue, Deadline, NoAck)
            end;
        Result ->
            Result
    end.

get_message(Chan, Queue, NoAck) ->
    case amqp_channel:call(Chan, #'basic.get'{queue = Queue, no_ack = NoAck}) of
        #'basic.get_empty'{} ->
            not_found;
        {#'basic.get_ok'{delivery_tag = Tag}, Msg} ->
            {Tag, Msg}
    end.

headers(#amqp_msg{props = #'P_basic'{headers = Headers}}) ->
    Headers.

cleanup_test_resources(Chan, Exchanges, Queues) ->
    lists:foreach(fun(Exchange) ->
                     _ = amqp_channel:call(Chan,
                                           #'exchange.delete'{exchange = Exchange, if_unused = false}),
                     ok
                  end,
                  Exchanges),
    lists:foreach(fun(Queue) ->
                     _ = amqp_channel:call(Chan, #'queue.delete'{queue = Queue, if_unused = false}),
                     ok
                  end,
                  Queues).

make_exchange_name(Config, Suffix) ->
    Base = rabbit_ct_helpers:get_config(Config, test_resource_name),
    erlang:list_to_binary("x-" ++ Base ++ "-" ++ Suffix).

make_queue_name(Config, Suffix) ->
    Base = rabbit_ct_helpers:get_config(Config, test_resource_name),
    erlang:list_to_binary("q-" ++ Base ++ "-" ++ Suffix).
