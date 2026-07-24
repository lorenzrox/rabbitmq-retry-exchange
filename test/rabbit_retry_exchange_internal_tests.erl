-module(rabbit_retry_exchange_internal_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit/include/mc.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").

-record(mc, {protocol, data, annotations = #{}}).

description_test() ->
    Description = rabbit_retry_exchange:description(),
    ?assertEqual({name, <<"x-retry">>}, lists:keyfind(name, 1, Description)).

calculate_delay_test() ->
    BaseArgs = [{<<"x-retry-delay">>, long, 100}],
    LinearArgs = BaseArgs ++ [{<<"x-retry-delay-strategy">>, longstr, <<"linear">>}],
    ExponentialArgs = BaseArgs ++ [{<<"x-retry-delay-strategy">>, longstr, <<"exponential">>}],
    RandomArgs = BaseArgs ++ [{<<"x-retry-delay-strategy">>, longstr, <<"random">>},
                              {<<"x-retry-max-delay">>, long, 250}],
    CappedArgs = BaseArgs ++ [{<<"x-retry-delay-strategy">>, longstr, <<"exponential">>},
                              {<<"x-retry-max-delay">>, long, 250}],
    rand:seed(exsplus, {1, 2, 3}),
    ?assertEqual(100, rabbit_retry_exchange:calculate_delay(1, BaseArgs)),
    ?assertEqual(300, rabbit_retry_exchange:calculate_delay(3, LinearArgs)),
    ?assertEqual(400, rabbit_retry_exchange:calculate_delay(3, ExponentialArgs)),
    ?assertEqual(250, rabbit_retry_exchange:calculate_delay(3, CappedArgs)),
    RandomDelay = rabbit_retry_exchange:calculate_delay(3, RandomArgs),
    ?assert(RandomDelay >= 1),
    ?assert(RandomDelay =< 250).

retry_info_test() ->
    Death = #death{count = 2, routing_keys = [<<"rk">>]},
    LastDeathKey = {<<"queue">>, rejected},
    Deaths = #deaths{last = LastDeathKey, records = #{LastDeathKey => Death}},
    Msg = #mc{annotations = #{deaths => Deaths}},
    ?assertEqual({2, <<"queue">>, <<"rk">>, Deaths},
                 rabbit_retry_exchange:get_retry_info(Msg)),
    ?assertEqual({1, undefined, undefined, undefined},
                 rabbit_retry_exchange:get_retry_info(#mc{annotations = #{}})).

push_pop_annotations_mc_test() ->
    DeathInfo = [{table,
                  [{<<"count">>, long, 2},
                   {<<"reason">>, longstr, <<"rejected">>},
                   {<<"queue">>, longstr, <<"queue">>},
                   {<<"routing-keys">>, array, [{longstr, <<"rk">>}]}]}],
    Msg0 =
        #mc{annotations =
                #{deaths => DeathInfo,
                  ttl => 50,
                  ?ANN_EXCHANGE => <<"retry-x">>,
                  ?ANN_ROUTING_KEYS => [<<"queue">>],
                  <<"x-first-death-exchange">> => <<"first-ex">>,
                  <<"x-first-death-reason">> => <<"rejected">>,
                  <<"x-first-death-queue">> => <<"first-q">>,
                  <<"x-last-death-exchange">> => <<"last-ex">>,
                  <<"x-last-death-reason">> => <<"expired">>,
                  <<"x-last-death-queue">> => <<"last-q">>,
                  foo => bar}},
    Msg1 = rabbit_retry_exchange:push_annotations(Msg0,
                                                  2,
                                                  150,
                                                  <<"queue">>,
                                                  <<"rk">>,
                                                  DeathInfo),
    Anns1 = Msg1#mc.annotations,
    ?assertMatch(#{foo := bar,
                   ttl := 150,
                   <<"x-retry-count">> := 2,
                   <<"x-retry-delay">> := 150,
                   <<"x-retry-last-death-reason">> := <<"rejected">>,
                   <<"x-retry-last-death-queue">> := <<"queue">>,
                   <<"x-retry-routing-key">> := <<"rk">>,
                   <<"x-retry-first-death-exchange">> := <<"first-ex">>,
                   <<"x-retry-first-death-reason">> := <<"rejected">>,
                   <<"x-retry-first-death-queue">> := <<"first-q">>,
                   <<"x-retry-last-death-exchange">> := <<"last-ex">>}, Anns1),
    ?assertEqual(false, maps:is_key(deaths, Anns1)),
    ?assertEqual(false, maps:is_key(<<"x-death">>, Anns1)),
    Msg2 = rabbit_retry_exchange:pop_annotations(Msg1),
    Anns2 = Msg2#mc.annotations,
    ?assertEqual(bar, maps:get(foo, Anns2)),
    ?assertEqual(<<"last-ex">>, maps:get(<<"x-last-death-exchange">>, Anns2)),
    ?assertEqual(<<"rejected">>, maps:get(<<"x-last-death-reason">>, Anns2)),
    ?assertEqual(<<"queue">>, maps:get(<<"x-last-death-queue">>, Anns2)),
    ?assertEqual([<<"rk">>], maps:get(?ANN_ROUTING_KEYS, Anns2)),
    ?assertEqual(<<"last-ex">>, maps:get(?ANN_EXCHANGE, Anns2)).

push_pop_annotations_basic_message_test() ->
    DeathInfo = [],
    Msg0 =
        #basic_message{content =
                           #content{properties =
                                        #'P_basic'{headers =
                                                       [{<<"x-first-death-exchange">>,
                                                         longstr,
                                                         <<"first-ex">>},
                                                        {<<"x-first-death-reason">>,
                                                         longstr,
                                                         <<"rejected">>},
                                                        {<<"x-first-death-queue">>,
                                                         longstr,
                                                         <<"first-q">>},
                                                        {<<"x-last-death-exchange">>,
                                                         longstr,
                                                         <<"last-ex">>},
                                                        {<<"x-last-death-reason">>,
                                                         longstr,
                                                         <<"expired">>},
                                                        {<<"x-last-death-queue">>,
                                                         longstr,
                                                         <<"last-q">>},
                                                        {<<"keep">>, longstr, <<"yes">>}]}}},
    Msg1 = rabbit_retry_exchange:push_annotations(Msg0,
                                                  2,
                                                  150,
                                                  <<"queue">>,
                                                  <<"rk">>,
                                                  DeathInfo),
    #basic_message{content = #content{properties = #'P_basic'{headers = Headers1}}} = Msg1,
    ?assertEqual(undefined, rabbit_basic:header(<<"x-death">>, Headers1)),
    ?assertMatch({<<"x-retry-death">>, array, _}, lists:keyfind(<<"x-retry-death">>, 1, Headers1)),
    ?assertMatch({<<"x-retry-count">>, long, 2}, lists:keyfind(<<"x-retry-count">>, 1, Headers1)),
    ?assertMatch({<<"x-retry-delay">>, long, 150}, lists:keyfind(<<"x-retry-delay">>, 1, Headers1)),
    ?assertMatch({<<"x-retry-last-death-queue">>, longstr, <<"queue">>},
                 lists:keyfind(<<"x-retry-last-death-queue">>, 1, Headers1)),
    Msg2 = rabbit_retry_exchange:pop_annotations(Msg1),
    #basic_message{content = #content{properties = #'P_basic'{headers = Headers2}}} = Msg2,
    ?assertMatch({<<"x-last-death-queue">>, longstr, <<"queue">>},
                 lists:keyfind(<<"x-last-death-queue">>, 1, Headers2)),
    ?assertMatch({<<"keep">>, longstr, <<"yes">>}, lists:keyfind(<<"keep">>, 1, Headers2)).

callbacks_test() ->
    ?assertEqual([], rabbit_retry_exchange:info(dummy)),
    ?assertEqual([], rabbit_retry_exchange:info(dummy, [name])),
    ?assertEqual(false, rabbit_retry_exchange:serialise_events()),
    ?assertEqual(ok, rabbit_retry_exchange:create(serial, exchange)),
    ?assertEqual(ok, rabbit_retry_exchange:recover(exchange, [])),
    ?assertEqual(ok, rabbit_retry_exchange:delete(serial, exchange)),
    ?assertEqual(ok, rabbit_retry_exchange:policy_changed(exchange, exchange)),
    ?assertEqual(ok, rabbit_retry_exchange:add_binding(serial, exchange, binding)),
    ?assertEqual(ok, rabbit_retry_exchange:remove_bindings(serial, exchange, [])).
