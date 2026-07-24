-module(rabbit_retry_exchange_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

description_test() ->
    Description = rabbit_retry_exchange:description(),
    ?assertEqual({name, <<"x-retry">>}, lists:keyfind(name, 1, Description)).

validate_ok_test() ->
    X = #exchange{arguments = [{<<"x-retry-delay">>, long, 100},
                               {<<"x-retry-max-attempts">>, long, 5},
                               {<<"x-retry-max-delay">>, long, 500},
                               {<<"x-retry-delay-strategy">>, longstr, <<"linear">>}]},
    ?assertEqual(ok, rabbit_retry_exchange:validate(X)).

validate_missing_required_test() ->
    X = #exchange{arguments = [{<<"x-retry-delay">>, long, 100}]},
    ?assertExit({amqp_error, precondition_failed, _, none}, rabbit_retry_exchange:validate(X)).

validate_invalid_optional_test() ->
    X = #exchange{arguments = [{<<"x-retry-delay">>, long, 100},
                               {<<"x-retry-max-attempts">>, long, 5},
                               {<<"x-retry-delay-strategy">>, longstr, <<"invalid">>}]},
    ?assertExit({amqp_error, precondition_failed, _, none}, rabbit_retry_exchange:validate(X)).

serialise_events_test() ->
    ?assertEqual(false, rabbit_retry_exchange:serialise_events()).

info_test() ->
    ?assertEqual([], rabbit_retry_exchange:info(dummy)),
    ?assertEqual([], rabbit_retry_exchange:info(dummy, [name])).

noop_callbacks_test() ->
    ?assertEqual(ok, rabbit_retry_exchange:create(serial, exchange)),
    ?assertEqual(ok, rabbit_retry_exchange:recover(exchange, [])),
    ?assertEqual(ok, rabbit_retry_exchange:delete(serial, exchange)),
    ?assertEqual(ok, rabbit_retry_exchange:policy_changed(exchange, exchange)),
    ?assertEqual(ok, rabbit_retry_exchange:add_binding(serial, exchange, binding)),
    ?assertEqual(ok, rabbit_retry_exchange:remove_bindings(serial, exchange, [])).
