-module(rabbit_retry_exchange_util_tests).

-include_lib("eunit/include/eunit.hrl").

validate_max_attempts_test() ->
    ?assertEqual(ok, rabbit_retry_exchange_util:validate_max_attempts({long, 1})),
    ?assertExit({amqp_error, precondition_failed, _, none},
                rabbit_retry_exchange_util:validate_max_attempts({long, 0})),
    ?assertExit({amqp_error, precondition_failed, _, none},
                rabbit_retry_exchange_util:validate_max_attempts({longstr, <<"1">>})).

validate_delay_strategy_test() ->
    ?assertEqual(ok, rabbit_retry_exchange_util:validate_delay_strategy({longstr, <<"fixed">>})),
    ?assertEqual(ok, rabbit_retry_exchange_util:validate_delay_strategy({longstr, <<"linear">>})),
    ?assertEqual(ok, rabbit_retry_exchange_util:validate_delay_strategy({longstr, <<"random">>})),
    ?assertEqual(ok,
                 rabbit_retry_exchange_util:validate_delay_strategy({longstr, <<"exponential">>})),
    ?assertExit({amqp_error, precondition_failed, _, none},
                rabbit_retry_exchange_util:validate_delay_strategy({longstr, <<"other">>})),
    ?assertExit({amqp_error, precondition_failed, _, none},
                rabbit_retry_exchange_util:validate_delay_strategy({long, 10})).

validate_delay_test() ->
    ?assertEqual(ok, rabbit_retry_exchange_util:validate_delay({long, 0})),
    ?assertEqual(ok, rabbit_retry_exchange_util:validate_delay({long, 250})),
    ?assertExit({amqp_error, precondition_failed, _, none},
                rabbit_retry_exchange_util:validate_delay({long, -1})),
    ?assertExit({amqp_error, precondition_failed, _, none},
                rabbit_retry_exchange_util:validate_delay({longstr, <<"100">>})).

validate_max_delay_test() ->
    Args = [{<<"x-retry-delay">>, long, 100}],
    ?assertEqual(ok, rabbit_retry_exchange_util:validate_max_delay({long, 100}, Args)),
    ?assertEqual(ok, rabbit_retry_exchange_util:validate_max_delay({long, 200}, Args)),
    ?assertExit({amqp_error, precondition_failed, _, none},
                rabbit_retry_exchange_util:validate_max_delay({long, 99}, Args)),
    ?assertExit({amqp_error, precondition_failed, _, none},
                rabbit_retry_exchange_util:validate_max_delay({long, 200}, [])),
    ?assertExit({amqp_error, precondition_failed, _, none},
                rabbit_retry_exchange_util:validate_max_delay({longstr, <<"200">>}, Args)).

validate_dlx_and_dlk_test() ->
    ?assertEqual(ok, rabbit_retry_exchange_util:validate_dlx({longstr, <<"dlx">>})),
    ?assertEqual(ok, rabbit_retry_exchange_util:validate_dlk({longstr, <<"rk">>})),
    ?assertExit({amqp_error, precondition_failed, _, none},
                rabbit_retry_exchange_util:validate_dlx({long, 10})),
    ?assertExit({amqp_error, precondition_failed, _, none},
                rabbit_retry_exchange_util:validate_dlk({long, 10})).

validate_args_required_and_optional_test() ->
    Args = [{<<"x-retry-delay">>, long, 100},
            {<<"x-retry-max-attempts">>, long, 3},
            {<<"x-retry-max-delay">>, long, 300},
            {<<"x-retry-delay-strategy">>, longstr, <<"linear">>}],
    Validations = [{<<"x-retry-delay">>, required, fun rabbit_retry_exchange_util:validate_delay/1},
                   {<<"x-retry-max-attempts">>,
                    required,
                    fun rabbit_retry_exchange_util:validate_max_attempts/1},
                   {<<"x-retry-max-delay">>,
                    optional,
                    fun rabbit_retry_exchange_util:validate_max_delay/2},
                   {<<"x-retry-delay-strategy">>,
                    optional,
                    fun rabbit_retry_exchange_util:validate_delay_strategy/1}],
    ?assertEqual(ok, rabbit_retry_exchange_util:validate_args(Args, Validations)).

validate_args_missing_required_test() ->
    Args = [{<<"x-retry-delay">>, long, 100}],
    Validations = [{<<"x-retry-delay">>, required, fun rabbit_retry_exchange_util:validate_delay/1},
                   {<<"x-retry-max-attempts">>,
                    required,
                    fun rabbit_retry_exchange_util:validate_max_attempts/1}],
    ?assertExit({amqp_error, precondition_failed, _, none},
                rabbit_retry_exchange_util:validate_args(Args, Validations)).

validate_args_optional_invalid_test() ->
    Args = [{<<"x-retry-delay">>, long, 100},
            {<<"x-retry-max-attempts">>, long, 3},
            {<<"x-retry-delay-strategy">>, longstr, <<"invalid">>}],
    Validations = [{<<"x-retry-delay">>, required, fun rabbit_retry_exchange_util:validate_delay/1},
                   {<<"x-retry-max-attempts">>,
                    required,
                    fun rabbit_retry_exchange_util:validate_max_attempts/1},
                   {<<"x-retry-delay-strategy">>,
                    optional,
                    fun rabbit_retry_exchange_util:validate_delay_strategy/1}],
    ?assertExit({amqp_error, precondition_failed, _, none},
                rabbit_retry_exchange_util:validate_args(Args, Validations)).

validate_args_unsupported_arity_test() ->
    Args = [{<<"x-retry-delay">>, long, 100}],
    Validations = [{<<"x-retry-delay">>, required, fun() -> ok end}],
    ?assertExit({amqp_error, precondition_failed, _, none},
                rabbit_retry_exchange_util:validate_args(Args, Validations)).

table_merge_test() ->
    Base = [{<<"keep">>, longstr, <<"yes">>},
            {<<"replace">>, long, 1},
            {<<"tail">>, longstr, <<"base">>}],
    Updates = [{<<"replace">>, long, 2},
               {<<"new">>, longstr, <<"value">>}],
    ?assertEqual([{<<"keep">>, longstr, <<"yes">>},
                  {<<"replace">>, long, 2},
                  {<<"tail">>, longstr, <<"base">>},
                  {<<"new">>, longstr, <<"value">>}],
                 rabbit_retry_exchange_util:table_merge(Base, Updates)),
    ?assertEqual(Base, rabbit_retry_exchange_util:table_merge(Base, [])),
    ?assertEqual(Updates, rabbit_retry_exchange_util:table_merge([], Updates)).
