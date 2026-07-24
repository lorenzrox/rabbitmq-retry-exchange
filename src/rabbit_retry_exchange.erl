%%==============================================================================
%% @author Lorenzo Rossoni <https://github.com/lorenzrox>
%% @end
%%==============================================================================

%% @doc Implement required exchange behaviors for the custom retry_exchange plugin.
%% Handles message dead-lettering, exponential/linear retry delay strategies,
%% and fallback dead-letter exchanges (DLX) upon reaching maximum attempts.
%% @end

-module(rabbit_retry_exchange).

-author("https://github.com/lorenzrox").

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include_lib("rabbit/include/mc.hrl").
-include_lib("rabbit/include/amqqueue.hrl").
-include_lib("kernel/include/logger.hrl").

-behaviour(rabbit_exchange_type).

%% Register the custom exchange type 'x-retry' into the RabbitMQ registry during startup
-rabbit_boot_step({?MODULE,
                   [{description, "exchange type retry"},
                    {mfa,
                     {rabbit_registry,
                      register,
                      [exchange, <<120, 45, 114, 101, 116, 114, 121>>, ?MODULE]}},
                    {requires, rabbit_registry},
                    {enables, kernel_ready}]}).

-export([add_binding/3, assert_args_equivalence/2, create/2, delete/2, delete/3, policy_changed/2,
         description/0, recover/2, remove_bindings/3, validate_binding/2, route/3,
         serialise_events/0, stateless/0, validate/1, info/1, info/2, info/3]).

%% Internal representation of a message container
-record(mc, {protocol, data, annotations = #{}}).

-define(DEFAULT_EXCHANGE_NAME, <<>>).

%% @doc Returns description metadata displayed in Management UI / CLI
description() ->
    [{name, <<"x-retry">>},
     {description, <<"Custom exchange with DLQ-based retry and DLX support">>}].

%% @doc Core routing function invoked when a message enters the retry exchange
route(#exchange{name = XName, arguments = Args}, Msg, _Opts) ->
    %% Check if this message is returning from a retry delay queue (indicated by x-retry-last-death-queue header)
    case mc:x_header(<<"x-retry-last-death-queue">>, Msg) of
        {utf8, RetryQueueName} ->
            %% Message has completed its delay phase; route it back directly to its target queue
            QRes = rabbit_misc:r(XName#resource.virtual_host, queue, RetryQueueName),
            Qs0 = case rabbit_db_queue:get(QRes) of
                      {error, _} -> [];
                      {ok, Q} -> [Q]
                  end,
            Qs1 = rabbit_amqqueue:prepend_extra_bcc(Qs0),
            Msg1 = pop_annotations(Msg),
            _ = rabbit_queue_type:deliver(Qs1, Msg1, #{}, stateless),
            [];

        undefined ->
            %% New dead-lettered message: parse retry attempts and dead-letter headers
            {RetryCount, OriginalQueueName, OriginalRoutingKey, DeathInfo} = get_retry_info(Msg),

            MaxAttempts =
                case rabbit_misc:table_lookup(Args, <<"x-retry-max-attempts">>) of
                    {long, Value} -> Value;
                    _ -> infinity
                end,

            case {RetryCount, OriginalQueueName} of
                {0, _Q} ->
                    %% Non-rejected death (e.g., message expired or queue length limit exceeded)
                    ?LOG_DEBUG("Retry Exchange (~s): Non-rejected dead-letter reason detected. "
                               "Routing to configured terminal DLX.",
                               [XName]),
                    route_to_fallback_dlx(XName, Msg, OriginalQueueName, OriginalRoutingKey);

                {_RC, undefined} ->
                    %% Cannot determine original queue origin from x-death header
                    ?LOG_DEBUG("Retry Exchange (~s): Original Queue not found in x-death. Dropping "
                               "(no route found).",
                               [XName#resource.name]),
                    [];

                {_RC, _Q} when RetryCount > MaxAttempts ->
                    %% Retry budget exhausted; pass to fallback DLX
                    ?LOG_DEBUG("Retry Exchange (~s): Max attempts (~p) reached. Routing to "
                               "configured terminal DLX.",
                               [XName#resource.name, MaxAttempts]),
                    route_to_fallback_dlx(XName, Msg, OriginalQueueName, OriginalRoutingKey);

                {_RC, _Q} ->
                    %% Message eligible for retry: find corresponding retry queue binding
                    Qs0 = rabbit_db_binding:match(XName,
                                                  fun(#binding{key = RoutingKey}) ->
                                                     RoutingKey == OriginalQueueName
                                                  end),
                    Qs1 = rabbit_db_queue:get_targets(Qs0),
                    Qs2 = rabbit_amqqueue:prepend_extra_bcc(Qs1),
                    Delay = calculate_delay(RetryCount, Args),

                    %% Annotate message with TTL delay & routing headers for the delay queue
                    Msg1 =
                        push_annotations(Msg,
                                         RetryCount,
                                         Delay,
                                         OriginalQueueName,
                                         OriginalRoutingKey,
                                         DeathInfo),
                    _ = rabbit_queue_type:deliver(Qs2, Msg1, #{}, stateless),
                    []
            end
    end.

%% @doc Annotates modern message containers (#mc{}) with custom retry metadata and expiration
push_annotations(#mc{annotations = Anns} = Msg,
                 RetryCount,
                 Delay,
                 OriginalQueueName,
                 OriginalRoutingKey,
                 DeathInfo) ->
    %% Clean up existing death annotations to avoid header duplication loops
    Anns0 =
        maps:fold(fun(Key, Value, Acc) ->
                     case Key of
                         deaths -> Acc;
                         ttl -> Acc;
                         ?ANN_EXCHANGE -> Acc;
                         ?ANN_ROUTING_KEYS -> Acc;
                         <<"x-first-death-exchange">> ->
                             Acc#{<<"x-retry-first-death-exchange">> => Value};
                         <<"x-first-death-reason">> ->
                             Acc#{<<"x-retry-first-death-reason">> => Value};
                         <<"x-first-death-queue">> ->
                             Acc#{<<"x-retry-first-death-queue">> => Value};
                         <<"x-last-death-exchange">> ->
                             Acc#{<<"x-retry-last-death-exchange">> => Value};
                         <<"x-last-death-reason">> -> Acc;
                         <<"x-last-death-queue">> -> Acc;
                         _ -> Acc#{Key => Value}
                     end
                  end,
                  #{},
                  Anns),
    Anns1 =
        Anns0#{ttl => Delay,
               ?ANN_EXCHANGE => ?DEFAULT_EXCHANGE_NAME,
               ?ANN_ROUTING_KEYS => [OriginalQueueName],
               <<"x-retry-death">> => DeathInfo,
               <<"x-retry-count">> => RetryCount,
               <<"x-retry-delay">> => Delay,
               <<"x-retry-last-death-reason">> => atom_to_binary(rejected),
               <<"x-retry-last-death-queue">> => OriginalQueueName},
    case OriginalRoutingKey of
        undefined ->
            Msg#mc{annotations = Anns1};
        _ ->
            Anns2 = Anns1#{<<"x-retry-routing-key">> => OriginalRoutingKey},
            Msg#mc{annotations = Anns2}
    end;

%% @doc Annotates standard AMQP 0-9-1 messages (#basic_message{}) with custom headers and TTL
push_annotations(#basic_message{content =
                                    #content{properties = #'P_basic'{headers = Headers} = Props} =
                                        Content} =
                     Msg,
                 RetryCount,
                 Delay,
                 OriginalQueueName,
                 OriginalRoutingKey,
                 DeathInfo) ->
    Headers0 =
        lists:flatmap(fun({Key, Type, Value} = Entry) ->
                         case Key of
                             <<"x-first-death-exchange">> ->
                                 [{<<"x-retry-first-death-exchange">>, Type, Value}];
                             <<"x-first-death-reason">> ->
                                 [{<<"x-retry-first-death-reason">>, Type, Value}];
                             <<"x-first-death-queue">> ->
                                 [{<<"x-retry-first-death-queue">>, Type, Value}];
                             <<"x-last-death-exchange">> ->
                                 [{<<"x-retry-last-death-exchange">>, Type, Value}];
                             <<"x-last-death-reason">> ->
                                 [{<<"x-retry-last-death-reason">>, Type, Value}];
                             <<"x-last-death-queue">> ->
                                 [{<<"x-retry-last-death-queue">>, Type, Value}];
                             <<"x-death">> -> [];
                             _ -> [Entry]
                         end
                      end,
                      Headers),
    Headers1 =
        rabbit_misc:table_merge(Headers0,
                                [{<<"x-retry-death">>, array, DeathInfo},
                                 {<<"x-retry-count">>, long, RetryCount},
                                 {<<"x-retry-delay">>, long, Delay},
                                 {<<"x-retry-last-death-reason">>,
                                  longstr,
                                  atom_to_binary(rejected)},
                                 {<<"x-retry-last-death-queue">>, longstr, OriginalQueueName}]),
    Headers2 =
        case OriginalRoutingKey of
            undefined ->
                Headers1;
            _ ->
                rabbit_misc:set_table_value(Headers1,
                                            <<"x-retry-routing-key">>,
                                            longstr,
                                            OriginalRoutingKey)
        end,
    Content0 =
        Content#content{properties =
                            Props#'P_basic'{expiration = integer_to_binary(Delay),
                                            headers = Headers2}},
    Msg#basic_message{content = Content0,
                      exchange_name = ?DEFAULT_EXCHANGE_NAME,
                      routing_keys = [OriginalQueueName]}.

%% @doc Removes temporary retry annotations from #mc{} before redelivering to target queue
pop_annotations(#mc{annotations = Anns} = Msg) ->
    Anns0 =
        maps:fold(fun(Key, Value, Acc) ->
                     case Key of
                         deaths -> Acc;
                         ttl -> Acc;
                         ?ANN_ROUTING_KEYS -> Acc;
                         ?ANN_EXCHANGE -> Acc;
                         <<"x-retry-death">> -> Acc#{deaths => Value};
                         <<"x-retry-routing-key">> -> Acc#{?ANN_ROUTING_KEYS => [Value]};
                         <<"x-retry-first-death-exchange">> ->
                             Acc#{<<"x-first-death-exchange">> => Value};
                         <<"x-retry-first-death-reason">> ->
                             Acc#{<<"x-first-death-reason">> => Value};
                         <<"x-retry-first-death-queue">> ->
                             Acc#{<<"x-first-death-queue">> => Value};
                         <<"x-retry-last-death-exchange">> ->
                             Acc#{<<"x-last-death-exchange">> => Value, ?ANN_EXCHANGE => Value};
                         <<"x-retry-last-death-reason">> ->
                             Acc#{<<"x-last-death-reason">> => Value};
                         <<"x-retry-last-death-queue">> -> Acc#{<<"x-last-death-queue">> => Value};
                         _ -> Acc#{Key => Value}
                     end
                  end,
                  #{},
                  Anns),
    Msg#mc{annotations = Anns0};

%% @doc Restores original AMQP 0-9-1 message headers and routing properties
pop_annotations(#basic_message{content =
                                   #content{properties = #'P_basic'{headers = Headers} = Props} =
                                       Content} =
                    Msg) ->
    Exchange =
        case rabbit_misc:table_lookup(Headers, <<"x-retry-last-death-exchange">>) of
            {longstr, Value0} -> Value0;
            _ -> ?DEFAULT_EXCHANGE_NAME
        end,
    RK = case rabbit_misc:table_lookup(Headers, <<"x-retry-routing-key">>) of
             {longstr, Value1} -> Value1;
             _ -> <<>>
         end,
    Headers0 =
        lists:flatmap(fun({Key, Type, Value} = Entry) ->
                         case Key of
                             <<"x-retry-death">> -> [{<<"x-death">>, Type, Value}];
                             <<"x-first-death-exchange">> ->
                                 [{<<"x-retry-first-death-exchange">>, Type, Value}];
                             <<"x-first-death-reason">> ->
                                 [{<<"x-retry-first-death-reason">>, Type, Value}];
                             <<"x-first-death-queue">> ->
                                 [{<<"x-retry-first-death-queue">>, Type, Value}];
                             <<"x-last-death-exchange">> ->
                                 [{<<"x-retry-last-death-exchange">>, Type, Value}];
                             <<"x-last-death-reason">> ->
                                 [{<<"x-retry-last-death-reason">>, Type, Value}];
                             <<"x-last-death-queue">> ->
                                 [{<<"x-retry-last-death-queue">>, Type, Value}];
                             <<"x-death">> -> [];
                             <<"x-retry-routing-key">> -> [];
                             _ -> [Entry]
                         end
                      end,
                      Headers),
    Content0 =
        Content#content{properties = Props#'P_basic'{expiration = undefined, headers = Headers0}},
    Msg#basic_message{content = Content0,
                      exchange_name = Exchange,
                      routing_keys = [RK]}.

%% @doc Routes messages to terminal Dead Letter Exchange (DLX) when retry limits are exceeded
route_to_fallback_dlx(XName, Msg, OriginalQueueName, OriginalRoutingKey) ->
    Bindings =
        case rabbit_db_binding:match_routing_key(XName, [OriginalQueueName], true) of
            [QName] -> rabbit_db_binding:get_all(XName, QName, false);
            _ -> undefined
        end,

    case Bindings of
        [#binding{args = Args} | _] ->
            case rabbit_misc:table_lookup(Args, <<"x-dead-letter-exchange">>) of
                undefined ->
                    ?LOG_WARNING("Retry Exchange (~s): No fallback DLX configured. Message dropped.",
                                 [XName#resource.name]),
                    [];
                {longstr, DLXName} ->
                    case rabbit_db_exchange:get(
                             rabbit_misc:r(XName#resource.virtual_host, exchange, DLXName))
                    of
                        {ok, DLX} ->
                            DLRKeys =
                                case rabbit_misc:table_lookup(Args, <<"x-dead-letter-routing-key">>)
                                of
                                    undefined ->
                                        ?LOG_DEBUG("Retry Exchange (~s): Routing message to fallback DLX ~s with "
                                                   "original routing key ~s.",
                                                   [XName#resource.name,
                                                    DLXName,
                                                    OriginalRoutingKey]),
                                        [OriginalRoutingKey];
                                    {longstr, RK} ->
                                        ?LOG_DEBUG("Retry Exchange (~s): Routing message to fallback DLX ~s with "
                                                   "routing key ~s.",
                                                   [XName#resource.name, DLXName, RK]),
                                        [RK]
                                end,
                            Msg1 = mc:set_annotation(?ANN_ROUTING_KEYS, DLRKeys, Msg),
                            DLMsg = mc:set_annotation(?ANN_EXCHANGE, DLXName, Msg1),
                            Routed0 =
                                rabbit_exchange:route(DLX, DLMsg, #{return_binding_keys => true}),
                            Qs0 = rabbit_db_queue:get_targets(Routed0),
                            Qs = rabbit_amqqueue:prepend_extra_bcc(Qs0),
                            _ = rabbit_queue_type:deliver(Qs, DLMsg, #{}, stateless),
                            [];
                        _ ->
                            ?LOG_WARNING("Retry Exchange (~s): Configured DLX ~s not found. Message dropped.",
                                         [XName#resource.name, DLXName]),
                            []
                    end
            end;
        _ ->
            ?LOG_WARNING("Retry Exchange (~s): Message max attempts reached, but no matching "
                         "bindings found to route to fallback DLX. Message dropped.",
                         [XName#resource.name]),
            []
    end.

%% @doc Extracts retry information (attempt count, queue name, routing key, death metadata)
get_retry_info(Msg) ->
    case mc:get_annotation(deaths, Msg) of
        %% Modern RabbitMQ message container format where death records are stored in a map.
        %% Guarding with map_size > 0 prevents pattern match errors on empty maps.
        Death0 = #deaths{last = LastDeathKey, records = Rs} when is_map(Rs), map_size(Rs) > 0 ->
            #death{count = Count, routing_keys = RKeys} = maps:get(LastDeathKey, Rs),
            {Queue, Reason} = LastDeathKey,

            RK = case RKeys of
                    [RK_bin | _] when is_binary(RK_bin) -> RK_bin;
                    _ -> undefined
                end,

            case Reason of
                rejected -> {Count, Queue, RK, Death0};
                _        -> {0, Queue, RK, Death0}
            end;

        %% Legacy / AMQP 0-9-1 format where death records are stored as a list of tuples.
        %% The most recent death record is guaranteed to be at the head of the list.
        Death1 = [{DeathKey, #death{count = Count, routing_keys = RKeys}} | _] ->
            {Queue, Reason} = DeathKey,

            RK = case RKeys of
                [RK_bin | _] when is_binary(RK_bin) -> RK_bin;
                _ -> undefined
            end,

            case Reason of
                rejected -> {Count, Queue, RK, Death1};
                _ ->        {0, Queue, RK, Death1}
            end;

        %% Default fallback when no x-death headers/annotations are present
        _ ->
            {1, undefined, undefined, undefined}
    end.

%% @doc Computes delay TTL (in ms) based on strategy: fixed, linear, exponential, or random
calculate_delay(RetryCount, Args) ->
    Delay =
        case rabbit_misc:table_lookup(Args, <<"x-retry-delay">>) of
            {long, Value} -> Value;
            _ -> 100
        end,
    ActualDelay =
        case rabbit_misc:table_lookup(Args, <<"x-retry-delay-strategy">>) of
            {longstr, <<"exponential">>} ->
                Delay * (1 bsl (RetryCount - 1));
            {longstr, <<"linear">>} ->
                Delay * RetryCount;
            {longstr, <<"random">>} ->
                MaxRandomDelay = Delay * RetryCount,
                rand:uniform(MaxRandomDelay);
            _ ->
                Delay
        end,
    case rabbit_misc:table_lookup(Args, <<"x-retry-max-delay">>) of
        {long, MaxDelay} when ActualDelay > MaxDelay -> MaxDelay;
        _ -> ActualDelay
    end.

%% Callback implementations for rabbit_exchange_type behavior
info(_X) -> [].
info(_X, _) -> [].

%% Handles 3-arity info calls required by rabbit_exchange_type in recent RabbitMQ versions
info(_X, _Items, _VirtualHost) -> [].

serialise_events() -> false.

%% Indicates to RabbitMQ core that this exchange plugin is stateless
stateless() -> true.

%% @doc Validates exchange declaration arguments (delay, max attempts, strategy)
validate(#exchange{arguments = Args}) ->
    rabbit_retry_exchange_util:validate_args(Args,
                                              [{<<"x-retry-delay">>,
                                                required,
                                                fun rabbit_retry_exchange_util:validate_delay/1},
                                               {<<"x-retry-max-attempts">>,
                                                required,
                                                fun rabbit_retry_exchange_util:validate_max_attempts/1},
                                               {<<"x-retry-max-delay">>,
                                                optional,
                                                fun rabbit_retry_exchange_util:validate_max_delay/2},
                                               {<<"x-retry-delay-strategy">>,
                                                optional,
                                                fun rabbit_retry_exchange_util:validate_delay_strategy/1}]).

create(_Serial, _X) -> ok.
recover(_X, _Bs) -> ok.
delete(_Serial, _X) -> ok.

%% Handles transactional/contextual exchange deletion calls
delete(_Serial, _X, _Bs) -> ok.

policy_changed(_X1, _X2) -> ok.
add_binding(_Serial, _X, _B) -> ok.
remove_bindings(_Serial, _X, _Bs) -> ok.

%% @doc Ensures bindings are valid and connected queues specify correct x-dead-letter-exchange settings
validate_binding(#exchange{name = XName},
                  #binding{args = Args, destination = #resource{kind = queue} = QName}) ->
    case rabbit_db_queue:get(QName) of
        {ok, Q} when ?is_amqqueue(Q) ->
            QArgs = amqqueue:get_arguments(Q),
            case rabbit_misc:table_lookup(QArgs, <<"x-dead-letter-exchange">>) of
                undefined ->
                    rabbit_misc:protocol_error(precondition_failed,
                                               "Binding destination must be a valid queue with x-dead-letter-exchang"
                                               "e argument set to ~s",
                                               [XName#resource.name]);
                {longstr, DLX} when DLX == XName#resource.name ->
                    rabbit_retry_exchange_util:validate_args(Args,
                                                              [{<<"x-dead-letter-exchange">>,
                                                                optional,
                                                                fun rabbit_retry_exchange_util:validate_dlx/1},
                                                               {<<"x-dead-letter-routing-key">>,
                                                                optional,
                                                                fun rabbit_retry_exchange_util:validate_dlk/1}]);
                {_, DLX} ->
                    rabbit_misc:protocol_error(precondition_failed,
                                               "Binding destination must be a valid queue with x-dead-letter-exchang"
                                               "e argument set to ~s, actual was ~tp",
                                               [XName#resource.name, DLX])
            end;
        _ ->
            rabbit_misc:protocol_error(precondition_failed,
                                       "Binding destination must be a valid queue, actually was ~s",
                                       [QName#resource.name])
    end;
validate_binding(_Tx, #binding{destination = #resource{kind = Kind}}) ->
    rabbit_misc:protocol_error(precondition_failed,
                               "Binding destination must be a queue, actually was ~s",
                               [Kind]).

assert_args_equivalence(X, Args) ->
    rabbit_exchange:assert_args_equivalence(X, Args).