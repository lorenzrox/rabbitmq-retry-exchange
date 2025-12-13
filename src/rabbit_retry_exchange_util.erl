%%==============================================================================
%% @author Lorenzo Rossoni <https://github.com/lorenzrox>
%% @end
%%==============================================================================

%% @doc Implement required exchange behaviors for the retry_exchange
%% @end

-module(rabbit_retry_exchange_util).

-author("https://github.com/lorenzrox").

-include_lib("kernel/include/logger.hrl").

-export([validate_args/2, validate_max_attempts/1, validate_delay_strategy/1,
         validate_delay/1, validate_max_delay/2, validate_dlx/1, validate_dlk/1]).

validate_max_attempts({long, Term}) when is_integer(Term) ->
    case Term >= 1 of
        true ->
            ok;
        false ->
            rabbit_misc:protocol_error(precondition_failed,
                                       "x-retry-max-attempts must be an integer greater than 0, actually "
                                       "was ~tp",
                                       [Term])
    end;
validate_max_attempts({_Type, Term}) ->
    rabbit_misc:protocol_error(precondition_failed,
                               "x-retry-max-attempts must be an integer, actually was ~tp",
                               [Term]).

validate_delay_strategy({longstr, Term}) when is_binary(Term) ->
    case Term of
        <<"fixed">> ->
            ok;
        <<"linear">> ->
            ok;
        <<"random">> ->
            ok;
        <<"exponential">> ->
            ok;
        _ ->
            rabbit_misc:protocol_error(precondition_failed,
                                       "x-retry-delay-strategy must be 'fixed', 'linear', 'exponential' "
                                       "or 'random', actually was ~s",
                                       [Term])
    end;
validate_delay_strategy({_Type, Term}) ->
    rabbit_misc:protocol_error(precondition_failed,
                               "x-retry-delay-strategy must be a binary/string, actually was "
                               "~tp",
                               [Term]).

validate_delay({long, Term}) when is_integer(Term) ->
    case Term >= 0 of
        true ->
            ok;
        false ->
            rabbit_misc:protocol_error(precondition_failed,
                                       "x-retry-delay must be an integer greater than or equal to 0ms, "
                                       "actually was ~tp",
                                       [Term])
    end;
validate_delay({_Type, Term}) ->
    rabbit_misc:protocol_error(precondition_failed,
                               "x-retry-delay must be an integer, actually was ~tp",
                               [Term]).

validate_max_delay({long, Term}, Args) when is_integer(Term) ->
    case rabbit_misc:table_lookup(Args, <<"x-retry-delay">>) of
        {long, Delay} ->
            case Term >= Delay of
                true ->
                    ok;
                false ->
                    rabbit_misc:protocol_error(precondition_failed,
                                               "x-retry-max-delay must be an integer greater or equal to x-retry-del"
                                               "ay, actually was ~tp",
                                               [Term])
            end;
        _ ->
            rabbit_misc:protocol_error(precondition_failed, "x-retry-delay must be an integer", [])
    end;
validate_max_delay({_Type, Term}, _Args) ->
    rabbit_misc:protocol_error(precondition_failed,
                               "x-retry-max-delay must be an integer, actually was ~tp",
                               [Term]).

validate_dlx({longstr, Term}) when is_binary(Term) ->
    ok;
validate_dlx({_Type, Term}) ->
    rabbit_misc:protocol_error(precondition_failed,
                               "x-dead-letter-exchange must be a binary, actually was ~tp",
                               [Term]).

validate_dlk({longstr, Term}) when is_binary(Term) ->
    ok;
validate_dlk({_Type, Term}) ->
    rabbit_misc:protocol_error(precondition_failed,
                               "x-dead-letter-routing-key must be a binary, actually was ~tp",
                               [Term]).

validate_args(Args, Validations) ->
    validate_args(Args, Validations, []).

validate_args(_Args, [], _AccumErrors) ->
    ok;
validate_args(Args, [{Key, required, Validator} | Rest], Errors) ->
    case rabbit_misc:table_lookup(Args, Key) of
        undefined ->
            ?LOG_ERROR("Required argument '~s' not found", [Key]),

            %% Missing required argument
            Error =
                rabbit_misc:protocol_error(precondition_failed,
                                           "Missing required argument '~s'",
                                           [Key]),
            validate_args(Args, Rest, [Error | Errors]);
        Value ->
            Result =
                case erlang:fun_info(Validator, arity) of
                    {arity, 1} ->
                        Validator(Value);
                    {arity, 2} ->
                        Validator(Value, Args);
                    {arity, Arity} ->
                        rabbit_misc:protocol_error(precondition_failed,
                                                   "Internal error: validator for '~s' has unsupported arity ~tp",
                                                   [Key, Arity])
                end,

            case Result of
                ok ->
                    ?LOG_DEBUG("Required argument '~s' successully validated", [Key]),

                    validate_args(Args, Rest, Errors);
                Error = {protocol_error, _, _, _} ->
                    validate_args(Args, Rest, [Error | Errors])
            end
    end;
validate_args(Args, [{Key, optional, Validator} | Rest], Errors) ->
    case rabbit_misc:table_lookup(Args, Key) of
        undefined ->
            ?LOG_DEBUG("Optional argument '~s' not found", [Key]),

            %% Optional and missing: skip validation
            validate_args(Args, Rest, Errors);
        Value ->
            Result =
                case erlang:fun_info(Validator, arity) of
                    {arity, 1} ->
                        Validator(Value);
                    {arity, 2} ->
                        Validator(Value, Args);
                    {arity, Arity} ->
                        rabbit_misc:protocol_error(precondition_failed,
                                                   "Internal error: validator for '~s' has unsupported arity ~tp",
                                                   [Key, Arity])
                end,

            case Result of
                ok ->
                    ?LOG_DEBUG("Optional argument '~s' successully validated", [Key]),

                    validate_args(Args, Rest, Errors);
                Error = {protocol_error, _, _, _} ->
                    validate_args(Args, Rest, [Error | Errors])
            end
    end;
validate_args(_Args, _, [Error | _]) ->
    Error.
