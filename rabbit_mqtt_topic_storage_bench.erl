-module(rabbit_mqtt_topic_storage_bench).

-export([run/0, run/1]).

-include("rabbit_mqtt_topic_storage_ets.hrl").

-define(DEFAULT_TOPIC_COUNT, 100000).
-define(ITERATIONS, 1000).

%%----------------------------------------------------------------------------
%% API
%%----------------------------------------------------------------------------

run() ->
  run(?DEFAULT_TOPIC_COUNT).

run(TopicCount) ->
  {ok, State} = rabbit_mqtt_topic_storage_ets:init(),

  io:format("~nBenchmarking MQTT Topic Storage with ~p topics~n", [TopicCount]),
  io:format("----------------------------------------~n"),

  % Best case - empty store
  bench_best_case(State),

  % Populate store with topics
  State1 = populate_store(State, TopicCount),

  % Worst case scenarios
  bench_worst_case_plus(State1),
  bench_worst_case_hash(State1),

  cleanup(State1).

%%----------------------------------------------------------------------------
%% Internal Functions
%%----------------------------------------------------------------------------

bench_best_case(State) ->
  io:format("Best case - exact match on empty store:~n"),
  {Time, _} =
    timer:tc(fun() ->
                lists:foreach(fun(_) -> rabbit_mqtt_topic_storage_ets:lookup(<<"a/b/c">>, State)
                              end,
                              lists:seq(1, ?ITERATIONS))
             end),
  print_results(Time, ?ITERATIONS).

bench_worst_case_plus(State) ->
  io:format("~nWorst case - single plus wildcard:~n"),
  {Time, {ok, Matches}} =
    timer:tc(fun() -> rabbit_mqtt_topic_storage_ets:lookup(<<"device/+/temperature">>, State)
             end),
  io:format("Matched ~p topics in ~.2f ms~n", [length(Matches), Time / 1000]).

bench_worst_case_hash(State) ->
  io:format("~nWorst case - hash wildcard:~n"),
  {Time, {ok, Matches}} =
    timer:tc(fun() -> rabbit_mqtt_topic_storage_ets:lookup(<<"device/#">>, State) end),
  io:format("Matched ~p topics in ~.2f ms~n", [length(Matches), Time / 1000]).

populate_store(State, Count) ->
  io:format("~nPopulating store with ~p topics...~n", [Count]),
  {Time, State1} =
    timer:tc(fun() ->
                lists:foldl(fun(N, AccState) ->
                               Topic = generate_topic(N),
                               {ok, NewState} =
                                 rabbit_mqtt_topic_storage_ets:insert(Topic, N, AccState),
                               NewState
                            end,
                            State,
                            lists:seq(1, Count))
             end),
  io:format("Population completed in ~.2f seconds~n", [Time / 1000000]),
  State1.

% TODO: also generate long topic -> will make traverse slower
generate_topic(N) ->
  % Generate topics like: device/123/temperature, device/123/humidity
  DeviceId = N div 2,
  case N rem 2 of
    0 ->
      iolist_to_binary(io_lib:format("device/~p/temperature", [DeviceId]));
    1 ->
      iolist_to_binary(io_lib:format("device/~p/humidity", [DeviceId]))
  end.

print_results(Time, Iterations) ->
  AvgTimeMs = Time / Iterations / 1000,
  io:format("Average time: ~.3f ms~n", [AvgTimeMs]),
  io:format("Operations per second: ~.2f~n", [1000 / AvgTimeMs]).

cleanup(State) ->
  ets:delete(State#state.node_table),
  ets:delete(State#state.edge_table),
  ets:delete(State#state.topic_table).

%%----------------------------------------------------------------------------
% Example Usage:
%
% 1> rabbit_mqtt_topic_storage_bench:run().
%
% Benchmarking MQTT Topic Storage with 10000 topics
% ----------------------------------------
% Best case - exact match on empty store:
% Average time: 0.000 ms
% Operations per second: 2298850.57

% Populating store with 10000 topics...
% Population completed in 0.04 seconds

% Worst case - single plus wildcard:
% Matched 5000 topics in 3.51 ms

% Worst case - hash wildcard:
% Matched 10000 topics in 12.21 ms
% true
%%----------------------------------------------------------------------------
