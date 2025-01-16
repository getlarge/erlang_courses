-module(rabbit_mqtt_topic_storage_bench).

-export([run/0, run/1]).

-include("rabbit_mqtt_topic_storage_ets.hrl").

% Test with exponentially increasing topic counts
-define(TOPIC_COUNTS, [1000, 2000, 4000, 8000, 16000, 32000, 64000, 128000, 256000]).
-define(DEPTH, 7).
-define(WARMUP_ITERATIONS, 1000).
-define(TEST_ITERATIONS, 5000).
-define(BATCH_SIZE, 100).

run() ->
    init_random(),
    run(?TOPIC_COUNTS).

run(TopicCounts) when is_list(TopicCounts) ->
    Results = lists:map(fun run_scenario/1, TopicCounts),
    print_results(Results),
    generate_charts(Results).

run_scenario(TopicCount) ->
    io:format("~nTesting with ~p topics~n", [TopicCount]),
    {ok, State} = rabbit_mqtt_topic_storage_ets:init(),

    % Create varied test data
    TestData = create_test_data(TopicCount, ?DEPTH),
    PopulatedState = populate_store(State, TestData),

    % Warm up heavily to ensure JIT stabilization
    _ = bench_warmup(PopulatedState, ?WARMUP_ITERATIONS),

    % Run actual benchmark
    {ExactTimes, WildcardTimes} = bench_lookups(PopulatedState, ?TEST_ITERATIONS, TestData),

    cleanup(PopulatedState),

    #{topic_count => TopicCount,
      exact_times => analyze_times(ExactTimes),
      wildcard_times => analyze_times(WildcardTimes)}.

% test data section
create_test_data(Count, Depth) ->
    % Create diverse topics that will match our wildcard patterns
    lists:map(fun(N) ->
                 % Generate base topic like "1/level1/level2/level3/level4"
                 Topic = generate_topic(N, Depth),

                 % Create wildcard pattern that will match this and similar topics
                 % For a topic "1/level1/level2/level3/level4"
                 % Pattern will be "1/+/level2/#" - guaranteed to match some topics
                 Parts = binary:split(Topic, <<"/">>, [global]),
                 WildcardPattern =
                     case Parts of
                         [First | _] ->
                             % Create pattern that will match this and similar topics
                             iolist_to_binary([First, "/+/level2/#"])
                     end,

                 {N, Topic, WildcardPattern}
              end,
              lists:seq(1, Count)).

populate_store(State, TestData) ->
    lists:foldl(fun({N, Topic, _}, AccState) ->
                   Value = iolist_to_binary(["msg", integer_to_list(N)]),
                   {ok, NewState} = rabbit_mqtt_topic_storage_ets:insert(Topic, Value, AccState),
                   NewState
                end,
                State,
                TestData).

generate_topic(N, Depth) ->
    % For each N, create several similar topics that will match the same wildcard
    TopicNum = N div 10,  % Group topics by tens to ensure wildcard matches
    Variation = N rem 10, % Use remainder to create variations
    Parts =
        [integer_to_list(TopicNum),      % First level is the group number
         lists:concat(["var", integer_to_list(Variation)]),  % Second level varies
         "level2"  % Fixed level that wildcards will match
         | [lists:concat(["level", integer_to_list(I)]) || I <- lists:seq(3, Depth - 1)]],
    iolist_to_binary(string:join(Parts, "/")).

cleanup(State) ->
    ets:delete(State#state.node_table),
    ets:delete(State#state.edge_table),
    ets:delete(State#state.topic_table).

% benchmark
bench_warmup(State, Iterations) ->
    % More intensive warm-up with varied patterns
    Topics = [generate_topic(N, ?DEPTH) || N <- lists:seq(1, 10)],
    Patterns = [iolist_to_binary([integer_to_list(N), "/+/#"]) || N <- lists:seq(1, 10)],

    lists:foreach(fun(_) ->
                     [rabbit_mqtt_topic_storage_ets:lookup(T, State) || T <- Topics],
                     [rabbit_mqtt_topic_storage_ets:lookup(P, State) || P <- Patterns]
                  end,
                  lists:seq(1, Iterations)).

bench_lookups(State, Iterations, TestData) ->
    % Select random test cases for each batch
    BatchCount = Iterations div ?BATCH_SIZE,
    ExactBatches =
        [bench_exact_batch(State, TestData, ?BATCH_SIZE) || _ <- lists:seq(1, BatchCount)],
    WildBatches =
        [bench_wildcard_batch(State, TestData, ?BATCH_SIZE) || _ <- lists:seq(1, BatchCount)],

    {lists:flatten(ExactBatches), lists:flatten(WildBatches)}.

bench_exact_batch(State, TestData, BatchSize) ->
    % Take random samples for each batch
    Samples = random_samples(TestData, BatchSize),
    [{Time, Matches}
     || {_, Topic, _} <- Samples,
        {Time, {ok, Matches}}
            <- [timer:tc(fun() -> rabbit_mqtt_topic_storage_ets:lookup(Topic, State) end)]].

bench_wildcard_batch(State, TestData, BatchSize) ->
    Samples = random_samples(TestData, BatchSize),
    [{Time, Matches}
     || {_, _, Pattern} <- Samples,
        {Time, {ok, Matches}}
            <- [timer:tc(fun() -> rabbit_mqtt_topic_storage_ets:lookup(Pattern, State) end)]].

random_samples(List, N) ->
    % Select random elements without replacement using older random functionality since rand was not available
    Length = length(List),
    Indices = lists:sort([{random:uniform(), X} || X <- lists:seq(1, Length)]),
    Selected = lists:sublist([I || {_, I} <- Indices], N),
    [lists:nth(I, List) || I <- Selected].

% Initialize random seed at the start
init_random() ->
    {A, B, C} = os:timestamp(),
    random:seed(A, B, C).

% measure
analyze_times(TimedResults) ->
    Times = [Time / 1000.0 || {Time, _} <- TimedResults],  % Convert to ms
    Matches = [length(M) || {_, M} <- TimedResults],

    #{times =>
          #{min => lists:min(Times),
            max => lists:max(Times),
            avg => lists:sum(Times) / length(Times),
            median => median(Times),
            p95 => percentile(Times, 95)},
      matches =>
          #{min => lists:min(Matches),
            max => lists:max(Matches),
            avg => lists:sum(Matches) / length(Matches)}}.

median(List) ->
    Sorted = lists:sort(List),
    Length = length(Sorted),
    Middle = Length div 2,
    case Length rem 2 of
        0 ->
            (lists:nth(Middle, Sorted) + lists:nth(Middle + 1, Sorted)) / 2;
        1 ->
            lists:nth(Middle + 1, Sorted)
    end.

percentile(List, P) when P >= 0, P =< 100 ->
    Sorted = lists:sort(List),
    Length = length(Sorted),
    N = round(P * Length / 100),
    lists:nth(max(1, min(N, Length)), Sorted).

print_results(Results) ->
    io:format("~n=== Benchmark Results for depth ~B ===~n", [?DEPTH]),
    io:format("~-12s ~-15s ~-15s ~-15s ~-15s ~-15s~n",
              ["Topics",
               "Exact Avg(ms)",
               "Exact P95(ms)",
               "Wild Avg(ms)",
               "Wild P95(ms)",
               "Wild Matches"]),
    io:format("~s~n", [string:copies("-", 87)]),
    lists:foreach(fun(R) ->
                     #{topic_count := Count,
                       exact_times := #{times := #{avg := ExactAvg, p95 := ExactP95}},
                       wildcard_times :=
                           #{times := #{avg := WildAvg, p95 := WildP95},
                             matches := #{avg := MatchAvg}}} =
                         R,
                     io:format("~-12B ~-15.3f ~-15.3f ~-15.3f ~-15.3f ~-15.1f~n",
                               [Count, ExactAvg, ExactP95, WildAvg, WildP95, MatchAvg])
                  end,
                  Results).

% generate charts section
generate_charts(Results) ->
    Charts = [generate_time_chart(Results), generate_matches_chart(Results)],
    file:write_file("complexity_analysis.md", Charts).

generate_time_chart(Results) ->
    XAxis = [integer_to_list(Count) || #{topic_count := Count} <- Results],
    YExact =
        [maps:get(avg, maps:get(times, ExactTimes)) || #{exact_times := ExactTimes} <- Results],
    YWild =
        [maps:get(avg, maps:get(times, WildTimes)) || #{wildcard_times := WildTimes} <- Results],

    ["```mermaid\n",
     "%%{init: {'theme': 'base'}}%%\n",
     "xychart-beta\n",
     "    title \"Average Lookup Time vs Topic Count with depth = ",
     integer_to_list(?DEPTH),
     "\"\n",
     "    x-axis [",
     string:join(XAxis, ", "),
     "]\n",
     "    y-axis \"Time (ms)\" 0 --> ",
     io_lib:format("~.3f", [lists:max(YWild) * 1.2]),
     "\n",
     "    bar [",
     string:join([io_lib:format("~.3f", [Y]) || Y <- YWild], ", "),
     "]\n",
     "    bar [",
     string:join([io_lib:format("~.3f", [Y]) || Y <- YExact], ", "),
     "]\n",
     "```\n\n"].

generate_matches_chart(Results) ->
    XAxis = [integer_to_list(Count) || #{topic_count := Count} <- Results],
    YMatches =
        [maps:get(avg, maps:get(matches, WildTimes))
         || #{wildcard_times := WildTimes} <- Results],

    ["```mermaid\n",
     "%%{init: {'theme': 'base'}}%%\n",
     "xychart-beta\n",
     "    title \"Average Wildcard Matches vs Topic Count\"\n",
     "    x-axis [",
     string:join(XAxis, ", "),
     "]\n",
     "    y-axis \"Matches\" 0 --> ",
     io_lib:format("~.1f", [lists:max(YMatches) * 1.2]),
     "\n",
     "    bar [",
     string:join([io_lib:format("~.1f", [Y]) || Y <- YMatches], ", "),
     "]\n",
     "```\n"].
