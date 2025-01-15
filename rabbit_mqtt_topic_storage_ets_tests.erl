-module(rabbit_mqtt_topic_storage_ets_tests).

-include_lib("eunit/include/eunit.hrl").

-include("rabbit_mqtt_topic_storage_ets.hrl").

%%----------------------------------------------------------------------------
%% Setup and Cleanup
%%----------------------------------------------------------------------------

setup() ->
  {ok, State} = rabbit_mqtt_topic_storage_ets:init(),
  State.

cleanup(State) ->
  ets:delete(State#state.node_table),
  ets:delete(State#state.edge_table),
  ets:delete(State#state.topic_table).

%%----------------------------------------------------------------------------
%% Test Cases
%%----------------------------------------------------------------------------

basic_operations_test_() ->
  [{foreach,
    fun setup/0,
    fun cleanup/1,
    [fun test_add_and_match/1, fun test_delete/1, fun test_clear/1]}].

wildcard_matching_test_() ->
  [{foreach,
    fun setup/0,
    fun cleanup/1,
    [fun test_plus_wildcard/1, fun test_hash_wildcard/1, fun test_combined_wildcards/1]}].

%%----------------------------------------------------------------------------
%% Individual Test Functions
%%----------------------------------------------------------------------------
test_add_and_match(State) ->
  % Add a simple topic
  {ok, State1} = rabbit_mqtt_topic_storage_ets:insert(<<"a/b/c">>, <<"msg1">>, State),
  {ok, Matches1} = rabbit_mqtt_topic_storage_ets:lookup(<<"a/b/c">>, State1),
  {ok, State2} = rabbit_mqtt_topic_storage_ets:insert(<<"a/b/d">>, <<"msg2">>, State1),
  {ok, Matches2} = rabbit_mqtt_topic_storage_ets:lookup(<<"a/b/d">>, State2),
  {ok, NoMatches} = rabbit_mqtt_topic_storage_ets:lookup(<<"x/y/z">>, State2),

  [?_assertEqual([<<"msg1">>], Matches1),
   ?_assertEqual([<<"msg2">>], Matches2),
   ?_assertEqual([], NoMatches)].

test_delete(State) ->
  {ok, State1} = rabbit_mqtt_topic_storage_ets:insert(<<"a/b/c">>, <<"msg1">>, State),
  {ok, State2} =
    rabbit_mqtt_topic_storage_ets:delete(<<"a/b/c">>, <<"msg1">>, State1),
  {ok, Matches} = rabbit_mqtt_topic_storage_ets:lookup(<<"a/b/c">>, State2),

  [?_assertEqual([], Matches)].

test_clear(State) ->
  {ok, State1} = rabbit_mqtt_topic_storage_ets:insert(<<"a/b/c">>, <<"msg1">>, State),
  {ok, State2} = rabbit_mqtt_topic_storage_ets:insert(<<"x/y/z">>, <<"msg2">>, State1),
  {ok, State3} = rabbit_mqtt_topic_storage_ets:clear(State2),
  {ok, Matches} = rabbit_mqtt_topic_storage_ets:lookup(<<"a/b/c">>, State3),

  [?_assertEqual([], Matches)].

test_plus_wildcard(State) ->
  {ok, State1} = rabbit_mqtt_topic_storage_ets:insert(<<"a/b/c">>, <<"msg1">>, State),
  {ok, State2} = rabbit_mqtt_topic_storage_ets:insert(<<"a/x/c">>, <<"msg2">>, State1),
  {ok, Matches} = rabbit_mqtt_topic_storage_ets:lookup(<<"a/+/c">>, State2),

  [?_assertEqual(lists:sort([<<"msg1">>, <<"msg2">>]), lists:sort(Matches))].

test_hash_wildcard(State) ->
  {ok, State1} = rabbit_mqtt_topic_storage_ets:insert(<<"a/b/c">>, <<"msg1">>, State),
  {ok, State2} = rabbit_mqtt_topic_storage_ets:insert(<<"a/b/c/d">>, <<"msg2">>, State1),
  {ok, State3} = rabbit_mqtt_topic_storage_ets:insert(<<"a/b/x/y">>, <<"msg3">>, State2),
  {ok, State4} = rabbit_mqtt_topic_storage_ets:insert(<<"a/q/x/y">>, <<"msg3">>, State3),
  {ok, Matches} = rabbit_mqtt_topic_storage_ets:lookup(<<"a/b/#">>, State4),

  [?_assertEqual([<<"msg1">>, <<"msg2">>, <<"msg3">>], lists:sort(Matches))].

test_combined_wildcards(State) ->
  {ok, State1} = rabbit_mqtt_topic_storage_ets:insert(<<"a/b/c/d">>, <<"msg1">>, State),
  {ok, State2} = rabbit_mqtt_topic_storage_ets:insert(<<"a/x/c/e">>, <<"msg2">>, State1),
  {ok, State3} =
    rabbit_mqtt_topic_storage_ets:insert(<<"a/y/c/f/g">>, <<"msg3">>, State2),
  {ok, State4} =
    rabbit_mqtt_topic_storage_ets:insert(<<"a/y/d/f/g">>, <<"msg4">>, State3),
  {ok, Matches} = rabbit_mqtt_topic_storage_ets:lookup(<<"a/+/c/#">>, State4),

  [?_assertEqual([<<"msg1">>, <<"msg2">>, <<"msg3">>], lists:sort(Matches))].
