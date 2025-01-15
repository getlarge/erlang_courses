-module(rabbit_mqtt_topic_storage_ets).

%-behaviour(rabbit_mqtt_topic_storage).

-export([init/0, insert/3, delete/3, lookup/2, clear/1, terminate/1]).

-include("rabbit_mqtt_topic_storage_ets.hrl").

%%----------------------------------------------------------------------------
%% API Implementation
%%----------------------------------------------------------------------------

-spec init() -> {ok, #state{}}.
init() ->
  % Node table - will store tuples of {node_id, edge_count, is_topic}
  NodeTable = ets:new(mqtt_topic_trie_nodes, [set, public]),

  % Edge table - will store {{from_id, word}, to_id}
  EdgeTable = ets:new(mqtt_topic_trie_edges, [ordered_set, public]),

  % Topic table - will store {node_id, topic, value}
  TopicTable = ets:new(mqtt_topic_trie_topics, [bag, public]),

  RootId = make_node_id(),

  RootEntry = {RootId, 0, false},
  ets:insert(NodeTable, RootEntry),

  {ok,
   #state{node_table = NodeTable,
          edge_table = EdgeTable,
          topic_table = TopicTable,
          root_id = RootId}}.

-spec insert(binary(), term(), #state{}) -> {ok, #state{}}.
insert(Topic, Value, State) ->
  Words = split_topic(Topic),
  NodeId = follow_or_create_path(Words, State),
  % Mark node as topic end and store value
  update_node(NodeId, true, State),
  TopicEntry = {NodeId, Topic, Value},
  ets:insert(State#state.topic_table, TopicEntry),
  {ok, State}.

-spec delete(binary(), term(), #state{}) -> {ok, #state{}}.
delete(Topic, Value, State) ->
  Words = split_topic(Topic),
  case follow_path(Words, State) of
    {ok, NodeId} ->
      % Remove topic using tuple format
      ets:delete_object(State#state.topic_table, {NodeId, Topic, Value}),

      % If no more topics at this node, mark as non-topic
      case ets:lookup(State#state.topic_table, NodeId) of
        [] ->
          update_node(NodeId, false, State);
        _ ->
          ok
      end,

      % Clean up unused path
      maybe_clean_path(NodeId, State);
    error ->
      ok
  end,
  {ok, State}.

-spec lookup(binary(), #state{}) -> {ok, [term()]}.
lookup(Pattern, State) ->
  Words = split_topic(Pattern),
  Matches = match_pattern_words(Words, State#state.root_id, State, []),
  Values =
    lists:flatmap(fun(NodeId) ->
                     case ets:lookup(State#state.topic_table, NodeId) of
                       [{_NodeId, _Topic, Value} | _] -> [Value];
                       [] -> []
                     end
                  end,
                  Matches),
  {ok, Values}.

-spec terminate(#state{}) -> ok.
terminate(State) ->
  ok = ets:tab2file(State#state.topic_table, [{extended_info, [object_count]}]).

-spec clear(#state{}) -> {ok, #state{}}.
clear(State) ->
  ets:delete_all_objects(State#state.node_table),
  ets:delete_all_objects(State#state.edge_table),
  ets:delete_all_objects(State#state.topic_table),

  % Recreate root node
  RootId = State#state.root_id,
  RootEntry = {RootId, 0, false},
  ets:insert(State#state.node_table, RootEntry),
  {ok, State}.

%%----------------------------------------------------------------------------
%% Internal Functions
%%----------------------------------------------------------------------------

split_topic(Topic) ->
  binary:split(Topic, <<"/">>, [global]).

make_node_id() ->
  crypto:strong_rand_bytes(16).

follow_or_create_path(Words, State) ->
  follow_or_create_path(Words, State#state.root_id, State).

follow_or_create_path([], NodeId, _State) ->
  NodeId;
follow_or_create_path([Word | Rest], NodeId, State) ->
  case find_edge(NodeId, Word, State) of
    {ok, ChildId} ->
      follow_or_create_path(Rest, ChildId, State);
    error ->
      ChildId = make_node_id(),
      add_edge(NodeId, Word, ChildId, State),
      follow_or_create_path(Rest, ChildId, State)
  end.

follow_path(Words, State) ->
  follow_path(Words, State#state.root_id, State).

follow_path([], NodeId, _State) ->
  {ok, NodeId};
follow_path([Word | Rest], NodeId, State) ->
  case find_edge(NodeId, Word, State) of
    {ok, ChildId} ->
      follow_path(Rest, ChildId, State);
    error ->
      error
  end.

match_pattern_words([], NodeId, _State, Acc) ->
  [NodeId | Acc];
match_pattern_words([<<"+">> | RestWords], NodeId, State, Acc) ->
  % + matches any single word
  Edges = get_all_edges(NodeId, State),
  lists:foldl(fun({_Key, ChildId}, EdgeAcc) ->
                 match_pattern_words(RestWords, ChildId, State, EdgeAcc)
              end,
              Acc,
              Edges);
match_pattern_words([<<"#">> | _], NodeId, State, Acc) ->
  % # matches zero or more words
  collect_descendants(NodeId, State, [NodeId | Acc]);
match_pattern_words([Word | RestWords], NodeId, State, Acc) ->
  case find_edge(NodeId, Word, State) of
    {ok, ChildId} ->
      match_pattern_words(RestWords, ChildId, State, Acc);
    error ->
      Acc
  end.

collect_descendants(NodeId, State, Acc) ->
  Edges = get_all_edges(NodeId, State),
  lists:foldl(fun({_Key, ChildId}, EdgeAcc) ->
                 collect_descendants(ChildId, State, [ChildId | EdgeAcc])
              end,
              Acc,
              Edges).

find_edge(NodeId, Word, State) ->
  Key = {NodeId, Word},
  EdgeTable = State#state.edge_table,
  case ets:lookup(EdgeTable, Key) of
    [{_Key, ToNode}] ->
      {ok, ToNode};
    [] ->
      error
  end.

get_all_edges(NodeId, State) ->
  % Match all edges from this node
  Pattern = {{NodeId, '_'}, '_'},
  ets:match_object(State#state.edge_table, Pattern).

add_edge(FromId, Word, ToId, State) ->
  Key = {FromId, Word},
  EdgeEntry = {Key, ToId},
  ets:insert(State#state.edge_table, EdgeEntry),
  NodeEntry = {ToId, 0, false},
  ets:insert(State#state.node_table, NodeEntry),
  update_edge_count(FromId, +1, State).

remove_edge(FromId, Word, State) ->
  ets:delete(State#state.edge_table, {FromId, Word}),
  update_edge_count(FromId, -1, State).

update_node(NodeId, IsTopic, State) ->
  % Update is_topic field (position 3) for the given node
  ets:update_element(State#state.node_table, NodeId, {3, IsTopic}).

update_edge_count(NodeId, Delta, State) ->
  case ets:lookup(State#state.node_table, NodeId) of
    [{_NodeId, EdgeCount, IsPattern}] ->
      % Update with new count
      NewCount = EdgeCount + Delta,
      ets:insert(State#state.node_table, {NodeId, NewCount, IsPattern});
    [] ->
      % Node not found
      error
  end.

% Recursively clean up node tree if it's now empty
maybe_clean_path(NodeId, State) ->
  case ets:lookup(State#state.node_table, NodeId) of
    [#trie_node{edge_count = 0, is_topic = false}] ->
      % Find edges pointing to this node
      MatchSpec =
        [{{{'$1', '$2'}, #trie_edge{child_id = NodeId, _ = '_'}}, [], [{{'$1', '$2'}}]}],
      case ets:select(State#state.edge_table, MatchSpec) of
        [{ParentId, Word}] ->
          remove_edge(ParentId, Word, State),
          ets:delete(State#state.node_table, NodeId),
          maybe_clean_path(ParentId, State);
        [] ->
          ok
      end;
    _ ->
      ok
  end.
