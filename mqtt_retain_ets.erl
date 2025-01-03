-module(mqtt_retain_ets).

-include("rabbit_mqtt_packet.hrl").

% -export([new/1, terminate/1, insert/3, lookup/2, delete/2]).
-compile([export_all, nowarn_export_all]).

-record(store_state, {table :: ets:tid(), filename :: file:filename_all()}).

-type store_state() :: #store_state{}.

-spec has_wildcards(topic()) -> boolean().
has_wildcards(Pattern) ->
  Parts = binary:split(Pattern, <<"/">>, [global]),
  lists:member(<<"#">>, Parts) orelse lists:member(<<"+">>, Parts).

-spec new(binary()) -> store_state().
new(TableName) ->
  Path = filename:join(<<"courses">>, TableName),
  _ = file:delete(Path),
  Tid = ets:new(TableName, [set, public, {keypos, #retained_message.topic}]),
  #store_state{table = Tid, filename = Path}.

-spec terminate(store_state()) -> ok.
terminate(#store_state{table = T, filename = Path}) ->
  ok = ets:tab2file(T, Path, [{extended_info, [object_count]}]).

-spec lookup(topic(), store_state()) -> mqtt_msg() | mqtt_msg_v0() | undefined.
lookup(Topic, #store_state{table = T}) ->
  case has_wildcards(Topic) of
    true ->
      case lookup_by_pattern(Topic, #store_state{table = T}) of
        undefined ->
          undefined;
        [#retained_message{mqtt_msg = Msg}] ->
          Msg
      end;
    false ->
      % Direct lookup for exact matches
      case ets:lookup(T, Topic) of
        [] ->
          undefined;
        [#retained_message{mqtt_msg = Msg}] ->
          Msg
      end
  end.

-spec delete(topic(), store_state()) -> ok.
delete(Topic, #store_state{table = T}) ->
  true = ets:delete(T, Topic),
  ok.

-spec insert(topic(), mqtt_msg(), store_state()) -> ok.
insert(Topic, Msg, #store_state{table = T}) ->
  true = ets:insert(T, #retained_message{topic = Topic, mqtt_msg = Msg}),
  ok.

-spec lookup_by_pattern(topic(), store_state()) -> [mqtt_msg()].
lookup_by_pattern(Pattern, #store_state{table = T}) ->
  % TODO:
  % 1.derive a pattern compatible with ETS
  % 2. use second argument to match by pattern and reduce the amount of keys to compare
  % this is a tradeoff between having enough keys to compare with Qlobber and not too many to improve performance
  Keys = ets:tab2list(T),
  Qlobber = qlobber:new(),
  qlobber:add(Qlobber, Pattern),
  Topic = match_retained(Keys, Qlobber),
  get_value(T, Topic).

get_value(T, Topic) ->
  case ets:lookup(T, Topic) of
    [] ->
      undefined;
    [#retained_message{mqtt_msg = Msg}] ->
      Msg
  end.

match_retained([], _Qlobber) ->
  [];
% TODO: make it recursive to match all retained messages that match the Qlobber pattern
match_retained([{Topic, _} | Rest], Qlobber) ->
  try
    case qlobber:match(Qlobber, Topic) of
      _ ->
        Topic
    end
  catch
    error:badarg ->
      undefined
  end.
