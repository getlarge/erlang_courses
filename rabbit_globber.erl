-module(rabbit_globber).

-export([new/0, new/3, add/2, add/3, remove/2, remove/3, test/2, match/2, match_iter/2,
         clear/1]).

-include_lib("rabbit_globber.hrl").

-spec new() -> globber().
new() ->
    #globber{}.

-spec new(binary(), binary(), binary()) -> globber().
new(Separator, WildcardOne, WildcardSome) ->
    #globber{separator = Separator,
             wildcard_one = WildcardOne,
             wildcard_some = WildcardSome,
             trie = maps:new()}.

%  TODO: fix Q1 = rabbit_globber:add(Q, <<"test.*">>, <<"it m">>). ** exception error: no match of right hand side value when calling add twice

-spec add(globber(), binary()) -> globber().
add(Globber, Pattern) ->
    add(Globber, Pattern, <<"match">>).

-spec add(globber(), binary(), any()) -> globber().
add(Globber, Pattern, Val) ->
    Words = split(Pattern, Globber#globber.separator),
    Trie = do_add(Words, Val, Globber#globber.trie),
    Globber#globber{trie = Trie}.

-spec remove(globber(), binary()) -> globber().
remove(Globber, Pattern) ->
    remove(Globber, Pattern, <<>>).

-spec remove(globber(), binary(), any()) -> globber().
remove(Globber, Pattern, Val) ->
    Words = split(Pattern, Globber#globber.separator),
    Trie = do_remove(Words, Val, Globber#globber.trie),
    Globber#globber{trie = Trie}.


% TODO: fix ** exception error: no match of right hand side value [] when calling match with bad pattern such as rabbit_globber:match(Q1, <<"test.bar">>).
-spec match(globber(), binary()) -> list().
match(Globber, Pattern) ->
    Words = split(Pattern, Globber#globber.separator),
    try do_match(Words, Globber#globber.trie, [], Globber) of
        Res ->
            Res
    catch
        % error:badmatch and all
        _:_ ->
            undefined
    end.

-spec test(globber(), binary()) -> boolean().
test(Globber, Pattern) ->
    case match(Globber, Pattern) of
        undefined ->
            false;
        [] ->
            false;
        _ ->
            true
    end.

-spec match_iter(globber(), binary()) -> list().
match_iter(Globber, Topic) ->
    Words = split(Topic, Globber#globber.separator),
    do_match_iter(Words, Globber#globber.trie, Globber).

-spec clear(globber()) -> globber().
clear(Globber) ->
    Globber#globber{trie = maps:new()}.

split(Topic, Separator) ->
    binary:split(Topic, Separator, [global]).

do_add([], Val, Trie) ->
    maps:put(<<".">>, [Val | maps:get(<<".">>, Trie, [])], Trie);
do_add([Word | Rest], Val, Trie) ->
    SubTrie = maps:get(Word, Trie, maps:new()),
    NewSubTrie = do_add(Rest, Val, SubTrie),
    maps:put(Word, NewSubTrie, Trie).

do_remove([], Val, Trie) ->
    case maps:get(<<".">>, Trie) of
        Vals when is_list(Vals) ->
            NewVals = lists:delete(Val, Vals),
            if NewVals =:= [] ->
                   maps:remove(<<".">>, Trie);
               true ->
                   maps:put(<<".">>, NewVals, Trie)
            end;
        _ ->
            Trie
    end;
do_remove([Word | Rest], Val, Trie) ->
    case maps:get(Word, Trie) of
        SubTrie when is_map(SubTrie) ->
            NewSubTrie = do_remove(Rest, Val, SubTrie),
            case maps:size(NewSubTrie) of
                0 ->
                    maps:remove(Word, Trie);
                _ ->
                    maps:put(Word, NewSubTrie, Trie)
            end;
        _ ->
            Trie
    end.

do_match([], Trie, Acc, _Globber) ->
    case maps:get(<<".">>, Trie, undefined) of
        undefined ->
            Acc;
        Vals ->
            lists:append(Vals, Acc)
    end;
do_match([Word | Rest], Trie, Acc, Globber) ->
    SubTrie =
        case maps:get(Word, Trie, undefined) of
            undefined ->
                maps:new();
            Sub ->
                Sub
        end,
    SubTrie1 =
        case maps:get(Globber#globber.wildcard_one, Trie, undefined) of
            undefined ->
                maps:new();
            Sub1 ->
                Sub1
        end,
    SubTrie2 =
        case maps:get(Globber#globber.wildcard_some, Trie, undefined) of
            undefined ->
                maps:new();
            Sub2 ->
                Sub2
        end,
    Acc1 = do_match(Rest, SubTrie, Acc, Globber),
    Acc2 = do_match(Rest, SubTrie1, Acc1, Globber),
    do_match([], SubTrie2, Acc2, Globber).

do_match_iter([], Trie, _Globber) ->
    maps:get(<<".">>, Trie, []);
do_match_iter([Word | Rest], Trie, Globber) ->
    SubTrie =
        case maps:get(Word, Trie, undefined) of
            undefined ->
                maps:new();
            Sub ->
                Sub
        end,
    SubTrie1 =
        case maps:get(Globber#globber.wildcard_one, Trie, undefined) of
            undefined ->
                maps:new();
            Sub1 ->
                Sub1
        end,
    SubTrie2 =
        case maps:get(Globber#globber.wildcard_some, Trie, undefined) of
            undefined ->
                maps:new();
            Sub2 ->
                Sub2
        end,
    lists:append([do_match_iter(Rest, SubTrie, Globber),
                  do_match_iter(Rest, SubTrie1, Globber),
                  do_match_iter([], SubTrie2, Globber)]).
