-module(qlobber).

-export([new/0, new/3, add/3, remove/3, match/2, match_iter/2, test/3, clear/1]).

-record(qlobber,
        {separator = <<".">>,
         wildcard_one = <<"*">>,
         wildcard_some = <<"#">>,
         max_words = 100,
         max_wildcard_somes = 3,
         trie = maps:new()}).

new() ->
    #qlobber{}.

new(Separator, WildcardOne, WildcardSome) ->
    #qlobber{separator = Separator,
             wildcard_one = WildcardOne,
             wildcard_some = WildcardSome,
             max_words = 100,
             max_wildcard_somes = 3,
             trie = maps:new()}.

add(Qlobber, Topic, Val) ->
    Words = split(Topic, Qlobber#qlobber.separator),
    Trie = do_add(Words, Val, Qlobber#qlobber.trie, Qlobber),
    Qlobber#qlobber{trie = Trie}.

remove(Qlobber, Topic, Val) ->
    Words = split(Topic, Qlobber#qlobber.separator),
    Trie = do_remove(Words, Val, Qlobber#qlobber.trie, Qlobber),
    Qlobber#qlobber{trie = Trie}.

%? catch ex and return undefined when not found
match(Qlobber, Topic) ->
    Words = split(Topic, Qlobber#qlobber.separator),
    do_match(Words, Qlobber#qlobber.trie, [], Qlobber).

match_iter(Qlobber, Topic) ->
    Words = split(Topic, Qlobber#qlobber.separator),
    do_match_iter(Words, Qlobber#qlobber.trie, Qlobber).

test(Qlobber, Topic, Val) ->
    Words = split(Topic, Qlobber#qlobber.separator),
    % TODO: add Val to the trie
    do_test(Words, Val, Qlobber#qlobber.trie, Qlobber).

clear(Qlobber) ->
    Qlobber#qlobber{trie = maps:new()}.

split(Topic, Separator) ->
    binary:split(Topic, Separator, [global]).

do_add([], Val, Trie, _Qlobber) ->
    maps:put(<<".">>, [Val | maps:get(<<".">>, Trie, [])], Trie);
do_add([Word | Rest], Val, Trie, Qlobber) ->
    SubTrie = maps:get(Word, Trie, maps:new()),
    NewSubTrie = do_add(Rest, Val, SubTrie, Qlobber),
    maps:put(Word, NewSubTrie, Trie).

do_remove([], Val, Trie, _Qlobber) ->
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
do_remove([Word | Rest], Val, Trie, Qlobber) ->
    case maps:get(Word, Trie) of
        SubTrie when is_map(SubTrie) ->
            NewSubTrie = do_remove(Rest, Val, SubTrie, Qlobber),
            case maps:size(NewSubTrie) of
                0 ->
                    maps:remove(Word, Trie);
                _ ->
                    maps:put(Word, NewSubTrie, Trie)
            end;
        _ ->
            Trie
    end.

do_match([], Trie, Acc, _Qlobber) ->
    case maps:get(<<".">>, Trie, undefined) of
        undefined ->
            Acc;
        Vals ->
            lists:append(Vals, Acc)
    end;
do_match([Word | Rest], Trie, Acc, Qlobber) ->
    SubTrie =
        case maps:get(Word, Trie, undefined) of
            undefined ->
                maps:new();
            Sub ->
                Sub
        end,
    SubTrie1 =
        case maps:get(Qlobber#qlobber.wildcard_one, Trie, undefined) of
            undefined ->
                maps:new();
            Sub1 ->
                Sub1
        end,
    SubTrie2 =
        case maps:get(Qlobber#qlobber.wildcard_some, Trie, undefined) of
            undefined ->
                maps:new();
            Sub2 ->
                Sub2
        end,
    Acc1 = do_match(Rest, SubTrie, Acc, Qlobber),
    Acc2 = do_match(Rest, SubTrie1, Acc1, Qlobber),
    do_match([], SubTrie2, Acc2, Qlobber).

do_match_iter([], Trie, _Qlobber) ->
    maps:get(<<".">>, Trie, []);
do_match_iter([Word | Rest], Trie, Qlobber) ->
    SubTrie =
        case maps:get(Word, Trie, undefined) of
            undefined ->
                maps:new();
            Sub ->
                Sub
        end,
    SubTrie1 =
        case maps:get(Qlobber#qlobber.wildcard_one, Trie, undefined) of
            undefined ->
                maps:new();
            Sub1 ->
                Sub1
        end,
    SubTrie2 =
        case maps:get(Qlobber#qlobber.wildcard_some, Trie, undefined) of
            undefined ->
                maps:new();
            Sub2 ->
                Sub2
        end,
    lists:append([do_match_iter(Rest, SubTrie, Qlobber),
                  do_match_iter(Rest, SubTrie1, Qlobber),
                  do_match_iter([], SubTrie2, Qlobber)]).

do_test([], Val, Trie, _Qlobber) ->
    case maps:get(<<".">>, Trie, undefined) of
        undefined ->
            false;
        Vals ->
            lists:member(Val, Vals)
    end;
do_test([Word | Rest], Val, Trie, Qlobber) ->
    SubTrie =
        case maps:get(Word, Trie, undefined) of
            undefined ->
                maps:new();
            Sub ->
                Sub
        end,
    SubTrie1 =
        case maps:get(Qlobber#qlobber.wildcard_one, Trie, undefined) of
            undefined ->
                maps:new();
            Sub1 ->
                Sub1
        end,
    SubTrie2 =
        case maps:get(Qlobber#qlobber.wildcard_some, Trie, undefined) of
            undefined ->
                maps:new();
            Sub2 ->
                Sub2
        end,
    do_test(Rest, Val, SubTrie, Qlobber)
    orelse do_test(Rest, Val, SubTrie1, Qlobber)
    orelse do_test([], Val, SubTrie2, Qlobber).
