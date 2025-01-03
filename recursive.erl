-module(recursive).

-export([tail_len/1, tail_duplicate/2, tail_reverse/1, tail_sublist/2, tail_zip/2, tail_lenient_zip/2]).

tail_len(L) ->
  tail_len(L, 0).

tail_len([], Acc) ->
  Acc;
tail_len([_ | T], Acc) ->
  tail_len(T, Acc + 1).

tail_duplicate(N, Term) ->
  tail_duplicate(N, Term, []).

tail_duplicate(0, _, List) ->
  List;
tail_duplicate(N, Term, List) when N > 0 ->
  tail_duplicate(N - 1, Term, [Term | List]).

tail_reverse(L) ->
  tail_reverse(L, []).

tail_reverse([], Acc) ->
  Acc;
tail_reverse([H | T], Acc) ->
  tail_reverse(T, [H | Acc]).

tail_sublist(L, N) ->
  lists:reverse(tail_sublist(L, N, [])).

tail_sublist([], _, List) ->
  List;
tail_sublist(_, 0, Sublist) ->
  Sublist;
tail_sublist([H | T], N, Sublist) when N > 0 ->
  tail_sublist(T, N - 1, [H | Sublist]).

%% tail recursive version of zip/2
tail_zip(X, Y) ->
  lists:reverse(tail_zip(X, Y, [])).

tail_zip([], [], Acc) ->
  Acc;
tail_zip([X | Xs], [Y | Ys], Acc) ->
  tail_zip(Xs, Ys, [{X, Y} | Acc]).

%% tail recursive version of lenient-zip/2
tail_lenient_zip(X, Y) ->
  lists:reverse(tail_lenient_zip(X, Y, [])).

tail_lenient_zip([], _, Acc) ->
  Acc;
tail_lenient_zip(_, [], Acc) ->
  Acc;
tail_lenient_zip([X | Xs], [Y | Ys], Acc) ->
  tail_lenient_zip(Xs, Ys, [{X, Y} | Acc]).
