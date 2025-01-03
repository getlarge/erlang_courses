-module(what_the_if).

-export([heh_fine/0, oh_god/1, help_me/1]).

heh_fine() ->
  if 1 =:= 1, true ->
       works
  end,
  if 2 =:= 3, false ->
       fails
  end.

oh_god(N) ->
  if N =:= 2 ->
       might_succeed;
     N =:= 4 ->
       definitely_fails;
     true ->
       always_works
  end.

help_me(Animal) ->
  Talk =
    if Animal == cat ->
         "meow";
       Animal == beef ->
         "mooo";
       Animal == dog ->
         "bark";
       Animal == tree ->
         "bark";
       true ->
         "fgdadfgna"
    end,
  {Animal, "says " ++ Talk ++ "!"}.
