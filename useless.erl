-module(useless).

-export([add/2, hello/0, greet_and_add_two/1]).

add(X, Y) ->
    X + Y.

hello() ->
    io:format("Hello World~n").

greet_and_add_two(X) ->
    hello(),
    add(X, 2).
