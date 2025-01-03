-module(functions).
-compile(export_all). % Replace with -export() later, for sanity's sake!

head([H|_]) -> H.
second([_,S|_]) -> S.
tail([_|T]) -> T.

valid_time({Date = {Y,M,D}, Time = {H,Min,S}}) ->
io:format("The Date tuple (~p) says today is: ~p/~p/~p,~n",[Date,Y,M,D]),
io:format("The time tuple (~p) indicates: ~p:~p:~p.~n", [Time,H,Min,S]);
valid_time(_) ->
io:format("Stop feeding me wrong data!~n").
