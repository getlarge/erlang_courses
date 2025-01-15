-module(rabbit_mqtt_topic_storage).

%% Storage behavior for topic trie
-callback init() -> {ok, State :: term()} | {error, Reason :: term()}.
-callback insert(Topic :: binary(), Value :: term(), State :: term()) ->
                  {ok, NewState :: term()} | {error, Reason :: term()}.
-callback delete(Topic :: binary(), Value :: term(), State :: term()) ->
                  {ok, NewState :: term()} | {error, Reason :: term()}.
-callback lookup(Pattern :: binary(), State :: term()) ->
                  {ok, [Value :: term()]} | {error, Reason :: term()}.
-callback terminate(State :: term()) -> ok.
-callback clear(State :: term()) -> {ok, NewState :: term()} | {error, Reason :: term()}.
