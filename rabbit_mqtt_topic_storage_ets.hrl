
%% Records for trie structure
-record(trie_node, {
    node_id,           % Unique node identifier
    edge_count = 0,    % Number of edges from this node
    is_topic = false   % Whether this node represents end of a topic
}).

-record(trie_edge, {
    node_id,           % From node
    word,              % Edge label (topic word)
    child_id           % To node
}).

-record(trie_topic, {
    node_id,           % Node where topic ends
    topic,             % Full topic string
    value             % Associated value (e.g. retained message)
}).

-record(state, {
    node_table,        % ETS table for nodes
    edge_table,        % ETS table for edges
    topic_table,       % ETS table for topics and their values
    root_id            % ID of root node
}).
