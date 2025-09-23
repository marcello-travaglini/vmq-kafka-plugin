%%%-------------------------------------------------------------------
%% @doc Dispatcher: routes incoming MQTT publish events to one of the
%% Kafka worker processes based on a consistent key.
%%
%% The dispatcher ensures that all messages sharing the same key
%% (e.g., ClientId) are always sent to the same worker. This preserves
%% ordering for that key while allowing parallelism across different keys.
%%
%% It uses values stored in persistent_term by the supervisor to know:
%%   - How many workers exist
%%   - The base name for worker processes
%%
%% The actual delivery to the worker is done via a non-blocking
%% gen_server:cast/2 so the dispatcher never waits for a reply.
%%%-------------------------------------------------------------------

-module(vmq_kafka_plugin_dispatcher).

%% Public API
-export([enqueue/6]).

%%--------------------------------------------------------------------
%% @doc Enqueue a message for processing by one of the Kafka workers.
%%
%% @param MountPoint - MQTT mountpoint (namespace) for the client
%% @param ClientId   - MQTT client identifier (binary)
%% @param Topic      - MQTT topic (binary or list of segments)
%% @param Payload    - Message payload (binary or iolist)
%% @param QoS        - Quality of Service level (0, 1, or 2)
%% @param IsRetain   - Boolean flag indicating if this is a retained message
%%
%% Behaviour:
%%   1. Reads the number of workers (N) and the base worker name from
%%      persistent_term (set by vmq_kafka_plugin_sup).
%%   2. Chooses a routing key. Here we use ClientId so that all messages
%%      from the same client go to the same worker, preserving order.
%%      (You could change this to {MountPoint, Topic} or another key.)
%%   3. Uses erlang:phash2/2 to map the key to a worker index in 0..N-1.
%%   4. Adds 1 to the index so worker names start from 1.
%%   5. Builds the worker's registered name by concatenating the base
%%      name with the index number and converting to an atom.
%%   6. Sends the message to that worker via gen_server:cast/2, which
%%      is asynchronous and non-blocking.
%%--------------------------------------------------------------------
enqueue(MountPoint, ClientId, Topic, Payload, QoS, IsRetain) ->
    %% Number of Kafka workers (default 1 if not set)
    N = persistent_term:get({vmq_kafka, worker_count}, 1),

    %% Base name for worker processes (binary, e.g. <<"vmq_kafka_worker_">>)
    Base = persistent_term:get({vmq_kafka, worker_name_base}, <<"vmq_kafka_worker_">>),

    %% Routing key for ordering: here we use ClientId
    %% (could also use {MountPoint, Topic} for different semantics)
    Key = ClientId,

    %% Map the key to a worker index in 0..N-1
    Idx = erlang:phash2(Key, N) + 1,

    %% Build the worker's registered name as an atom
    WorkerName = list_to_atom(binary_to_list(Base) ++ integer_to_list(Idx)),

    %% Send the message to the chosen worker asynchronously
    gen_server:cast(WorkerName, {msg, MountPoint, ClientId, Topic, Payload, QoS, IsRetain}).
