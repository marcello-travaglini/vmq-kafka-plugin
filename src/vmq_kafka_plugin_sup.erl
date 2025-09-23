%%%-------------------------------------------------------------------
%% @doc Top-level supervisor for the vmq_kafka_plugin.
%%
%% This supervisor starts and monitors N independent Kafka worker
%% processes (vmq_kafka_plugin_kafka_worker), each with its own
%% client(s) and producer(s).
%%
%% The number of workers, broker list, client options, and MQTT→Kafka
%% topic mappings are read from the application environment (which can
%% be set in advanced.config).
%%
%% Each worker is given a unique name so they can be addressed
%% individually if needed.
%%%-------------------------------------------------------------------

-module(vmq_kafka_plugin_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-define(SERVER, ?MODULE).

%%--------------------------------------------------------------------
%% @doc Starts the supervisor process and registers it locally.
%%--------------------------------------------------------------------
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%--------------------------------------------------------------------
%% @doc Supervisor initialization callback.
%%
%% Reads configuration from the application environment, normalizes
%% mappings, stores some values in persistent_term for fast access,
%% and builds the child specifications for the Kafka workers.
%%
%% Returns {ok, {SupFlags, ChildSpecs}} to start supervision.
%%--------------------------------------------------------------------
init([]) ->
    %% Read configuration values from application env
    Brokers     = application:get_env(vmq_kafka_plugin, brokers, []),
    WorkerCount = application:get_env(vmq_kafka_plugin, worker_count, 1),
    ClientOpts  = application:get_env(vmq_kafka_plugin, client_opts, []),
    RawMappings = application:get_env(vmq_kafka_plugin, mappings, []),

    %% Normalize mappings to tuples {PatternBinary, TopicBinary}
    TupleMappings =
        [{ensure_binary(maps:get(pattern, M)), ensure_binary(maps:get(topic, M))}
         || M <- RawMappings],

    %% Log the loaded configuration for visibility
    error_logger:info_msg(
        "vmq_kafka_plugin config ~n  Brokers=~p~n  WorkerCount=~p~n  ClientOpts=~p~n  Mappings=~p",
        [Brokers, WorkerCount, ClientOpts, TupleMappings]
    ),

    %% Store some values in persistent_term for quick global access
    %% (used by the dispatcher or other components)
    persistent_term:put({vmq_kafka, worker_count}, WorkerCount),
    persistent_term:put({vmq_kafka, worker_name_base}, <<"vmq_kafka_worker_">>),

    %% Base options map passed to each worker
    OptsBase = #{
        brokers     => Brokers,
        client_opts => ClientOpts,
        mappings    => TupleMappings
    },

    %% Supervisor restart strategy and limits
    SupFlags = #{
        strategy => one_for_one, %% restart only the crashed child
        intensity => 5,          %% max 5 restarts...
        period => 10             %% ...within 10 seconds before giving up
    },

    %% Build N child specs, one per Kafka worker
    ChildSpecs = [
        begin
            %% Unique atom name for each worker process
            Name = list_to_atom("vmq_kafka_worker_" ++ integer_to_list(I)),
            #{
              id => Name,
              start => {vmq_kafka_plugin_kafka_worker, start_link, [Name, OptsBase]},
              restart => permanent,  %% always restart if it terminates
              shutdown => 5000,      %% milliseconds to wait on shutdown
              type => worker,        %% this is a worker process
              modules => [vmq_kafka_plugin_kafka_worker]
            }
        end
        || I <- lists:seq(1, WorkerCount)
    ],

    {ok, {SupFlags, ChildSpecs}}.

%%--------------------------------------------------------------------
%% @doc Helper to ensure a value is a binary.
%% Accepts binaries or iolists and converts to binary if needed.
%%--------------------------------------------------------------------
ensure_binary(B) when is_binary(B) -> B;
ensure_binary(IoD) -> iolist_to_binary(IoD).
