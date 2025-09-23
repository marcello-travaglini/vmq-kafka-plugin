%%%-------------------------------------------------------------------
%% @doc vmq_kafka_plugin public API / Application entry point
%%
%% This module implements the `application` behaviour, which defines
%% how the Erlang/OTP runtime starts and stops this plugin as an
%% application.
%%
%% When VerneMQ loads the plugin, it starts this application, which in
%% turn starts the top-level supervisor (`vmq_kafka_plugin_sup`) that
%% manages all Kafka worker processes.
%%
%% The application also ensures that the `brod` Kafka client library
%% is started before any workers are launched.
%%%-------------------------------------------------------------------

-module(vmq_kafka_plugin_app).
-behaviour(application).

%% Exported callbacks required by the `application` behaviour
-export([start/2, stop/1]).

%%--------------------------------------------------------------------
%% @doc Called by the Erlang/OTP runtime to start the application.
%%
%% @param _StartType - Type of start (normal, takeover, etc.), unused here.
%% @param _StartArgs - Arguments passed to the application, unused here.
%%
%% Behaviour:
%%   1. Ensure that the `brod` application (Kafka client) and its
%%      dependencies are started. This is critical because our workers
%%      depend on `brod` to connect to Kafka.
%%   2. If `brod` starts successfully, start the top-level supervisor
%%      (`vmq_kafka_plugin_sup`) which will spawn and monitor the Kafka
%%      worker processes.
%%   3. If starting `brod` fails, return `{error, Reason}` so the
%%      application start is aborted.
%%--------------------------------------------------------------------
start(_StartType, _StartArgs) ->
    case application:ensure_all_started(brod) of
        {ok, _} ->
            %% Start the top-level supervisor for Kafka workers
            vmq_kafka_plugin_sup:start_link();
        {error, Reason} ->
            %% Abort application start if brod could not be started
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc Called by the Erlang/OTP runtime to stop the application.
%%
%% @param _State - The application state (unused here).
%%
%% Behaviour:
%%   - Perform any necessary cleanup before the application stops.
%%   - In this implementation, there is nothing to clean up explicitly,
%%     so we simply return `ok`.
%%--------------------------------------------------------------------
stop(_State) ->
    ok.
