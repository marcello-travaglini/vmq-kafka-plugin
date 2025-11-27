%%--------------------------------------------------------------------
%% vmq_kafka_plugin.erl
%%
%% This module implements the VerneMQ on_publish_hook behaviour.
%% It is invoked by VerneMQ whenever a client publishes a message.
%% The hook receives details about the publishing client, the topic,
%% the payload, QoS, and retain flag.
%%
%% In this implementation, the hook does not block or modify the
%% publish process. Instead, it forwards the message asynchronously
%% to a dispatcher process (vmq_kafka_plugin_dispatcher) which will
%% handle the actual processing (e.g., forwarding to Kafka).
%%
%% The hook always returns 'ok' so that VerneMQ continues processing
%% the publish as normal.
%%--------------------------------------------------------------------

-module(vmq_kafka_plugin).

%% Include VerneMQ development header for hook behaviour definitions
-include_lib("vernemq_dev/include/vernemq_dev.hrl").

%% Declare that this module implements the on_publish_hook behaviour
-behaviour(on_publish_hook).
-behaviour(on_publish_m5_hook).

%% Export the on_publish/6 callback so VerneMQ can call it
-export([on_publish/6,
        on_publish_m5/7
    ]).

%%--------------------------------------------------------------------
%% on_publish/6
%%
%% @param _UserName   - Username of the publishing client (ignored here)
%% @param {MountPoint, ClientId} - Tuple containing:
%%        MountPoint: binary identifying the mountpoint (namespace) of the client
%%        ClientId:   binary identifying the MQTT client ID
%% @param _QoS        - Quality of Service level of the publish (0, 1, or 2)
%%                      Ignored here, but passed along to the dispatcher.
%% @param Topic       - MQTT topic (binary or list of segments)
%% @param Payload     - Message payload (binary or iolist)
%% @param _IsRetain   - Boolean flag indicating if this is a retained message
%%
%% Behaviour:
%%   - This hook is called synchronously by VerneMQ when a message is published with MQTT 2/3 protocol.
%%   - We immediately enqueue the message into our dispatcher process for
%%     asynchronous handling (e.g., mapping to Kafka topics and producing).
%%   - We do not perform any blocking operations here to avoid slowing down
%%     the broker's publish path.
%%   - Always return 'ok' to let VerneMQ continue delivering the message
%%     to subscribers and other hooks.
%%--------------------------------------------------------------------
on_publish(_UserName, {MountPoint, ClientId}, _QoS, Topic, Payload, _IsRetain) ->
    %% Forward the publish event to the dispatcher.
    %% The dispatcher will decide how to handle the message (e.g., match
    %% against configured mappings, send to Kafka via worker processes).
    %% We pass MountPoint, ClientId, Topic, Payload, QoS and _IsRetain (even if unused here).
    vmq_kafka_plugin_dispatcher:enqueue(MountPoint, ClientId, Topic, Payload, _QoS, _IsRetain),

    %% Always return 'ok' so VerneMQ continues processing the publish normally.
    ok.

%%--------------------------------------------------------------------
%% on_publish/6
%%
%% @param _UserName   - Username of the publishing client (ignored here)
%% @param {MountPoint, ClientId} - Tuple containing:
%%        MountPoint: binary identifying the mountpoint (namespace) of the client
%%        ClientId:   binary identifying the MQTT client ID
%% @param _QoS        - Quality of Service level of the publish (0, 1, or 2)
%%                      Ignored here, but passed along to the dispatcher.
%% @param Topic       - MQTT topic (binary or list of segments)
%% @param Payload     - Message payload (binary or iolist)
%% @param _IsRetain   - Boolean flag indicating if this is a retained message
%% @param _Properties - Contains specific propertie for MQTT 5 protocol
%%
%% Behaviour:
%%   - This hook is called synchronously by VerneMQ when a message is published with MQTT 5 protocol.
%%   - We immediately enqueue the message into our dispatcher process for
%%     asynchronous handling (e.g., mapping to Kafka topics and producing).
%%   - We do not perform any blocking operations here to avoid slowing down
%%     the broker's publish path.
%%   - Always return 'ok' to let VerneMQ continue delivering the message
%%     to subscribers and other hooks.
%%--------------------------------------------------------------------
on_publish_m5(_UserName, {MountPoint, ClientId}, _QoS, Topic, Payload, _IsRetain, _Properties) ->
    %% Forward the publish event to the dispatcher.
    %% The dispatcher will decide how to handle the message (e.g., match
    %% against configured mappings, send to Kafka via worker processes).
    %% We pass MountPoint, ClientId, Topic, Payload, QoS and _IsRetain (even if unused here).
    vmq_kafka_plugin_dispatcher:enqueue(MountPoint, ClientId, Topic, Payload, _QoS, _IsRetain),

    %% Always return 'ok' so VerneMQ continues processing the publish normally.
    ok.
