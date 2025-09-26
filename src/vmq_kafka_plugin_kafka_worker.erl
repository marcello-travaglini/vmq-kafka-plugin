%%%--------------------------------------------------------------------
%%% Kafka Worker with dual clients (FAST/SAFE), header profiles, client reuse,
%%% and clean shutdown. Reads all options from application env (advanced.config).
%%%
%%% Config shape (example):
%%% [
%%%  {vmq_kafka_plugin, [
%%%     {brokers, [{"localhost", 9092}]},
%%%     {client_opts, [{client_id_base, "vmq_kafka"}, {request_timeout_ms, 15000}]},
%%%     {fast_opts, [...]}, %% QoS 0 (acks=0)
%%%     {safe_opts, [...]}, %% QoS 1/2 (acks=-1, idempotence=true)
%%%     {headers_profiles, #{
%%%        gateway_std => #{ <<"k">> => <<"v">> }, sensor_raw => #{...}
%%%     }},
%%%     {mappings, [
%%%        #{ pattern => "sensors/#", topic => "sensor-data", headers_profile => gateway_std },
%%%        #{ pattern => "alerts/+",  topic => "alerts",      headers_profile => sensor_raw }
%%%     ]}
%%%  ]}
%%% ].
%%%--------------------------------------------------------------------
-module(vmq_kafka_plugin_kafka_worker).
-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-export([start_link/2]).
-export([init/1, handle_cast/2, handle_call/3, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
    name        :: atom(),
    client_fast :: atom(), %% FAST client (acks=0)
    client_safe :: atom(), %% SAFE client (acks=all, idempotent)
    mappings    :: [{re:re_pattern(), binary(), map()}],
    topics      :: [binary()],
    part_cache  :: #{binary() => pos_integer()}  %% topic => partitions_count
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(atom(), map()) -> {ok, pid()} | {error, any()}.
start_link(Name, Opts) ->
    gen_server:start_link({local, Name}, ?MODULE, Opts#{name => Name}, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(Opts) ->
    %% Name can still come from supervisor; default to module name for uniqueness context
    Name = maps:get(name, Opts, ?MODULE),

    %% Read all config from application env (advanced.config)
    Brokers         = application:get_env(vmq_kafka_plugin, brokers, []),
    ClientOpts      = application:get_env(vmq_kafka_plugin, client_opts, []),
    FastOpts        = fast_opts(),
    SafeOpts        = safe_opts(),
    HeadersProfiles = application:get_env(vmq_kafka_plugin, headers_profiles, #{}),
    TupleMaps       = application:get_env(vmq_kafka_plugin, mappings, []),

    %% Compile MQTT filters -> regex and resolve headers profile
    Mappings = compile_mappings_with_headers(TupleMaps, HeadersProfiles),

    %% Build unique FAST/SAFE client names for this worker
    ClientFast = make_client_name(Name, fast),
    ClientSafe = make_client_name(Name, safe),

    %% Start or reuse both clients and producers; return cleanly on error
    Topics = unique_kafka_topics(Mappings),
    case do_bootstrap(Brokers, ClientOpts, ClientFast, ClientSafe, Topics, FastOpts, SafeOpts) of
        {ok, _} ->
            ok;
        {error, Reason} ->
            throw({stop, Reason})
    end,

    %% Preload and cache partition counts for stable partitioning
    PartCache = ensure_partitions_cache(ClientSafe, Topics), %% either client works

    {ok, #state{
        name        = Name,
        client_fast = ClientFast,
        client_safe = ClientSafe,
        mappings    = Mappings,
        topics      = Topics,
        part_cache  = PartCache
    }}.

%% Handle incoming publish messages from dispatcher
%% Message shape must be: {msg, Mountpoint, ClientId, Topic, Payload, QoS, IsRetain}
handle_cast({msg, Mountpoint, ClientId, Topic, Payload, QoS, IsRetain}, State) ->
    MqttTopic = ensure_topic_binary(Topic),
    Payload   = ensure_binary(Payload),

    lists:foreach(
      fun({MP, KTopic, ProfileHeaders}) ->
          case re:run(MqttTopic, MP, [{capture, none}]) of
              match ->
                  Part = pick_partition(State, KTopic, ClientId),
                  %% Convert boolean to binary
                  RetainBin = if IsRetain -> <<"true">>; true -> <<"false">> end,
                  %% Merge static headers from profile + dynamic MQTT headers
                  StaticHeaders = maps:to_list(ProfileHeaders),
                  MqttHeaders = [
                      {<<"mqtt_mountpoint">>, ensure_binary(Mountpoint)},
                      {<<"mqtt_client_id">>, ensure_binary(ClientId)},
                      {<<"mqtt_topic">>,     ensure_binary(MqttTopic)},
                      {<<"mqtt_qos">>,       integer_to_binary(QoS)},
                      {<<"mqtt_isretain">>,  RetainBin}
                  ],
                  AllHeaders = StaticHeaders ++ MqttHeaders,
                  %% Build a single message map
                  MsgValue = [
                      #{
                      key     => ensure_binary(ClientId),  %% Kafka message key
                      value   => Payload,                  %% Kafka message value
                      headers => AllHeaders                %% list of {KeyBin, ValBin}
                      }
                  ],
                  case QoS of
                      0 ->
                          _ = brod:produce_no_ack(
                                State#state.client_fast, KTopic, Part, ClientId, MsgValue),
                          ok;
                      1 ->
                          case brod:produce(
                                  State#state.client_safe, KTopic, Part, ClientId, MsgValue) of
                              {ok, _CallRef} -> ok;
                              {error, Reason1} ->
                                  logger:warning("QoS1 send error: ~p", [Reason1])
                          end;
                      2 ->
                          %% Same as QoS1 unless transactional logic is added
                          case brod:produce(
                                  State#state.client_safe, KTopic, Part, ClientId, MsgValue) of
                              {ok, _CallRef} -> ok;
                              {error, Reason2} ->
                                  logger:warning("QoS2 send error: ~p", [Reason2])
                          end;
                      _ ->
                          logger:warning("Unknown QoS ~p, defaulting to SAFE", [QoS]),
                          case brod:produce(
                                  State#state.client_safe, KTopic, Part, ClientId, MsgValue) of
                              {ok, _CallRef} -> ok;
                              {error, R3} ->
                                  logger:warning("Send error with unknown QoS: ~p", [R3])
                          end
                  end;
              _ -> ok
          end
      end,
      State#state.mappings
    ),
    {noreply, State};

handle_cast(_Other, State) ->
    {noreply, State}.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    %% Clean shutdown of both clients
    catch brod:stop_client(State#state.client_fast),
    catch brod:stop_client(State#state.client_safe),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Bootstrap helpers
%%%===================================================================

%% Orchestrates client start/reuse and producer start.
%% Returns {ok, started} or {error, Reason} to be handled by init/1.
do_bootstrap(Brokers, ClientOpts, ClientFast, ClientSafe, Topics, FastOpts, SafeOpts) ->
    case start_or_reuse_client(Brokers, ClientFast, ClientOpts) of
        ok ->
            ok;
        {error, ReasonF} ->
            logger:error("Unable to start FAST client ~p: ~p", [ClientFast, ReasonF]),
            return_error({cannot_start_client_fast, ReasonF})
    end,
    case start_or_reuse_client(Brokers, ClientSafe, ClientOpts) of
        ok ->
            ok;
        {error, ReasonS} ->
            logger:error("Unable to start SAFE client ~p: ~p", [ClientSafe, ReasonS]),
            return_error({cannot_start_client_safe, ReasonS})
    end,
    case ensure_dual_producers(ClientFast, ClientSafe, Topics, FastOpts, SafeOpts) of
        ok ->
            logger:info("FAST/SAFE producers started (topics=~p)", [Topics]),
            {ok, started};
        {error, ReasonP} ->
            return_error({cannot_start_producers, ReasonP})
    end.

%% Small helper to make control-flow explicit
return_error(Reason) ->
    {error, Reason}.

%%%===================================================================
%%% Client/producer helpers
%%%===================================================================

%% Build FAST/SAFE client names from worker name
make_client_name(WorkerName, fast) ->
    list_to_atom(atom_to_list(WorkerName) ++ "_client_fast");
make_client_name(WorkerName, safe) ->
    list_to_atom(atom_to_list(WorkerName) ++ "_client_safe").

%% Start a brod client or reuse if already started
start_or_reuse_client(Brokers, ClientName, ClientOpts) ->
    case brod:start_client(Brokers, ClientName, ClientOpts) of
        ok ->
            logger:info("Client ~p started", [ClientName]),
            ok;
        {error, {already_started, _Pid}} ->
            logger:info("Client ~p already started, reusing", [ClientName]),
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

%% Start FAST and SAFE producers for each topic on their respective clients
ensure_dual_producers(ClientFast, ClientSafe, Topics, FastOpts, SafeOpts) ->
    case ensure_topic_producers(ClientFast, Topics, FastOpts) of
        ok -> ok;
        ErrorF -> ErrorF
    end,
    ensure_topic_producers(ClientSafe, Topics, SafeOpts).

%%--------------------------------------------------------------------
%% start_producer(ClientRef, Topic, ProdOpts, Retries, Delay)
%%
%% Tries to start a Kafka producer using brod. If the topic does not
%% yet exist (auto-create in Kafka may take some time to propagate),
%% it retries with exponential backoff until either success or the
%% maximum number of retries is reached.
%%
%% Parameters:
%%   ClientRef - brod client reference (atom or pid)
%%   Topic     - Kafka topic name (binary or string)
%%   ProdOpts  - Producer options for brod:start_producer/3
%%   Retries   - Maximum number of retry attempts
%%   Delay     - Initial delay in milliseconds before retry
%%
%% Returns:
%%   {ok, Pid} on success
%%   {error, max_retries_exceeded} if retries are exhausted
%%   {error, Reason} for other errors
%%--------------------------------------------------------------------
start_producer(_ClientRef, _Topic, _ProdOpts, 0, _Delay) ->
    io:format("Max retries exceeded, giving up.~n", []),
    {error, max_retries_exceeded};

start_producer(ClientRef, Topic, ProdOpts, Retries, Delay) ->
    case brod:start_producer(ClientRef, Topic, ProdOpts) of
        ok ->
            io:format("Producer successfully started on topic ~p~n", [Topic]),
            ok;

        {error, unknown_topic_or_partition} ->
            io:format("Topic ~p not yet available, retrying in ~p ms (remaining retries: ~p)~n",
                      [Topic, Delay, Retries]),
            timer:sleep(Delay),
            %% Exponential backoff: double the delay each retry
            start_producer(ClientRef, Topic, ProdOpts, Retries - 1, Delay * 2);

        {error, Reason} ->
            io:format("Unhandled error while starting producer: ~p~n", [Reason]),
            {error, Reason}
    end.

%% Start producers for a given client and topic list
ensure_topic_producers(ClientRef, Topics, ProdOpts) ->
    lists:foldl(
      fun(Topic, Acc) ->
          case Acc of
              ok ->
                  case start_producer(ClientRef, Topic, ProdOpts, 3, 5000) of
                      ok -> ok;
                      {error, {already_started, _}} -> ok;
                      {error, Reason} -> {error, {Topic, Reason}}
                  end;
              Error -> Error
          end
      end,
      ok,
      Topics
    ).

%% FAST producer options (QoS 0) loaded from application env
fast_opts() ->
    application:get_env(vmq_kafka_plugin, fast_opts, []).

%% SAFE producer options (QoS 1/2) loaded from application env
safe_opts() ->
    application:get_env(vmq_kafka_plugin, safe_opts, []).

%% Load and cache partition counts for each topic
ensure_partitions_cache(ClientRef, Topics) ->
    lists:foldl(
      fun(Topic, Acc) ->
          case brod:get_partitions_count(ClientRef, Topic) of
              {ok, Count} when is_integer(Count), Count > 0 ->
                  Acc#{ Topic => Count };
              {error, Reason} ->
                  logger:warning("Partition count error for ~p: ~p, defaulting to 1", [Topic, Reason]),
                  Acc#{ Topic => 1 }
          end
      end,
      #{},
      Topics
    ).

%% Choose partition: stable hashing on key and known partition count
pick_partition(State, Topic, Key) ->
    PartCount = maps:get(Topic, State#state.part_cache, 1),
    %% Kafka partitions are 0..PartCount-1
    erlang:phash2(Key, PartCount).

%%%===================================================================
%%% Mapping, headers, and string/binary utilities
%%%===================================================================

%% Compile MQTT topic filters to anchored regex patterns with resolved headers
%% Input mapping example:
%%   #{ pattern => "sensors/#", topic => "sensor-data", headers_profile => gateway_std }
compile_mappings_with_headers(TupleMaps, HeadersProfiles) ->
    lists:flatmap(
      fun(Map) ->
          PatBin = maps:get(pattern, Map),
          Topic  = ensure_binary(maps:get(topic, Map)),
          %% headers_profile is optional
          Headers = case maps:get(headers_profile, Map, undefined) of
              undefined -> #{};
              ProfileKey when is_atom(ProfileKey) ->
                  maps:get(ProfileKey, HeadersProfiles, #{});
              _Other -> #{}
          end,
          case mqtt_to_re(binary_to_list(ensure_binary(PatBin))) of
              {ok, ReStr} ->
                  case re:compile(ReStr, [unicode]) of
                      {ok, MP} ->
                          [{MP, Topic, ensure_headers_map(Headers)}];
                      {error, ReasonC} ->
                          logger:warning("Regex compile failed for pattern ~p: ~p", [PatBin, ReasonC]),
                          []
                  end;
              {error, ReasonP} ->
                  logger:warning("Invalid MQTT pattern ~p: ~p", [PatBin, ReasonP]),
                  []
          end
      end,
      TupleMaps
    ).

%% Convert a valid MQTT pattern to an anchored regex string
mqtt_to_re(PatStr) ->
    case valid_mqtt_pattern(PatStr) of
        false -> {error, invalid_mqtt_wildcard};
        true  ->
            %% Escape all regex metacharacters except '+' and '#'
            Escaped = re:replace(
                PatStr,
                "([\\.\\^\\$\\*\\?\\(\\)\

\[\\]

\\{\\}\\|\\\\]

)",
                "\\\\1",
                [global, {return, list}]
            ),
            %% '+' -> one level (no '/')
            Step1 = re:replace(Escaped, "\\+", "[^/]+", [global, {return, list}]),
            %% '#' -> match the rest (any)
            Step2 = re:replace(Step1, "#", ".*", [global, {return, list}]),
            {ok, "^" ++ Step2 ++ "$"}
    end.

%% Validate MQTT pattern:
%% - '#' allowed only alone "#" or as trailing "/#"
%% - no multiple '#'
valid_mqtt_pattern(Pattern) when is_list(Pattern) ->
    case string:chr(Pattern, $#) of
        0 -> true; %% no '#'
        Pos ->
            Rest = string:substr(Pattern, Pos+1, length(Pattern) - Pos),
            case string:chr(Rest, $#) of
                0 -> validate_single_hash(Pattern, Pos);
                _ -> false %% more than one '#'
            end
    end.

validate_single_hash(Pattern, 1) ->
    %% '#' only
    Pattern =:= "#";
validate_single_hash(Pattern, Pos) ->
    %% '/#' at the very end
    Len = length(Pattern),
    Pos =:= Len andalso Len >= 2 andalso string:substr(Pattern, Len-1, 2) =:= "/#".

%% Ensure MQTT topic is a binary (handles list bin or string)
ensure_topic_binary(B) when is_binary(B) -> B;
ensure_topic_binary(S) when is_list(S), S =/= [], is_integer(hd(S)) -> list_to_binary(S);
ensure_topic_binary(Segs) when is_list(Segs) -> join_segments(Segs, <<"/">>);
ensure_topic_binary(_) -> <<>>.

%% Join a list of binary segments with a separator into a binary
join_segments([], _Sep) -> <<>>;
join_segments([Bin], _Sep) when is_binary(Bin) -> Bin;
join_segments([First|Rest], Sep) ->
    iolist_to_binary([First | lists:flatmap(fun(X) -> [Sep, X] end, Rest)]).

%% Ensure a value is a binary (iolist accepted)
ensure_binary(Bin) when is_binary(Bin) -> Bin;
ensure_binary(IoD) -> iolist_to_binary(IoD).

%% Ensure headers map has binary keys/values
ensure_headers_map(Map) when is_map(Map) ->
    maps:from_list([
        {ensure_binary(K), ensure_binary(V)}
     || {K, V} <- maps:to_list(Map)
    ]);
ensure_headers_map(_) -> #{}.

%% Extract the unique Kafka topics from the compiled mappings
unique_kafka_topics(Mappings) ->
    lists:usort([ensure_binary(KTopic) || {_MP, KTopic, _Hdrs} <- Mappings]).

