%%==============================================================================
%% Copyright 2013 Jan Henry Nystrom <JanHenryNystrom@gmail.com>
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%==============================================================================

%%%-------------------------------------------------------------------
%%% @doc
%%% The protocol encoding/decoding for Kafka.
%%% @end
%%%
%% @author Jan Henry Nystrom <JanHenryNystrom@gmail.com>
%% @copyright (C) 2013, Jan Henry Nystrom <JanHenryNystrom@gmail.com>
%%%-------------------------------------------------------------------
-module(kafka_protocol).
-copyright('Jan Henry Nystrom <JanHenryNystrom@gmail.com>').

%% Library functions
-export([encode/4,
         decode/2]).

%% Includes
-include_lib("kafka/include/kafka_protocol.hrl").

%% Types
-type request() :: #metadata{} | #produce{} | #offset{}.
-type response() :: #metadata_response{} | #produce_response{} |
                    #offset_response{}.

%% Records

%% Defines

%% API version
-define(API_VERSION, 0:16/signed).

%% Request type keys
-define(ApiKey, 16/signed).
-define(PRODUCE_REQUEST, 0).
-define(FETCH_REQUEST, 1).
-define(OFFSET_REQUEST, 2).
-define(METADATA_REQUEST, 3).
-define(LEADER_AND_IS_RREQUEST, 4).
-define(STOP_REPLICA_REQUEST, 5).
-define(OFFSET_COMMIT_REQUEST, 6).
-define(OFFSET_FETCH_REQUEST, 7).

%% -----------
%% Field sizes.
%% -----------

%% All types
-define(SIZE_S, 32/signed).
-define(CORR_ID_S, 32/signed).
-define(ARRAY_SIZE_S, 32/signed).
-define(STRING_SIZE_S, 16/signed).
-define(ID_S, 32/signed).
-define(ERROR_CODE, 16/signed).

%% produce, offset
-define(OFFSETS, binary-unit:64).

%% produce, fetch
-define(TIMEOUT_S, 32/signed).

%% metadata
-define(LEADER, 32/signed).
-define(IDS, binary-unit:32).
-define(PORT, 32/signed).

%% produce
-define(ACKS_S, 16/signed).
-define(BYTES_SIZE_S, 32/signed).
-define(CRC_S, 32/signed).
-define(MAGIC_S, 8/signed).
-define(ATTRIBUTES_S, 8/signed).
-define(OFFSET_S, 64/signed).

%% fetch
-define(MIN_S, 32/signed).

%% offset
-define(TIME_S, 64/signed).
-define(MAX_S, 32/signed).

%% Error code tables.
-define(ERROR_CODES_TABLE,
        [{0, 'NoError'},
         {-1, 'Unknown'},
         {1, 'OffsetOutOfRange'},
         {2, 'InvalidMessage'},
         {3, 'UnknownTopicOrPartition'},
         {4, 'InvalidMessageSize'},
         {5, 'LeaderNotAvailable'},
         {6, 'NotLeaderForPartition'},
         {7, 'RequestTimedOut'},
         {8, 'BrokerNotAvailable'},
         {9, 'ReplicaNotAvailable'},
         {10, 'MessageSizeTooLarge'},
         {11, 'StaleControllerEpochCode'},
         {12, 'OffsetMetadataTooLargeCode'}]).

%% What decoder/type keys to use.
-define(API_KEY_TABLE, [{metadata, ?METADATA_REQUEST},
                        {produce, ?PRODUCE_REQUEST},
                        {fetch, ?FETCH_REQUEST},
                        {offset, ?OFFSET_REQUEST}
                       ]).

%% ===================================================================
%% Library functions.
%% ===================================================================

%%--------------------------------------------------------------------
%% Function: encode(Type, CorrId, ClientId, Args) -> iolist().
%% @doc
%%   Encodes a kafka request.
%% @end
%%--------------------------------------------------------------------
-spec encode(atom(), integer(), string(), request()) -> iolist().
%%--------------------------------------------------------------------
encode(Type, CorrId, ClientId, Args) ->
    {Type, ApiKey} = lists:keyfind(Type, 1, ?API_KEY_TABLE),
    Request = [<<ApiKey:?ApiKey, ?API_VERSION, CorrId:?CORR_ID_S>>,
               encode_latin1_to_string(ClientId), encode(Args)],
    Size = iolist_size(Request),
    [<<Size:?SIZE_S>>, Request].

%%--------------------------------------------------------------------
%% Function: decode(Type, Response) -> ResponseRecord.
%% @doc
%%   Deccodes a kafka response.
%% @end
%%--------------------------------------------------------------------
-spec decode(atom(), binary()) -> response().
%%--------------------------------------------------------------------
decode(metadata, <<_:?SIZE_S,CorrId:?CORR_ID_S,No:?ARRAY_SIZE_S,T/binary>>) ->
    {Brokers, <<No1:?ARRAY_SIZE_S, T1/binary>>} = decode_brokers(No, T, []),
    #metadata_response{corr_id = CorrId,
                       brokers = Brokers,
                       topics = decode_topics(metadata, No1, T1, [])};
decode(produce, <<_:?SIZE_S,CorrId:?CORR_ID_S,No:?ARRAY_SIZE_S,T/binary>>) ->
    #produce_response{corr_id = CorrId,
                      topics = decode_topics(produce, No, T, [])};
decode(fetch, <<_:?SIZE_S,CorrId:?CORR_ID_S,No:?ARRAY_SIZE_S,T/binary>>) ->
    #fetch_response{corr_id = CorrId,
                    topics = decode_topics(fetch, No, T, [])};
decode(offset, <<_:?SIZE_S, CorrId:?CORR_ID_S,No:?ARRAY_SIZE_S,T/binary>>) ->
    #offset_response{corr_id = CorrId,
                    topics = decode_topics(offset, No, T, [])}.

%% ===================================================================
%% Internal functions.
%% ===================================================================

%% ===================================================================
%% Encoding
%% ===================================================================

encode(#metadata{topics = Topics}) -> encode_string_array(Topics);
encode(#produce{acks = RequiredAcks, timeout = Timeout, topics = Topics}) ->
     [<<RequiredAcks:?ACKS_S, Timeout:?TIMEOUT_S,
        (length(Topics)):?ARRAY_SIZE_S>>,
      [encode_topic(produce, Topic) || Topic <- Topics]];
encode(#fetch{replica = Id,timeout =Timeout,min_bytes = Min,topics = Topics}) ->
    [<<Id:?ID_S, Timeout:?TIMEOUT_S, Min:?MIN_S,
       (length(Topics)):?ARRAY_SIZE_S>>,
     [encode_topic(fetch, Topic) || Topic <- Topics]];
encode(#offset{replica = Id, topics = Topics}) ->
    [<<Id:?ID_S, (length(Topics)):?ARRAY_SIZE_S>>,
     [encode_topic(offset, Topic) || Topic <- Topics]].

encode_topic(Type, #topic{name = Name, partitions = Partitions}) ->
    [encode_latin1_to_string(Name),
     <<(length(Partitions)):?ARRAY_SIZE_S>>,
     [encode_partition(Type, Partition) || Partition <- Partitions]].

encode_partition(produce, #partition{id = Id, set = Set}) ->
    EncodedSet = encode_set(produce, Set),
    [<<Id:?ID_S, (iolist_size(EncodedSet)):?SIZE_S>>, EncodedSet];
encode_partition(fetch, #partition{id = Id, offset = Offset,max_bytes = Max}) ->
    [<<Id:?ID_S, Offset:?OFFSET_S, Max:?MAX_S>>];
encode_partition(offset, Partition = #partition{}) ->
    #partition{id = Id, time = Time, max_number_of_offsets = Max} = Partition,
    <<Id:?ID_S, Time:?TIME_S, Max:?MAX_S>>.

%% N.B., MessageSets are not preceded by an int32 like other array elements
%%       in the protocol.
encode_set(produce, #set{messages = Messages}) ->
    [encode_message(produce, Message) || Message <- Messages].

encode_message(produce, Message = #message{}) ->
    #message{offset = Offset,
             magic = Magic,
             attributes = Attributes,
             key = Key,
             value = Value} = Message,
    Payload =
        [<<Magic:?MAGIC_S, Attributes:?ATTRIBUTES_S>>,
         encode_bytes(Key),
         encode_bytes(Value)],
    EncodedMessage = [<<(erlang:crc32(Payload)):?CRC_S>>, Payload],
    [<<Offset:?OFFSET_S, (iolist_size(EncodedMessage)):?SIZE_S>>,
     EncodedMessage].

encode_string_array(Strings) ->
    [<<(length(Strings)):?ARRAY_SIZE_S>>,
     [encode_latin1_to_string(String) || String <- Strings]].

encode_latin1_to_string("") -> <<-1:?STRING_SIZE_S>>;
encode_latin1_to_string(<<"">>) -> <<-1:?STRING_SIZE_S>>;
encode_latin1_to_string(String) ->
    Binary = unicode:characters_to_binary(String, latin1, utf8),
    <<(byte_size(Binary)):?STRING_SIZE_S, Binary/binary>>.

encode_bytes(<<>>) -> <<-1:?BYTES_SIZE_S>>;
encode_bytes(Binary) -> <<(byte_size(Binary)):?BYTES_SIZE_S, Binary/binary>>.


%% ===================================================================
%% Decoding
%% ===================================================================

decode_brokers(0, T, Acc) -> {lists:reverse(Acc), T};
decode_brokers(N, Binary, Acc) ->
    <<NodeId:?ID_S, HS:?STRING_SIZE_S, Host:HS/bytes, Port:?PORT, T/binary>> =
        Binary,
    H = #broker{node_id = NodeId, host = Host, port = Port},
    decode_brokers(N - 1, T, [H | Acc]).

decode_topics(_, 0, <<>>, Acc) -> lists:reverse(Acc);
decode_topics(metadata, N, Binary, Acc) ->
    <<ErrorCode:?ERROR_CODE,
      NS:?STRING_SIZE_S,
      Name:NS/bytes,
      No:?ARRAY_SIZE_S,
      Parts/binary>> = Binary,
    {Partitions, T} = decode_partitions(metadata, No, Parts, []),
    H = #topic_response{name = Name,
                        partitions = Partitions,
                        error_code = decode_error_code(ErrorCode)
                       },
    decode_topics(metadata, N - 1, T, [H | Acc]);
decode_topics(produce, N, Binary, Acc) ->
    <<NS:?STRING_SIZE_S, Name:NS/bytes,No:?ARRAY_SIZE_S,Parts/binary>> = Binary,
    {Partitions, T} = decode_partitions(produce, No, Parts, []),
    H = #topic_response{name = Name, partitions = Partitions},
    decode_topics(produce, N - 1, T, [H | Acc]);
decode_topics(fetch, N, Binary, Acc) ->
    <<NS:?STRING_SIZE_S, Name:NS/bytes,No:?ARRAY_SIZE_S,Parts/binary>> = Binary,
    {Partitions, T} = decode_partitions(produce, No, Parts, []),
    H = #topic_response{name = Name, partitions = Partitions},
    decode_topics(fetch, N - 1, T, [H | Acc]);
decode_topics(offset, N, Binary, Acc) ->
    <<NS:?STRING_SIZE_S, Name:NS/bytes,No:?ARRAY_SIZE_S,Parts/binary>> = Binary,
    {Partitions, T} = decode_partitions(offset, No, Parts, []),
    H = #topic_response{name = Name, partitions = Partitions},
    decode_topics(offset, N - 1, T, [H | Acc]).

decode_partitions(_, 0, T, Acc) -> {lists:reverse(Acc), T};
decode_partitions(metadata, N, Binary, Acc) ->
    <<ErrorCode:?ERROR_CODE,
      Id:?ID_S,
      Leader:?LEADER,
      ReplicasNo:?ARRAY_SIZE_S,
      Replicas:ReplicasNo/?IDS,
      IsrsNo:?ARRAY_SIZE_S,
      Isrs:IsrsNo/?IDS,
      T/binary>> = Binary,
    TheReplicas = [Replica || <<Replica:?ID_S>> <= Replicas],
    TheIsrs = [Isr || <<Isr:?ID_S>> <= Isrs],
    H = #partition_response{id = Id,
                            leader = Leader,
                            replicas = TheReplicas,
                            isrs = TheIsrs,
                            error_code = decode_error_code(ErrorCode)
                           },
    decode_partitions(metadata, N - 1, T, [H | Acc]);
decode_partitions(produce, N, Binary, Acc) ->
    <<Id:?ID_S, ErrorCode:?ERROR_CODE, Offset:?OFFSET_S, T/binary>> = Binary,
    H = #partition_response{id = Id,
                            offset = Offset,
                            error_code = decode_error_code(ErrorCode)},
    decode_partitions(produce, N - 1, T, [H | Acc]);
decode_partitions(fetch, N, Binary, Acc) ->
    <<Id:?ID_S,
      ErrorCode:?ERROR_CODE,
      Offset:?OFFSET_S,
      HighWaterOffset:?OFFSET_S,
      SetSize:?SIZE_S,
      Messages:SetSize/bytes,
      T/binary>> = Binary,
    H = #partition_response{id = Id,
                            offset = Offset,
                            high_watermark = HighWaterOffset,
                            set = decode_set(fetch, Messages, []),
                            error_code = decode_error_code(ErrorCode)},
    decode_partitions(fetch, N - 1, T, [H | Acc]);
decode_partitions(offset, N, Binary, Acc) ->
    <<Id:?ID_S,
      ErrorCode:?ERROR_CODE,
      No:?ARRAY_SIZE_S,
      Offsets:No/?OFFSETS, T/binary>> = Binary,
    TheOffsets = [Offset || <<Offset:?OFFSET_S>> <= Offsets],
    H = #partition_response{id = Id,
                            offset = TheOffsets,
                            error_code = decode_error_code(ErrorCode)},
    decode_partitions(offset, N - 1, T, [H | Acc]).

decode_set(fetch, <<>>, Acc) -> lists:reverse(Acc);
decode_set(fetch, Messages, Acc) ->
    <<Offsets:?OFFSET_S, Size:?SIZE_S, Message:Size/bytes,T/binary>> = Messages,
    <<CRC:?CRC_S, Payload/binary>> = Message,
    <<Magic:?MAGIC_S, Attributes:?ATTRIBUTES_S,
      KeySize:?BYTES_SIZE_S, Key:KeySize/bytes,
      ValueSize:?BYTES_SIZE_S, Value:ValueSize/bytes>> = Payload,
    H = case erlang:crc32(Payload) of
            CRC ->
                #set_response{offset = Offsets,
                              magic = Magic,
                              attributes = Attributes,
                              key = Key,
                              value = Value};
            _ ->
                #set_response{offset = Offsets, error_code = 'InvalidMessage'}
        end,
    decode_set(fetch, T, [H | Acc]).

decode_error_code(N) ->
    {_, Code} = lists:keyfind(N, 1, ?ERROR_CODES_TABLE),
    Code.

%% ===================================================================
%% Common parts
%% ===================================================================

