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
-export([encode/4 ]).

%% Includes
-include_lib("kafka/include/kafka_protocol.hrl").

%% Types

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

%% Field sizes.

%% All types
-define(SIZE_S, 32/signed).
-define(CORR_ID_S, 32/signed).
-define(ARRAY_SIZE_S, 32/signed).
-define(STRING_SIZE_S, 16/signed).

%% produce, offset
-define(ID, 32/signed).

%% produce
-define(ACKS_S, 16/signed).
-define(TIMEOUT_S, 32/signed).
-define(BYTES_SIZE_S, 32/signed).
-define(CRC_S, 32/signed).
-define(MAGIC_S, 8/signed).
-define(ATTRIBUTES_S, 8/signed).
-define(OFFSET_S, 64/signed).

%% offset
-define(TIME_S, 64/signed).
-define(MAX_S, 32/signed).


%% What decoder/type keys to use.
-define(API_KEY_TABLE, [{metadata, ?METADATA_REQUEST},
                        {produce, ?PRODUCE_REQUEST},
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
-spec encode(atom(), integer(), string(), _) -> iolist().
%%--------------------------------------------------------------------
encode(Type, CorrId, ClientId, Args) ->
    {Type, ApiKey} = lists:keyfind(Type, 1, ?API_KEY_TABLE),
    Request = [<<ApiKey:?ApiKey, ?API_VERSION, CorrId:?CORR_ID_S>>,
               encode_latin1_to_string(ClientId), encode(Type, Args)],
    Size = iolist_size(Request),
    [<<Size:?SIZE_S>>, Request].

%% ===================================================================
%% Internal functions.
%% ===================================================================

%% ===================================================================
%% Encoding
%% ===================================================================

encode(metadata, Topics) -> encode_string_array(Topics);
encode(produce, {RequiredAcks, Timeout, Topics}) ->
     [<<RequiredAcks:?ACKS_S, Timeout:?TIMEOUT_S,
        (length(Topics)):?ARRAY_SIZE_S>>,
      [encode_topic(produce, Topic) || Topic <- Topics]];
encode(offset, #offset{replica = Id, topics = Topics}) ->
    [<<Id:?ID, (length(Topics)):?ARRAY_SIZE_S>>,
     [encode_topic(offset, Topic) || Topic <- Topics]].

encode_topic(produce, #topic{name = Name, partitions = Partitions}) ->
    [encode_latin1_to_string(Name),
     <<(length(Partitions)):?ARRAY_SIZE_S>>,
     [encode_partition(produce, Partition) || Partition <- Partitions]];
encode_topic(offset, #topic{name = Name, partitions = Parts}) ->
    [encode_latin1_to_string(Name),
     <<(length(Parts)):?ARRAY_SIZE_S>>,
     [encode_partition(offset, Part) || Part <- Parts]].

encode_partition(produce, #partition{id = Id, set = Set}) ->
    EncodedSet = encode_set(produce, Set),
    [<<Id:?ID, (iolist_size(EncodedSet)):?SIZE_S>>, EncodedSet];
encode_partition(offset, Partition = #partition{}) ->
    #partition{id = Id, time = Time, max_number_of_offsets = Max} = Partition,
    <<Id:?ID, Time:?TIME_S, Max:?MAX_S>>.

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
encode_latin1_to_string(String) ->
    Binary = unicode:characters_to_binary(String, latin1, utf8),
    <<(byte_size(Binary)):?STRING_SIZE_S, Binary/binary>>.

encode_bytes(<<>>) -> <<-1:?BYTES_SIZE_S>>;
encode_bytes(Binary) -> <<(byte_size(Binary)):?BYTES_SIZE_S, Binary/binary>>.


%% ===================================================================
%% Decoding
%% ===================================================================

%% ===================================================================
%% Common parts
%% ===================================================================

