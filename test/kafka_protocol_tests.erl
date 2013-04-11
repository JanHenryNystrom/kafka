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
%%%   eunit unit tests for the kafak protocol module.
%%% @end
%%%
%% @author Jan Henry Nystrom <JanHenryNystrom@gmail.com>
%% @copyright (C) 2013, Jan Henry Nystrom <JanHenryNystrom@gmail.com>
%%%-------------------------------------------------------------------
-module(kafka_protocol_tests).
-copyright('Jan Henry Nystrom <JanHenryNystrom@gmail.com>').

%% Includes
-include_lib("eunit/include/eunit.hrl").
-include_lib("kafka/include/kafka_protocol.hrl").

%% Defines
-define(ENCODE,
        [{metadata, 1, "myid", #metadata{topics = ["topic"]},
          <<0,3,0,0,0,0,0,1,0,4,109,121,105,100,0,0,0,1,0,5,
            116,111,112,105,99>>},
         {metadata, 2, "myid1", #metadata{topics = []},
          <<0,3,0,0,0,0,0,2,0,5,109,121,105,100,49,0,0,0,0>>},
         {metadata, 3, "myid3", #metadata{topics = ["topic1", "topic2"]},
          <<0,3,0,0,0,0,0,3,0,5,109,121,105,100,51,0,0,0,2,
            0,6,116,111,112,105,99,49,0,6,116,111,112,105,99,50>>},
         {metadata, 4, <<"myid4">>,
          #metadata{topics = [<<"topic1">>, <<"topic2">>]},
         <<0,3,0,0,0,0,0,4,0,5,109,121,105,100,52,0,0,0,2,
           0,6,116,111,112,105,99,49,0,6,116,111,112,105,99,50>>},
         {produce, 14, "client1",
          #produce{
             acks = 1,
             timeout = 100,
             topics =
                 [#topic{
                     name = "mytopic1",
                     partitions =
                         [#partition{
                             id = 0,
                             set = #set{messages =
                                            [#message{value = <<"foo">>}]}}]}]},
          <<0,0,0,0,0,0,0,14,0,7,99,108,105,101,110,116,49,
            0,1,0,0,0,100,0,0,0,1,0,8,109,121,116,111,112,105,99,49,
            0,0,0,1,0,0,0,0,0,0,0,29,0,0,0,0,0,0,0,0,0,0,0,17,39,29,
            194,201,1,0,255,255,255,255,0,0,0,3,102,111,111>>},
         {produce, 14, "client1",
          #produce{
             acks = 0,
             timeout = 100,
             topics =
                 [#topic{
                     name = "mytopic1",
                     partitions =
                         [#partition{
                             id = 0,
                             set = #set{messages =
                                            [#message{
                                                key = <<"">>,
                                                value = <<"foo">>}]}}]}]},
          <<0,0,0,0,0,0,0,14,0,7,99,108,105,101,110,116,49,
            0,0,0,0,0,100,0,0,0,1,0,8,109,121,116,111,112,105,99,49,
            0,0,0,1,0,0,0,0,0,0,0,29,0,0,0,0,0,0,0,0,0,0,0,17,39,29,
            194,201,1,0,255,255,255,255,0,0,0,3,102,111,111>>},
         {produce, 14, "",
          #produce{
             acks = 0,
             timeout = 100,
             topics =
                 [#topic{
                     name = "mytopic1",
                     partitions =
                         [#partition{
                             id = 0,
                             set = #set{messages =
                                            [#message{
                                                key = <<"">>,
                                                value = <<"foo">>}]}}]}]},
          <<0,0,0,0,0,0,0,14,255,255,0,0,0,0,0,100,0,0,0,1,
            0,8,109,121,116,111,112,105,99,49,0,0,0,1,0,0,0,0,0,0,0,
            29,0,0,0,0,0,0,0,0,0,0,0,17,39,29,194,201,1,0,255,255,
            255,255,0,0,0,3,102,111,111>>},
         {produce, 14, <<"">>,
          #produce{
             acks = 0,
             timeout = 100,
             topics =
                 [#topic{
                     name = "mytopic1",
                     partitions =
                         [#partition{
                             id = 0,
                             set = #set{messages =
                                            [#message{
                                                key = <<"">>,
                                                value = <<"foo">>}]}}]}]},
          <<0,0,0,0,0,0,0,14,255,255,0,0,0,0,0,100,0,0,0,1,
            0,8,109,121,116,111,112,105,99,49,0,0,0,1,0,0,0,0,0,0,0,
            29,0,0,0,0,0,0,0,0,0,0,0,17,39,29,194,201,1,0,255,255,
            255,255,0,0,0,3,102,111,111>>},
         {produce, 14, "client1",
          #produce{
             acks = 0,
             timeout = 100,
             topics =
                 [#topic{
                     name = "mytopic1",
                     partitions =
                         [#partition{
                             id = 0,
                             set = #set{messages = []}}]}]},
          <<0,0,0,0,0,0,0,14,0,7,99,108,105,101,110,116,49,
            0,0,0,0,0,100,0,0,0,1,0,8,109,121,116,111,112,105,99,49,
            0,0,0,1,0,0,0,0,0,0,0,0>>},
         {fetch, 92, "myId",
          #fetch{timeout = 1000,
                 min_bytes = 20,
                 topics = [#topic{name = <<"mytopic1">>,
                                  partitions=[#partition{
                                                 id = 0,
                                                 offset = 0,
                                                 max_bytes = 1048576}]}]},
          <<0,1,0,0,0,0,0,92,0,4,109,121,73,100,255,255,
            255,255,0,0,3,232,0,0,0,20,0,0,0,1,0,8,109,121,116,111,
            112,105,99,49,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,16,0,0>>},
         {offset, 15, "client1",
          #offset{replica = 0,
                  topics =
                      [#topic{name = "mytopic1",
                              partitions =
                                  [#partition{id = 0,
                                              time = -2,
                                              max_number_of_offsets = 1024}]}]},
          <<0,2,0,0,0,0,0,15,0,7,99,108,105,101,110,116,49,
            0,0,0,0,0,0,0,1,0,8,109,121,116,111,112,105,99,49,0,0,0,
            1,0,0,0,0,255,255,255,255,255,255,255,254,0,0,4,0>>}
        ]).

-define(DECODE,
        [{metadata,
          #metadata_response{
             corr_id = 2,
             brokers = [#broker{node_id = 0,
                                host = <<"localhost">>,
                                port = 9092}],
             topics =
                 [#topic_response{
                     name = <<"mytopic1">>,
                     partitions =
                         [#partition_response{id = 0,
                                              leader = 0,
                                              replicas = [0],
                                              isrs = [0],
                                              error_code = 'NoError'}],
                    error_code = 'NoError'}]},
          <<0,0,0,2,0,0,0,1,0,0,0,0,0,9,108,111,99,97,108,104,
            111,115,116,0,0,35,132,0,0,0,1,0,0,0,8,109,121,116,111,112,
            105,99,49,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,
            1,0,0,0,0>>},
         {produce,
          #produce_response{
             corr_id = 14,
             topics =
                 [#topic_response{
                     name = <<"mytopic1">>,
                     partitions =
                         [#partition_response{
                             id = 0,
                             offset = 5,
                             error_code = 'NoError'}]}]},
          <<0,0,0,14,0,0,0,1,0,8,109,121,116,111,112,105,99,49,
            0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,5>>},
         {offset,
          #offset_response{
             corr_id = 0,
             topics =
                 [#topic_response{
                     name = <<"mytopic1">>,
                     partitions =
                         [#partition_response{
                             id = 0,error_code = 'NoError',
                             offset = [0]}]}]},
          <<0,0,0,0,0,0,0,1,0,8,109,121,116,111,112,105,99,49,0,
            0,0,1,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0>>},
         {fetch,
          #fetch_response{
             corr_id = 92,
             topics =
                 [#topic_response{
                     name = <<"mytopic1">>,
                     partitions =
                         [#partition_response{
                             id = 0,error_code = 'NoError',leader = undefined,
                             replicas = undefined,isrs = undefined,
                             offset = undefined,high_watermark = 10,
                             set =
                                 [#set_response{
                                     offset = 0,magic = undefined,
                                     attributes = undefined,key = undefined,
                                     value = undefined,
                                     error_code = 'InvalidMessage'},
                                  #set_response{
                                     offset = 1,magic = undefined,
                                     attributes = undefined,key = undefined,
                                     value = undefined,
                                     error_code = 'InvalidMessage'},
                                  #set_response{
                                     offset = 2,magic = undefined,
                                     attributes = undefined,key = undefined,
                                     value = undefined,
                                     error_code = 'InvalidMessage'},
                                  #set_response{
                                     offset = 3,
                                     magic = undefined,
                                     attributes = undefined,
                                     key = undefined,
                                     value = undefined,
                                     error_code = 'InvalidMessage'},
                                  #set_response{
                                     offset = 4,
                                     magic = 1,
                                     attributes = 0,
                                     key = <<>>,
                                     value = <<"foo">>,error_code = 'NoError'},
                                  #set_response{
                                     offset = 5,
                                     magic = 1,
                                     attributes = 0,
                                     key = <<>>,
                                     value = <<"foo">>,
                                     error_code = 'NoError'},
                                  #set_response{
                                     offset = 6,
                                     magic = 1,
                                     attributes = 0,
                                     key = <<>>,
                                     value = <<"foo">>,
                                     error_code = 'NoError'},
                                  #set_response{
                                     offset = 7,
                                     magic = 1,
                                     attributes = 0,
                                     key = <<>>,
                                     value = <<"foo">>,
                                     error_code = 'NoError'},
                                  #set_response{
                                     offset = 8,
                                     magic = 1,
                                     attributes = 0,
                                     key = <<>>,
                                     value = <<"foo">>,
                                     error_code = 'NoError'},
                                  #set_response{
                                     offset = 9,
                                     magic = 1,
                                     attributes = 0,
                                     key = <<>>,
                                     value = <<"foo">>,
                                     error_code = 'NoError'}]}],
                     error_code = 'NoError'}]},
          <<0,0,0,92,0,0,0,1,
            0,8,109,121,116,111,112,105,99,49,0,0,0,1,
            0,0,0,0,0,0,0,0,0,0,0,0,0,10,0,0,1,34,
            0,0,0,0,0,0,0,0,0,0,0,17,179,101,140,218,1,0,0,0,0,0,0,0,0,3,
            102,111,111,0,0,0,0,0,0,0,1,0,0,0,17,179,101,140,218,1,0,0,0,
            0,0,0,0,0,3,102,111,111,0,0,0,0,0,0,0,2,0,0,0,17,179,101,140,
            218,1,0,0,0,0,0,0,0,0,3,102,111,111,0,0,0,0,0,0,0,3,0,0,0,17,
            179,101,140,218,1,0,0,0,0,0,0,0,0,3,102,111,111,0,0,0,0,0,0,
            0,4,0,0,0,17,39,29,194,201,1,0,255,255,255,255,0,0,0,3,102,
            111,111,0,0,0,0,0,0,0,5,0,0,0,17,39,29,194,201,1,0,255,255,
            255,255,0,0,0,3,102,111,111,0,0,0,0,0,0,0,6,0,0,0,17,39,29,
            194,201,1,0,255,255,255,255,0,0,0,3,102,111,111,0,0,0,0,0,0,
            0,7,0,0,0,17,39,29,194,201,1,0,255,255,255,255,0,0,0,3,102,
            111,111,0,0,0,0,0,0,0,8,0,0,0,17,39,29,194,201,1,0,255,255,
            255,255,0,0,0,3,102,111,111,0,0,0,0,0,0,0,9,0,0,0,17,39,29,
            194,201,1,0,255,255,255,255,0,0,0,3,102,111,111>>},
         {fetch,
          #fetch_response{
             corr_id = 92,
             topics =
                 [#topic_response{
                     name = <<"mytopic1">>,
                     partitions =
                         [#partition_response{
                             id = 0,error_code = 'NoError',leader = undefined,
                             replicas = undefined,isrs = undefined,
                             offset = undefined,high_watermark = 11,
                             set =
                                 [#set_response{
                                     offset = 0,magic = undefined,
                                     attributes = undefined,key = undefined,
                                     value = undefined,
                                     error_code = 'InvalidMessage'},
                                  #set_response{
                                     offset = 1,magic = undefined,
                                     attributes = undefined,key = undefined,
                                     value = undefined,
                                     error_code = 'InvalidMessage'},
                                  #set_response{
                                     offset = 2,magic = undefined,
                                     attributes = undefined,key = undefined,
                                     value = undefined,
                                     error_code = 'InvalidMessage'},
                                  #set_response{
                                     offset = 3,magic = undefined,
                                     attributes = undefined,key = undefined,
                                     value = undefined,
                                     error_code = 'InvalidMessage'},
                                  #set_response{
                                     offset = 4,magic = 1,attributes = 0,
                                     key = <<>>,
                                     value = <<"foo">>,error_code = 'NoError'},
                                  #set_response{
                                     offset = 5,magic = 1,attributes = 0,
                                     key = <<>>,
                                     value = <<"foo">>,error_code = 'NoError'},
                                  #set_response{
                                     offset = 6,magic = 1,attributes = 0,
                                     key = <<>>,
                                     value = <<"foo">>,error_code = 'NoError'},
                                  #set_response{
                                     offset = 7,magic = 1,attributes = 0,
                                     key = <<>>,
                                     value = <<"foo">>,error_code = 'NoError'},
                                  #set_response{
                                     offset = 8,magic = 1,attributes = 0,
                                     key = <<>>,
                                     value = <<"foo">>,error_code = 'NoError'},
                                  #set_response{
                                     offset = 9,magic = 1,attributes = 0
                                     ,key = <<>>,
                                     value = <<"foo">>,error_code = 'NoError'},
                                  #set_response{
                                     offset = 10,magic = 1,attributes = 0,
                                     key = <<"bar">>,value = <<"baz">>,
                                     error_code = 'NoError'}]}],
                     error_code = 'NoError'}]},
          <<0,0,0,92,0,0,0,1,0,8,109,121,116,111,112,105,99,49,0,0,0,
            1,
            0,0,0,0,0,0,0,0,0,0,0,0,0,11,0,0,1,66,
            0,0,0,0,0,0,0,0,0,0,0,17,179,101,140,218,1,0,0,0,0,0,0,0,0,3,
            102,111,111,0,0,0,0,0,0,0,1,0,0,0,17,179,101,140,218,1,0,0,0,
            0,0,0,0,0,3,102,111,111,0,0,0,0,0,0,0,2,0,0,0,17,179,101,140,
            218,1,0,0,0,0,0,0,0,0,3,102,111,111,0,0,0,0,0,0,0,3,0,0,0,17,
            179,101,140,218,1,0,0,0,0,0,0,0,0,3,102,111,111,0,0,0,0,0,0,
            0,4,0,0,0,17,39,29,194,201,1,0,255,255,255,255,0,0,0,3,102,
            111,111,0,0,0,0,0,0,0,5,0,0,0,17,39,29,194,201,1,0,255,255,
            255,255,0,0,0,3,102,111,111,0,0,0,0,0,0,0,6,0,0,0,17,39,29,
            194,201,1,0,255,255,255,255,0,0,0,3,102,111,111,0,0,0,0,0,0,
            0,7,0,0,0,17,39,29,194,201,1,0,255,255,255,255,0,0,0,3,102,
            111,111,0,0,0,0,0,0,0,8,0,0,0,17,39,29,194,201,1,0,255,255,
            255,255,0,0,0,3,102,111,111,0,0,0,0,0,0,0,9,0,0,0,17,39,29,
            194,201,1,0,255,255,255,255,0,0,0,3,102,111,111,0,0,0,0,0,0,
            0,10,0,0,0,20,62,158,25,230,1,0,0,0,0,3,98,97,114,0,0,0,3,98,
            97,122>>},
         {offset,
          #offset_response{
             corr_id = 0,
             topics =
                 [#topic_response{
                     name = <<"mytopic1">>,
                     partitions =
                         [#partition_response{
                             id = 0,error_code = 'NoError',
                             offset = [5,0]}]}]},
          <<0,0,0,0,0,0,0,1,0,8,109,121,116,111,112,105,99,49,0,
            0,0,1,0,0,0,0,0,0,0,0,0,2,0,0,0,0,0,0,0,5,0,0,0,0,0,0,0,0>>}
        ]).

%% ===================================================================
%% Tests.
%% ===================================================================

%% ===================================================================
%% Encoding
%% ===================================================================

%%--------------------------------------------------------------------
%% encode/4
%%--------------------------------------------------------------------
encode_4_test_() ->
    [?_test(?assertEqual(Result,
           iolist_to_binary(
             kafka_protocol:encode(Type, CorrId, ClientId, Args)))) ||
        {Type, CorrId, ClientId, Args, Result} <- ?ENCODE].

%% ===================================================================
%% Decoding
%% ===================================================================

%%--------------------------------------------------------------------
%% decode/2
%%--------------------------------------------------------------------
decode_2_test_() ->
    [?_test(?assertEqual(Result, kafka_protocol:decode(Type, Response))) ||
        {Type, Result, Response} <- ?DECODE].


%% ===================================================================
%% Internal functions.
%% ===================================================================

