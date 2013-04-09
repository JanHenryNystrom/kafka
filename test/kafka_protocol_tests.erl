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
        [{metadata, 1, "myid", ["topic"],
          <<0,0,0,25,0,3,0,0,0,0,0,1,0,4,109,121,105,100,0,0,0,1,0,5,
            116,111,112,105,99>>},
         {metadata, 2, "myid1", [],
          <<0,0,0,19,0,3,0,0,0,0,0,2,0,5,109,121,105,100,49,0,0,0,0>>},
         {metadata, 3, "myid3", ["topic1", "topic2"],
          <<0,0,0,35,0,3,0,0,0,0,0,3,0,5,109,121,105,100,51,0,0,0,2,
            0,6,116,111,112,105,99,49,0,6,116,111,112,105,99,50>>},
         {metadata, 4, <<"myid4">>, [<<"topic1">>, <<"topic2">>],
         <<0,0,0,35,0,3,0,0,0,0,0,4,0,5,109,121,105,100,52,0,0,0,2,
           0,6,116,111,112,105,99,49,0,6,116,111,112,105,99,50>>},
         {produce, 14, "client1",
          {1, 100,
           [#topic{
               name = "mytopic1",
               partitions =
                   [#partition{
                       id = 0,
                       set = #set{messages =
                                      [#message{value = <<"foo">>}]}}]}]},
          <<0,0,0,78,0,0,0,0,0,0,0,14,0,7,99,108,105,101,110,116,49,
            0,1,0,0,0,100,0,0,0,1,0,8,109,121,116,111,112,105,99,49,
            0,0,0,1,0,0,0,0,0,0,0,29,0,0,0,0,0,0,0,0,0,0,0,17,39,29,
            194,201,1,0,255,255,255,255,0,0,0,3,102,111,111>>}
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
    [?_test(
        ?assertEqual(
           Result,
           iolist_to_binary(
             kafka_protocol:encode(Type, CorrId, ClientId, Args)))) ||
        {Type, CorrId, ClientId, Args, Result} <- ?ENCODE].

%% ===================================================================
%% Decoding
%% ===================================================================

%% ===================================================================
%% Internal functions.
%% ===================================================================

