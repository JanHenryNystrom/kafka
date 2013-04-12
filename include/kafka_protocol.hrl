%% -*-erlang-*-
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


%% ===================================================================
%% Encoding
%% ===================================================================

%% produce request
-record(message, {offset = 0 :: integer(),
                  magic = 1 :: integer(),
                  attributes = 0 :: integer(),
                  key  = <<>> :: binary(),
                  value :: binary()}).
-record(set, {messages = [] :: [#message{}]}).

%% offset, produce requests
-record(partition, {id :: integer(),
                    set = [] :: [#set{}],
                    time :: integer(),
                    offset :: integer(),
                    max_bytes :: integer(),
                    max_number_of_offsets :: integer(),
                    %% offset_commit
                    metadata = <<>> :: binary()
                   }).
-record(topic, {name :: string() | binary(),
                partitions = [] :: [#partition{}]}).

%% requests
-record(metadata, {topics = [] :: [#topic{}]}).
-record(fetch, {replica = -1 :: integer(),
                timeout :: pos_integer(),
                min_bytes :: pos_integer(),
                topics = [] :: [#topic{}]}).
-record(produce, {acks = 0 :: integer(),
                  timeout = 100 :: non_neg_integer(),
                  topics = [] :: [#topic{}]}).
-record(offset, {replica = -1 :: non_neg_integer(),
                 topics = [#topic{}]}).
-record(offset_commit, {consumer_group :: string() | binary(),
                        topics = [] :: #topic{}}).
-record(offset_fetch, {consumer_group :: string() | binary(),
                       topics = [] :: #topic{}}).

%% ===================================================================
%% Decoding
%% ===================================================================

%% responses
-record(set_response, {offset :: integer(),
                       magic :: integer(),
                       attributes :: integer(),
                       key :: binary(),
                       value :: binary(),
                       error_code  = 'NoError' :: atom()
                      }).
-record(partition_response, {id :: integer(),
                             error_code :: atom(),
                             %% metadata
                             leader :: integer(),
                             replicas :: [integer()],
                             isrs :: [integer()],
                             %% produce, fetch, offset
                             offset :: integer() | [integer()],
                             %% fetch,
                             high_watermark :: integer(),
                             set :: [#set_response{}],
                             %% offset_fetch
                             metadata :: binary()
                            }).
-record(topic_response, {name :: binary(),
                         partitions = [] :: [#partition_response{}],
                         error_code = 'NoError' :: atom()
                        }).

-record(broker, {node_id :: integer(),
                 host :: binary(),
                 port :: integer()}).

-record(metadata_response, {corr_id :: integer(),
                            brokers = [] :: [#broker{}],
                            topics = [] :: [#topic_response{}]}).
-record(produce_response, {corr_id :: integer(),
                           topics = [] :: [#topic_response{}]}).
-record(fetch_response, {corr_id :: integer(),
                         topics = [] :: [#topic_response{}]}).
-record(offset_response, {corr_id :: integer(),
                         topics = []:: [#topic_response{}]}).
-record(offset_commit_response, {corr_id :: integer(),
                                 topics = []:: [#topic_response{}]}).
-record(offset_fetch_response, {corr_id :: integer(),
                                topics = []:: [#topic_response{}]}).
