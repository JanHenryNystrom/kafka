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

%% offset request
-record(offset, {replica = 0, topics = []}).

%% offset, produce requests
-record(topic, {name, partitions = []}).
-record(partition, {id, set = [], time, max_number_of_offsets}).

%% produce request
-record(set, {messages}).
-record(message, {offset = 0,
                  crc,
                  magic = 1,
                  attributes = 0,
                  key  = <<>> :: binary(),
                  value :: binary()}).


