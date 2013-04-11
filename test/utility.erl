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
%%%   A utility module to make manual testsing during development.
%%% @end
%%%
%% @author Jan Henry Nystrom <JanHenryNystrom@gmail.com>
%% @copyright (C) 2013, Jan Henry Nystrom <JanHenryNystrom@gmail.com>
%%%-------------------------------------------------------------------
-module(utility).
-copyright('Jan Henry Nystrom <JanHenryNystrom@gmail.com>').

%% API
-export([connect/0, connect/2, disconnect/1,
         send/5, send_rec/5]).

%% Includes
-include_lib("kafka/include/kafka_protocol.hrl").

%% Types
-type client_id() :: string() | atom().
-type socket() :: port().


%% ===================================================================
%% Library functions.
%% ===================================================================

%%--------------------------------------------------------------------
%% Function: connect() -> Socket
%% @doc
%%   Connect to kafka started locally.
%% @end
%%--------------------------------------------------------------------
-spec connect() -> socket().
%%--------------------------------------------------------------------
connect() -> connect("127.0.0.1", 9092).

%%--------------------------------------------------------------------
%% Function: connect(Host, Port) -> Socket
%% @doc
%%   Connect to kafka on Host using Port.
%% @end
%%--------------------------------------------------------------------
-spec connect(string(), integer()) -> socket().
%%--------------------------------------------------------------------
connect(Host, Port) ->
    {ok, Socket} = gen_tcp:connect(Host, Port, [binary, {packet, 4}]),
    Socket.

%%--------------------------------------------------------------------
%% Function: disconnect(Socket) -> ok
%% @doc
%%   Disconnect from the kafka broker.
%% @end
%%--------------------------------------------------------------------
-spec disconnect(socket()) -> ok.
%%--------------------------------------------------------------------
disconnect(Socket) -> gen_tcp:close(Socket).

%%--------------------------------------------------------------------
%% Function: send(Socket, Type, CorrId, ClientId, Payload) -> ok.
%% @doc
%%   Encodes and sends a request to the kafka server without waiting reply,
%%   e.g., produce acks set to 0.
%% @end
%%--------------------------------------------------------------------
-spec send(socket(),atom(),integer(),client_id(), kafka_protocol:request()) ->
          ok.
%%--------------------------------------------------------------------
send(Socket, Type, CorrId, ClientId, Payload) ->
    ok = gen_tcp:send(Socket,
                      kafka_protocol:encode(Type, CorrId, ClientId, Payload)).

%%--------------------------------------------------------------------
%% Function: send_rec(Socket, Type, CorrId, ClientId, Payload) -> Response.
%% @doc
%%   Encodes and sends a request to the kafka server, wait for a response.
%%   Returns the decoded response.
%% @end
%%--------------------------------------------------------------------
-spec send_rec(socket(),
               atom(),
               integer(),
               client_id(),
               kafka_protocol:request()) ->
          kafka_protocol:response().
%%--------------------------------------------------------------------
send_rec(Socket, Type, CorrId, ClientId, Payload) ->
    ok = gen_tcp:send(Socket,
                      kafka_protocol:encode(Type, CorrId, ClientId, Payload)),
    receive
        {tcp, Socket, Packet} ->
            kafka_protocol:decode(Type, Packet)
    after 10000 ->
            timeout
    end.
