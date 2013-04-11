-module(utility).

-export([connect/0, connect/2,
         send/5, send_rec/5]).

connect() -> connect("127.0.0.1", 9092).

connect(Host, Port) ->
    {ok, Socket} = gen_tcp:connect(Host, Port, [binary, {packet, 4}]),
    Socket.

send(Socket, Type, CorrId, ClientId, Payload) ->
    ok = gen_tcp:send(Socket,
                      kafka_protocol:encode(Type, CorrId, ClientId, Payload)).

send_rec(Socket, Type, CorrId, ClientId, Payload) ->
    ok = gen_tcp:send(Socket,
                      kafka_protocol:encode(Type, CorrId, ClientId, Payload)),
    receive
        {tcp, Socket, Packet} ->
            kafka_protocol:decode(Type, Packet)
    after 10000 ->
            timeout
    end.
