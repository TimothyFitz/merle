-module(merle_test).

-import(erlymock_tcp).
-import(erlymock).
-include_lib("eunit/include/eunit.hrl").

mocked_socket_tests() ->
    [
        {
            "Empty Get Test",
            fun (Socket) ->
                erlymock_tcp:strict(Socket,<<"get test\r\n">>, [{reply, <<"END\r\n">>}])
            end,
            fun () -> undefined = merle:getkey("test") end
        }, {
            "Delete Test",
            fun (Socket) ->
                erlymock_tcp:strict(Socket, <<"delete dtest 0\r\n">>,[{reply, <<"DELETED\r\n">>}]) 
            end,
            fun () -> merle:delete("dtest") end
        }, {
            "Get Test",
            fun (Socket) ->
                Foo = term_to_binary("foo"),
                FooSize = integer_to_list(byte_size(Foo)),
                erlymock_tcp:strict(
                    Socket,
                    <<"get test\r\n">>, 
                    [{reply, iolist_to_binary([<<"VALUE test 0 ">>, FooSize, <<"\r\n">>, Foo, <<"\r\nEND\r\n">>])}]
                )
            end,
            fun () -> "foo" = merle:getkey("test") end
        }, {
            "Get Complex Value Test",
            fun (Socket) ->
                Foo = term_to_binary({bar, "foo"}),
                FooSize = integer_to_list(byte_size(Foo)),
                erlymock_tcp:strict(
                    Socket,
                    <<"get test\r\n">>, 
                    [{reply, iolist_to_binary([<<"VALUE test 0 ">>, FooSize, <<"\r\n">>, Foo, <<"\r\nEND\r\n">>])}]
                )
            end,
            fun () -> {bar, "foo"} = merle:getkey("test") end
        }, {
            "Flush All Test",
            fun (Socket) ->
                erlymock_tcp:strict(Socket, <<"flush_all\r\n">>, [{reply, <<"OK\r\n">>}])
            end,
            fun () -> merle:flushall() end
        }, {
            "Version Test",
            fun (Socket) ->
                erlymock_tcp:strict(Socket, <<"version\r\n">>, [{reply, <<"VERSION foo\r\n">>}])
            end,
            fun () -> "VERSION foo" = merle:version() end
        }, {
            "Set Test",
            fun (Socket) ->
                Value = term_to_binary("value"),
                ValueSize = integer_to_list(byte_size(Value)),
                erlymock_tcp:strict(
                    Socket, 
                    iolist_to_binary([<<"set tkey 0 60 ">>, ValueSize, <<"\r\n">>, Value, <<"\r\n">>]),
                    [{reply, <<"STORED\r\n">>}]
                )
            end,
            fun () -> ok = merle:set("tkey", 0, 60, "value") end
        }
    ].
       
mocked_socket_fixture_test_() ->
    lists:map(
        fun({Name, Setup, Run}) -> 
            {Name, fun() -> mocked_socket_fixture(Setup, Run) end}
        end,
        mocked_socket_tests()
    ).
    
mocked_socket_fixture(MockSetup, ActualRun) ->
    erlymock:start(),
    {ok, Socket} = erlymock_tcp:open(),
    MockSetup(Socket),
    erlymock:replay(),
    inet:setopts(Socket, [{active,true}]),
    merle:create(Socket),
    ActualRun(),
    erlymock:verify(),
    merle:disconnect().

% stats
% version
% flushall
% delete
% set
% add
% replace
% cas
% Pull in and test: incr/decr
% Pull in and test: 
