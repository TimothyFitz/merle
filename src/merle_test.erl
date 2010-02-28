-module(merle_test).

-import(erlymock_tcp).
-import(erlymock).
-include_lib("eunit/include/eunit.hrl").

mocked_socket_tests() ->
    Value = term_to_binary("value"),
    ValueSize = integer_to_list(byte_size(Value)),
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
                erlymock_tcp:strict(Socket, iolist_to_binary([<<"set tkey 0 60 ">>, ValueSize, <<"\r\n">>]), []),
                erlymock_tcp:strict(Socket, iolist_to_binary([Value, <<"\r\n">>]), [{reply, <<"STORED\r\n">>}])
            end,
            fun () -> ok = merle:set("tkey", 0, 60, "value") end
        }, {
            "Add (Not Stored) Test",
            fun (Socket) ->
                erlymock_tcp:strict(Socket, iolist_to_binary([<<"add tkey 0 60 ">>, ValueSize, <<"\r\n">>]), []),
                erlymock_tcp:strict(Socket, iolist_to_binary([Value, <<"\r\n">>]), [{reply, <<"NOT_STORED\r\n">>}])
            end,
            fun () -> not_stored = merle:add("tkey", 0, 60, "value") end
        }, {
            "Cas (Exists) Test",
            fun (Socket) ->
                erlymock_tcp:strict(Socket, iolist_to_binary([<<"cas tkey 0 60 ">>, ValueSize, <<" 1337\r\n">>]), []),
                erlymock_tcp:strict(Socket, iolist_to_binary([Value, <<"\r\n">>]), [{reply, <<"EXISTS\r\n">>}])
            end,
            fun () -> "EXISTS" = merle:cas("tkey", 0, 60, 1337, "value") end
        }, {
            "Replace (Not Stored) Test",
            fun (Socket) ->
                erlymock_tcp:strict(Socket, iolist_to_binary([<<"replace tkey 0 60 ">>, ValueSize, <<"\r\n">>]), []),
                erlymock_tcp:strict(Socket, iolist_to_binary([Value, <<"\r\n">>]), [{reply, <<"NOT_STORED\r\n">>}])
            end,
            fun () -> not_stored = merle:replace("tkey", 0, 60, "value") end
        }, {
            "Stats Test",
            fun (Socket) ->
                erlymock_tcp:strict(Socket, <<"stats\r\n">>, [{reply, <<"STAT skey svalue\r\nEND\r\n">>}])
            end,
            fun () -> ["STAT skey svalue", "END"] = merle:stats() end
        }
    ].
       
mocked_socket_noop_serializer_tests() ->
    [
        {
            "Get Test",
            fun (Socket) ->
                erlymock_tcp:strict(
                    Socket,
                    <<"get test\r\n">>, 
                    [{reply, iolist_to_binary([<<"VALUE test 0 3\r\nfoo\r\nEND\r\n">>])}]
                )
            end,
            fun () -> <<"foo">> = merle:getkey("test") end
        }
    ].
       
mocked_socket_fixture_test_() ->
    lists:map(
        fun({Name, Setup, Run}) -> 
            {"Mocked Socket (term_to_binary) Fixture: " ++ Name, fun() -> mocked_socket_fixture(Setup, Run) end}
        end,
        mocked_socket_tests()
    ).
    
    
mocked_socket_noop_serializer_fixture_test_() ->
    lists:map(
        fun({Name, Setup, Run}) -> 
            {"Mocked Socket (noop Serializer) Fixture: " ++ Name, fun() -> mocked_socket_noop_serializer_fixture(Setup, Run) end}
        end,
        mocked_socket_noop_serializer_tests()
    ).
    
       
mocked_socket_fixture_template(Create) ->
    fun (MockSetup, ActualRun) ->
        erlymock:start(),
        {ok, Socket} = erlymock_tcp:open(),
        MockSetup(Socket),
        erlymock:replay(),
        inet:setopts(Socket, [{active,true}]),
        try 
            Create(Socket),
            ActualRun(),
            erlymock:verify()
        after 
            merle:disconnect()
        end
    end.
    
mocked_socket_fixture(MockSetup, ActualRun) ->
    CreateDefault = fun (Socket) -> merle:create(Socket) end,
    (mocked_socket_fixture_template(CreateDefault))(MockSetup, ActualRun).
    
mocked_socket_noop_serializer_fixture(MockSetup, ActualRun) ->
    Noop = fun (X) -> X end,
    CreateNoop = fun (Socket) -> merle:create(Socket, {Noop, Noop}) end,
    (mocked_socket_fixture_template(CreateNoop))(MockSetup, ActualRun).

% Pull in and test: incr/decr
% Pull in and test: ketama
