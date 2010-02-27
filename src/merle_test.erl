-module(merle_test).

-import(erlymock_tcp).
-import(erlymock).
-include_lib("eunit/include/eunit.hrl").



get_empty_test() ->
    erlymock:start(),
    {ok, Socket} = erlymock_tcp:open(),
    erlymock_tcp:strict(Socket,<<"get test\r\n">>, [{reply, <<"END\r\n">>}]),
    erlymock:replay(),
    inet:setopts(Socket, [{active,true}]),
    merle:create(Socket),
    undefined = merle:getkey("test"),
    erlymock:verify(),
    merle:disconnect().
        
get_value_test() ->
    erlymock:start(),
    {ok, Socket} = erlymock_tcp:open(),
    Foo = term_to_binary("foo"),
    FooSize = integer_to_list(byte_size(Foo)),
    erlymock_tcp:strict(
        Socket,
        <<"get test\r\n">>, 
        [{reply, iolist_to_binary([<<"VALUE test 0 ">>, FooSize, <<"\r\n">>, Foo, <<"\r\nEND\r\n">>])}]
    ),
    erlymock:replay(),
    inet:setopts(Socket, [{active,true}]),
    merle:create(Socket),
    "foo" = merle:getkey("test"),
    erlymock:verify(),
    merle:disconnect().
    
get_complex_value_test() ->
    erlymock:start(),
    {ok, Socket} = erlymock_tcp:open(),
    Foo = term_to_binary({bar, "foo"}),
    FooSize = integer_to_list(byte_size(Foo)),
    erlymock_tcp:strict(
        Socket,
        <<"get test\r\n">>, 
        [{reply, iolist_to_binary([<<"VALUE test 0 ">>, FooSize, <<"\r\n">>, Foo, <<"\r\nEND\r\n">>])}]
    ),
    erlymock:replay(),
    inet:setopts(Socket, [{active,true}]),
    merle:create(Socket),
    {bar, "foo"} = merle:getkey("test"),
    erlymock:verify(),
    merle:disconnect().
    

