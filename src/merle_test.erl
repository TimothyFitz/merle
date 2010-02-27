-module(merle_test).

-import(erlymock_tcp).
-import(erlymock).
-include_lib("eunit/include/eunit.hrl").



get_test() ->
    erlymock:start(),
    {ok, Socket} = erlymock_tcp:open(),
    erlymock_tcp:strict(Socket,<<"get test\r\n">>, [{reply, <<"END\r\n">>}]),
    erlymock:replay(),
    inet:setopts(Socket, [{active,true}]),
    merle:create(Socket),
    undefined = merle:getkey("test"),
    erlymock:verify(),
    merle:disconnect().
