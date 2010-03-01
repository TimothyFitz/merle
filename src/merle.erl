%% Copyright 2009, Joe Williams <joe@joetify.com>
%% Copyright 2009, Nick Gerakines <nick@gerakines.net>
%%
%% Permission is hereby granted, free of charge, to any person
%% obtaining a copy of this software and associated documentation
%% files (the "Software"), to deal in the Software without
%% restriction, including without limitation the rights to use,
%% copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the
%% Software is furnished to do so, subject to the following
%% conditions:
%%
%% The above copyright notice and this permission notice shall be
%% included in all copies or substantial portions of the Software.
%%
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
%% EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
%% OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
%% NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
%% HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
%% WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
%% FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
%% OTHER DEALINGS IN THE SOFTWARE.
%%
%% @author Joseph Williams <joe@joetify.com>
%% @copyright 2008 Joseph Williams
%% @version 0.3
%% @seealso http://code.sixapart.com/svn/memcached/trunk/server/doc/protocol.txt
%% @doc An Erlang memcached client.
%%
%% This code is available as Open Source Software under the MIT license.
%%
%% Updates at http://github.com/joewilliams/merle/

-module(merle).
-behaviour(gen_server2).

-author("Joe Williams <joe@joetify.com>").
-version("Version: 0.3").

-define(SERVER, ?MODULE).
-define(TIMEOUT, 5000).
-define(RANDOM_MAX, 65535).
-define(DEFAULT_HOST, "localhost").
-define(DEFAULT_PORT, 11211).
-define(TCP_OPTS, [
    binary, {packet, raw}, {nodelay, true},{reuseaddr, true}, {active, true}
]).

%% gen_server2 API
-export([
    stats/0, stats/1, version/0, get/1, delete/2, set/4, add/4, replace/2,
    replace/4, cas/5, set/2, flush_all/0, flush_all/1, verbosity/1, add/2,
    cas/3, gets/1, connect/0, connect/2, connect/3, delete/1, disconnect/0,
    create/1, create/2, serializer/1, incr/1, incr/2, decr/1, decr/2
]).

%% gen_server2 callbacks
-export([
    init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3
]).

-record(connection, {socket, to_binary, from_binary}).

maybe_atl(Key) when is_atom(Key) ->
    atom_to_list(Key);
maybe_atl(Key) ->
    Key.
    
maybe_itl(Integer) when is_integer(Integer) ->
    integer_to_list(Integer);
maybe_itl(Integer) ->
    Integer.
    
serializer(Type) ->
    case Type of
        default -> serializer(term_to_binary);
        term_to_binary -> {fun term_to_binary/1, fun binary_to_term/1};
        noop -> {fun (X) -> X end, fun (X) -> X end}
    end.

%% @doc retrieve memcached stats
stats() ->
    gen_server2:call(?SERVER, {generic, stats}).

%% @doc retrieve memcached stats based on args
stats(Args) ->
    gen_server2:call(?SERVER, {generic, stats, {maybe_atl(Args)}}).

parse_ok(["OK"]) -> ok;
parse_ok([X]) -> X.

parse_end(["END"]) -> undefined;
parse_end([X]) -> X.

%% @doc retrieve memcached version
version() ->
    [Version] = gen_server2:call(?SERVER, {generic, version}),
    Version.

%% @doc set the verbosity level of the logging output
verbosity(Args)->
    parse_ok(gen_server2:call(?SERVER, {generic, verbosity, {maybe_itl(Args)}})).
    
%% @doc invalidate all existing items immediately
flush_all() ->
    parse_ok(gen_server2:call(?SERVER, {generic, flush_all})).

%% @doc invalidate all existing items based on the expire time argument
flush_all(Delay) ->
    parse_ok(gen_server2:call(?SERVER, {generic, flushall, {maybe_itl(Delay)}})).

%% @doc retrieve value based off of key
get(Key) ->
    parse_end(gen_server2:call(?SERVER, {get, get, maybe_atl(Key)})).

%% @doc retrieve value based off of key for use with cas
gets(Key) ->
    parse_end(gen_server2:call(?SERVER, {get, gets, maybe_atl(Key)})).

%% @doc delete a key
delete(Key) ->
    delete(Key, "0").
delete(Key, Time) ->
    case gen_server2:call(?SERVER, {delete, {maybe_atl(Key), maybe_itl(Time)}}) of
        ["DELETED"] -> ok;
        ["NOT_FOUND"] -> not_found;
        [X] -> X
    end.
    
incr(Key) ->
    incr(Key, 1).
incr(Key, Value) ->
    gen_server2:call(?SERVER, {counter, incr, Key, maybe_itl(Value)}).
    
decr(Key) ->
    decr(Key, 1).
decr(Key, Value) ->
    gen_server2:call(?SERVER, {counter, decr, Key, maybe_itl(Value)}).
    

%% Time is the amount of time in seconds
%% the client wishes the server to refuse
%% "add" and "replace" commands with this key.

%%
%% Storage Commands
%%

%% *Flag* is an arbitrary 16-bit unsigned integer (written out in
%% decimal) that the server stores along with the Value and sends back
%% when the item is retrieved.
%%
%% *ExpTime* is expiration time. If it's 0, the item never expires
%% (although it may be deleted from the cache to make place for other
%%  items).
%%
%% *CasUniq* is a unique 64-bit value of an existing entry.
%% Clients should use the value returned from the "gets" command
%% when issuing "cas" updates.
%%
%% *Value* is the value you want to store.

parse_store(["STORED"]) -> ok;
parse_store(["NOT_STORED"]) -> not_stored;
parse_store([X]) -> X.

mutate(Type, Key, Value) ->
    Flag = random:uniform(?RANDOM_MAX),
    mutate(Type, Key, Flag, "0", Value).
mutate(Type, Key, Flag, ExpTime, Value) ->
    Args = {maybe_atl(Key), maybe_itl(Flag), maybe_itl(ExpTime), Value},
    parse_store(gen_server2:call(?SERVER, {mutate, Type, Args})).

%% @doc Store a key/value pair.
set(Key, Value) ->
    mutate(set, Key, Value).
set(Key, Flag, ExpTime, Value) ->
    mutate(set, Key, Flag, ExpTime, Value).

%% @doc Store a key/value pair if it doesn't already exist.
add(Key, Value) ->
    mutate(add, Key, Value).
add(Key, Flag, ExpTime, Value) ->
    mutate(add, Key, Flag, ExpTime, Value).

%% @doc Replace an existing key/value pair.
replace(Key, Value) ->
    mutate(replace, Key, Value).
replace(Key, Flag, ExpTime, Value) ->
    mutate(replace, Key, Flag, ExpTime, Value).

%% @doc Store a key/value pair if possible.
cas(Key, CasUniq, Value) ->
    Flag = random:uniform(?RANDOM_MAX),
    cas(Key, integer_to_list(Flag), "0", CasUniq, Value).

cas(Key, Flag, ExpTime, CasUniq, Value) ->
    Args = {cas, {maybe_atl(Key), maybe_itl(Flag), maybe_itl(ExpTime), maybe_itl(CasUniq), Value}},
    parse_store(gen_server2:call(?SERVER, Args)).
    
%% @doc connect to memcached with defaults
connect() ->
    connect(?DEFAULT_HOST, ?DEFAULT_PORT).    
    
connect(Host, Port) ->
    connect(Host, Port, serializer(default)).

%% @doc connect to memcached
connect(Host, Port, Serializer) ->
    start_link([{endpoint, Host, Port}, Serializer]).  

%% Create a server from an existing connection
create(Socket) ->
    create(Socket, serializer(default)).
    
create(Socket, Serializer) ->
    {ok, Pid} = start_link([{socket, Socket}, Serializer]),
    ok = gen_tcp:controlling_process(Socket, Pid),
    Pid.

%% @private
start_link(Args) ->
    gen_server2:start_link({local, ?SERVER}, ?MODULE, Args, []).

%% @doc disconnect from memcached
disconnect() ->
    {'EXIT', {normal, _}} = (catch gen_server2:call(?SERVER, {stop})),
    ok.

%% @private
init([{endpoint, Host, Port}, Serializer]) ->
    {ok, Socket} = gen_tcp:connect(Host, Port, ?TCP_OPTS),
    {ToBinary, FromBinary} = Serializer,
    {ok, #connection{socket=Socket, to_binary=ToBinary, from_binary=FromBinary}};

init([{socket, Socket}, Serializer]) ->
    {ToBinary, FromBinary} = Serializer,
    {ok, #connection{socket=Socket, to_binary=ToBinary, from_binary=FromBinary}}.

handle_call({stop}, _From, Connection) ->
    {stop, normal, Connection};
    
handle_call({generic, Term}, _From, Connection) ->
    Reply = send_generic_cmd(Connection, atom_to_binary(Term, latin1)),
    {reply, Reply, Connection};
    
handle_call({generic, Term, Arg}, _From, Connection) ->
    Binterm = atom_to_binary(Term, latin1),
    Reply = send_generic_cmd(Connection, iolist_to_binary([Binterm, <<" ">>, Arg])),
    {reply, Reply, Connection};

handle_call({get, Type, Key}, _From, Connection) ->
    Reply = send_get_cmd(Connection, Type, Key),
    {reply, Reply, Connection};

handle_call({delete, {Key, Time}}, _From, Connection) ->
    Reply = send_generic_cmd(
        Connection,
        iolist_to_binary([<<"delete ">>, Key, <<" ">>, Time])
    ),
    {reply, Reply, Connection};
    
handle_call({counter, Type, Key, Value}, _From, Connection) ->
    Cmd = atom_to_binary(Type, latin1),
    [CounterString] = send_generic_cmd(Connection, iolist_to_binary([Cmd, <<" ">>, Key, <<" ">>, Value])),
    {reply, list_to_integer(CounterString), Connection};
    
handle_call({mutate, Type, {Key, Flag, ExpTime, Value}}, _From, Connection) ->
    value_mutate(atom_to_binary(Type, latin1), Key, Flag, ExpTime, Value, [], Connection);

handle_call({cas, {Key, Flag, ExpTime, CasUniq, Value}}, _From, Connection) ->
    value_mutate(<<"cas">>, Key, Flag, ExpTime, Value, [<<" ">>, CasUniq], Connection).

value_mutate(Type, Key, Flag, ExpTime, Value, Extras, Connection) ->
    Bin = (Connection#connection.to_binary)(Value),
    Bytes = integer_to_list(size(Bin)),
    Command = [Type, <<" ">>, Key, <<" ">>, Flag, <<" ">>, ExpTime, <<" ">>, Bytes] ++ Extras,
    Reply = send_storage_cmd(
        Connection,
        iolist_to_binary(Command),
        Bin
    ),
    {reply, Reply, Connection}.

%% @private
handle_cast(_Msg, State) -> {noreply, State}.

%% @private
handle_info(_Info, State) -> {noreply, State}.

%% @private
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% @private
%% @doc Closes the Connection
terminate(_Reason, Connection) ->
    gen_tcp:close(Connection#connection.socket),
    ok.

%% @private
%% @doc send_generic_cmd/2 function for simple informational and deletion commands
send_generic_cmd(Connection, Cmd) ->
    gen_tcp:send(Connection#connection.socket, <<Cmd/binary, "\r\n">>),
    Reply = recv_simple_reply(),
    Reply.

%% @private
%% @doc send_storage_cmd/3 funtion for storage commands
send_storage_cmd(Connection, Cmd, Value) ->
    gen_tcp:send(Connection#connection.socket, <<Cmd/binary, "\r\n">>),
    gen_tcp:send(Connection#connection.socket, <<Value/binary, "\r\n">>),
    Reply = recv_simple_reply(),
       Reply.

%% @private
%% @doc send_get_cmd/2 function for retreival commands
send_get_cmd(Connection, Type, Key) ->
    Message = iolist_to_binary([atom_to_binary(Type, latin1), <<" ">>, Key, <<"\r\n">>]),
    gen_tcp:send(Connection#connection.socket, Message),
    Reply = recv_complex_get_reply(Type, Connection),
    Reply.

%% @private
%% @doc receive function for simple responses (not containing VALUEs)
recv_simple_reply() ->
    receive
          {tcp,_,Data} ->
            string:tokens(binary_to_list(Data), "\r\n");
        {error, closed} ->
              connection_closed
    after ?TIMEOUT -> timeout
    end.

recv_complex_get_reply(Type, Connection) ->
    Socket = Connection#connection.socket,
    receive
        %% For receiving get responses where the key does not exist
        {tcp, Socket, <<"END\r\n">>} -> ["END"];
        %% For receiving get responses containing data
        {tcp, Socket, Data} ->
            case Type of
                get -> 
                    %% Reply format <<"VALUE SOMEKEY FLAG BYTES\r\nSOMEVALUE\r\nEND\r\n">>
                    Parse = io_lib:fread("~s ~s ~u ~u\r\n", binary_to_list(Data)),
                    {ok,[_,_,_,Bytes], ListBin} = Parse,
                    Bin = list_to_binary(ListBin),
                    Reply = get_data(Connection, Bin, Bytes, length(ListBin)),
                    [Reply];
                gets ->
                    %% Reply format <<"VALUE SOMEKEY FLAG BYTES CASUNIQ\r\nSOMEVALUE\r\nEND\r\n">>
                    Parse = io_lib:fread("~s ~s ~u ~u ~u\r\n", binary_to_list(Data)),
                    {ok,[_,_,_,Bytes,CasUniq], ListBin} = Parse,
                    Bin = list_to_binary(ListBin),
                    Reply = get_data(Connection, Bin, Bytes, length(ListBin)),
                    [CasUniq, Reply]
            end;
          {error, closed} ->
              connection_closed
    after ?TIMEOUT -> timeout
    end.

%% @private
%% @doc recieve loop to get all data
get_data(Connection, Bin, Bytes, Len) when Len < Bytes + 7->
    Socket = Connection#connection.socket,
    receive
        {tcp, Socket, Data} ->
            Combined = <<Bin/binary, Data/binary>>,
            get_data(Connection, Combined, Bytes, size(Combined));
         {error, closed} ->
              connection_closed
        after ?TIMEOUT -> timeout
    end;
get_data(Connection, Data, Bytes, _) ->
    <<Bin:Bytes/binary, "\r\nEND\r\n">> = Data,
    (Connection#connection.from_binary)(Bin).
