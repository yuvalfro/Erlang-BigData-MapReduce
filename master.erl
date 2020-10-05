%%%-------------------------------------------------------------------
%%% @author oem
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 02. Oct 2020 10:21
%%%-------------------------------------------------------------------
-module(master).
-author("oem").

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-include("mapReduce2.erl").

-define(SERVER, ?MODULE).
-define(PC1, 'PC1@127.0.0.1').
-define(PC2, 'PC2@127.0.0.1').
-define(PC3, 'PC3@127.0.0.1').
-define(PC4, 'PC4@127.0.0.1').

-record(gen_server_state, {}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Spawns the server and registers the local name (unique)
-spec(start_link() ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
  gen_server:start_link({global, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
%% @doc Initializes the server
-spec(init(Args :: term()) ->
  {ok, State :: #gen_server_state{}} | {ok, State :: #gen_server_state{}, timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).
init([]) ->
  net_kernel:monitor_nodes(true),
  timer:sleep(200),
  net_kernel:connect_node(?PC1),
  timer:sleep(200),
  net_kernel:connect_node(?PC2),
  timer:sleep(200),
  net_kernel:connect_node(?PC3),
  timer:sleep(200),
  net_kernel:connect_node(?PC4),
  timer:sleep(200),
  put(?PC1,?PC1),
  put(?PC2,?PC2),
  put(?PC3,?PC3),
  put(?PC4,?PC4),
  File1 = "file1.csv",
  File2 = "file2.csv",
  File3 = "file3.csv",
  File4 = "file4.csv",
  ListPC1 = gen_server:call({local_server,?PC1},File1,infinity),
  io:format("master got table from PC1...~n"),
  ListPC2 = gen_server:call({local_server,?PC2},File2,infinity),
  io:format("master got table from PC2...~n"),
  ListPC3 = gen_server:call({local_server,?PC3},File3,infinity),
  io:format("master got table from PC3...~n"),
  ListPC4 = gen_server:call({local_server,?PC4},File4,infinity),
  io:format("master got table from PC4...~n"),
  ListOfAll1 = orddict:merge(fun(_,X,Y) -> X++Y end, orddict:from_list(ListPC1), orddict:from_list(ListPC2)),
  ListOfAll2 = orddict:merge(fun(_,X,Y) -> X++Y end, orddict:from_list(ListOfAll1), orddict:from_list(ListPC3)),
  ListOfAll = orddict:merge(fun(_,X,Y) -> X++Y end, orddict:from_list(ListOfAll2), orddict:from_list(ListPC4)),
  io:format("List of all ~p~n",[ListOfAll]),
  %ets:new(authors,[bag,named_table,public]),
  %mapReduce2:start2(["output_example.csv"]),
  {ok, #gen_server_state{}}.

%% @private
%% @doc Handling call messages
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #gen_server_state{}) ->
  {reply, Reply :: term(), NewState :: #gen_server_state{}} |
  {reply, Reply :: term(), NewState :: #gen_server_state{}, timeout() | hibernate} |
  {noreply, NewState :: #gen_server_state{}} |
  {noreply, NewState :: #gen_server_state{}, timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: #gen_server_state{}} |
  {stop, Reason :: term(), NewState :: #gen_server_state{}}).
handle_call(_Request, _From, State = #gen_server_state{}) ->
  {reply, ok, State}.

%% @private
%% @doc Handling cast messages
-spec(handle_cast(Request :: term(), State :: #gen_server_state{}) ->
  {noreply, NewState :: #gen_server_state{}} |
  {noreply, NewState :: #gen_server_state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #gen_server_state{}}).
handle_cast(_Request, State = #gen_server_state{}) ->
  {noreply, State}.

%% @private
%% @doc Handling all non call/cast messages
-spec(handle_info(Info :: timeout() | term(), State :: #gen_server_state{}) ->
  {noreply, NewState :: #gen_server_state{}} |
  {noreply, NewState :: #gen_server_state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #gen_server_state{}}).
handle_info(_Info, State = #gen_server_state{}) ->
  {noreply, State}.

%% @private
%% @doc This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #gen_server_state{}) -> term()).
terminate(_Reason, _State = #gen_server_state{}) ->
  ok.

%% @private
%% @doc Convert process state when code is changed
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #gen_server_state{},
    Extra :: term()) ->
  {ok, NewState :: #gen_server_state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State = #gen_server_state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================