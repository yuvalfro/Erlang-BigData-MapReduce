%%%-------------------------------------------------------------------
%%% @author oem
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 02. Oct 2020 10:21
%%%-------------------------------------------------------------------
-module(server).
-author("oem").

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-include("mapReduce2.erl").

-define(SERVER, ?MODULE).

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
  ets:new(authors,[bag,named_table,public]),
  mapReduce2:start2(["output_example.csv"]),
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
