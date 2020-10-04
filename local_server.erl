%%%-------------------------------------------------------------------
%%% @author oem
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 04. Oct 2020 18:11
%%%-------------------------------------------------------------------
-module(local_server).
-author("oem").

-behaviour(gen_server).

%% API
-export([start_link/0,start/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).

-record(local_server_state, {}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Spawns the server and registers the local name (unique)
-spec(start_link() ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

start(Name, File) ->
  gen_server:start({local, Name}, local_server, [File]).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
%% @doc Initializes the server
-spec(init(Args :: term()) ->
  {ok, State :: #local_server_state{}} | {ok, State :: #local_server_state{}, timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).
init([File]) ->
  ets:new(authors,[bag,named_table,public,{write_concurrency,true}]),
  mapReduce1:start1([File]),
  TableList = ets:tab2list(authors),
  {TableList,done}.

%% @private
%% @doc Handling call messages
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #local_server_state{}) ->
  {reply, Reply :: term(), NewState :: #local_server_state{}} |
  {reply, Reply :: term(), NewState :: #local_server_state{}, timeout() | hibernate} |
  {noreply, NewState :: #local_server_state{}} |
  {noreply, NewState :: #local_server_state{}, timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: #local_server_state{}} |
  {stop, Reason :: term(), NewState :: #local_server_state{}}).
handle_call(_Request, _From, State = #local_server_state{}) ->
  {reply, ok, State}.

%% @private
%% @doc Handling cast messages
-spec(handle_cast(Request :: term(), State :: #local_server_state{}) ->
  {noreply, NewState :: #local_server_state{}} |
  {noreply, NewState :: #local_server_state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #local_server_state{}}).
handle_cast(_Request, State = #local_server_state{}) ->
  {noreply, State}.

%% @private
%% @doc Handling all non call/cast messages
-spec(handle_info(Info :: timeout() | term(), State :: #local_server_state{}) ->
  {noreply, NewState :: #local_server_state{}} |
  {noreply, NewState :: #local_server_state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #local_server_state{}}).
handle_info(_Info, State = #local_server_state{}) ->
  {noreply, State}.

%% @private
%% @doc This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #local_server_state{}) -> term()).
terminate(_Reason, _State = #local_server_state{}) ->
  ok.

%% @private
%% @doc Convert process state when code is changed
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #local_server_state{},
    Extra :: term()) ->
  {ok, NewState :: #local_server_state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State = #local_server_state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
