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
-export([start_link/0,start/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).
-include_lib("stdlib/include/qlc.hrl").
%-include("mapReduce1.erl").

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

start(Name) ->
  io:format("Start ~n"),
  ets:delete(authors),
  gen_server:start({local, Name}, local_server, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
%% @doc Initializes the server
-spec(init(Args :: term()) ->
  {ok, State :: #local_server_state{}} | {ok, State :: #local_server_state{}, timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).
init([]) ->
  {ok, #local_server_state{}}.

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
handle_call(File, _, State = #local_server_state{}) ->
  case File of
    "file1.csv" -> PCNUM = 1;
    "file2.csv" -> PCNUM = 2;
    "file3.csv" -> PCNUM = 3;
    "file4.csv" -> PCNUM = 4
  end,
  %ets:new(authors,[bag,named_table,public]),
  dets:open_file(authors, [{type, bag}]),
  io:format("PC~p start Map-Reduce1...~n",[PCNUM]),
  mapReduce1:start1([File],self(),PCNUM),
  %TableList = ets:tab2list(authors),
  QH3 = qlc:q([{X,Y} || {X,Y} <- dets:table(authors), is_list(Y)]),
  TableList = qlc:e(QH3),
  io:format("Finish creating table...~n"),
  %ets:delete(authors),
  dets:delete_all_objects(authors),
  dets:close(authors),
  {reply, TableList, State}.

%handle_call(_Request, _From, State = #local_server_state{}) ->
 % {reply, ok, State}.

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
