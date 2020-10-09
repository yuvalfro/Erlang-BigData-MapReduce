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

-define(SERVER, ?MODULE).
-define(Master, 'master@127.0.0.1').

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
  file:delete("authors1"),
  file:delete("authors2"),
  file:delete("authors3"),
  file:delete("authors4"),
  file:delete("keycounter1"),
  file:delete("keycounter2"),
  file:delete("keycounter3"),
  file:delete("keycounter4"),
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
handle_call([File,PC,MainAuthor], _, State = #local_server_state{}) ->
  case File of
    "file1.csv" -> PCNUM = 1, Table = authors1;
    "file2.csv" -> PCNUM = 2, Table = authors2;
    "file3.csv" -> PCNUM = 3, Table = authors3;
    "file4.csv" -> PCNUM = 4, Table = authors4
  end,
  %ets:new(authors,[bag,named_table,public]),
  dets:open_file(Table, [{type, bag}]),
  dets:safe_fixtable(Table, true),
  io:format("PC~p start Map-Reduce1...~n",[PCNUM]),
  %mapReduce1:start1([File],self(),PCNUM),
  mapReduce1Ver2:startMR(File,self(),PCNUM,MainAuthor),
  %TableList = ets:tab2list(authors),
  QH = qlc:q([{X,Y} || {X,Y} <- dets:table(Table), is_list(Y)]),
  TableList = qlc:e(QH),
  io:format("table list ~p~n",[TableList]),
  %ets:delete(authors),
  %dets:delete_all_objects(authors),
  %dets:close(authors),
  case File of
    "file1.csv" -> dets:delete_all_objects(authors1), dets:close(authors1), Send = "PC1";
    "file2.csv" -> dets:delete_all_objects(authors2), dets:close(authors2), Send = "PC2";
    "file3.csv" -> dets:delete_all_objects(authors3), dets:close(authors3), Send = "PC3";
    "file4.csv" -> dets:delete_all_objects(authors4), dets:close(authors4), Send = "PC4"
  end,
  PC ! {TableList, Send},
  io:format("Finish creating table...~n"),
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
