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
  file:delete("AuthorsTree.png"),
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
  Self = self(),
  MainAuthor = "Anthony Hartley",   %%% JUST FOR NOW! NEED TO BE INPUT FROM WX!!!!
  spawn(fun() -> gen_server:call({local_server,?PC1},[File1,Self,MainAuthor],infinity) end),
  spawn(fun() -> gen_server:call({local_server,?PC2},[File2,Self,MainAuthor],infinity) end),
  spawn(fun() -> gen_server:call({local_server,?PC3},[File3,Self,MainAuthor],infinity) end),
  spawn(fun() -> gen_server:call({local_server,?PC4},[File4,Self,MainAuthor],infinity) end),
  Map = maps:new(),
  AuthorsMap = gatherMaster(4,Map),
  List1 = maps:get("PC1",AuthorsMap),
  List2 = maps:get("PC2",AuthorsMap),
  List3 = maps:get("PC3",AuthorsMap),
  List4 = maps:get("PC4",AuthorsMap),
  ListOfAll1 = orddict:merge(fun(_,X,Y) -> X++Y end, orddict:from_list(List1), orddict:from_list(List2)),
  ListOfAll2 = orddict:merge(fun(_,X,Y) -> X++Y end, orddict:from_list(ListOfAll1), orddict:from_list(List3)),
  ListOfAll = orddict:merge(fun(_,X,Y) -> X++Y end, orddict:from_list(ListOfAll2), orddict:from_list(List4)),
  ets:new(authors,[bag,named_table,public]),
  lists:foreach(fun(X) -> ets:insert(authors,{element(1,X),element(2,X)}) end, ListOfAll),
  io:format("master start Map-Reduce2...~n"),
  {G,TabL1,TabL2,TabL3} = mapReduce2:start2(MainAuthor,self()),
  %io:format("Letters count in L1: ~p~n",[TabL1]),
  %io:format("Letters count in L2: ~p~n",[TabL2]),
  %io:format("Letters count in L3: ~p~n",[TabL3]),
  io:format("master start creating the graph...~n"),
  digraphTographviz(G),
  mapReduce1:gather(1),
  ets:delete(authors),
  io:format("master finish! you can see the graph and tables now!~n"),
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

%% Function that create the final graph with graphviz
digraphTographviz(G) ->
  graphviz:graph("G",self()),
  EdgeList = getEdgesList(G),
  lists:foreach(fun(X) -> FirstName1 = lists:nth(1,string:tokens(element(3,X),[$ ])),                        % First name of author
                          FirstLetFam1= lists:sublist(lists:nth(2,string:tokens(element(3,X),[$ ])),1,1),    % First letter of family name
                          Father = FirstName1 ++ "_" ++ FirstLetFam1,
                          FirstName2 = lists:nth(1,string:tokens(element(2,X),[$ ])),
                          FirstLetFam2= lists:sublist(lists:nth(2,string:tokens(element(2,X),[$ ])),1,1),
                          Son = FirstName2 ++ "_" ++ FirstLetFam2,
                          graphviz:add_edge(Father,Son) end, EdgeList),
  mapReduce1:gather(length(EdgeList)),
  graphviz:to_file("AuthorsTree.png", "png"),
  graphviz:delete().

%% Return the elements of the edges
getEdgesList(G)->
  B=digraph:edges(G),
  [digraph:edge(G,E) || E <- B].

%% Gather function - wait until all processes finish and gather the answers to a map
gatherMaster(0,Map) -> Map;
gatherMaster(N,Map) ->
  receive
    {nodeup,_} -> do_nothing;
    {nodedown,_} -> do_nothing;
    {TableList,Send} ->
      io:format("master got table from ~p...~n",[Send]),
      NewMap = maps:put(Send,TableList,Map),
      gatherMaster(N-1,NewMap)
  end.
