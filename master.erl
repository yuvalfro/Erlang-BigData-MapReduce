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
  file:delete("authors1"),
  file:delete("authors2"),
  file:delete("authors3"),
  file:delete("authors4"),
  net_kernel:monitor_nodes(true),
  timer:sleep(200),
  Connect1 = net_kernel:connect_node(?PC1),
  timer:sleep(200),
  Connect2 = net_kernel:connect_node(?PC2),
  timer:sleep(200),
  Connect3 = net_kernel:connect_node(?PC3),
  timer:sleep(200),
  Connect4 = net_kernel:connect_node(?PC4),
  timer:sleep(200),
  PCcounter = counters:new(1,[atomics]),
  put(?PC1,?PC1),
  put(?PC2,?PC2),
  put(?PC3,?PC3),
  put(?PC4,?PC4),
  File1 = "file1.csv",
  File2 = "file2.csv",
  File3 = "file3.csv",
  File4 = "file4.csv",
  Self = self(),
  Map = maps:new(),
  MainAuthor = "Anthony Hartley",   %%% JUST FOR NOW! NEED TO BE INPUT FROM WX!!!!
  % Only if there is connection to the nodes than spawn them
  case Connect1 of
    true ->  spawn(fun() -> gen_server:call({local_server,?PC1},[File1,Self,MainAuthor]) end), counters:add(PCcounter,1,1), M1 =maps:put("PC1",ok,Map);
    false -> M1 =maps:put("PC1",nodedown,Map)
  end,
  case Connect2 of
    true ->  spawn(fun() -> gen_server:call({local_server,?PC2},[File2,Self,MainAuthor]) end), counters:add(PCcounter,1,1), M2 =maps:put("PC2",ok,Map);
    false -> M2 =maps:put("PC2",nodedown,Map)
  end,
  case Connect3 of
    true ->  spawn(fun() -> gen_server:call({local_server,?PC3},[File3,Self,MainAuthor]) end), counters:add(PCcounter,1,1), M3 =maps:put("PC3",ok,Map);
    false -> M3 =maps:put("PC3",nodedown,Map)
  end,
  case Connect4 of
    true ->  spawn(fun() -> gen_server:call({local_server,?PC4},[File4,Self,MainAuthor]) end), counters:add(PCcounter,1,1), M4 =maps:put("PC4",ok,Map);
    false -> M4 =maps:put("PC4",nodedown,Map)
  end,
  % Merge the maps into one map
  M12 = maps:fold(fun(K, V, Map1) -> maps:update_with(K, fun(X) -> X + V end, V, Map1) end, M1, M2),
  M123 = maps:fold(fun(K, V, Map2) -> maps:update_with(K, fun(X) -> X + V end, V, Map2) end, M12, M3),
  Mall = maps:fold(fun(K, V, Map3) -> maps:update_with(K, fun(X) -> X + V end, V, Map3) end, M123, M4),
  % Number of connected nodes
  NumOfPC = counters:get(PCcounter,1),
  AuthorsMap = gatherMaster(NumOfPC,Mall),
  case maps:get("PC1",AuthorsMap) of
    nodedown -> List1 = [];%helpme(MainAuthor,File1,pc1);
    Authors1 -> List1 = Authors1
  end,
  case maps:get("PC2",AuthorsMap) of
    nodedown -> List2 = [];
    Authors2 -> List2 = Authors2
  end,
  case maps:get("PC3",AuthorsMap) of
    nodedown -> List3 = [];
    Authors3 -> List3 = Authors3
  end,
  case maps:get("PC4",AuthorsMap) of
    nodedown -> List4 = [];
    Authors4 -> List4 = Authors4
  end,
  ListOfAll1 = orddict:merge(fun(_,X,Y) -> X++Y end, orddict:from_list(List1), orddict:from_list(List2)),
  ListOfAll2 = orddict:merge(fun(_,X,Y) -> X++Y end, orddict:from_list(ListOfAll1), orddict:from_list(List3)),
  ListOfAll = orddict:merge(fun(_,X,Y) -> X++Y end, orddict:from_list(ListOfAll2), orddict:from_list(List4)),
  ets:new(authors,[bag,named_table,public]),
  lists:foreach(fun(X) -> ets:insert(authors,{element(1,X),element(2,X)}) end, ListOfAll),
  io:format("master start Map-Reduce2...~n"),
  {G,TabL1,TabL2,TabL3} = mapReduce2:start2(MainAuthor,self()),
  L12 = merge(TabL1,TabL2),
  L123 = merge(L12,TabL3),
  L = lists:keysort(1,L123),
  Lnew = lists:map(fun(X) -> {element(1,X),lists:flatten(element(2,X))} end, L),
  Ltop = [{letter,'L1','L2','L3'}],
  FamilyNameData = Ltop ++ Lnew,
  io:format("Data for family name table ~n ~p ~n",[FamilyNameData]),
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
    % nodeup message - keep waiting
    {nodeup,_} -> gatherMaster(N,Map);
    % nodedown message - insert 'nodedown' into map and continue
    {nodedown,LocalServer} ->
      Server = lists:nth(1,string:tokens(atom_to_list(LocalServer),[$@])),
      io:format("Node ~p is down! ~n",[Server]),
      NewMap = maps:put(Server,nodedown,Map),
      gatherMaster(N-1,NewMap);
    % Tablelist message - insert tablelist into map and continue
    {TableList,Send} ->
      io:format("master got table from ~p...~n",[Send]),
      NewMap = maps:put(Send,TableList,Map),
      gatherMaster(N-1,NewMap)
  end.

%% Function to merge the count of family name first letter of each letter into one data structure
merge(In1,In2) ->
  Combined = In1 ++ In2,
  Fun      = fun(Key) -> {Key,proplists:get_all_values(Key,Combined)} end,
  lists:map(Fun,proplists:get_keys(Combined)).


%% Help function - if one of the nodes down, another node will process his data
helpme(MainAuthor,File,PC) ->
  case PC of
    pc1 -> OtherPC = {?PC2,?PC3,?PC4}, file:delete(authors1);
    pc2 -> OtherPC = {?PC1,?PC3,?PC4}, file:delete(authors2);
    pc3 -> OtherPC = {?PC1,?PC2,?PC4}, file:delete(authors3);
    pc4 -> OtherPC = {?PC1,?PC2,?PC3}, file:delete(authors4)
  end,
  FirstPC = element(1,OtherPC),
  SecondPC = element(2,OtherPC),
  ThirdPC = element(3,OtherPC),
  Self = self(),
  FirstAlive = net_kernel:connect_node(FirstPC),
  SecondAlive = net_kernel:connect_node(SecondPC),
  ThirdAlive = net_kernel:connect_node(ThirdPC),
  List = [],
  case FirstAlive of
    false -> CheckNextPC3 = true;
    true -> List = gen_server:call({local_server,FirstPC},[File,Self,MainAuthor],infinity), CheckNextPC3 = false
  end,
  case CheckNextPC3 of
    false -> do_nothing;
    true ->
      case SecondAlive of
        false -> CheckNextPC4 = true;
        true -> List = gen_server:call({local_server,SecondPC},[File,Self,MainAuthor],infinity), CheckNextPC4 = false
      end,
      case CheckNextPC4 of
        false -> do_nothing;
        true ->
          case ThirdAlive of
            false -> do_nothing;
            true -> List = gen_server:call({local_server,ThirdPC},[File,Self,MainAuthor],infinity)
          end
      end
  end,
  List.
