%%%-------------------------------------------------------------------
%%% @author oem
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 30. Sep 2020 12:23
%%%-------------------------------------------------------------------
-module(mapReduce2).
-author("oem").
-export([start2/2]).
%-include("mapReduce1.erl").
%% API

start2(MainAuthor,PC) ->
  ets:new(etsL1,[set,named_table,public]),
  ets:new(etsL2,[set,named_table,public]),
  ets:new(etsL3,[set,named_table,public]),
  ets:new(tableL1,[set,named_table]),
  ets:new(tableL2,[set,named_table]),
  ets:new(tableL3,[set,named_table]),  %%% MAYBE CHANGE TO BAG - IF YES SO NEED TO CHANGE insertETStoTable FUNCTION (WILL COUNT THE SAME AUTHOR TWICE)
  G = digraph:new(),
  ets:insert(tableL1,[{'A',0},{'B',0},{'C',0},{'D',0},{'E',0},{'F',0},{'G',0},{'H',0},{'I',0},{'J',0},{'K',0},{'L',0},{'M',0},{'N',0},{'O',0},{'P',0},{'Q',0},{'R',0},{'S',0},{'T',0},{'U',0},{'V',0},{'W',0},{'X',0},{'Y',0},{'Z',0}]),
  ets:insert(tableL2,[{'A',0},{'B',0},{'C',0},{'D',0},{'E',0},{'F',0},{'G',0},{'H',0},{'I',0},{'J',0},{'K',0},{'L',0},{'M',0},{'N',0},{'O',0},{'P',0},{'Q',0},{'R',0},{'S',0},{'T',0},{'U',0},{'V',0},{'W',0},{'X',0},{'Y',0},{'Z',0}]),
  ets:insert(tableL3,[{'A',0},{'B',0},{'C',0},{'D',0},{'E',0},{'F',0},{'G',0},{'H',0},{'I',0},{'J',0},{'K',0},{'L',0},{'M',0},{'N',0},{'O',0},{'P',0},{'Q',0},{'R',0},{'S',0},{'T',0},{'U',0},{'V',0},{'W',0},{'X',0},{'Y',0},{'Z',0}]),
  ValuesMain = ets:lookup(authors,MainAuthor),
  Authors = element(2,lists:nth(1,ValuesMain)),   %% ADD TRY CATCH FOR EMPTY LIST
  %% Foreach author insert to etsL1 and spawn findL2
  lists:foreach(fun(X) -> ets:insert(etsL1,{X,atom_to_list(MainAuthor)}),
                          Y = list_to_atom(X),
                          register(mapReduce1:getProcessName(Y), spawn(fun() -> findL2(X,MainAuthor,PC) end)) end,Authors),
  AllChildren = lists:map(fun(X) -> length(element(2,lists:nth(1,ets:lookup(authors,list_to_atom(X))))) end, Authors),
  mapReduce1:gather(lists:sum(AllChildren)), % Count all the children of authors to know when to finish
  TableList1 = ets:tab2list(etsL1),
  TableList2 = ets:tab2list(etsL2),
  TableList3 = ets:tab2list(etsL3),
  digraph:add_vertex(G,atom_to_list(MainAuthor)), % Add main author as vertex
  addVertices(G, TableList1),       % Add all the authors in level L1 as vertex
  addVertices(G, TableList2),       % Add all the authors in level L2 as vertex
  addVertices(G, TableList3),       % Add all the authors in level L3 as vertex
  addEdges(G,TableList1),           % Build edges between L1 authors to the main author
  addEdges(G,TableList2),           % Build edges between L2 authors to their fathers in L1
  addEdges(G,TableList3),           % Build edges between L3 authors to their fathers in L2
  insertETStoTable(TableList1,1),
  insertETStoTable(TableList2,2),
  insertETStoTable(TableList3,3),
  TabL1 = ets:tab2list(tableL1),
  TabL2 = ets:tab2list(tableL2),
  TabL3 = ets:tab2list(tableL3),
  ets:delete(etsL1),
  ets:delete(etsL2),
  ets:delete(etsL3),
  ets:delete(tableL1),
  ets:delete(tableL2),
  ets:delete(tableL3),
  killAll(Authors),
  {G,TabL1,TabL2,TabL3}.


%% NOTE: We want that an author will be in the highest level - if he suppose to be in L1 and L2, he will be in L1
%% We activate gather function on the number of processes from L1. In findL2, if true send a message and count down in the gather
%% If false, create another process, and wait for him to send message after finish the function findL3, and only then count down the gather

findL2(A,MainAuthor,PC) ->
  Values = ets:lookup(authors, list_to_atom(A)),
  Authors = element(2,lists:nth(1,Values)),
  lists:foreach(fun(X) -> MainAuthorCheck = list_to_atom(X) == MainAuthor,
                         case MainAuthorCheck or ets:member(etsL1,X) of  % Check if the author in etsL1 or he is the main author
                            % The author in L1, don't insert again
                            true -> PC ! {"Finish"};
                            %% The author not in L1, insert to etsL2 and spawn findL3
                            false -> ets:insert(etsL2,{X,A}),
                                     CurrPRS = list_to_atom(X ++ integer_to_list(os:system_time(microsecond))),
                                     register(mapReduce1:getProcessName(CurrPRS),spawn(fun() -> findL3(X,MainAuthor,PC) end)) end
                          end, Authors).

findL3(A,MainAuthor,PC) ->
  Values = ets:lookup(authors,list_to_atom(A)),
  Authors = element(2,lists:nth(1,Values)),
  lists:foreach(fun(X) -> MainAuthorCheck = list_to_atom(X) == MainAuthor,
                          case MainAuthorCheck or ets:member(etsL1,X) or ets:member(etsL2,X)  of  % Check if the author in etsL2 or etsL1 or he is the main author
                            % The author in L1, don't insert again
                            true -> do_nothing;
                            %% The author not in L1, insert to etsL2 and spawn findL3
                            false -> ets:insert(etsL3,{X,A}) end
                          end, Authors),
  PC ! {"Finish"}.

%% Count for each level the name of authors that the family name starts in each letter
insertETStoTable([],_) -> do_nothing;
insertETStoTable([H|T],L) ->
  Char = lists:sublist(lists:nth(2,string:tokens(element(1,H),[$ ])),1,1),
  Key = list_to_atom(Char),
  case L of % L represent the number of the level
    1 -> CharInETS = ets:lookup(tableL1,Key);
    2 -> CharInETS = ets:lookup(tableL2,Key);
    3 -> CharInETS = ets:lookup(tableL3,Key)
  end,
  NewVal = element(2,lists:nth(1,CharInETS)) + 1, % Add 1 to the letter counter
  case L of
    1 -> ets:insert(tableL1,{Key,NewVal});
    2 -> ets:insert(tableL2,{Key,NewVal});
    3 -> ets:insert(tableL3,{Key,NewVal})
  end,
  insertETStoTable(T,L).

%% Add vertices to the graph - each vertex is an author from the etsL1/L2/L3 list
addVertices(G,ListETS) ->
  L = lists:map(fun(X) -> element(1,X) end, ListETS),
  [digraph:add_vertex(G,Author) || Author <- L].

%% Connect edges - we get list to tuples {son,father}
addEdges(_,[]) -> do_nothing;
addEdges(G,[H|T]) ->
  digraph:add_edge(G,element(1,H),element(2,H)),
  addEdges(G,T).

%% Kill all processes
killAll([]) -> do_nothing;
killAll([H|T]) ->
  X = list_to_atom(H),
  case whereis(mapReduce1:getProcessName(X)) of
       undefined -> do_nothing;
       PRS -> unregister(mapReduce1:getProcessName(X)),exit(PRS,kill)
  end,
  killAll(T).

