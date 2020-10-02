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
-export([start2/1]).
-include("mapReduce1.erl").
%% API

start2([File]) ->
  file:delete("test.ets"),
  ets:new(authors,[bag,named_table,public]),
  mapReduce1:start1([File]),
  MainAuthor = 'Anthony Hartley',   %%% JUST FOR NOW! NEED TO BE INPUT FROM WX!!!!
  ets:new(etsL1,[set,named_table,public]),
  ets:new(etsL2,[set,named_table,public]),
  ets:new(etsL3,[set,named_table,public]),
  ets:new(tableL1,[set,named_table]),
  ets:new(tableL2,[set,named_table]),
  ets:new(tableL3,[set,named_table]),  %%% MAYBE CHANGE TO BAG - IF YES SO NEED TO CHANGE insertETStoTable FUNCTION (WILL COUNT THE SAME AUTHOR TWICE)
  ets:insert(tableL1,[{'A',0},{'B',0},{'C',0},{'D',0},{'E',0},{'F',0},{'G',0},{'H',0},{'I',0},{'J',0},{'K',0},{'L',0},{'M',0},{'N',0},{'O',0},{'P',0},{'Q',0},{'R',0},{'S',0},{'T',0},{'U',0},{'V',0},{'W',0},{'X',0},{'Y',0},{'Z',0}]),
  ets:insert(tableL2,[{'A',0},{'B',0},{'C',0},{'D',0},{'E',0},{'F',0},{'G',0},{'H',0},{'I',0},{'J',0},{'K',0},{'L',0},{'M',0},{'N',0},{'O',0},{'P',0},{'Q',0},{'R',0},{'S',0},{'T',0},{'U',0},{'V',0},{'W',0},{'X',0},{'Y',0},{'Z',0}]),
  ets:insert(tableL3,[{'A',0},{'B',0},{'C',0},{'D',0},{'E',0},{'F',0},{'G',0},{'H',0},{'I',0},{'J',0},{'K',0},{'L',0},{'M',0},{'N',0},{'O',0},{'P',0},{'Q',0},{'R',0},{'S',0},{'T',0},{'U',0},{'V',0},{'W',0},{'X',0},{'Y',0},{'Z',0}]),
  ValuesMain = ets:lookup(authors,MainAuthor),
  Authors = element(2,lists:nth(1,ValuesMain)),   %% ADD TRY CATCH FOR EMPTY LIST
  %% Foreach author insert to etsL1 and spawn findL2
  lists:foreach(fun(X) -> ets:insert(etsL1,{X,MainAuthor}),
                          Y = list_to_atom(X),
                          register(getProcessName(Y), spawn(fun() -> findL2(X,MainAuthor) end)) end,Authors),
  AllChildren = lists:map(fun(X) -> length(element(2,lists:nth(1,ets:lookup(authors,list_to_atom(X))))) end, Authors),
  mapReduce1:gather(lists:sum(AllChildren)), % Count all the children of authors to know when to finish
  TableList1 = ets:tab2list(etsL1),
  TableList2 = ets:tab2list(etsL2),
  TableList3 = ets:tab2list(etsL3),
  insertETStoTable(TableList1,1),
  insertETStoTable(TableList2,2),
  insertETStoTable(TableList3,3),
  %TabL1 = ets:tab2list(tableL1),
  %TabL2 = ets:tab2list(tableL2),
  %TabL3 = ets:tab2list(tableL3),
  io:format("etsL1: ~p~n",[TableList1]),
  io:format("length etsL1: ~p~n",[length(TableList1)]),
  io:format("etsL2: ~p~n",[TableList2]),
  io:format("length etsL2: ~p~n",[length(TableList2)]),
  io:format("etsL3: ~p~n",[TableList3]),
  io:format("length etsL2: ~p~n",[length(TableList3)]),
  %io:format("Letter count L1: ~p~n",[TabL1]),
  %io:format("Letter count L2: ~p~n",[TabL2]),
  %io:format("Letter count L3: ~p~n",[TabL3]),
  ets:delete(authors),
  ets:delete(etsL1),
  ets:delete(etsL2),
  ets:delete(etsL3),
  ets:delete(tableL1),
  ets:delete(tableL2),
  ets:delete(tableL3),
  killAll(Authors),
  unregister(mainPRS).


%% NOTE: We want that an author will be in the highest level - if he suppose to be in L1 and L2, he will be in L1
%% We activate gather function on the number of processes from L1. In findL2, if true send a message and count down in the gather
%% If false, create another process, and wait for him to send message after finish the function findL3, and only then count down the gather

findL2(A,MainAuthor) ->
  Values = ets:lookup(authors,list_to_atom(A)),
  Authors = element(2,lists:nth(1,Values)),
  lists:foreach(fun(X) -> MainAuthorCheck = list_to_atom(X) == MainAuthor,
                         case MainAuthorCheck or ets:member(etsL1,X) of  % Check if the author in etsL1 or he is the main author
                            % The author in L1, don't insert again
                            true -> mainPRS ! {"Finish"};
                            %% The author not in L1, insert to etsL2 and spawn findL3
                            false -> ets:insert(etsL2,{X,A}),
                                     CurrPRS = list_to_atom(X ++ integer_to_list(os:system_time(microsecond))),
                                     register(getProcessName(CurrPRS),spawn(fun() -> findL3(X,MainAuthor) end)) end
                          end, Authors).

findL3(A,MainAuthor) ->
  Values = ets:lookup(authors,list_to_atom(A)),
  Authors = element(2,lists:nth(1,Values)),
  lists:foreach(fun(X) -> MainAuthorCheck = list_to_atom(X) == MainAuthor,
                          case MainAuthorCheck or ets:member(etsL1,X) or ets:member(etsL2,X)  of  % Check if the author in etsL2 or etsL1 or he is the main author
                            % The author in L1, don't insert again
                            true -> do_nothing;
                            %% The author not in L1, insert to etsL2 and spawn findL3
                            false -> ets:insert(etsL3,{X,A}) end
                          end, Authors),
  mainPRS ! {"Finish"}.

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

%% Kill all processes
killAll([]) -> do_nothing;
killAll([H|T]) ->
  X = list_to_atom(H),
  case whereis(getProcessName(X)) of
       undefined -> do_nothing;
       PRS -> unregister(getProcessName(X)),exit(PRS,kill)
  end,
  killAll(T).

