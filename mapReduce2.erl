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
  mapReduce1:start1([File]),
  MainAuthor = 'Anthony Hartley',   %%% JUST FOR NOW! NEED TO BE INPUT FROM WX!!!!
  ets:new(etsL1,[set,named_table,public]),
  ets:new(etsL2,[set,named_table,public]),
  ets:new(etsL3,[set,named_table,public]),
  ValuesMain = ets:lookup(authors,MainAuthor),
  Authors = element(2,lists:nth(1,ValuesMain)),   %% ADD TRY CATCH FOR EMPTY LIST
  %% Foreach author insert to etsL1 and spawn findL2
  lists:foreach(fun(X) -> ets:insert(etsL1,{X,MainAuthor}), Y = list_to_atom(X), register(getProcessName(Y), spawn(fun() -> findL2(X,MainAuthor) end)) end,Authors),
  mapReduce1:gather(length(Authors)),
  TableList1 = ets:tab2list(etsL1),
  TableList2 = ets:tab2list(etsL2),
  TableList3 = ets:tab2list(etsL3),
  io:format("etsL1: ~p~n",[TableList1]),
  io:format("etsL2: ~p~n",[TableList2]),
  io:format("etsL3: ~p~n",[TableList3]),
  ets:delete(authors),
  ets:delete(etsL1),
  ets:delete(etsL2),
  ets:delete(etsL3),
  unregister(mainPRS).

%% NOTE: We want that an author will be in the highest level - if he suppose to be in L1 and L2, he will be in L1

findL2(A,MainAuthor) ->
  Values = ets:lookup(authors,list_to_atom(A)),
  Authors = element(2,lists:nth(1,Values)),
  lists:foreach(fun(X) -> MainAuthorCheck = list_to_atom(X) == MainAuthor,
                         case MainAuthorCheck or ets:member(etsL1,X) of  % Check if the author in etsL1 or he is the main author
                            % The author in L1, don't insert again
                            true -> mainPRS ! {"Finish"};
                            %% The author not in L1, insert to etsL2 and spawn findL3
                            false -> ets:insert(etsL2,{X,A}), spawn(fun() -> findL3(A,X,MainAuthor) end) end
                          end, Authors),
  receive
    {"Finish"} -> mainPRS ! {"Finish"}
  end.

findL3(L1A,A,MainAuthor) ->
  Values = ets:lookup(authors,list_to_atom(A)),
  Authors = element(2,lists:nth(1,Values)),
  lists:foreach(fun(X) -> MainAuthorCheck = list_to_atom(X) == MainAuthor,
                          case MainAuthorCheck or ets:member(etsL1,X) or ets:member(etsL2,X)  of  % Check if the author in etsL2 or etsL1 or he is the main author
                            % The author in L1, don't insert again
                            true -> do_nothing;
                            %% The author not in L1, insert to etsL2 and spawn findL3
                            false -> ets:insert(etsL3,{X,A}) end
                          end, Authors),
  X = list_to_atom(L1A),
  getProcessName(X) ! {"Finish"}.

