%%%-------------------------------------------------------------------
%%% @author oem
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 27. Sep 2020 14:58
%%%-------------------------------------------------------------------
-module(main).
-author("oem").
-include("parse_csv.erl").
%% API
-export([main/1]).

main([File]) ->
  register(mainPRS,self()),
  CSV = parse_csv:main([File]),
  N = length(CSV),
  NumOfProc = 1000,
  RowsPerProc = erlang:round(N / NumOfProc),
  ets:new(authors,[bag,named_value]),
  createProcceses(NumOfProc,RowsPerProc,CSV,1).


%% Finish creating all the processes
createProcceses(NumOfProc, _, _,Curr) when Curr =:= NumOfProc  ->      % Finish creating all the processes, start sending messages
  do_nothing;                                                          % Master process sending the start message
%% Otherwise keep creating the processes
createProcceses(NumOfProc, RowsPerProc, CSV, Curr) ->                  % Create process number i, register him as 'pidi'
  register(getProcessName(Curr), spawn(fun() -> extractAuthors(Curr,RowsPerProc,CSV) end)),
  createProcceses(NumOfProc, RowsPerProc, CSV, Curr+1).

%% Return a name represent a process with the index 'Index'
%% Used later to register the PID with this name
getProcessName(Index) -> list_to_atom("pid" ++ integer_to_list(Index)).


extractAuthors(Curr,RowsPerProc,CSV) ->
  Start_Row = Curr*RowsPerProc,
  End_Row = Curr*RowsPerProc + RowsPerProc,
  extractAuthors(Start_Row,Start_Row,End_Row,CSV).

extractAuthors(Index,_,End_Row,CSV) when Index =/= End_Row->
  CurrRow = lists:nth(Index,CSV),
  Authors = string:tokens(element(2,CurrRow),[$|]),
  lists:foreach(authorsToETS(Authors),Authors).

authorsToETS(Authors,A) ->
  Key = list_to_atom(A),
  Values = ets:lookup(authors, Key),
  case Values of
    % There author is new: add him as key and others authors as value
    [] -> ets:insert(authors,{Key,list_to_tuple(lists:delete(A,lists:flatten(lists:map(fun(X) -> tuple_to_list(X) end, Authors))))});
    [_] -> Temp = lists:delete(A,lists:flatten(lists:map(fun(X) -> tuple_to_list(X) end, Values))),
           NewVal = sets:to_list(sets:from_list(Temp ++ Authors)),
           ets:insert(authors,{Key,list_to_tuple(NewVal)})
  end.

