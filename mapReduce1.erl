%%%-------------------------------------------------------------------
%%% @author oem
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 27. Sep 2020 14:58
%%%-------------------------------------------------------------------
-module('mapReduce1').
-author("oem").
-export([start1/3,gather/1,getProcessName/1]).
-include("parse_csv.erl").
%% API

start1([File],PC,PCNUM) ->
  %register(mainPRS,self()),
  CSV = parse_csv:main([File]),
  N = length(CSV),
  NumOfProc = 200,
  RowsPerProc = erlang:trunc(N / NumOfProc),
  ExtraRows = RowsPerProc*NumOfProc < N,
  case ExtraRows of    % In case we have more rows than processes, the last process will have extra rows
    true -> Extra = N - RowsPerProc*NumOfProc ;
    false -> Extra = 0
  end,
  %ets:new(authors,[bag,named_table,public,{write_concurrency,true}]),
  ets:new(keycounter,[set,named_table,public]),
  ets:insert(keycounter, {count, 0}),
  createProcceses(NumOfProc,RowsPerProc,CSV,0,Extra,PC,PCNUM),
  gather(NumOfProc),               % Gather function - make the program wait until all processes finish
  Temp = element(2,lists:nth(1,ets:lookup(keycounter, count))),
  case Temp of                     % If all processes finish - find all the keys in the ets
    NumOfProc -> reducer(keys());
    _ -> do_nothing
  end,
  {ok,WriteFile} = file:open("test.ets",[write]),         % Create result file
  TableList = ets:tab2list(authors),
  write_text(TableList,WriteFile),
 % ets:delete(authors),
  ets:delete(keycounter),
  killAll(NumOfProc,0,PCNUM).
  %unregister(mainPRS).

%% Write to etsRes_204265110.ets
write_text([],_) -> ok;
write_text([{K,V}|T],WriteFile) ->
  io:format(WriteFile,"~s ~s~n",[K,V]),
  write_text(T,WriteFile).

%% Finish creating all the processes
createProcceses(NumOfProc, RowsPerProc, CSV, Curr, Extra, PC, PCNUM) when Curr + 1 =:= NumOfProc  ->               % Create the last process - process number NumOfProc
  CurrPRS = list_to_atom((integer_to_list(Curr) ++ integer_to_list(PCNUM))),
  register(getProcessName(CurrPRS), spawn(fun() -> extractAuthors(Curr,RowsPerProc,CSV,Extra,NumOfProc,PC) end));
%% Otherwise keep creating the processes
createProcceses(NumOfProc, RowsPerProc, CSV, Curr, Extra, PC, PCNUM) ->                                            % Create process number i, register him as 'pidi'
  CurrPRS = list_to_atom((integer_to_list(Curr) ++ integer_to_list(PCNUM))),
  register(getProcessName(CurrPRS), spawn(fun() -> extractAuthors(Curr,RowsPerProc,CSV,Extra,NumOfProc,PC) end)),
  createProcceses(NumOfProc, RowsPerProc, CSV, Curr+1, Extra, PC, PCNUM).

%% Return a name represent a process with the index 'Index'
%% Used later to register the PID with this name
getProcessName(Index) when is_number(Index) -> list_to_atom("pid" ++ integer_to_list(Index));
getProcessName(Index) when is_atom(Index) -> list_to_atom("pid" ++ atom_to_list(Index)).

%% Extract the authors from the CSV file rows - each process working on RowsPerProc rows
extractAuthors(Curr,RowsPerProc,CSV,Extra,NumOfProc,PC) ->
  Start_Row = Curr*RowsPerProc+1,
  case Curr + 1 =:= NumOfProc of  % Extra rows for the last process
    true -> End_Row = Curr*RowsPerProc + RowsPerProc + Extra;
    false -> End_Row = Curr*RowsPerProc + RowsPerProc
  end,
  extractAuthors(Start_Row,End_Row,CSV,PC).

extractAuthors(Index,End_Row,_,PC) when Index =:= End_Row + 1->
  ets:update_counter(keycounter, count , {2,1}), % Process finish - +1 to counter
  PC ! {"Finish"};                          % Send message to main process for gather function
extractAuthors(Index,End_Row,CSV,PC) ->
  CurrRow = lists:nth(Index,CSV),
  Authors = string:tokens(element(2,CurrRow),[$|]),
  lists:foreach(fun(A) -> authorsToETS(Authors,A) end ,Authors), % For every author in authors insert it to the ets
  extractAuthors(Index+1,End_Row,CSV,PC).

%% Move the authors to ETS table - author A is the key and the rest are values
authorsToETS(Authors,A) ->
  Key = list_to_atom(A),
  Values = ets:lookup(authors, Key),
  ets:delete(authors,Key),
  case Values of
    % The author is new: add him as key and others authors as value
    [] -> Temp = lists:delete(A,Authors),
          ets:insert(authors,{Key,Temp});
    % The author already exist - insert the old and the new values together
    [H|T] -> case T of
               % Values has only one value (=one tuple)
               [] -> Auth = element(2,H),
                    Temp = Auth ++ lists:delete(A,Authors),
                    NewVal = removedup(Temp),
                    ets:insert(authors,{Key,NewVal});
               % Values has only multiple values (=multiple tuples)
               [_] -> NewVal = [],
                    Temp = lists:flatten(lists:map(fun(X) -> NewVal ++ element(2,X) end, Values)),
                    List = removedup(Temp),
                    ets:insert(authors,{Key,List})
             end
  end.

%% Gather function - wait until all processes finish
gather(0) -> do_nothing;
gather(N) ->
  receive
    {"Finish"} -> gather(N-1)
  end.

%% Remove duplicates from list
removedup([]) -> [];
removedup([H|T]) -> [H | [X || X <- removedup(T), X =/= H]].

%% Get a list of the keys in the ets
keys() ->
  FirstKey = ets:first(authors),
  keys(authors, FirstKey, [FirstKey]).
keys(_TableName, '$end_of_table', ['$end_of_table'|Acc]) ->
  Acc;
keys(TableName, CurrentKey, Acc) ->
  NextKey = ets:next(TableName, CurrentKey),
  keys(TableName, NextKey, [NextKey|Acc]).

%% Check if there are duplicates in the ets keys
reducer([H|T]) ->
  case lists:member(H, T) of
    % Found duplication - merge the values into one value and insert
    true -> Values = ets:lookup(authors,H),
            NewVal = [],
            Temp = lists:flatten(lists:map(fun(X) -> NewVal ++ element(2,X) end, Values)),
            List = removedup(Temp),
            ets:delete(authors,H),
            ets:insert(authors,{H,List});
    false -> reducer(T)
  end;
reducer([]) -> false.

%% Kill all processes
killAll(NumOfProc,Curr,PCNUM) when Curr + 1 =:= NumOfProc -> CurrPRS = list_to_atom((integer_to_list(Curr) ++ integer_to_list(PCNUM))),
                                                      case whereis(getProcessName(CurrPRS)) of
                                                         undefined -> do_nothing;
                                                         PRS -> unregister(getProcessName(CurrPRS)), exit(PRS,kill)
                                                       end;
killAll(NumOfProc,Curr,PCNUM) ->  CurrPRS = list_to_atom((integer_to_list(Curr) ++ integer_to_list(PCNUM))),
                            case whereis(getProcessName(CurrPRS)) of
                             undefined -> do_nothing;
                             PRS -> unregister(getProcessName(CurrPRS)), exit(PRS,kill)
                           end,
                          killAll(NumOfProc,Curr+1,PCNUM).