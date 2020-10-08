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
%-include("parse_csv.erl").
-include_lib("stdlib/include/qlc.hrl").
%% API

start1([File],PC,PCNUM) ->
  case PCNUM of
    1 -> Table = authors1;
    2 -> Table = authors2;
    3 -> Table = authors3;
    4 -> Table = authors4
  end,
  CSV = parse_csv:main([File]),
  N = length(CSV),
  NumOfProc = 300,
  RowsPerProc = erlang:trunc(N / NumOfProc),
  ExtraRows = RowsPerProc*NumOfProc < N,
  case ExtraRows of    % In case we have more rows than processes, the last process will have extra rows
    true -> Extra = N - RowsPerProc*NumOfProc ;
    false -> Extra = 0
  end,
  %ets:new(authors,[bag,named_table,public,{write_concurrency,true}]),
  %ets:new(keycounter,[set,named_table,public]),
  case PCNUM of
    1 -> Counter = keycounter1;
    2 -> Counter = keycounter2;
    3 -> Counter = keycounter3;
    4 -> Counter = keycounter4
  end,
  dets:open_file(Counter,[{type,set}]),
  dets:insert(Counter, {count, 0}),
  createProcceses(NumOfProc,RowsPerProc,CSV,0,Extra,PC,Table),
  gather(NumOfProc),               % Gather function - make the program wait until all processes finish
  Temp = element(2,lists:nth(1,dets:lookup(Counter, count))),
  case Temp of                     % If all processes finish - find all the keys in the ets
    NumOfProc -> reducer(keys(PCNUM),PCNUM);
    _ -> do_nothing
  end,
  %{ok,WriteFile} = file:open("test.ets",[write]),         % Create result file
  %TableList = ets:tab2list(authors),
  %write_text(TableList,WriteFile),
  %ets:delete(keycounter),
  dets:delete_all_objects(Counter),
  dets:close(Counter).
  %killAll(NumOfProc,0,PCNUM).

%% Write to etsRes_204265110.ets
%write_text([],_) -> ok;
%write_text([{K,V}|T],WriteFile) ->
%  io:format(WriteFile,"~s ~s~n",[K,V]),
%  write_text(T,WriteFile).

%% Finish creating all the processes
createProcceses(NumOfProc, RowsPerProc, CSV, Curr, Extra, PC, Table) when Curr + 1 =:= NumOfProc  ->               % Create the last process - process number NumOfProc
  %CurrPRS = list_to_atom((integer_to_list(Curr) ++ integer_to_list(PCNUM))),
  %register(getProcessName(CurrPRS), spawn(fun() -> extractAuthors(Curr,RowsPerProc,CSV,Extra,NumOfProc,PC) end));
  spawn(fun() -> extractAuthors(Curr,RowsPerProc,CSV,Extra,NumOfProc,PC,Table) end);
%% Otherwise keep creating the processes
createProcceses(NumOfProc, RowsPerProc, CSV, Curr, Extra, PC,Table) ->                                            % Create process number i, register him as 'pidi'
  %CurrPRS = list_to_atom((integer_to_list(Curr) ++ integer_to_list(PCNUM))),
  %register(getProcessName(CurrPRS), spawn(fun() -> extractAuthors(Curr,RowsPerProc,CSV,Extra,NumOfProc,PC) end)),
  spawn(fun() -> extractAuthors(Curr,RowsPerProc,CSV,Extra,NumOfProc,PC,Table) end),
  createProcceses(NumOfProc, RowsPerProc, CSV, Curr+1, Extra, PC,Table).

%% Return a name represent a process with the index 'Index'
%% Used later to register the PID with this name
getProcessName(Index) when is_number(Index) -> list_to_atom("pid" ++ integer_to_list(Index));
getProcessName(Index) when is_atom(Index) -> list_to_atom("pid" ++ atom_to_list(Index)).

%% Extract the authors from the CSV file rows - each process working on RowsPerProc rows
extractAuthors(Curr,RowsPerProc,CSV,Extra,NumOfProc,PC,Table) ->
  Start_Row = Curr*RowsPerProc+1,
  case Curr + 1 =:= NumOfProc of  % Extra rows for the last process
    true -> End_Row = Curr*RowsPerProc + RowsPerProc + Extra;
    false -> End_Row = Curr*RowsPerProc + RowsPerProc
  end,
  extractAuthors(Start_Row,End_Row,CSV,PC,Table).

extractAuthors(Index,End_Row,_,PC,Table) when Index =:= End_Row + 1->
  case Table of
    authors1 -> Counter = keycounter1;
    authors2 -> Counter = keycounter2;
    authors3 -> Counter = keycounter3;
    authors4 -> Counter = keycounter4
  end,
  dets:update_counter(Counter, count , {2,1}), % Process finish - +1 to counter
  PC ! {"Finish"};                          % Send message to main process for gather function
extractAuthors(Index,End_Row,CSV,PC,Table) ->
  CurrRow = lists:nth(Index,CSV),
  Authors = string:tokens(element(2,CurrRow),[$|]),
  lists:foreach(fun(A) -> authorsToETS(Authors,A,Table) end ,Authors), % For every author in authors insert it to the ets
  extractAuthors(Index+1,End_Row,CSV,PC,Table).

%% Move the authors to ETS table - author A is the key and the rest are values
authorsToETS(Authors,A,Table) ->
  Key = list_to_atom(A),
  %Values = ets:lookup(authors, Key),
  QH = qlc:q([{X,Y} || {X,Y} <- dets:table(Table), is_list(Y) and X == Key]),
  Values = qlc:e(QH),
  dets:delete(Table,Key),
  case Values of
    % The author is new: add him as key and others authors as value
    [] -> Temp = lists:delete(A,Authors),
          dets:insert(Table,{Key,Temp});
    % The author already exist - insert the old and the new values together
    [H|T] -> case T of
               % Values has only one value (=one tuple)
               [] -> Auth = element(2,H),
                    Temp = Auth ++ lists:delete(A,Authors),
                    NewVal = removedup(Temp),
                    dets:insert(Table,{Key,NewVal});
               % Values has only multiple values (=multiple tuples)
               [_] -> NewVal = [],
                    Temp = lists:flatten(lists:map(fun(X) -> NewVal ++ element(2,X) end, Values)),
                    List = removedup(Temp),
                    dets:insert(Table,{Key,List})
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
keys(PCNUM) ->
  case PCNUM of
    1 -> Table = authors1;
    2 -> Table = authors2;
    3 -> Table = authors3;
    4 -> Table = authors4
  end,
  FirstKey = dets:first(Table),
  keys(Table, FirstKey, [FirstKey]).
keys(_TableName, '$end_of_table', ['$end_of_table'|Acc]) ->
  Acc;
keys(TableName, CurrentKey, Acc) ->
  NextKey = dets:next(TableName, CurrentKey),
  keys(TableName, NextKey, [NextKey|Acc]).

%% Check if there are duplicates in the ets keys
reducer([H|T],PCNUM) ->
  case lists:member(H, T) of
    % Found duplication - merge the values into one value and insert
    true -> %Values = ets:lookup(authors,H),
            case PCNUM of
              1 -> Table = authors1;
              2 -> Table = authors2;
              3 -> Table = authors3;
              4 -> Table = authors4
            end,
            QH = qlc:q([{X,Y} || {X,Y} <- dets:table(Table), is_list(Y) and X == H]),
            Values = qlc:e(QH),
            NewVal = [],
            Temp = lists:flatten(lists:map(fun(X) -> NewVal ++ element(2,X) end, Values)),
            List = removedup(Temp),
            dets:delete(Table,H),
            dets:insert(Table,{H,List});
    false -> reducer(T,PCNUM)
  end;
reducer([],_) -> false.

%% Kill all processes
%killAll(NumOfProc,Curr,PCNUM) when Curr + 1 =:= NumOfProc -> CurrPRS = list_to_atom((integer_to_list(Curr) ++ integer_to_list(PCNUM))),
%                                                      case whereis(getProcessName(CurrPRS)) of
%                                                         undefined -> do_nothing;
%                                                         PRS -> unregister(getProcessName(CurrPRS)), exit(PRS,kill)
%                                                       end;
%killAll(NumOfProc,Curr,PCNUM) ->  CurrPRS = list_to_atom((integer_to_list(Curr) ++ integer_to_list(PCNUM))),
%                            case whereis(getProcessName(CurrPRS)) of
%                             undefined -> do_nothing;
%                             PRS -> unregister(getProcessName(CurrPRS)), exit(PRS,kill)
%                           end,
%                          killAll(NumOfProc,Curr+1,PCNUM).