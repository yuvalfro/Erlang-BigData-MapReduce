%%%-------------------------------------------------------------------
%%% @author oem
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 09. Oct 2020 10:14
%%%-------------------------------------------------------------------
-module(mapReduce1Ver2).
-author("oem").

%% API
-export([startMR/4]).
-include_lib("stdlib/include/qlc.hrl").

startMR(File,PC,PCNUM,MainAuthor) ->
  case PCNUM of
    1 -> Table = authors1;
    2 -> Table = authors2;
    3 -> Table = authors3;
    4 -> Table = authors4
  end,
  dets:open_file(Table, [{type, bag}]),
  %MainAuthor = "Pierangela Samarati",
  CSV = parse_csv:main([File]),
  % Create a list of all articles that main author appear in
  ListofMA = [Authors || Authors <- CSV, lists:member(MainAuthor,string:tokens(element(2,Authors),[$|]))],
  % Extract from each article only authors names
  ListofAllAuthors = lists:map(fun(X) -> string:tokens(element(2,X),[$|]) end,ListofMA),
  % Create one list (instead of list of lists) and remove duplicates
  QH = qlc:q([X || X <- qlc:append(ListofAllAuthors)], {unique, true}),
  ListOfAuthors = qlc:e(QH),
  % create list without the main authors - this list is the value in the table
  ListNoMA = lists:delete(MainAuthor,ListOfAuthors),
  dets:insert(Table,{MainAuthor,ListNoMA}),
  NumOfProc = length(ListNoMA),
  createProcceses(NumOfProc,CSV,1,PC,Table,ListNoMA,2),
  gather(NumOfProc).

%% Finish creating all the processes
createProcceses(NumOfProc, CSV, Curr, PC, Table,ListNoMA, Level) when Curr =:= NumOfProc  ->
  case length(ListNoMA) of
    0 -> do_nothing;
    _ -> Author = lists:nth(Curr,ListNoMA),
         spawn(fun() -> authorsToDETS(CSV,PC,Table,Author, Level) end)
  end;
%% Otherwise keep creating the processes
createProcceses(NumOfProc, CSV, Curr, PC, Table, ListNoMA, Level) ->
  case length(ListNoMA) of
    0 -> do_nothing;
    _ -> Author = lists:nth(Curr,ListNoMA),
         spawn(fun() -> authorsToDETS(CSV,PC,Table,Author, Level) end),
         createProcceses(NumOfProc, CSV, Curr+1, PC, Table, ListNoMA, Level)
  end.

%% We repeat the same steps that we did for the main author
authorsToDETS(CSV,PC,Table,Author,Level) ->
  ListofAuthor = [Authors || Authors <- CSV, lists:member(Author,string:tokens(element(2,Authors),[$|]))],
  ListofAllAuthors = lists:map(fun(X) -> string:tokens(element(2,X),[$|]) end,ListofAuthor),
  QH = qlc:q([X || X <- qlc:append(ListofAllAuthors)], {unique, true}),
  ListOfAuthors = qlc:e(QH),
  ListNoAuthor = lists:delete(Author,ListOfAuthors),
  dets:insert(Table,{Author,ListNoAuthor}),
  case Level of
    2 -> NumOfProc = length(ListNoAuthor),
         createProcceses(NumOfProc,CSV,1,PC,Table,ListNoAuthor,3),
         gather((length(ListNoAuthor)));
    3 -> do_nothing
  end,
  PC ! {"Finish"}.

%% Gather function - wait until all processes finish
gather(0) -> do_nothing;
gather(N) ->
  receive
    {"Finish"} -> gather(N-1)
  end.