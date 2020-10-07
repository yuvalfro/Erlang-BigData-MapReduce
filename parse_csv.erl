%!/usr/bin/env escript
-module(parse_csv).
-export([main/1]).
-record(ecsv,{
state = field_start, %%field_start|normal|quoted|post_quoted
cols = undefined, %%how many fields per record
current_field = [],
current_record = [],
fold_state,
fold_fun %%user supplied fold function
}).

main([File]) ->
	Rows = parse_file(File),
	Rows.
	%print_rows(Rows,1);

%print_rows([],_) ->
%	ok;
%print_rows([Row|T],Num) ->
%	Authors = string:tokens(element(2,Row),[$|]),
%	io:format("The authors in row ~p are: ~ts~n", [Num,list_to_binary(Authors)]),
%	print_rows(T,Num+1).

%% ——— Exported ——————————
%parse_file(FileName,InitialState,Fun) ->
%   {ok, Binary} = file:read_file(FileName),
%    parse(Binary,InitialState,Fun).

parse_file(FileName)  ->
   {ok, Binary} = file:read_file(FileName),
    parse(Binary).

parse(X) ->
   R = parse(X,[],fun(Fold,Record) -> [Record|Fold] end),
   lists:reverse(R).
		
parse(X,InitialState,Fun) ->
   do_parse(X,#ecsv{fold_state=InitialState,fold_fun = Fun}).

%% --------- Field_start state ---------------------
%%whitespace, loop in field_start state
do_parse(<<32,Rest/binary>>,S = #ecsv{state=field_start,current_field=Field})->		
	do_parse(Rest,S#ecsv{current_field=[32|Field]});

%%its a quoted field, discard previous whitespaces		
do_parse(<<$",Rest/binary>>,S = #ecsv{state=field_start})->		
	do_parse(Rest,S#ecsv{state=quoted,current_field=[]});

%%anything else, is a unquoted field		
do_parse(Bin,S = #ecsv{state=field_start})->
	do_parse(Bin,S#ecsv{state=normal});	
		
		
%% --------- Quoted state ---------------------	
%%Escaped quote inside a quoted field	
do_parse(<<$",$",Rest/binary>>,S = #ecsv{state=quoted,current_field=Field})->
	do_parse(Rest,S#ecsv{current_field=[$"|Field]});		
	
%%End of quoted field
do_parse(<<$",Rest/binary>>,S = #ecsv{state=quoted})->
	do_parse(Rest,S#ecsv{state=post_quoted});
	
%%Anything else inside a quoted field
do_parse(<<X,Rest/binary>>,S = #ecsv{state=quoted,current_field=Field})->
	do_parse(Rest,S#ecsv{current_field=[X|Field]});
	
do_parse(<<>>, #ecsv{state=quoted})->
	throw({ecsv_exception,unclosed_quote});
	
	
%% --------- Post_quoted state ---------------------		
%%consume whitespaces after a quoted field	
do_parse(<<32,Rest/binary>>,S = #ecsv{state=post_quoted})->	
	do_parse(Rest,S);


%%---------Comma and New line handling. ------------------
%%---------Common code for post_quoted and normal state---

%%EOF in a new line, return the records
do_parse(<<>>, #ecsv{current_record=[],fold_state=State})->
	State;
%%EOF in the last line, add the last record and continue
do_parse(<<>>,S)->	
	do_parse([],new_record(S));

%% skip carriage return (windows files uses CRLF)
do_parse(<<$\r,Rest/binary>>,S = #ecsv{})->
	do_parse(Rest,S);		
		
%% new record
do_parse(<<$\n,Rest/binary>>,S = #ecsv{}) ->	
	do_parse(Rest,new_record(S));
	
do_parse(<<$, ,Rest/binary>>,S = #ecsv{current_field=Field,current_record=Record})->	
	do_parse(Rest,S#ecsv{state=field_start,
					  current_field=[],
					  current_record=[lists:reverse(Field)|Record]});


%%A double quote in any other place than the already managed is an error
do_parse(<<$",_Rest/binary>>, #ecsv{})->
	throw({ecsv_exception,bad_record});
	
%%Anything other than whitespace or line ends in post_quoted state is an error
do_parse(<<_X,_Rest/binary>>, #ecsv{state=post_quoted})->
 	throw({ecsv_exception,bad_record});

%%Accumulate Field value
do_parse(<<X,Rest/binary>>,S = #ecsv{state=normal,current_field=Field})->
	do_parse(Rest,S#ecsv{current_field=[X|Field]}).

%%check	the record size against the previous, and actualize state.
new_record(S=#ecsv{cols=Cols,current_field=Field,current_record=Record,fold_state=State,fold_fun=Fun}) ->
	NewRecord = list_to_tuple(lists:reverse([lists:reverse(Field)|Record])),
	if
		(tuple_size(NewRecord) =:= Cols) or (Cols =:= undefined) ->
			NewState = Fun(State,NewRecord),
			S#ecsv{state=field_start,cols=tuple_size(NewRecord),
					current_record=[],current_field=[],fold_state=NewState};
		
		(tuple_size(NewRecord) =/= Cols) ->
			io:format("~p~n~p~n",[tuple_size(NewRecord),Cols]),
			throw({ecsv_exception,bad_record_size})
	end.
