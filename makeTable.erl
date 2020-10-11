%%%-------------------------------------------------------------------
%%% @author yarden
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 06. Oct 2020 14:26
%%%-------------------------------------------------------------------

-module(makeTable).

%-compile(export_all).
-export([start/1]).

-include_lib("wx/include/wx.hrl").
start(List) ->
	Wx = wx:new(),
	Frame = wxFrame:new(Wx, ?wxID_ANY, "Family name table", [{pos,{300,200}},{size,{200,200}}]),
	Text = "         This table shows how much authors family
	names start with each letter in each level",
	Label = wxStaticText:new(Frame, 2, Text, [{style, ?wxALIGN_CENTER_HORIZONTAL}]),

	Panel = wxPanel:new(Frame, []),
	
    %% Setup sizers
    MainSizer = wxBoxSizer:new(?wxVERTICAL),
    Sizer = wxStaticBoxSizer:new(?wxVERTICAL, Panel),
    Grid = create_grid(Panel,List),
    Options = [{flag, ?wxEXPAND}, {proportion, 1}],
   	wxSizer:add(Sizer, Label, [{flag, ?wxALL bor ?wxALIGN_CENTRE}, {border, 15}]),
    wxSizer:add(Sizer, Grid, Options),
    wxSizer:add(MainSizer, Sizer, Options),
    wxPanel:setSizer(Panel, MainSizer),
    wxSizer:fit(Sizer, Panel),
    wxFrame:fit(Frame),
    wxFrame:show(Frame).

create_grid(Panel,List) ->
    %% Create the grid with 5 * 5 cells
    Grid = wxGrid:new(Panel, 2, []),
    wxGrid:createGrid(Grid, 26, 3),
	%set backround
	%doFontR(Grid, 0, 0),
	%doFontR(Grid, 1, 0),
	%doFontR(Grid, 2, 0),
	%doFontR(Grid, 3, 0),
	%doFontR(Grid, 4, 0),
        
	wxGrid:setColLabelValue(Grid, 0, "L1"),
	wxGrid:setColLabelValue(Grid, 1, "L2"),
	wxGrid:setColLabelValue(Grid, 2, "L3"),
  	%set valus from listOfTuple


  	WithIndex = lists:zip(lists:seq(1, length(List)), List), %make list with index
lists:foreach(fun(X) -> L1 = lists:nth(1,element(2,element(2,X))), L2 = lists:nth(2,element(2,element(2,X))), 				L3 = lists:nth(3,element(2,element(2,X))),
			setValue(Grid,element(1,X),L1,L2,L3) end, WithIndex),

	timer:sleep(50),

	wxGrid:setRowLabelValue(Grid, 0, "A"),
	wxGrid:setRowLabelValue(Grid, 1, "B"),
	wxGrid:setRowLabelValue(Grid, 2, "C"),
	wxGrid:setRowLabelValue(Grid, 3, "D"),
	wxGrid:setRowLabelValue(Grid, 4, "E"),
	wxGrid:setRowLabelValue(Grid, 5, "F"),
	wxGrid:setRowLabelValue(Grid, 6, "G"),
	wxGrid:setRowLabelValue(Grid, 7, "H"),
	wxGrid:setRowLabelValue(Grid, 8, "I"),
	wxGrid:setRowLabelValue(Grid, 9, "J"),
	wxGrid:setRowLabelValue(Grid, 10, "K"),
	wxGrid:setRowLabelValue(Grid, 11, "L"),
	wxGrid:setRowLabelValue(Grid, 12, "M"),
	wxGrid:setRowLabelValue(Grid, 13, "N"),
	wxGrid:setRowLabelValue(Grid, 14, "O"),
	wxGrid:setRowLabelValue(Grid, 15, "P"),
	wxGrid:setRowLabelValue(Grid, 16, "Q"),
	wxGrid:setRowLabelValue(Grid, 17, "R"),
	wxGrid:setRowLabelValue(Grid, 18, "S"),
	wxGrid:setRowLabelValue(Grid, 19, "T"),
	wxGrid:setRowLabelValue(Grid, 20, "U"),
	wxGrid:setRowLabelValue(Grid, 21, "V"),
	wxGrid:setRowLabelValue(Grid, 22, "W"),
	wxGrid:setRowLabelValue(Grid, 23, "X"),
	wxGrid:setRowLabelValue(Grid, 24, "Y"),
	wxGrid:setRowLabelValue(Grid, 25, "Z"),	

Grid.


doFontR(_, 0, 6) -> ok;
doFontR(_, 1, 6) -> ok;
doFontR(_, 2, 6) -> ok;
doFontR(_, 3, 6) -> ok;
doFontR(_, 4, 6) -> ok;
doFontR(_, 5, 6) -> ok;

doFontR(Grid, R, C) -> 
	wxGrid:setReadOnly(Grid, R, C, [{isReadOnly,true}]),
        Font = wxFont:new(5, ?wxFONTFAMILY_SWISS, ?wxFONTSTYLE_NORMAL, ?wxFONTWEIGHT_NORMAL, []),
	wxGrid:setCellBackgroundColour(Grid, R, C, {34,117,76}),
	wxGrid:setCellFont(Grid, R, C, Font),
	doFontR(Grid, R, C+1).

setValue(Grid,Row,L1,L2,L3)-> 
	wxGrid:setCellValue(Grid,Row-1,0,integer_to_list(L1)),
	wxGrid:setCellValue(Grid,Row-1,1,integer_to_list(L2)),
	wxGrid:setCellValue(Grid,Row-1,2,integer_to_list(L3)).


