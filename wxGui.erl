%%%-------------------------------------------------------------------
%%% @author yarden
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 06. Oct 2020 14:26
%%%-------------------------------------------------------------------
-module(wxGui).
-behaviour(gen_server).
-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-include_lib("wx/include/wx.hrl").
-define(SERVER, ?MODULE).
-define(Master, 'master@127.0.0.1').
-record(state, {counter, button, counting_down, tref}).

start_link() ->    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
  Server = wx:new(),
  master:start_link(),
  Frame = wxFrame:new(Server, 1, "DBLP map-reduce Project",[{pos,{500,200}},{size,{400,400}}]),   %% build and layout the GUI components
  wxWindow:setBackgroundColour(Frame, {123,162,252}),
  Label = wxStaticText:new(Frame, 2, "Please enter name of author", [{style, ?wxALIGN_CENTRE_HORIZONTAL}]),
  %Panel  = wxPanel:new(Frame),
  %Background = wxBitmap:new("include/background2.jpg", [{type, ?wxBITMAP_TYPE_PNG}]),
  %G= wxStaticBitmap:new(Panel,1,Background,[{size,{650,700}}]),
  wxStaticText:wrap(Label,5000),
  Counter = wxTextCtrl:new(Frame, 60, [{value, ""}, {style, ?wxTE_LEFT}]),   %text box value and align
  Font = wxFont:new(14, ?wxFONTFAMILY_DEFAULT, ?wxFONTSTYLE_NORMAL, ?wxFONTWEIGHT_NORMAL),    %font size and design
  wxTextCtrl:setFont(Counter, Font),
  Button = wxButton:new(Frame, ?wxID_ANY, [{label, "search"},{size,{200,30}}]),    %new button with text
  Button1 = wxButton:new(Frame, ?wxID_ANY, [{label, "information"},{size,{200,30}}]),    %for more information
  CounterSizer = wxBoxSizer:new(?wxVERTICAL), %vertical mean that the bottom will be down the text
  wxSizer:add(CounterSizer, Label, [{flag, ?wxALL bor ?wxALIGN_CENTRE}, {border, 15}]),
  wxSizer:add(CounterSizer, Counter, [{proportion,10},{flag, ?wxEXPAND bor ?wxALL}, {border, 15}]),
  Image = wxImage:new("idea.png", []),
  Bitmap = wxBitmap:new(wxImage:scale(Image, round(wxImage:getWidth(Image)), round(wxImage:getHeight(Image)), [{quality, ?wxIMAGE_QUALITY_HIGH}])),
  StaticBitmap = wxStaticBitmap:new(Frame, ?wxID_ANY, Bitmap),
  MainSizer = wxBoxSizer:new(?wxVERTICAL),
  wxSizer:add(MainSizer, StaticBitmap, [{flag, ?wxALL bor ?wxEXPAND}]),
  wxSizer:add(MainSizer, CounterSizer, [{flag, ?wxALIGN_CENTER},{border,100}]),
  wxSizer:add(MainSizer, Button, [{flag, ?wxALIGN_CENTER},{border,80}]),
  wxSizer:add(MainSizer, Button1, [{flag, ?wxALIGN_CENTER},{border,150}]),
  wxWindow:setSizer(Frame, MainSizer),
  %wxSizer:setSizeHints(MainSizer, Frame),
  %wxWindow:setMinSize(Frame, wxWindow:getSize(Frame)),
  wxButton:connect(Button, command_button_clicked),   %connect the button to handler-star fuction  "command_button_clicked"
  wxEvtHandler:connect(Button1, command_button_clicked, [{callback, fun handle_click_event/2},
    {userData, {wx:get_env(), Button1}}]),
  %wxEvtHandler:connect(Button1, command_button_clicked,{callback, fun handle_click_event/0}),   %connect the button to handler-star fuction  "command_button_clicked"
  wxFrame:show(Frame),
  {ok, #state{counter = Counter, button = Button, counting_down = false}}.

handle_click_event(_A = #wx{}, _B) ->
  Frame3 = wxFrame:new(wx:null(), 3, "Information about dplb",[{pos,{500,250}}]),
  wxWindow:setBackgroundColour(Frame3, {123,162,252}),
  Text = "   About dblp:

   The dblp computer science bibliography
   provides open bibliographic information
   on major computer science journals and proceedings.

   Originally created at the University of Trier in 1993,
   dblp is now operated and further developed by Schloss
   Dagstuhl.

   For more information: https://dblp.org/",
  Label2 = wxStaticText:new(Frame3, 3, Text, [{style, ?wxALIGN_LEFT}]),
  CounterSizer = wxBoxSizer:new(?wxVERTICAL), %vertical mean that the bottom will be down the text
  wxSizer:add(CounterSizer, Label2, [{flag, ?wxALL bor ?wxALIGN_CENTRE}, {border, 15}]),
  wxFrame:show(Frame3).

handle_call(_Request, _From, State) -> Reply = ok,
  {reply, Reply, State}.

handle_cast(_Msg, State) ->    {noreply, State}.

%when counting_down=false it mean      need to start count
handle_info(#wx{obj = Button, event = #wxCommand{type = command_button_clicked}}, #state{counter = _, counting_down = false} = State) ->   %% build and layout the GUI components
  wxButton:setLabel(Button, "Waiting for result"), %% set the bottom to stop (because start to count).
  TRef = erlang:send_after(1000, self(), update_gui),{noreply, State#state{tref = TRef, 		counting_down =   true}} ;

handle_info(update_gui, #state{button = Button, counter = Counter, counting_down = true} = State) ->
  MainAuthor = wxTextCtrl:getValue(Counter),
  net_kernel:monitor_nodes(true),
  timer:sleep(200),
  ConnectMaster = net_kernel:connect_node(?Master),
  timer:sleep(200),
  case ConnectMaster of % Check if the master is connected
    false ->
      io:format("Please Connect master~n"),
      Frame1 = wxFrame:new(wx:null(), 3, "Error",[{pos,{550,200}}]),
      Label = wxStaticText:new(Frame1, 2, "Please Connect master", [{style, ?wxALIGN_CENTRE_HORIZONTAL}]),
      CounterSizer = wxBoxSizer:new(?wxVERTICAL), %vertical mean that the bottom will be down the text
      wxSizer:add(CounterSizer, Label, [{flag, ?wxALL bor ?wxALIGN_CENTRE}, {border, 15}]),
      wxWindow:setBackgroundColour(Frame1, {123,162,252}),
      wxFrame:show(Frame1),
      wxTextCtrl:setValue(Counter, ""),     %when counter=1
      wxTextCtrl:setEditable(Counter, true),
      wxButton:setLabel(Button, "search");
    true ->
      Self = self(),
      spawn(fun() -> gen_server:call({global,master},[MainAuthor,Self]) end),
      List = receiveMaster(),
      case List of % Check if the author exist
        [] ->
          io:format("This author doesn't exist in dblp~n"),
          Frame1 = wxFrame:new(wx:null(), 3, "Error",[{pos,{550,200}}]),
          Label = wxStaticText:new(Frame1, 2, "This author doesn't exist in dblp,  please enter a new name", [{style, ?wxALIGN_CENTRE_HORIZONTAL}]),
          CounterSizer = wxBoxSizer:new(?wxVERTICAL), %vertical mean that the bottom will be down the text
          wxSizer:add(CounterSizer, Label, [{flag, ?wxALL bor ?wxALIGN_CENTRE}, {border, 15}]),
          wxWindow:setBackgroundColour(Frame1, {123,162,252}),
          wxFrame:show(Frame1),
          wxTextCtrl:setValue(Counter, ""),     %when counter=1
          wxTextCtrl:setEditable(Counter, true),
          wxButton:setLabel(Button, "search");
        _ ->
          makeTable:start(List),
          os:cmd("xdg-open AuthorsTree.png"),
          wxTextCtrl:setValue(Counter, ""),     %when counter=1
          wxTextCtrl:setEditable(Counter, true),
          wxButton:setLabel(Button, "search")
      end
  end,
  {noreply, State#state{counting_down = false}}.

receiveMaster() ->
  receive
  % nodeup message - keep waiting
    {nodeup,_} -> receiveMaster();
  % nodedown message - insert 'nodedown' into map and continue
    {nodedown,_} ->
      ConnectMaster = net_kernel:connect_node(?Master),
      case ConnectMaster of
        % Falling of PC1/2/3/4 make the master also send a node down message, ignore it
        true -> receiveMaster();
        false ->
          io:format("Master is down! ~n"),
          Frame1 = wxFrame:new(wx:null(), 3, "Error",[{pos,{550,200}}]),
          Label = wxStaticText:new(Frame1, 2, "Master is down! Please Connect it", [{style, ?wxALIGN_CENTRE_HORIZONTAL}]),
          CounterSizer = wxBoxSizer:new(?wxVERTICAL), %vertical mean that the bottom will be down the text
          wxSizer:add(CounterSizer, Label, [{flag, ?wxALL bor ?wxALIGN_CENTRE}, {border, 15}]),
          wxFrame:show(Frame1)
      end;
  % FamilyNameData message - doesn't matter if error or finish. return the list
    {FamilyNameData,"Finish"} ->
      FamilyNameData;
    {FamilyNameData,"Error"} ->
      FamilyNameData
  end.

terminate(_Reason, _State) ->    wx:destroy(),
  ok.

code_change(_OldVsn, State, _Extra) ->    {ok, State}.
