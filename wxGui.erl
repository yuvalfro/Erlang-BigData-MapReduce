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
  Text = "   About dblp:

   The dblp computer science bibliography
   provides open bibliographic information
   on major computer science journals and proceedings.
   Originally created at the University of Trier in 1993,
   dblp is now operated and further developed by Schloss Dagstuhl.
   For more information: https://dblp.org/",
  Frame = wxFrame:new(Server, 1, "DBLP map-reduce Project",[{pos,{550,200}},{size,{500,500}}]),   %% build and layout the GUI components
  Label = wxStaticText:new(Frame, 2, "Please enter name of author", [{style, ?wxALIGN_CENTRE_HORIZONTAL}]),
  Label2 = wxStaticText:new(Frame, 3, Text, [{style, ?wxALIGN_LEFT}]),
  %Panel  = wxPanel:new(Frame), 
  %Background = wxBitmap:new("include/background2.jpg", [{type, ?wxBITMAP_TYPE_PNG}]),
  %G= wxStaticBitmap:new(Panel,1,Background,[{size,{650,700}}]),   
  wxStaticText:wrap(Label,5000),
  Counter = wxTextCtrl:new(Frame, 60, [{value, ""}, {style, ?wxTE_LEFT}]),   %text box value and align
  Font = wxFont:new(14, ?wxFONTFAMILY_DEFAULT, ?wxFONTSTYLE_NORMAL, ?wxFONTWEIGHT_NORMAL),    %font size and design
  wxTextCtrl:setFont(Counter, Font),
  Button = wxButton:new(Frame, ?wxID_ANY, [{label, "search"}]),    %new button with text
  CounterSizer = wxBoxSizer:new(?wxVERTICAL), %vertical mean that the bottom will be down the text
  wxSizer:add(CounterSizer, Label2, [{flag, ?wxALL bor ?wxALIGN_CENTRE}, {border, 15}]),
  wxSizer:add(CounterSizer, Label, [{flag, ?wxALL bor ?wxALIGN_CENTRE}, {border, 15}]),
  wxSizer:add(CounterSizer, Counter, [{proportion,10},{flag, ?wxEXPAND bor ?wxALL}, {border, 15}]),
  Image = wxImage:new("dblp-logo.png", []),
  Bitmap = wxBitmap:new(wxImage:scale(Image, round(wxImage:getWidth(Image)), round(wxImage:getHeight(Image)), [{quality, ?wxIMAGE_QUALITY_HIGH}])),
  StaticBitmap = wxStaticBitmap:new(Frame, ?wxID_ANY, Bitmap),
  MainSizer = wxBoxSizer:new(?wxVERTICAL),
  wxSizer:add(MainSizer, StaticBitmap, [{flag, ?wxALL bor ?wxEXPAND}]),
  wxSizer:add(MainSizer, CounterSizer, [{flag, ?wxALIGN_CENTER},{border,100}]),
  wxSizer:add(MainSizer, Button, [{flag,?wxALIGN_CENTER},{border,80}]),
  wxWindow:setSizer(Frame, MainSizer),
  %wxSizer:setSizeHints(MainSizer, Frame),
  %wxWindow:setMinSize(Frame, wxWindow:getSize(Frame)),
  wxButton:connect(Button, command_button_clicked),   %connect the button to handler-star fuction  "command_button_clicked"
  wxFrame:show(Frame),
  {ok, #state{counter = Counter, button = Button, counting_down = false}}.

handle_call(_Request, _From, State) -> Reply = ok,
  {reply, Reply, State}.

handle_cast(_Msg, State) ->    {noreply, State}.

%when counting_down=false it mean      need to start count
handle_info(#wx{obj = Button, event = #wxCommand{type = command_button_clicked}}, #state{counter = Counter, counting_down = false} = State) ->   %% build and layout the GUI components
  wxButton:setLabel(Button, "Waiting for result"), %% set the bottom to stop (because start to count).
  TRef = erlang:send_after(1000, self(), update_gui),{noreply, State#state{tref = TRef, 		counting_down =   true}} ;

%when counting_down=true need to continue the counting
%handle_info(#wx{obj = Button, event = #wxCommand{type = command_button_clicked}}, #state{counter = Counter, counting_down = true, tref = TRef} = State) ->    erlang:cancel_timer(TRef),
%  wxTextCtrl:setEditable(Counter, true),
%  wxButton:setLabel(Button, "search"),
%  {noreply, State#state{tref = undefined, counting_down = false}};


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
      wxFrame:show(Frame1),
      wxTextCtrl:setValue(Counter, ""),     %when counter=1
      wxTextCtrl:setEditable(Counter, true),
      wxButton:setLabel(Button, "search");
    true ->
      List = gen_server:call({global,master},[MainAuthor]),
      case List of % Check if the author exist
        [] ->
          io:format("This author doesn't exist in dblp~n"),
          Frame1 = wxFrame:new(wx:null(), 3, "Error",[{pos,{550,200}}]),
          Label = wxStaticText:new(Frame1, 2, "This author doesn't exist in dblp", [{style, ?wxALIGN_CENTRE_HORIZONTAL}]),
          CounterSizer = wxBoxSizer:new(?wxVERTICAL), %vertical mean that the bottom will be down the text
          wxSizer:add(CounterSizer, Label, [{flag, ?wxALL bor ?wxALIGN_CENTRE}, {border, 15}]),
          wxFrame:show(Frame1),
          wxTextCtrl:setValue(Counter, ""),     %when counter=1
          wxTextCtrl:setEditable(Counter, true),
          wxButton:setLabel(Button, "search");
        _ ->
          Frame1 = wxFrame:new(wx:null(), 3, "Authors Tree",[{pos,{550,200}}]),
          makeTable:start(List),
          MainSizer1 = wxBoxSizer:new(?wxHORIZONTAL),
          wxWindow:setSizer(Frame1, MainSizer1),
          Image = wxImage:new("AuthorsTree.png", []),
          Bitmap = wxBitmap:new(wxImage:scale(Image, round(wxImage:getWidth(Image) * 0.5), round(wxImage:getHeight(Image) * 0.5), [{quality, ?wxIMAGE_QUALITY_HIGH}])),
          StaticBitmap = wxStaticBitmap:new(Frame1, ?wxID_ANY, Bitmap),
          wxSizer:add(MainSizer1, StaticBitmap, [{flag, ?wxALL bor ?wxEXPAND}]),
          wxFrame:show(Frame1),
          wxTextCtrl:setValue(Counter, ""),     %when counter=1
          wxTextCtrl:setEditable(Counter, true),
          wxButton:setLabel(Button, "search")
      end
  end,
  {noreply, State#state{counting_down = false}}.

terminate(_Reason, _State) ->    wx:destroy(),
  ok.

code_change(_OldVsn, State, _Extra) ->    {ok, State}.

