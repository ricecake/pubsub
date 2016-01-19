-module(pubsub_srv).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0]).
-export([
	init_tables/0,
	subscribe/2,
	subscribe/3,
	unsubscribe/2,
	publish/3,
	lookup/2,
	garbage_collect/0
]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
	gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init_tables() ->
	ets:new(?MODULE, [
		bag,
		public,
		named_table,
		{read_concurrency, true}
	]),
	ok.

subscribe(Exchange, Topics) when is_binary(Exchange), is_list(Topics)->
	gen_server:call(?MODULE, {subscribe, Exchange, Topics});
subscribe(Exchange, Topic) -> subscribe(Exchange, [Topic]).

subscribe(Exchange, Topics, Callback) when is_list(Topics)->
	gen_server:call(?MODULE, {subscribe, Exchange, Topics, Callback});
subscribe(Exchange, Topic, Callback) -> subscribe(Exchange, [Topic], Callback).

unsubscribe(Exchange, Topics) when is_list(Topics)->
	gen_server:call(?MODULE, {unsubscribe, Exchange, Topics});
unsubscribe(Exchange, Topic) -> unsubscribe(Exchange, [Topic]).

publish(Exchange, Topic, Message) ->
	[send_event(Message, self(), Topic, Rec) || Rec <- lookup(Exchange, Topic)],
	ok.

lookup(Exchange, Route) ->
	Path = binary:split(Route, <<".">>, [global]),
	lists:usort([ Data || {_, Data} <- do_lookup({Exchange, null}, Path, []), Data =/= undefined]).


%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(_Args) ->
	{ok, #{ subscribers => [] }}.

handle_call({subscribe, Exchange, Topics}, {From, _}, #{ subscribers := Subs } = State) ->
	NewSubs = ensure_monitor(From, Subs),
	true = ets:insert(?MODULE, lists:flatten([routify(Exchange, Topic, From) || Topic <- Topics])),
	{reply, ok, State#{ subscribers :=  NewSubs }};
handle_call({subscribe, Exchange, Topics, Callback}, {From, _}, #{ subscribers := Subs } = State) ->
	NewSubs = ensure_monitor(From, Subs),
	true = ets:insert(?MODULE, lists:flatten([routify(Exchange, Topic, {From, Callback}) || Topic <- Topics])),
	{reply, ok, State#{ subscribers :=  NewSubs }};
handle_call({unsubscribe, Exchange, Topics}, {From, _}, State) ->
	[begin
		{ok, [Key |_]} = path(Exchange, Topic),
		true = ets:delete_object(?MODULE, {Key, From})
	end || Topic <- Topics],
	{reply, ok, State};
handle_call(_Request, _From, State) ->
	{reply, ok, State}.

handle_cast({send, Exchange, From, Topic, Message}, State) ->
	[send_event(Message, From, Topic, Rec)|| Rec <- lookup(Exchange, Topic)],
	{noreply, State};
handle_cast(_Msg, State) ->
	{noreply, State}.

handle_info({'DOWN', Ref, _Type, Subscriber, _Exit}, #{ subscribers := Subs } = State) ->
	ets:select_delete(?MODULE, [
		{{'_',Subscriber},[],[true]},
		{{'_',{Subscriber,'_'}},[],[true]}
	]),
	{noreply, State#{ subscribers := Subs -- [{Subscriber, Ref}] }};
handle_info(_Info, State) ->
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

send_event(Message, From, Topic, {Subscriber, Fun}) ->
	Fun(Subscriber, From, {Topic, Message}),
	ok;
send_event(Message, From, Topic, Subscriber) ->
	Subscriber ! {pubsub_event, From, {Topic, Message}},
	ok.

ensure_monitor(New, Existing) when is_pid(New) ->
	case lists:keyfind(New, 1, Existing) of
		{New, _MonRef} -> Existing;
		false          ->
			MonRef = monitor(process, New),
			[{New, MonRef} |Existing]
	end.

routify(Exchange, Key, Data) ->
	{ok, [Terminal |Nodes]} = path(Exchange, Key),
	[{Terminal, Data}|[ {Node, undefined} || Node <- Nodes]].

path(Exchange, Key) when is_binary(Exchange), is_binary(Key) ->
	Path = binary:split(Key, <<".">>, [global]),
	{_, Nodes} = lists:foldl(fun
		(Node, {null, List}) ->
			{Node, [{{Exchange, null}, Node} |List]};
		(Node, {Parent, List}) ->
			{<< Parent/bits, $., Node/bits >>, [{{Exchange, Parent}, Node} |List]}
	end, {null, []}, Path),
	{ok, Nodes}.

do_lookup(_, [], Callbacks) -> Callbacks;
do_lookup(Parent, [Label], Callbacks) ->
	NewCallbacks = resolve_wildcards(Parent, [], Callbacks),
	ets:lookup(?MODULE, {Parent, Label}) ++ NewCallbacks;
do_lookup(Parent, [Label |Path], Callbacks) ->
	NewParent = derive_newpath(Parent, Label),
	NewCallbacks = resolve_wildcards(Parent, Path, Callbacks),
	do_lookup(NewParent, Path, NewCallbacks).

subpaths(Path) ->
	case lists:foldl(fun
		(El, {null, Paths})->
			{[El], Paths};
		(El, {Curr, Paths})->
			{[El |Curr], [Curr |Paths]}
		end,
		{null, []},
		lists:reverse(Path)
	) of
		{null, []} -> [];
		{Last, SubPaths} -> [Last |SubPaths]
	end.

resolve_wildcards(Parent, Path, Callbacks) ->
	StarCallbacks = case ets:lookup(?MODULE, {Parent, <<"*">>}) of
		[]     -> Callbacks;
		Found ->
			if
				Path == [] -> [ Node || Node = {_, Data} <- Found, Data =/= undefined ];
				Path /= [] ->
					StarParent = derive_newpath(Parent, <<"*">>),
					do_lookup(StarParent, Path, Callbacks)
			end
	end,
	case ets:lookup(?MODULE, {Parent, <<"#">>}) of
		[]     -> StarCallbacks;
		Nodes  ->
			HashParent = derive_newpath(Parent, <<"#">>),
			Nodes ++ lists:flatten([ do_lookup(HashParent, ThisPath, StarCallbacks) || ThisPath <- subpaths(Path)])
	end.

derive_newpath({Exchange, OldPath}, Label) ->
	if
		OldPath ==  null -> {Exchange, Label};
		OldPath =/= null -> {Exchange, << OldPath/bits, $., Label/bits >>}
	end.


garbage_collect() ->
	TopLevelNodes = [
		{Ex, Parent, Node, Val}
			|| {{{Ex,Parent},Node},Val} <- ets:select(?MODULE, [{{{{'_',null},'_'},'_'}, [], ['$_']}])
	],
	do_gc(TopLevelNodes).

do_gc([]) -> {false, []};
do_gc([Node |Rest]) ->
	case check_prune(Node) of
		{false, Candidates} -> do_gc(fastConcat(Candidates, Rest));
		{true, []} ->
			ok = purge(Node),
			do_gc(Rest)
	end.

check_prune({Ex, Parent, Node, Val}) ->
	HasInfo = Val =/= undefined,
	Key = derive_newpath({Ex, Parent}, Node),
	{Children, Candidates} = case ets:select(?MODULE, [{{{Key,'_'},'_'}, [], ['$_']}]) of
		[] -> {false, []};
		Found when is_list(Found) ->
			do_gc([{NewEx, NewParent, NewNode, NewVal} || {{{NewEx,NewParent},NewNode},NewVal} <- Found])
	end,
	{not (HasInfo or Children), Candidates}.


purge(Node) -> io:format("~p~n", [Node]), ok.

fastConcat([], B) -> B;
fastConcat(A,B) when length(A) > length(B) -> fastConcat(B,A);
fastConcat(A,B) -> A++B.
