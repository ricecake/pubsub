-module(pubsub).

-export([
	start/0,
	declare_exchange/1,
	publish/2,
	publish/3,
	subscribe/1,
	subscribe/2,
	subscribe/3,
	unsubscribe/2,
	route_escape/1
]).

start() ->
	application:ensure_all_started(pubsub).

declare_exchange(Exchange) -> pubsub_srv:declare_exchange(Exchange).

publish(Topic, Message) -> publish(default, Topic, Message).

publish(Exchange, Topic, Message) -> pubsub_srv:publish(Exchange, Topic, Message).

subscribe(Topic) -> subscribe(default, Topic).

subscribe(Exchange, Topic) when is_atom(Exchange) -> pubsub_srv:subscribe(Exchange, Topic);
subscribe(Topic, Callback) when is_function(Callback) -> subscribe(default, Topic, Callback).

subscribe(Exchange, Topic, Callback) -> pubsub_srv:subscribe(Exchange, Topic, Callback).

unsubscribe(Exchange, Topic) -> pubsub_srv:unsubscribe(Exchange, Topic).

route_escape(Binary) -> binary:replace(Binary, <<$.>>, <<$+>>, [global]).
