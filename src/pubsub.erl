-module(pubsub).

-export([
	start/0,
	publish/2,
	subscribe/1,
	subscribe/2,
	unsubscribe/1
]).

start() ->
	application:ensure_all_started(pubsub).

publish(Topic, Message) -> pubsub_srv:publish(Topic, Message).

subscribe(Topic) -> pubsub_srv:subscribe(Topic).

subscribe(Topic, Callback) -> pubsub_srv:subscribe(Topic, Callback).

unsubscribe(Topic) -> pubsub_srv:unsubscribe(Topic).
