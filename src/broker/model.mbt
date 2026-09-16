///|
/// Exchange routing behavior supported by the embedded broker.
pub(all) enum ExchangeKind {
  ExchangeDirect
  ExchangeFanout
  ExchangeTopic
} derive(Eq, @debug.Debug)

///|
/// A message stored or delivered by the embedded broker.
pub struct Message {
  body : Bytes
  exchange : String
  routing_key : String
  redelivered : Bool
  properties : @protocol.BasicProperties
} derive(Eq, @debug.Debug)

///|
pub fn Message::new(body : Bytes, routing_key : String) -> Message {
  Message::new_with_exchange("", body, routing_key)
}

///|
/// Create a message while preserving the exchange that accepted it.
pub fn Message::new_with_exchange(
  exchange : String,
  body : Bytes,
  routing_key : String,
) -> Message {
  Message::new_with_properties(
    exchange,
    body,
    routing_key,
    @protocol.BasicProperties::new(),
  )
}

///|
/// Create a message with its exchange and AMQP Basic properties.
pub fn Message::new_with_properties(
  exchange : String,
  body : Bytes,
  routing_key : String,
  properties : @protocol.BasicProperties,
) -> Message {
  { body, exchange, routing_key, redelivered: false, properties, }
}

///|
pub fn Message::body(self : Message) -> Bytes {
  self.body
}

///|
pub fn Message::exchange(self : Message) -> String {
  self.exchange
}

///|
pub fn Message::properties(self : Message) -> @protocol.BasicProperties {
  self.properties
}

///|
pub fn Message::routing_key(self : Message) -> String {
  self.routing_key
}

///|
pub fn Message::redelivered(self : Message) -> Bool {
  self.redelivered
}

///|
/// A message delivered to a pull client or a registered consumer.
pub struct Delivery {
  tag : UInt64
  queue : String
  consumer_tag : String
  message : Message
} derive(Eq, @debug.Debug)

///|
pub fn Delivery::tag(self : Delivery) -> UInt64 {
  self.tag
}

///|
pub fn Delivery::queue(self : Delivery) -> String {
  self.queue
}

///|
pub fn Delivery::consumer_tag(self : Delivery) -> String {
  self.consumer_tag
}

///|
pub fn Delivery::body(self : Delivery) -> Bytes {
  self.message.body()
}

///|
pub fn Delivery::exchange(self : Delivery) -> String {
  self.message.exchange()
}

///|
pub fn Delivery::properties(self : Delivery) -> @protocol.BasicProperties {
  self.message.properties()
}

///|
pub fn Delivery::routing_key(self : Delivery) -> String {
  self.message.routing_key()
}

///|
pub fn Delivery::redelivered(self : Delivery) -> Bool {
  self.message.redelivered()
}

///|
/// Handle returned by `Broker::consume`.
pub struct ConsumerHandle {
  queue : String
  tag : String
} derive(Eq, @debug.Debug)

///|
pub enum BrokerError {
  UnknownExchange
  UnknownQueue
  ExchangeKindMismatch
  DuplicateBinding
  DuplicateConsumer
  UnknownConsumer
  UnknownDelivery
  InvalidPrefetch
} derive(Eq, @debug.Debug)

///|
/// Ready-message and active-consumer counts for one queue.
pub struct QueueStats {
  message_count : Int
  consumer_count : Int
} derive(Eq, @debug.Debug)

///|
pub fn QueueStats::message_count(self : QueueStats) -> Int {
  self.message_count
}

///|
pub fn QueueStats::consumer_count(self : QueueStats) -> Int {
  self.consumer_count
}

///|
struct Binding {
  exchange : String
  queue : String
  pattern : String
}

///|
struct ExchangeState {
  name : String
  kind : ExchangeKind
}

///|
priv struct ConsumerState {
  tag : String
  consumer_tag : String
  owner : String
  prefetch : Int
  mut unacked : Int
  inbox : Array[Delivery]
}

///|
struct QueueState {
  name : String
  mut messages : Array[Message]
  consumers : Array[ConsumerState]
  mut next_consumer : Int
}

///|
struct InFlight {
  tag : UInt64
  owner : String
  queue : String
  consumer_tag : String
  message : Message
}

///|
/// An isolated in-memory broker. It is deliberately not a socket server.
pub struct Broker {
  exchanges : Array[ExchangeState]
  queues : Array[QueueState]
  bindings : Array[Binding]
  in_flight : Array[InFlight]
  mut next_tag : UInt64
  mut next_queue_id : Int
}
