///|
/// Stages of the portable AMQP connection handshake.
pub(all) enum SessionState {
  AwaitProtocolHeader
  AwaitStartOk
  AwaitTuneOk
  AwaitOpen
  Open
  Closed
} derive(Eq, @debug.Debug)

///|
/// Errors that keep transport concerns outside the portable session layer.
pub enum SessionError {
  InvalidState
  InvalidChannel
  InvalidFrameKind
  MissingContent
  PendingContent
  UnsupportedOperation
  ProtocolError(@protocol.AmqpError)
  BrokerError(@broker.BrokerError)
} derive(Eq, @debug.Debug)

///|
struct ChannelState {
  id : UInt16
  mut prefetch : UInt16
  mut open : Bool
}

///|
struct SessionConsumer {
  channel : UInt16
  tag : String
  no_ack : Bool
  handle : @broker.ConsumerHandle
}

///|
struct PendingPublish {
  channel : UInt16
  exchange : String
  routing_key : String
}

///|
struct PendingContent {
  channel : UInt16
  exchange : String
  routing_key : String
  assembler : @protocol.ContentAssembler
}

///|
/// A portable AMQP session that translates complete protocol frames to Broker actions.
pub struct Session {
  mut state : SessionState
  broker : @broker.Broker
  channels : Array[ChannelState]
  consumers : Array[SessionConsumer]
  mut pending_publish : PendingPublish?
  mut pending_content : PendingContent?
}
