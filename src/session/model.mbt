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
  AuthenticationFailed
  InvalidVirtualHost
  InvalidFrameSize
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
  mandatory : Bool
}

///|
struct PendingContent {
  channel : UInt16
  exchange : String
  routing_key : String
  mandatory : Bool
  assembler : @protocol.ContentAssembler
}

///|
struct SessionDelivery {
  channel : UInt16
  tag : UInt64
  consumer_tag : String
}

///|
/// A portable AMQP session that translates complete protocol frames to Broker actions.
pub struct Session {
  mut state : SessionState
  broker : @broker.Broker
  consumer_owner : String
  channels : Array[ChannelState]
  consumers : Array[SessionConsumer]
  mut pending_publishes : Array[PendingPublish]
  mut pending_contents : Array[PendingContent]
  mut deliveries : Array[SessionDelivery]
  mut negotiated_channel_max : UInt16
  mut negotiated_frame_max : Int
}
