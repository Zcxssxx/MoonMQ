///|
/// Typed AMQP 0-9-1 methods covered by the MoonMQ Core Profile.
pub(all) enum Method {
  ConnectionStart(
    version_major~ : Byte,
    version_minor~ : Byte,
    server_properties~ : FieldTable,
    mechanisms~ : Bytes,
    locales~ : Bytes
  )
  ConnectionStartOk(
    client_properties~ : FieldTable,
    mechanism~ : String,
    response~ : Bytes,
    locale~ : String
  )
  ConnectionTune(channel_max~ : UInt16, frame_max~ : UInt, heartbeat~ : UInt16)
  ConnectionTuneOk(
    channel_max~ : UInt16,
    frame_max~ : UInt,
    heartbeat~ : UInt16
  )
  ConnectionOpen(virtual_host~ : String, capabilities~ : String, insist~ : Bool)
  ConnectionOpenOk
  ConnectionClose(
    reply_code~ : UInt16,
    reply_text~ : String,
    class_id~ : UInt16,
    method_id~ : UInt16
  )
  ConnectionCloseOk
  ChannelOpen(out_of_band~ : String)
  ChannelOpenOk
  ChannelClose(
    reply_code~ : UInt16,
    reply_text~ : String,
    class_id~ : UInt16,
    method_id~ : UInt16
  )
  ChannelCloseOk
  ExchangeDeclare(
    ticket~ : UInt16,
    exchange~ : String,
    kind~ : String,
    passive~ : Bool,
    durable~ : Bool,
    auto_delete~ : Bool,
    internal~ : Bool,
    nowait~ : Bool,
    arguments~ : FieldTable
  )
  ExchangeDeclareOk
  ExchangeDelete(
    ticket~ : UInt16,
    exchange~ : String,
    if_unused~ : Bool,
    nowait~ : Bool
  )
  ExchangeDeleteOk
  QueueDeclare(
    ticket~ : UInt16,
    queue~ : String,
    passive~ : Bool,
    durable~ : Bool,
    exclusive~ : Bool,
    auto_delete~ : Bool,
    nowait~ : Bool,
    arguments~ : FieldTable
  )
  QueueDeclareOk(queue~ : String, message_count~ : UInt, consumer_count~ : UInt)
  QueueBind(
    ticket~ : UInt16,
    queue~ : String,
    exchange~ : String,
    routing_key~ : String,
    nowait~ : Bool,
    arguments~ : FieldTable
  )
  QueueBindOk
  QueueUnbind(
    ticket~ : UInt16,
    queue~ : String,
    exchange~ : String,
    routing_key~ : String,
    arguments~ : FieldTable
  )
  QueueUnbindOk
  QueueDelete(
    ticket~ : UInt16,
    queue~ : String,
    if_unused~ : Bool,
    if_empty~ : Bool,
    nowait~ : Bool
  )
  QueueDeleteOk(message_count~ : UInt)
  BasicQos(prefetch_size~ : UInt, prefetch_count~ : UInt16, global~ : Bool)
  BasicQosOk
  BasicConsume(
    ticket~ : UInt16,
    queue~ : String,
    consumer_tag~ : String,
    no_local~ : Bool,
    no_ack~ : Bool,
    exclusive~ : Bool,
    nowait~ : Bool,
    arguments~ : FieldTable
  )
  BasicConsumeOk(consumer_tag~ : String)
  BasicCancel(consumer_tag~ : String, nowait~ : Bool)
  BasicCancelOk(consumer_tag~ : String)
  BasicPublish(
    ticket~ : UInt16,
    exchange~ : String,
    routing_key~ : String,
    mandatory~ : Bool,
    immediate~ : Bool
  )
  BasicDeliver(
    consumer_tag~ : String,
    delivery_tag~ : UInt64,
    redelivered~ : Bool,
    exchange~ : String,
    routing_key~ : String
  )
  BasicGet(ticket~ : UInt16, queue~ : String, no_ack~ : Bool)
  BasicGetOk(
    delivery_tag~ : UInt64,
    redelivered~ : Bool,
    exchange~ : String,
    routing_key~ : String,
    message_count~ : UInt
  )
  BasicGetEmpty(cluster_id~ : String)
  BasicAck(delivery_tag~ : UInt64, multiple~ : Bool)
  BasicReject(delivery_tag~ : UInt64, requeue~ : Bool)
  BasicReturn(
    reply_code~ : UInt16,
    reply_text~ : String,
    exchange~ : String,
    routing_key~ : String
  )
} derive(Eq, @debug.Debug)
