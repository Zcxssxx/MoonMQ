///|
/// Typed AMQP 0-9-1 methods covered by the first MoonMQ Core Profile slice.
pub(all) enum Method {
  BasicPublish(
    ticket~ : UInt16,
    exchange~ : String,
    routing_key~ : String,
    mandatory~ : Bool,
    immediate~ : Bool,
  )
  BasicAck(delivery_tag~ : UInt64, multiple~ : Bool)
  BasicReject(delivery_tag~ : UInt64, requeue~ : Bool)
} derive(Eq, @debug.Debug)
