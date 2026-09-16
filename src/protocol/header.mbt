///|
/// Errors returned by the backend-independent protocol codec.
pub enum AmqpError {
  InvalidProtocolHeader
  TruncatedInput
  ValueTooLong
  InvalidUtf8
  InvalidFrameTerminator
  UnsupportedFrameType
  FrameTooLarge
  UnsupportedMethod
}

///|
/// Encode the fixed AMQP 0-9-1 protocol header.
pub fn encode_protocol_header() -> Bytes {
  b"AMQP\x00\x00\x09\x01"
}

///|
/// Validate a fixed AMQP 0-9-1 protocol header.
pub fn decode_protocol_header(input : Bytes) -> Result[Unit, AmqpError] {
  if input == encode_protocol_header() {
    Ok(())
  } else {
    Err(InvalidProtocolHeader)
  }
}
