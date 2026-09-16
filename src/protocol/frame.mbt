///|
/// AMQP frame kinds defined by the 0-9-1 wire format.
pub(all) enum FrameKind {
  FrameMethod
  FrameHeader
  FrameBody
  FrameHeartbeat
} derive(Eq, @debug.Debug)

///|
/// A complete AMQP frame without a transport-specific stream wrapper.
pub struct Frame {
  kind : FrameKind
  channel : UInt16
  payload : Bytes
}

///|
pub fn Frame::new(kind : FrameKind, channel : UInt16, payload : Bytes) -> Frame {
  { kind, channel, payload, }
}

///|
pub fn Frame::kind(self : Frame) -> FrameKind {
  self.kind
}

///|
pub fn Frame::channel(self : Frame) -> UInt16 {
  self.channel
}

///|
pub fn Frame::payload(self : Frame) -> Bytes {
  self.payload
}

///|
const DEFAULT_MAX_FRAME_PAYLOAD : Int = 131072

///|
fn frame_kind_byte(kind : FrameKind) -> Byte {
  match kind {
    FrameMethod => b'\x01'
    FrameHeader => b'\x02'
    FrameBody => b'\x03'
    FrameHeartbeat => b'\x08'
  }
}

///|
fn frame_kind_from_byte(value : Byte) -> Result[FrameKind, AmqpError] {
  match value {
    b'\x01' => Ok(FrameMethod)
    b'\x02' => Ok(FrameHeader)
    b'\x03' => Ok(FrameBody)
    b'\x08' => Ok(FrameHeartbeat)
    _ => Err(UnsupportedFrameType)
  }
}

///|
/// Encode one complete AMQP frame, including its `0xCE` terminator.
pub fn encode_frame(frame : Frame) -> Result[Bytes, AmqpError] {
  let payload_size = frame.payload.length()
  if payload_size > DEFAULT_MAX_FRAME_PAYLOAD {
    return Err(FrameTooLarge)
  }
  let channel = encode_u16(frame.channel)
  let size = encode_u32(payload_size.reinterpret_as_uint())
  let output : Array[Byte] = []
  output.push(frame_kind_byte(frame.kind))
  for byte in channel {
    output.push(byte)
  }
  for byte in size {
    output.push(byte)
  }
  for byte in frame.payload {
    output.push(byte)
  }
  output.push(b'\xCE')
  Ok(Bytes::from_array(output[:]))
}

///|
/// Decode exactly one complete AMQP frame.
pub fn decode_frame(input : Bytes) -> Result[Frame, AmqpError] {
  if input.length() < 8 {
    return Err(TruncatedInput)
  }
  let kind = match frame_kind_from_byte(input[0]) {
    Ok(value) => value
    Err(error) => return Err(error)
  }
  let channel = match decode_u16(input[1:3].to_owned()) {
    Ok(value) => value
    Err(error) => return Err(error)
  }
  let size = match decode_u32(input[3:7].to_owned()) {
    Ok(value) => value.reinterpret_as_int()
    Err(error) => return Err(error)
  }
  if size < 0 || size > DEFAULT_MAX_FRAME_PAYLOAD {
    return Err(FrameTooLarge)
  }
  let total_size = 8 + size
  if input.length() != total_size {
    return Err(TruncatedInput)
  }
  if input[total_size - 1] != b'\xCE' {
    return Err(InvalidFrameTerminator)
  }
  Ok(Frame::new(kind, channel, input[7:7 + size].to_owned()))
}
