///|
fn append_bytes(output : Array[Byte], input : Bytes) -> Unit {
  for byte in input {
    output.push(byte)
  }
}

///|
fn remove_prefix(input : Array[Byte], count : Int) -> Array[Byte] {
  let output : Array[Byte] = []
  for index = count; index < input.length(); index = index + 1 {
    output.push(input[index])
  }
  output
}

///|
pub enum TransportError {
  ProtocolError(@protocol.AmqpError)
  SessionError(@session.SessionError)
  FrameTooLarge
  Closed
} derive(Eq, @debug.Debug)

///|
/// A bounded native-side byte-stream adapter around the portable Session.
///
/// `feed` is deliberately independent of a socket so that fragmented reads,
/// protocol errors, and response encoding can be tested deterministically.
pub struct NativeConnection {
  session : @session.Session
  mut input : Array[Byte]
  max_frame_size : Int
  mut header_complete : Bool
  mut closed : Bool
}

///|
pub fn NativeConnection::new(
  broker : @broker.Broker,
  max_frame_size : Int,
) -> NativeConnection {
  let limit = if max_frame_size <= 0 { 131072 } else { max_frame_size }
  {
    session: @session.Session::new(broker),
    input: [],
    max_frame_size: limit,
    header_complete: false,
    closed: false,
  }
}

///|
pub fn NativeConnection::session(self : NativeConnection) -> @session.Session {
  self.session
}

///|
fn NativeConnection::emit_frame(
  self : NativeConnection,
  output : Array[Byte],
  frame : @protocol.Frame,
) -> Result[Unit, TransportError] {
  let encoded = match
    @protocol.encode_frame_with_max(
      frame,
      if self.session.frame_max() < self.max_frame_size {
        self.session.frame_max()
      } else {
        self.max_frame_size
      },
    ) {
    Ok(value) => value
    Err(error) => return Err(TransportError::ProtocolError(error))
  }
  append_bytes(output, encoded)
  Ok(())
}

///|
fn NativeConnection::emit_session_frames(
  self : NativeConnection,
  output : Array[Byte],
  frames : Array[@protocol.Frame],
) -> Result[Unit, TransportError] {
  for frame in frames {
    match self.emit_frame(output, frame) {
      Ok(_) => ()
      Err(error) => return Err(error)
    }
  }
  Ok(())
}

///|
fn NativeConnection::consume_protocol_header(
  self : NativeConnection,
  output : Array[Byte],
) -> Result[Bool, TransportError] {
  if self.input.length() < 8 {
    return Ok(false)
  }
  let header = Bytes::from_array(self.input[:8])
  match self.session.receive_protocol_header(header) {
    Ok(server_header) => {
      append_bytes(output, server_header)
      self.input = remove_prefix(self.input, 8)
      self.header_complete = true
      let start = match self.session.server_start_frame() {
        Ok(value) => value
        Err(error) => {
          self.closed = true
          return Err(TransportError::SessionError(error))
        }
      }
      match self.emit_frame(output, start) {
        Ok(_) => Ok(true)
        Err(error) => Err(error)
      }
    }
    Err(error) => {
      self.closed = true
      Err(TransportError::SessionError(error))
    }
  }
}

///|
fn NativeConnection::consume_one_frame(
  self : NativeConnection,
  output : Array[Byte],
) -> Result[Bool, TransportError] {
  if self.input.length() < 7 {
    return Ok(false)
  }
  let size = match @protocol.decode_u32(Bytes::from_array(self.input[3:7])) {
    Ok(value) => value.reinterpret_as_int()
    Err(error) => return Err(TransportError::ProtocolError(error))
  }
  let limit = if self.session.frame_max() < self.max_frame_size {
    self.session.frame_max()
  } else {
    self.max_frame_size
  }
  if size < 0 || limit < 8 || size > limit - 8 {
    return Err(TransportError::FrameTooLarge)
  }
  let total = size + 8
  if self.input.length() < total {
    return Ok(false)
  }
  let encoded = Bytes::from_array(self.input[:total])
  self.input = remove_prefix(self.input, total)
  let frame = match @protocol.decode_frame_with_max(encoded, limit) {
    Ok(value) => value
    Err(error) => return Err(TransportError::ProtocolError(error))
  }
  let responses = match self.session.receive_frame(frame) {
    Ok(value) => value
    Err(error) => {
      self.closed = true
      return Err(TransportError::SessionError(error))
    }
  }
  match self.emit_session_frames(output, responses) {
    Ok(_) => {
      if self.session.state() is @session.SessionState::Closed {
        self.closed = true
      }
      Ok(true)
    }
    Err(error) => Err(error)
  }
}

///|
/// Feed arbitrary-sized input and return all complete server responses.
pub fn NativeConnection::feed(
  self : NativeConnection,
  input : Bytes,
) -> Result[Bytes, TransportError] {
  if self.closed {
    return Err(TransportError::Closed)
  }
  append_bytes(self.input, input)
  let output : Array[Byte] = []
  if !self.header_complete {
    match self.consume_protocol_header(output) {
      Ok(_) => ()
      Err(error) => {
        self.closed = true
        return Err(error)
      }
    }
  }
  if !self.header_complete {
    return Ok(Bytes::from_array(output[:]))
  }
  let mut progress = true
  while progress {
    progress = match self.consume_one_frame(output) {
      Ok(value) => value
      Err(error) => {
        self.closed = true
        return Err(error)
      }
    }
  }
  Ok(Bytes::from_array(output[:]))
}
