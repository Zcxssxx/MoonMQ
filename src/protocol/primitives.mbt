///|
fn concat_bytes(first : Bytes, second : Bytes) -> Bytes {
  let result : Array[Byte] = []
  for byte in first {
    result.push(byte)
  }
  for byte in second {
    result.push(byte)
  }
  Bytes::from_array(result[:])
}

///|
/// Encode an unsigned 16-bit integer in AMQP network byte order.
pub fn encode_u16(value : UInt16) -> Bytes {
  let number = value.to_int()
  Bytes::from_array([
    ((number >> 8) & 0xFF).to_byte(),
    (number & 0xFF).to_byte(),
  ])
}

///|
/// Encode an unsigned 32-bit integer in AMQP network byte order.
pub fn encode_u32(value : UInt) -> Bytes {
  Bytes::from_array([
    (value >> 24).to_byte(),
    (value >> 16).to_byte(),
    (value >> 8).to_byte(),
    value.to_byte(),
  ])
}

///|
/// Decode an unsigned 16-bit integer in AMQP network byte order.
pub fn decode_u16(input : Bytes) -> Result[UInt16, AmqpError] {
  if input.length() != 2 {
    Err(TruncatedInput)
  } else {
    Ok((input[0].to_uint16() << 8) | input[1].to_uint16())
  }
}

///|
/// Decode an unsigned 32-bit integer in AMQP network byte order.
pub fn decode_u32(input : Bytes) -> Result[UInt, AmqpError] {
  if input.length() != 4 {
    Err(TruncatedInput)
  } else {
    Ok(
      (input[0].to_uint() << 24) |
      (input[1].to_uint() << 16) |
      (input[2].to_uint() << 8) |
      input[3].to_uint(),
    )
  }
}

///|
/// Encode an unsigned 64-bit integer in AMQP network byte order.
pub fn encode_u64(value : UInt64) -> Bytes {
  Bytes::from_array([
    (value >> 56).to_byte(),
    (value >> 48).to_byte(),
    (value >> 40).to_byte(),
    (value >> 32).to_byte(),
    (value >> 24).to_byte(),
    (value >> 16).to_byte(),
    (value >> 8).to_byte(),
    value.to_byte(),
  ])
}

///|
/// Decode an unsigned 64-bit integer in AMQP network byte order.
pub fn decode_u64(input : Bytes) -> Result[UInt64, AmqpError] {
  if input.length() != 8 {
    return Err(TruncatedInput)
  }
  Ok(
    (input[0].to_uint64() << 56) |
    (input[1].to_uint64() << 48) |
    (input[2].to_uint64() << 40) |
    (input[3].to_uint64() << 32) |
    (input[4].to_uint64() << 24) |
    (input[5].to_uint64() << 16) |
    (input[6].to_uint64() << 8) |
    input[7].to_uint64(),
  )
}

///|
/// Encode an AMQP short string, whose length is measured in UTF-8 bytes.
pub fn encode_short_string(value : String) -> Result[Bytes, AmqpError] {
  let encoded = @utf8.encode(value[:])
  if encoded.length() > 255 {
    Err(ValueTooLong)
  } else {
    for byte in encoded {
      if byte == b'\x00' {
        return Err(InvalidShortString)
      }
    }
    Ok(concat_bytes(Bytes::from_array([encoded.length().to_byte()]), encoded))
  }
}

///|
/// Decode an AMQP short string and reject malformed UTF-8 or an incorrect size.
pub fn decode_short_string(input : Bytes) -> Result[String, AmqpError] {
  if input.length() < 1 {
    return Err(TruncatedInput)
  }
  let size = input[0].to_int()
  if input.length() != size + 1 {
    return Err(TruncatedInput)
  }
  for byte in input[1:] {
    if byte == b'\x00' {
      return Err(InvalidShortString)
    }
  }
  try @utf8.decode(input[1:]) catch {
    _ => Err(InvalidUtf8)
  } noraise {
    value => Ok(value)
  }
}

///|
const MAX_LONG_STRING_SIZE : Int = 1048576

///|
/// Encode an AMQP long string with a four-byte byte-length prefix.
pub fn encode_long_string(value : Bytes) -> Result[Bytes, AmqpError] {
  if value.length() > MAX_LONG_STRING_SIZE {
    Err(ValueTooLong)
  } else {
    Ok(concat_bytes(encode_u32(value.length().reinterpret_as_uint()), value))
  }
}

///|
/// Decode one exact AMQP long string and preserve its bytes unchanged.
pub fn decode_long_string(input : Bytes) -> Result[Bytes, AmqpError] {
  if input.length() < 4 {
    return Err(TruncatedInput)
  }
  let size = match decode_u32(input[0:4].to_owned()) {
    Ok(value) => value.reinterpret_as_int()
    Err(error) => return Err(error)
  }
  if size < 0 || size > MAX_LONG_STRING_SIZE {
    return Err(ValueTooLong)
  }
  if input.length() != 4 + size {
    return Err(TruncatedInput)
  }
  Ok(input[4:4 + size].to_owned())
}
