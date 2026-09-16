///|
fn method_prefix(class_id : UInt16, method_id : UInt16) -> Bytes {
  concat_bytes(encode_u16(class_id), encode_u16(method_id))
}

///|
fn method_flags(first : Bool, second : Bool) -> Bytes {
  let bits = (if first { 1 } else { 0 }) + (if second { 2 } else { 0 })
  Bytes::from_array([bits.to_byte()])
}

///|
fn unwrap_short_string(value : String) -> Result[Bytes, AmqpError] {
  encode_short_string(value)
}

///|
/// Encode a typed method payload, excluding the enclosing frame header.
pub fn encode_method(value : Method) -> Result[Bytes, AmqpError] {
  match value {
    BasicPublish(ticket~, exchange~, routing_key~, mandatory~, immediate~) => {
      let exchange = match unwrap_short_string(exchange) {
        Ok(bytes) => bytes
        Err(error) => return Err(error)
      }
      let routing_key = match unwrap_short_string(routing_key) {
        Ok(bytes) => bytes
        Err(error) => return Err(error)
      }
      let prefix = method_prefix(UInt16(60), UInt16(40))
      let with_ticket = concat_bytes(prefix, encode_u16(ticket))
      let with_exchange = concat_bytes(with_ticket, exchange)
      let with_routing_key = concat_bytes(with_exchange, routing_key)
      Ok(concat_bytes(with_routing_key, method_flags(mandatory, immediate)))
    }
    BasicAck(delivery_tag~, multiple~) => {
      let with_tag = concat_bytes(
        method_prefix(UInt16(60), UInt16(80)),
        encode_u64(delivery_tag),
      )
      Ok(concat_bytes(with_tag, method_flags(multiple, false)))
    }
    BasicReject(delivery_tag~, requeue~) => {
      let with_tag = concat_bytes(
        method_prefix(UInt16(60), UInt16(90)),
        encode_u64(delivery_tag),
      )
      Ok(concat_bytes(with_tag, method_flags(requeue, false)))
    }
  }
}

///|
fn read_u16_at(input : Bytes, offset : Int) -> Result[(UInt16, Int), AmqpError] {
  if offset + 2 > input.length() {
    Err(TruncatedInput)
  } else {
    match decode_u16(input[offset:offset + 2].to_owned()) {
      Ok(value) => Ok((value, offset + 2))
      Err(error) => Err(error)
    }
  }
}

///|
fn read_u64_at(input : Bytes, offset : Int) -> Result[(UInt64, Int), AmqpError] {
  if offset + 8 > input.length() {
    Err(TruncatedInput)
  } else {
    match decode_u64(input[offset:offset + 8].to_owned()) {
      Ok(value) => Ok((value, offset + 8))
      Err(error) => Err(error)
    }
  }
}

///|
fn read_short_string_at(
  input : Bytes,
  offset : Int,
) -> Result[(String, Int), AmqpError] {
  if offset >= input.length() {
    return Err(TruncatedInput)
  }
  let size = input[offset].to_int()
  if offset + 1 + size > input.length() {
    return Err(TruncatedInput)
  }
  match decode_short_string(input[offset:offset + 1 + size].to_owned()) {
    Ok(value) => Ok((value, offset + 1 + size))
    Err(error) => Err(error)
  }
}

///|
/// Decode one typed method payload, excluding the enclosing frame header.
pub fn decode_method(input : Bytes) -> Result[Method, AmqpError] {
  if input.length() < 4 {
    return Err(TruncatedInput)
  }
  let class_id = match decode_u16(input[0:2].to_owned()) {
    Ok(value) => value
    Err(error) => return Err(error)
  }
  let method_id = match decode_u16(input[2:4].to_owned()) {
    Ok(value) => value
    Err(error) => return Err(error)
  }
  if class_id == UInt16(60) && method_id == UInt16(40) {
    let (ticket, offset) = match read_u16_at(input, 4) {
      Ok(value) => value
      Err(error) => return Err(error)
    }
    let (exchange, offset) = match read_short_string_at(input, offset) {
      Ok(value) => value
      Err(error) => return Err(error)
    }
    let (routing_key, offset) = match read_short_string_at(input, offset) {
      Ok(value) => value
      Err(error) => return Err(error)
    }
    if offset >= input.length() {
      return Err(TruncatedInput)
    }
    let flags = input[offset].to_int()
    if offset + 1 != input.length() || (flags & 0xFC) != 0 {
      return Err(UnsupportedMethod)
    }
    Ok(
      BasicPublish(
        ticket~,
        exchange~,
        routing_key~,
        mandatory=(flags & 0x01) != 0,
        immediate=(flags & 0x02) != 0,
      ),
    )
  } else if class_id == UInt16(60) && method_id == UInt16(80) {
    let (delivery_tag, offset) = match read_u64_at(input, 4) {
      Ok(value) => value
      Err(error) => return Err(error)
    }
    if offset + 1 != input.length() {
      return Err(TruncatedInput)
    }
    let flags = input[offset].to_int()
    if (flags & 0xFE) != 0 {
      return Err(UnsupportedMethod)
    }
    Ok(BasicAck(delivery_tag~, multiple=(flags & 0x01) != 0))
  } else if class_id == UInt16(60) && method_id == UInt16(90) {
    let (delivery_tag, offset) = match read_u64_at(input, 4) {
      Ok(value) => value
      Err(error) => return Err(error)
    }
    if offset + 1 != input.length() {
      return Err(TruncatedInput)
    }
    let flags = input[offset].to_int()
    if (flags & 0xFE) != 0 {
      return Err(UnsupportedMethod)
    }
    Ok(BasicReject(delivery_tag~, requeue=(flags & 0x01) != 0))
  } else {
    Err(UnsupportedMethod)
  }
}
