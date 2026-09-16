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
fn method_flags3(first : Bool, second : Bool, third : Bool) -> Bytes {
  let bits = (if first { 1 } else { 0 }) +
    (if second { 2 } else { 0 }) +
    (if third { 4 } else { 0 })
  Bytes::from_array([bits.to_byte()])
}

///|
fn method_flags4(
  first : Bool,
  second : Bool,
  third : Bool,
  fourth : Bool,
) -> Bytes {
  let bits = (if first { 1 } else { 0 }) +
    (if second { 2 } else { 0 }) +
    (if third { 4 } else { 0 }) +
    (if fourth { 8 } else { 0 })
  Bytes::from_array([bits.to_byte()])
}

///|
fn method_flags5(
  first : Bool,
  second : Bool,
  third : Bool,
  fourth : Bool,
  fifth : Bool,
) -> Bytes {
  let bits = (if first { 1 } else { 0 }) +
    (if second { 2 } else { 0 }) +
    (if third { 4 } else { 0 }) +
    (if fourth { 8 } else { 0 }) +
    (if fifth { 16 } else { 0 })
  Bytes::from_array([bits.to_byte()])
}

///|
fn encode_parts(
  parts : Array[Result[Bytes, AmqpError]],
) -> Result[Bytes, AmqpError] {
  let output : Array[Byte] = []
  for part in parts {
    match part {
      Ok(bytes) =>
        for byte in bytes {
          output.push(byte)
        }
      Err(error) => return Err(error)
    }
  }
  Ok(Bytes::from_array(output[:]))
}

///|
fn valid_exchange_name(value : String) -> Bool {
  let encoded = @utf8.encode(value[:])
  if encoded.length() > 127 {
    return false
  }
  for byte in encoded {
    let code = byte.to_int()
    if !((code >= 0x30 && code <= 0x39) ||
      (code >= 0x41 && code <= 0x5A) ||
      (code >= 0x61 && code <= 0x7A) ||
      code == 0x2D ||
      code == 0x2E ||
      code == 0x5F ||
      code == 0x3A) {
      return false
    }
  }
  true
}

///|
/// Encode a typed AMQP method payload, excluding its enclosing frame header.
pub fn encode_method(value : Method) -> Result[Bytes, AmqpError] {
  match value {
    ConnectionStart(
      version_major~,
      version_minor~,
      server_properties~,
      mechanisms~,
      locales~
    ) =>
      encode_parts([
        Ok(method_prefix(UInt16(10), UInt16(10))),
        Ok(Bytes::from_array([version_major, version_minor])),
        encode_field_table(server_properties),
        encode_long_string(mechanisms),
        encode_long_string(locales),
      ])
    ConnectionStartOk(client_properties~, mechanism~, response~, locale~) =>
      encode_parts([
        Ok(method_prefix(UInt16(10), UInt16(11))),
        encode_field_table(client_properties),
        encode_short_string(mechanism),
        encode_long_string(response),
        encode_short_string(locale),
      ])
    ConnectionTune(channel_max~, frame_max~, heartbeat~) =>
      encode_parts([
        Ok(method_prefix(UInt16(10), UInt16(30))),
        Ok(encode_u16(channel_max)),
        Ok(encode_u32(frame_max)),
        Ok(encode_u16(heartbeat)),
      ])
    ConnectionTuneOk(channel_max~, frame_max~, heartbeat~) =>
      encode_parts([
        Ok(method_prefix(UInt16(10), UInt16(31))),
        Ok(encode_u16(channel_max)),
        Ok(encode_u32(frame_max)),
        Ok(encode_u16(heartbeat)),
      ])
    ConnectionOpen(virtual_host~, capabilities~, insist~) =>
      encode_parts([
        Ok(method_prefix(UInt16(10), UInt16(40))),
        encode_short_string(virtual_host),
        encode_short_string(capabilities),
        Ok(method_flags(insist, false)),
      ])
    ConnectionOpenOk => Ok(method_prefix(UInt16(10), UInt16(41)))
    ConnectionClose(reply_code~, reply_text~, class_id~, method_id~) =>
      encode_parts([
        Ok(method_prefix(UInt16(10), UInt16(50))),
        Ok(encode_u16(reply_code)),
        encode_short_string(reply_text),
        Ok(encode_u16(class_id)),
        Ok(encode_u16(method_id)),
      ])
    ConnectionCloseOk => Ok(method_prefix(UInt16(10), UInt16(51)))
    ChannelOpen(out_of_band~) =>
      encode_parts([
        Ok(method_prefix(UInt16(20), UInt16(10))),
        encode_short_string(out_of_band),
      ])
    ChannelOpenOk => Ok(method_prefix(UInt16(20), UInt16(11)))
    ChannelClose(reply_code~, reply_text~, class_id~, method_id~) =>
      encode_parts([
        Ok(method_prefix(UInt16(20), UInt16(40))),
        Ok(encode_u16(reply_code)),
        encode_short_string(reply_text),
        Ok(encode_u16(class_id)),
        Ok(encode_u16(method_id)),
      ])
    ChannelCloseOk => Ok(method_prefix(UInt16(20), UInt16(41)))
    ExchangeDeclare(
      ticket~,
      exchange~,
      kind~,
      passive~,
      durable~,
      auto_delete~,
      internal~,
      nowait~,
      arguments~
    ) =>
      encode_parts([
        Ok(method_prefix(UInt16(40), UInt16(10))),
        Ok(encode_u16(ticket)),
        encode_short_string(exchange),
        encode_short_string(kind),
        Ok(method_flags5(passive, durable, auto_delete, internal, nowait)),
        encode_field_table(arguments),
      ])
    ExchangeDeclareOk => Ok(method_prefix(UInt16(40), UInt16(11)))
    ExchangeDelete(ticket~, exchange~, if_unused~, nowait~) =>
      encode_parts([
        Ok(method_prefix(UInt16(40), UInt16(20))),
        Ok(encode_u16(ticket)),
        encode_short_string(exchange),
        Ok(method_flags(if_unused, nowait)),
      ])
    ExchangeDeleteOk => Ok(method_prefix(UInt16(40), UInt16(21)))
    QueueDeclare(
      ticket~,
      queue~,
      passive~,
      durable~,
      exclusive~,
      auto_delete~,
      nowait~,
      arguments~
    ) =>
      encode_parts([
        Ok(method_prefix(UInt16(50), UInt16(10))),
        Ok(encode_u16(ticket)),
        encode_short_string(queue),
        Ok(method_flags5(passive, durable, exclusive, auto_delete, nowait)),
        encode_field_table(arguments),
      ])
    QueueDeclareOk(queue~, message_count~, consumer_count~) =>
      encode_parts([
        Ok(method_prefix(UInt16(50), UInt16(11))),
        encode_short_string(queue),
        Ok(encode_u32(message_count)),
        Ok(encode_u32(consumer_count)),
      ])
    QueueBind(ticket~, queue~, exchange~, routing_key~, nowait~, arguments~) =>
      encode_parts([
        Ok(method_prefix(UInt16(50), UInt16(20))),
        Ok(encode_u16(ticket)),
        encode_short_string(queue),
        encode_short_string(exchange),
        encode_short_string(routing_key),
        Ok(method_flags(nowait, false)),
        encode_field_table(arguments),
      ])
    QueueBindOk => Ok(method_prefix(UInt16(50), UInt16(21)))
    QueueUnbind(ticket~, queue~, exchange~, routing_key~, arguments~) =>
      encode_parts([
        Ok(method_prefix(UInt16(50), UInt16(50))),
        Ok(encode_u16(ticket)),
        encode_short_string(queue),
        encode_short_string(exchange),
        encode_short_string(routing_key),
        encode_field_table(arguments),
      ])
    QueueUnbindOk => Ok(method_prefix(UInt16(50), UInt16(51)))
    QueueDelete(ticket~, queue~, if_unused~, if_empty~, nowait~) =>
      encode_parts([
        Ok(method_prefix(UInt16(50), UInt16(40))),
        Ok(encode_u16(ticket)),
        encode_short_string(queue),
        Ok(method_flags3(if_unused, if_empty, nowait)),
      ])
    QueueDeleteOk(message_count~) =>
      encode_parts([
        Ok(method_prefix(UInt16(50), UInt16(41))),
        Ok(encode_u32(message_count)),
      ])
    BasicQos(prefetch_size~, prefetch_count~, global~) =>
      encode_parts([
        Ok(method_prefix(UInt16(60), UInt16(10))),
        Ok(encode_u32(prefetch_size)),
        Ok(encode_u16(prefetch_count)),
        Ok(method_flags(global, false)),
      ])
    BasicQosOk => Ok(method_prefix(UInt16(60), UInt16(11)))
    BasicConsume(
      ticket~,
      queue~,
      consumer_tag~,
      no_local~,
      no_ack~,
      exclusive~,
      nowait~,
      arguments~
    ) =>
      encode_parts([
        Ok(method_prefix(UInt16(60), UInt16(20))),
        Ok(encode_u16(ticket)),
        encode_short_string(queue),
        encode_short_string(consumer_tag),
        Ok(method_flags4(no_local, no_ack, exclusive, nowait)),
        encode_field_table(arguments),
      ])
    BasicConsumeOk(consumer_tag~) =>
      encode_parts([
        Ok(method_prefix(UInt16(60), UInt16(21))),
        encode_short_string(consumer_tag),
      ])
    BasicCancel(consumer_tag~, nowait~) =>
      encode_parts([
        Ok(method_prefix(UInt16(60), UInt16(30))),
        encode_short_string(consumer_tag),
        Ok(method_flags(nowait, false)),
      ])
    BasicCancelOk(consumer_tag~) =>
      encode_parts([
        Ok(method_prefix(UInt16(60), UInt16(31))),
        encode_short_string(consumer_tag),
      ])
    BasicPublish(ticket~, exchange~, routing_key~, mandatory~, immediate~) => {
      if ticket != UInt16(0) {
        return Err(UnsupportedMethod)
      }
      if !valid_exchange_name(exchange) {
        return Err(InvalidExchangeName)
      }
      encode_parts([
        Ok(method_prefix(UInt16(60), UInt16(40))),
        Ok(encode_u16(ticket)),
        encode_short_string(exchange),
        encode_short_string(routing_key),
        Ok(method_flags(mandatory, immediate)),
      ])
    }
    BasicDeliver(
      consumer_tag~,
      delivery_tag~,
      redelivered~,
      exchange~,
      routing_key~
    ) =>
      encode_parts([
        Ok(method_prefix(UInt16(60), UInt16(60))),
        encode_short_string(consumer_tag),
        Ok(encode_u64(delivery_tag)),
        Ok(method_flags(redelivered, false)),
        encode_short_string(exchange),
        encode_short_string(routing_key),
      ])
    BasicGet(ticket~, queue~, no_ack~) =>
      encode_parts([
        Ok(method_prefix(UInt16(60), UInt16(70))),
        Ok(encode_u16(ticket)),
        encode_short_string(queue),
        Ok(method_flags(no_ack, false)),
      ])
    BasicGetOk(
      delivery_tag~,
      redelivered~,
      exchange~,
      routing_key~,
      message_count~
    ) =>
      encode_parts([
        Ok(method_prefix(UInt16(60), UInt16(71))),
        Ok(encode_u64(delivery_tag)),
        Ok(method_flags(redelivered, false)),
        encode_short_string(exchange),
        encode_short_string(routing_key),
        Ok(encode_u32(message_count)),
      ])
    BasicGetEmpty(cluster_id~) =>
      encode_parts([
        Ok(method_prefix(UInt16(60), UInt16(72))),
        encode_short_string(cluster_id),
      ])
    BasicAck(delivery_tag~, multiple~) => {
      if delivery_tag == UInt64(0) && !multiple {
        return Err(InvalidDeliveryTag)
      }
      encode_parts([
        Ok(method_prefix(UInt16(60), UInt16(80))),
        Ok(encode_u64(delivery_tag)),
        Ok(method_flags(multiple, false)),
      ])
    }
    BasicReject(delivery_tag~, requeue~) => {
      if delivery_tag == UInt64(0) {
        return Err(InvalidDeliveryTag)
      }
      encode_parts([
        Ok(method_prefix(UInt16(60), UInt16(90))),
        Ok(encode_u64(delivery_tag)),
        Ok(method_flags(requeue, false)),
      ])
    }
    BasicReturn(reply_code~, reply_text~, exchange~, routing_key~) =>
      encode_parts([
        Ok(method_prefix(UInt16(60), UInt16(50))),
        Ok(encode_u16(reply_code)),
        encode_short_string(reply_text),
        encode_short_string(exchange),
        encode_short_string(routing_key),
      ])
  }
}

///|
fn read_u16_at(input : Bytes, offset : Int) -> Result[(UInt16, Int), AmqpError] {
  if offset < 0 || offset + 2 > input.length() {
    Err(TruncatedInput)
  } else {
    match decode_u16(input[offset:offset + 2].to_owned()) {
      Ok(value) => Ok((value, offset + 2))
      Err(error) => Err(error)
    }
  }
}

///|
fn read_u32_at(input : Bytes, offset : Int) -> Result[(UInt, Int), AmqpError] {
  if offset < 0 || offset + 4 > input.length() {
    Err(TruncatedInput)
  } else {
    match decode_u32(input[offset:offset + 4].to_owned()) {
      Ok(value) => Ok((value, offset + 4))
      Err(error) => Err(error)
    }
  }
}

///|
fn read_u64_at(input : Bytes, offset : Int) -> Result[(UInt64, Int), AmqpError] {
  if offset < 0 || offset + 8 > input.length() {
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
  if offset < 0 || offset >= input.length() {
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
fn read_long_string_at(
  input : Bytes,
  offset : Int,
) -> Result[(Bytes, Int), AmqpError] {
  let (size, start) = match read_u32_at(input, offset) {
    Ok(value) => value
    Err(error) => return Err(error)
  }
  let size = size.reinterpret_as_int()
  if size < 0 || size > MAX_LONG_STRING_SIZE {
    return Err(ValueTooLong)
  }
  if start + size > input.length() {
    return Err(TruncatedInput)
  }
  Ok((input[start:start + size].to_owned(), start + size))
}

///|
fn read_table_at(
  input : Bytes,
  offset : Int,
) -> Result[(FieldTable, Int), AmqpError] {
  let (size, start) = match read_u32_at(input, offset) {
    Ok(value) => value
    Err(error) => return Err(error)
  }
  let size = size.reinterpret_as_int()
  if size < 0 || size > MAX_FIELD_TABLE_SIZE {
    return Err(FieldTableTooLarge)
  }
  if start + size > input.length() {
    return Err(TruncatedInput)
  }
  let encoded = concat_bytes(
    encode_u32(size.reinterpret_as_uint()),
    input[start:start + size].to_owned(),
  )
  match decode_field_table(encoded) {
    Ok(value) => Ok((value, start + size))
    Err(error) => Err(error)
  }
}

///|
fn read_byte_at(input : Bytes, offset : Int) -> Result[(Byte, Int), AmqpError] {
  if offset < 0 || offset >= input.length() {
    Err(TruncatedInput)
  } else {
    Ok((input[offset], offset + 1))
  }
}

///|
fn read_flags_at(
  input : Bytes,
  offset : Int,
  allowed : Int,
) -> Result[(Int, Int), AmqpError] {
  let (value, next) = match read_byte_at(input, offset) {
    Ok(result) => result
    Err(error) => return Err(error)
  }
  let bits = value.to_int()
  let invalid = if allowed == 1 {
    bits & 0xFE
  } else if allowed == 3 {
    bits & 0xFC
  } else if allowed == 7 {
    bits & 0xF8
  } else if allowed == 15 {
    bits & 0xF0
  } else if allowed == 31 {
    bits & 0xE0
  } else {
    0
  }
  if invalid != 0 {
    Err(UnsupportedMethod)
  } else {
    Ok((bits, next))
  }
}

///|
fn require_end(input : Bytes, offset : Int) -> Result[Unit, AmqpError] {
  if offset == input.length() {
    Ok(())
  } else {
    Err(UnsupportedMethod)
  }
}

///|
/// Decode one typed AMQP method payload, excluding its enclosing frame header.
pub fn decode_method(input : Bytes) -> Result[Method, AmqpError] {
  if input.length() < 4 {
    return Err(TruncatedInput)
  }
  let (class_id, _) = match read_u16_at(input, 0) {
    Ok(value) => value
    Err(error) => return Err(error)
  }
  let (method_id, _) = match read_u16_at(input, 2) {
    Ok(value) => value
    Err(error) => return Err(error)
  }
  match (class_id.to_int(), method_id.to_int()) {
    (10, 10) => {
      if input.length() < 6 {
        return Err(TruncatedInput)
      }
      let version_major = input[4]
      let version_minor = input[5]
      let (server_properties, offset) = match read_table_at(input, 6) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (mechanisms, offset) = match read_long_string_at(input, offset) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (locales, offset) = match read_long_string_at(input, offset) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      match require_end(input, offset) {
        Ok(_) =>
          Ok(
            ConnectionStart(
              version_major~,
              version_minor~,
              server_properties~,
              mechanisms~,
              locales~,
            ),
          )
        Err(error) => Err(error)
      }
    }
    (10, 11) => {
      let (client_properties, offset) = match read_table_at(input, 4) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (mechanism, offset) = match read_short_string_at(input, offset) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (response, offset) = match read_long_string_at(input, offset) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (locale, offset) = match read_short_string_at(input, offset) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      match require_end(input, offset) {
        Ok(_) =>
          Ok(
            ConnectionStartOk(
              client_properties~,
              mechanism~,
              response~,
              locale~,
            ),
          )
        Err(error) => Err(error)
      }
    }
    (10, 30) | (10, 31) => {
      let (channel_max, offset) = match read_u16_at(input, 4) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (frame_max, offset) = match read_u32_at(input, offset) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (heartbeat, offset) = match read_u16_at(input, offset) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      match require_end(input, offset) {
        Ok(_) =>
          if method_id == UInt16(30) {
            Ok(ConnectionTune(channel_max~, frame_max~, heartbeat~))
          } else {
            Ok(ConnectionTuneOk(channel_max~, frame_max~, heartbeat~))
          }
        Err(error) => Err(error)
      }
    }
    (10, 40) => {
      let (virtual_host, offset) = match read_short_string_at(input, 4) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (capabilities, offset) = match read_short_string_at(input, offset) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (bits, offset) = match read_flags_at(input, offset, 1) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      match require_end(input, offset) {
        Ok(_) =>
          Ok(
            ConnectionOpen(virtual_host~, capabilities~, insist=(bits & 1) != 0),
          )
        Err(error) => Err(error)
      }
    }
    (10, 41) =>
      match require_end(input, 4) {
        Ok(_) => Ok(ConnectionOpenOk)
        Err(error) => Err(error)
      }
    (10, 50) => {
      let (reply_code, offset) = match read_u16_at(input, 4) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (reply_text, offset) = match read_short_string_at(input, offset) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (close_class, offset) = match read_u16_at(input, offset) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (close_method, offset) = match read_u16_at(input, offset) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      match require_end(input, offset) {
        Ok(_) =>
          Ok(
            ConnectionClose(
              reply_code~,
              reply_text~,
              class_id=close_class,
              method_id=close_method,
            ),
          )
        Err(error) => Err(error)
      }
    }
    (10, 51) =>
      match require_end(input, 4) {
        Ok(_) => Ok(ConnectionCloseOk)
        Err(error) => Err(error)
      }
    (20, 10) => {
      let (out_of_band, offset) = match read_short_string_at(input, 4) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      match require_end(input, offset) {
        Ok(_) => Ok(ChannelOpen(out_of_band~))
        Err(error) => Err(error)
      }
    }
    (20, 11) =>
      match require_end(input, 4) {
        Ok(_) => Ok(ChannelOpenOk)
        Err(error) => Err(error)
      }
    (20, 40) => {
      let (reply_code, offset) = match read_u16_at(input, 4) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (reply_text, offset) = match read_short_string_at(input, offset) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (close_class, offset) = match read_u16_at(input, offset) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (close_method, offset) = match read_u16_at(input, offset) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      match require_end(input, offset) {
        Ok(_) =>
          Ok(
            ChannelClose(
              reply_code~,
              reply_text~,
              class_id=close_class,
              method_id=close_method,
            ),
          )
        Err(error) => Err(error)
      }
    }
    (20, 41) =>
      match require_end(input, 4) {
        Ok(_) => Ok(ChannelCloseOk)
        Err(error) => Err(error)
      }
    (40, 10) => {
      let (ticket, offset) = match read_u16_at(input, 4) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (exchange, offset) = match read_short_string_at(input, offset) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (kind, offset) = match read_short_string_at(input, offset) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (bits, offset) = match read_flags_at(input, offset, 31) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (arguments, offset) = match read_table_at(input, offset) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      match require_end(input, offset) {
        Ok(_) =>
          Ok(
            ExchangeDeclare(
              ticket~,
              exchange~,
              kind~,
              passive=(bits & 1) != 0,
              durable=(bits & 2) != 0,
              auto_delete=(bits & 4) != 0,
              internal=(bits & 8) != 0,
              nowait=(bits & 16) != 0,
              arguments~,
            ),
          )
        Err(error) => Err(error)
      }
    }
    (40, 11) =>
      match require_end(input, 4) {
        Ok(_) => Ok(ExchangeDeclareOk)
        Err(error) => Err(error)
      }
    (40, 20) => {
      let (ticket, offset) = match read_u16_at(input, 4) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (exchange, offset) = match read_short_string_at(input, offset) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (bits, offset) = match read_flags_at(input, offset, 3) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      match require_end(input, offset) {
        Ok(_) =>
          Ok(
            ExchangeDelete(
              ticket~,
              exchange~,
              if_unused=(bits & 1) != 0,
              nowait=(bits & 2) != 0,
            ),
          )
        Err(error) => Err(error)
      }
    }
    (40, 21) =>
      match require_end(input, 4) {
        Ok(_) => Ok(ExchangeDeleteOk)
        Err(error) => Err(error)
      }
    (50, 10) => {
      let (ticket, offset) = match read_u16_at(input, 4) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (queue, offset) = match read_short_string_at(input, offset) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (bits, offset) = match read_flags_at(input, offset, 31) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (arguments, offset) = match read_table_at(input, offset) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      match require_end(input, offset) {
        Ok(_) =>
          Ok(
            QueueDeclare(
              ticket~,
              queue~,
              passive=(bits & 1) != 0,
              durable=(bits & 2) != 0,
              exclusive=(bits & 4) != 0,
              auto_delete=(bits & 8) != 0,
              nowait=(bits & 16) != 0,
              arguments~,
            ),
          )
        Err(error) => Err(error)
      }
    }
    (50, 11) => {
      let (queue, offset) = match read_short_string_at(input, 4) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (message_count, offset) = match read_u32_at(input, offset) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (consumer_count, offset) = match read_u32_at(input, offset) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      match require_end(input, offset) {
        Ok(_) => Ok(QueueDeclareOk(queue~, message_count~, consumer_count~))
        Err(error) => Err(error)
      }
    }
    (50, 20) => {
      let (ticket, offset) = match read_u16_at(input, 4) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (queue, offset) = match read_short_string_at(input, offset) {
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
      let (bits, offset) = match read_flags_at(input, offset, 1) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (arguments, offset) = match read_table_at(input, offset) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      match require_end(input, offset) {
        Ok(_) =>
          Ok(
            QueueBind(
              ticket~,
              queue~,
              exchange~,
              routing_key~,
              nowait=(bits & 1) != 0,
              arguments~,
            ),
          )
        Err(error) => Err(error)
      }
    }
    (50, 21) =>
      match require_end(input, 4) {
        Ok(_) => Ok(QueueBindOk)
        Err(error) => Err(error)
      }
    (50, 50) => {
      let (ticket, offset) = match read_u16_at(input, 4) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (queue, offset) = match read_short_string_at(input, offset) {
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
      let (arguments, offset) = match read_table_at(input, offset) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      match require_end(input, offset) {
        Ok(_) =>
          Ok(QueueUnbind(ticket~, queue~, exchange~, routing_key~, arguments~))
        Err(error) => Err(error)
      }
    }
    (50, 51) =>
      match require_end(input, 4) {
        Ok(_) => Ok(QueueUnbindOk)
        Err(error) => Err(error)
      }
    (50, 40) => {
      let (ticket, offset) = match read_u16_at(input, 4) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (queue, offset) = match read_short_string_at(input, offset) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (bits, offset) = match read_flags_at(input, offset, 7) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      match require_end(input, offset) {
        Ok(_) =>
          Ok(
            QueueDelete(
              ticket~,
              queue~,
              if_unused=(bits & 1) != 0,
              if_empty=(bits & 2) != 0,
              nowait=(bits & 4) != 0,
            ),
          )
        Err(error) => Err(error)
      }
    }
    (50, 41) => {
      let (message_count, offset) = match read_u32_at(input, 4) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      match require_end(input, offset) {
        Ok(_) => Ok(QueueDeleteOk(message_count~))
        Err(error) => Err(error)
      }
    }
    (60, 10) => {
      let (prefetch_size, offset) = match read_u32_at(input, 4) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (prefetch_count, offset) = match read_u16_at(input, offset) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (bits, offset) = match read_flags_at(input, offset, 1) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      match require_end(input, offset) {
        Ok(_) =>
          Ok(BasicQos(prefetch_size~, prefetch_count~, global=(bits & 1) != 0))
        Err(error) => Err(error)
      }
    }
    (60, 11) =>
      match require_end(input, 4) {
        Ok(_) => Ok(BasicQosOk)
        Err(error) => Err(error)
      }
    (60, 20) => {
      let (ticket, offset) = match read_u16_at(input, 4) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (queue, offset) = match read_short_string_at(input, offset) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (consumer_tag, offset) = match read_short_string_at(input, offset) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (bits, offset) = match read_flags_at(input, offset, 15) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (arguments, offset) = match read_table_at(input, offset) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      match require_end(input, offset) {
        Ok(_) =>
          Ok(
            BasicConsume(
              ticket~,
              queue~,
              consumer_tag~,
              no_local=(bits & 1) != 0,
              no_ack=(bits & 2) != 0,
              exclusive=(bits & 4) != 0,
              nowait=(bits & 8) != 0,
              arguments~,
            ),
          )
        Err(error) => Err(error)
      }
    }
    (60, 21) => {
      let (consumer_tag, offset) = match read_short_string_at(input, 4) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      match require_end(input, offset) {
        Ok(_) => Ok(BasicConsumeOk(consumer_tag~))
        Err(error) => Err(error)
      }
    }
    (60, 30) => {
      let (consumer_tag, offset) = match read_short_string_at(input, 4) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (bits, offset) = match read_flags_at(input, offset, 1) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      match require_end(input, offset) {
        Ok(_) => Ok(BasicCancel(consumer_tag~, nowait=(bits & 1) != 0))
        Err(error) => Err(error)
      }
    }
    (60, 31) => {
      let (consumer_tag, offset) = match read_short_string_at(input, 4) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      match require_end(input, offset) {
        Ok(_) => Ok(BasicCancelOk(consumer_tag~))
        Err(error) => Err(error)
      }
    }
    (60, 40) => {
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
      let (bits, offset) = match read_flags_at(input, offset, 3) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      if ticket != UInt16(0) || !valid_exchange_name(exchange) {
        return Err(UnsupportedMethod)
      }
      match require_end(input, offset) {
        Ok(_) =>
          Ok(
            BasicPublish(
              ticket~,
              exchange~,
              routing_key~,
              mandatory=(bits & 1) != 0,
              immediate=(bits & 2) != 0,
            ),
          )
        Err(error) => Err(error)
      }
    }
    (60, 50) => {
      let (reply_code, offset) = match read_u16_at(input, 4) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (reply_text, offset) = match read_short_string_at(input, offset) {
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
      match require_end(input, offset) {
        Ok(_) =>
          Ok(BasicReturn(reply_code~, reply_text~, exchange~, routing_key~))
        Err(error) => Err(error)
      }
    }
    (60, 60) => {
      let (consumer_tag, offset) = match read_short_string_at(input, 4) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (delivery_tag, offset) = match read_u64_at(input, offset) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (bits, offset) = match read_flags_at(input, offset, 1) {
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
      match require_end(input, offset) {
        Ok(_) =>
          Ok(
            BasicDeliver(
              consumer_tag~,
              delivery_tag~,
              redelivered=(bits & 1) != 0,
              exchange~,
              routing_key~,
            ),
          )
        Err(error) => Err(error)
      }
    }
    (60, 70) => {
      let (ticket, offset) = match read_u16_at(input, 4) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (queue, offset) = match read_short_string_at(input, offset) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (bits, offset) = match read_flags_at(input, offset, 1) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      match require_end(input, offset) {
        Ok(_) => Ok(BasicGet(ticket~, queue~, no_ack=(bits & 1) != 0))
        Err(error) => Err(error)
      }
    }
    (60, 71) => {
      let (delivery_tag, offset) = match read_u64_at(input, 4) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (bits, offset) = match read_flags_at(input, offset, 1) {
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
      let (message_count, offset) = match read_u32_at(input, offset) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      match require_end(input, offset) {
        Ok(_) =>
          Ok(
            BasicGetOk(
              delivery_tag~,
              redelivered=(bits & 1) != 0,
              exchange~,
              routing_key~,
              message_count~,
            ),
          )
        Err(error) => Err(error)
      }
    }
    (60, 72) => {
      let (cluster_id, offset) = match read_short_string_at(input, 4) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      match require_end(input, offset) {
        Ok(_) => Ok(BasicGetEmpty(cluster_id~))
        Err(error) => Err(error)
      }
    }
    (60, 80) => {
      let (delivery_tag, offset) = match read_u64_at(input, 4) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (bits, offset) = match read_flags_at(input, offset, 1) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      if delivery_tag == UInt64(0) && (bits & 1) == 0 {
        return Err(InvalidDeliveryTag)
      }
      match require_end(input, offset) {
        Ok(_) => Ok(BasicAck(delivery_tag~, multiple=(bits & 1) != 0))
        Err(error) => Err(error)
      }
    }
    (60, 90) => {
      let (delivery_tag, offset) = match read_u64_at(input, 4) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let (bits, offset) = match read_flags_at(input, offset, 1) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      if delivery_tag == UInt64(0) {
        return Err(InvalidDeliveryTag)
      }
      match require_end(input, offset) {
        Ok(_) => Ok(BasicReject(delivery_tag~, requeue=(bits & 1) != 0))
        Err(error) => Err(error)
      }
    }
    _ => Err(UnsupportedMethod)
  }
}
