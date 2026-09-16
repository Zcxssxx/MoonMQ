///|
/// The bounded AMQP field values used by MoonMQ's Core Profile.
pub(all) enum FieldValue {
  FieldBoolean(Bool)
  FieldInt(Int)
  FieldLongString(Bytes)
  FieldTableValue(FieldTable)
  FieldArray(Array[FieldValue])
  FieldTimestamp(UInt64)
  FieldVoid
} derive(Eq, @debug.Debug)

///|
/// An AMQP field table represented in deterministic insertion order.
pub struct FieldTable {
  entries : Array[(String, FieldValue)]
} derive(Eq, @debug.Debug)

///|
pub fn FieldTable::new(entries : Array[(String, FieldValue)]) -> FieldTable {
  { entries, }
}

///|
pub fn FieldTable::entries(self : FieldTable) -> Array[(String, FieldValue)] {
  self.entries
}

///|
const MAX_FIELD_TABLE_SIZE : Int = 1048576

///|
fn append_bytes(output : Array[Byte], input : Bytes) -> Unit {
  for byte in input {
    output.push(byte)
  }
}

///|
fn encode_field_value(value : FieldValue) -> Result[Bytes, AmqpError] {
  match value {
    FieldBoolean(value) =>
      Ok(Bytes::from_array([b't', if value { b'\x01' } else { b'\x00' }]))
    FieldInt(value) =>
      Ok(
        concat_bytes(
          Bytes::from_array([b'I']),
          encode_u32(value.reinterpret_as_uint()),
        ),
      )
    FieldLongString(value) => {
      if value.length() > MAX_FIELD_TABLE_SIZE {
        return Err(FieldTableTooLarge)
      }
      Ok(
        concat_bytes(
          Bytes::from_array([b'S']),
          concat_bytes(encode_u32(value.length().reinterpret_as_uint()), value),
        ),
      )
    }
    FieldTableValue(value) => {
      let encoded = match encode_field_table(value) {
        Ok(bytes) => bytes
        Err(error) => return Err(error)
      }
      Ok(concat_bytes(Bytes::from_array([b'F']), encoded))
    }
    FieldArray(values) => {
      let body : Array[Byte] = []
      for item in values {
        let encoded = match encode_field_value(item) {
          Ok(bytes) => bytes
          Err(error) => return Err(error)
        }
        append_bytes(body, encoded)
        if body.length() > MAX_FIELD_TABLE_SIZE {
          return Err(FieldTableTooLarge)
        }
      }
      let encoded_body = Bytes::from_array(body[:])
      Ok(
        concat_bytes(
          Bytes::from_array([b'A']),
          concat_bytes(
            encode_u32(encoded_body.length().reinterpret_as_uint()),
            encoded_body,
          ),
        ),
      )
    }
    FieldTimestamp(value) =>
      Ok(concat_bytes(Bytes::from_array([b'T']), encode_u64(value)))
    FieldVoid => Ok(Bytes::from_array([b'V']))
  }
}

///|
/// Encode an AMQP field table with its four-byte byte-length prefix.
pub fn encode_field_table(table : FieldTable) -> Result[Bytes, AmqpError] {
  let body : Array[Byte] = []
  for (key, value) in table.entries {
    let encoded_key = match encode_short_string(key) {
      Ok(bytes) => bytes
      Err(error) => return Err(error)
    }
    let encoded_value = match encode_field_value(value) {
      Ok(bytes) => bytes
      Err(error) => return Err(error)
    }
    append_bytes(body, encoded_key)
    append_bytes(body, encoded_value)
    if body.length() > MAX_FIELD_TABLE_SIZE {
      return Err(FieldTableTooLarge)
    }
  }
  let encoded_body = Bytes::from_array(body[:])
  Ok(
    concat_bytes(
      encode_u32(encoded_body.length().reinterpret_as_uint()),
      encoded_body,
    ),
  )
}

///|
fn read_u32_at_field(
  input : Bytes,
  offset : Int,
) -> Result[(UInt, Int), AmqpError] {
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
fn read_short_string_field(
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
fn read_field_value(
  input : Bytes,
  offset : Int,
) -> Result[(FieldValue, Int), AmqpError] {
  if offset < 0 || offset >= input.length() {
    return Err(TruncatedInput)
  }
  let tag = input[offset]
  let next = offset + 1
  match tag {
    b't' => {
      if next >= input.length() {
        return Err(TruncatedInput)
      }
      match input[next] {
        b'\x00' => Ok((FieldBoolean(false), next + 1))
        b'\x01' => Ok((FieldBoolean(true), next + 1))
        _ => Err(InvalidFieldValue)
      }
    }
    b'I' => {
      let (value, end) = match read_u32_at_field(input, next) {
        Ok(result) => result
        Err(error) => return Err(error)
      }
      Ok((FieldInt(value.reinterpret_as_int()), end))
    }
    b'S' => {
      let (size, start) = match read_u32_at_field(input, next) {
        Ok(result) => result
        Err(error) => return Err(error)
      }
      let size = size.reinterpret_as_int()
      if size < 0 ||
        size > MAX_FIELD_TABLE_SIZE ||
        start + size > input.length() {
        return Err(FieldTableTooLarge)
      }
      Ok((FieldLongString(input[start:start + size].to_owned()), start + size))
    }
    b'F' => {
      let (size, start) = match read_u32_at_field(input, next) {
        Ok(result) => result
        Err(error) => return Err(error)
      }
      let size = size.reinterpret_as_int()
      if size < 0 ||
        size > MAX_FIELD_TABLE_SIZE ||
        start + size > input.length() {
        return Err(FieldTableTooLarge)
      }
      let end = start + size
      let nested = match
        decode_field_table(
          concat_bytes(
            encode_u32(size.reinterpret_as_uint()),
            input[start:end].to_owned(),
          ),
        ) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      Ok((FieldTableValue(nested), end))
    }
    b'A' => {
      let (size, start) = match read_u32_at_field(input, next) {
        Ok(result) => result
        Err(error) => return Err(error)
      }
      let size = size.reinterpret_as_int()
      if size < 0 ||
        size > MAX_FIELD_TABLE_SIZE ||
        start + size > input.length() {
        return Err(FieldTableTooLarge)
      }
      let end = start + size
      let mut cursor = start
      let values : Array[FieldValue] = []
      while cursor < end {
        let (value, next_cursor) = match
          read_field_value(input[start:end].to_owned(), cursor - start) {
          Ok(result) => result
          Err(error) => return Err(error)
        }
        values.push(value)
        cursor = start + next_cursor
      }
      if cursor != end {
        return Err(TruncatedInput)
      }
      Ok((FieldArray(values), end))
    }
    b'T' => {
      if next + 8 > input.length() {
        return Err(TruncatedInput)
      }
      match decode_u64(input[next:next + 8].to_owned()) {
        Ok(value) => Ok((FieldTimestamp(value), next + 8))
        Err(error) => Err(error)
      }
    }
    b'V' => Ok((FieldVoid, next))
    _ => Err(InvalidFieldValue)
  }
}

///|
/// Decode one exact AMQP field table, including its four-byte length prefix.
pub fn decode_field_table(input : Bytes) -> Result[FieldTable, AmqpError] {
  let (size, start) = match read_u32_at_field(input, 0) {
    Ok(result) => result
    Err(error) => return Err(error)
  }
  let size = size.reinterpret_as_int()
  if size < 0 || size > MAX_FIELD_TABLE_SIZE {
    return Err(FieldTableTooLarge)
  }
  if start + size != input.length() {
    return Err(TruncatedInput)
  }
  let body = input[start:start + size].to_owned()
  let mut cursor = 0
  let entries : Array[(String, FieldValue)] = []
  while cursor < body.length() {
    let (key, next_key) = match read_short_string_field(body, cursor) {
      Ok(result) => result
      Err(error) => return Err(error)
    }
    let (value, next_value) = match read_field_value(body, next_key) {
      Ok(result) => result
      Err(error) => return Err(error)
    }
    entries.push((key, value))
    cursor = next_value
  }
  if cursor != body.length() {
    return Err(TruncatedInput)
  }
  Ok(FieldTable::new(entries))
}
