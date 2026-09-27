///|
/// The optional properties carried by an AMQP Basic content header.
pub struct BasicProperties {
  content_type : String?
  content_encoding : String?
  headers : FieldTable?
  delivery_mode : Byte?
  priority : Byte?
  correlation_id : String?
  reply_to : String?
  expiration : String?
  message_id : String?
  timestamp : UInt64?
  message_type : String?
  user_id : String?
  app_id : String?
  cluster_id : String?
} derive(Eq, @debug.Debug)

///|
pub fn BasicProperties::new() -> BasicProperties {
  {
    content_type: None,
    content_encoding: None,
    headers: None,
    delivery_mode: None,
    priority: None,
    correlation_id: None,
    reply_to: None,
    expiration: None,
    message_id: None,
    timestamp: None,
    message_type: None,
    user_id: None,
    app_id: None,
    cluster_id: None,
  }
}

///|
pub fn BasicProperties::with_content_type(
  self : BasicProperties,
  value : String,
) -> BasicProperties {
  { ..self, content_type: Some(value), }
}

///|
pub fn BasicProperties::with_content_encoding(
  self : BasicProperties,
  value : String,
) -> BasicProperties {
  { ..self, content_encoding: Some(value), }
}

///|
pub fn BasicProperties::with_headers(
  self : BasicProperties,
  value : FieldTable,
) -> BasicProperties {
  { ..self, headers: Some(value), }
}

///|
pub fn BasicProperties::with_delivery_mode(
  self : BasicProperties,
  value : Byte,
) -> BasicProperties {
  { ..self, delivery_mode: Some(value), }
}

///|
pub fn BasicProperties::with_priority(
  self : BasicProperties,
  value : Byte,
) -> BasicProperties {
  { ..self, priority: Some(value), }
}

///|
pub fn BasicProperties::with_correlation_id(
  self : BasicProperties,
  value : String,
) -> BasicProperties {
  { ..self, correlation_id: Some(value), }
}

///|
pub fn BasicProperties::with_reply_to(
  self : BasicProperties,
  value : String,
) -> BasicProperties {
  { ..self, reply_to: Some(value), }
}

///|
pub fn BasicProperties::with_expiration(
  self : BasicProperties,
  value : String,
) -> BasicProperties {
  { ..self, expiration: Some(value), }
}

///|
pub fn BasicProperties::with_message_id(
  self : BasicProperties,
  value : String,
) -> BasicProperties {
  { ..self, message_id: Some(value), }
}

///|
pub fn BasicProperties::with_timestamp(
  self : BasicProperties,
  value : UInt64,
) -> BasicProperties {
  { ..self, timestamp: Some(value), }
}

///|
pub fn BasicProperties::with_message_type(
  self : BasicProperties,
  value : String,
) -> BasicProperties {
  { ..self, message_type: Some(value), }
}

///|
pub fn BasicProperties::with_user_id(
  self : BasicProperties,
  value : String,
) -> BasicProperties {
  { ..self, user_id: Some(value), }
}

///|
pub fn BasicProperties::with_app_id(
  self : BasicProperties,
  value : String,
) -> BasicProperties {
  { ..self, app_id: Some(value), }
}

///|
pub fn BasicProperties::with_cluster_id(
  self : BasicProperties,
  value : String,
) -> BasicProperties {
  { ..self, cluster_id: Some(value), }
}

///|
pub fn BasicProperties::content_type(self : BasicProperties) -> String? {
  self.content_type
}

///|
pub fn BasicProperties::content_encoding(self : BasicProperties) -> String? {
  self.content_encoding
}

///|
pub fn BasicProperties::headers(self : BasicProperties) -> FieldTable? {
  self.headers
}

///|
pub fn BasicProperties::delivery_mode(self : BasicProperties) -> Byte? {
  self.delivery_mode
}

///|
pub fn BasicProperties::priority(self : BasicProperties) -> Byte? {
  self.priority
}

///|
pub fn BasicProperties::correlation_id(self : BasicProperties) -> String? {
  self.correlation_id
}

///|
pub fn BasicProperties::reply_to(self : BasicProperties) -> String? {
  self.reply_to
}

///|
pub fn BasicProperties::expiration(self : BasicProperties) -> String? {
  self.expiration
}

///|
pub fn BasicProperties::message_id(self : BasicProperties) -> String? {
  self.message_id
}

///|
pub fn BasicProperties::timestamp(self : BasicProperties) -> UInt64? {
  self.timestamp
}

///|
pub fn BasicProperties::message_type(self : BasicProperties) -> String? {
  self.message_type
}

///|
pub fn BasicProperties::user_id(self : BasicProperties) -> String? {
  self.user_id
}

///|
pub fn BasicProperties::app_id(self : BasicProperties) -> String? {
  self.app_id
}

///|
pub fn BasicProperties::cluster_id(self : BasicProperties) -> String? {
  self.cluster_id
}

///|
/// The fixed portion and Basic properties of one AMQP content header.
pub struct ContentHeader {
  class_id : UInt16
  weight : UInt16
  body_size : UInt64
  properties : BasicProperties
} derive(Eq, @debug.Debug)

///|
pub fn ContentHeader::new(
  class_id~ : UInt16,
  weight~ : UInt16,
  body_size~ : UInt64,
  properties~ : BasicProperties,
) -> ContentHeader {
  { class_id, weight, body_size, properties, }
}

///|
pub fn ContentHeader::class_id(self : ContentHeader) -> UInt16 {
  self.class_id
}

///|
pub fn ContentHeader::weight(self : ContentHeader) -> UInt16 {
  self.weight
}

///|
pub fn ContentHeader::body_size(self : ContentHeader) -> UInt64 {
  self.body_size
}

///|
pub fn ContentHeader::properties(self : ContentHeader) -> BasicProperties {
  self.properties
}

///|
pub struct ContentMessage {
  header : ContentHeader
  body : Bytes
} derive(Eq, @debug.Debug)

///|
pub fn ContentMessage::header(self : ContentMessage) -> ContentHeader {
  self.header
}

///|
pub fn ContentMessage::body(self : ContentMessage) -> Bytes {
  self.body
}

///|
pub struct ContentAssembler {
  header : ContentHeader
  max_body_size : Int
  body : Array[Byte]
}

///|
pub fn ContentAssembler::new(
  header : ContentHeader,
  max_body_size : Int,
) -> ContentAssembler {
  let limit = if max_body_size <= 0 {
    DEFAULT_MAX_FRAME_SIZE
  } else {
    max_body_size
  }
  { header, max_body_size: limit, body: [], }
}

///|
fn append_content_bytes(output : Array[Byte], bytes : Bytes) -> Unit {
  for byte in bytes {
    output.push(byte)
  }
}

///|
fn append_optional_short_string(
  output : Array[Byte],
  value : String?,
) -> Result[Unit, AmqpError] {
  match value {
    None => Ok(())
    Some(value) =>
      match encode_short_string(value) {
        Ok(bytes) => {
          append_content_bytes(output, bytes)
          Ok(())
        }
        Err(error) => Err(error)
      }
  }
}

///|
fn append_optional_table(
  output : Array[Byte],
  value : FieldTable?,
) -> Result[Unit, AmqpError] {
  match value {
    None => Ok(())
    Some(value) =>
      match encode_field_table(value) {
        Ok(bytes) => {
          append_content_bytes(output, bytes)
          Ok(())
        }
        Err(error) => Err(error)
      }
  }
}

///|
fn append_optional_byte(output : Array[Byte], value : Byte?) -> Unit {
  match value {
    None => ()
    Some(value) => output.push(value)
  }
}

///|
fn append_optional_u64(output : Array[Byte], value : UInt64?) -> Unit {
  match value {
    None => ()
    Some(value) => append_content_bytes(output, encode_u64(value))
  }
}

///|
fn content_property_flags(properties : BasicProperties) -> UInt16 {
  let mut bits = 0
  if properties.content_type is Some(_) {
    bits = bits | 0x8000
  }
  if properties.content_encoding is Some(_) {
    bits = bits | 0x4000
  }
  if properties.headers is Some(_) {
    bits = bits | 0x2000
  }
  if properties.delivery_mode is Some(_) {
    bits = bits | 0x1000
  }
  if properties.priority is Some(_) {
    bits = bits | 0x0800
  }
  if properties.correlation_id is Some(_) {
    bits = bits | 0x0400
  }
  if properties.reply_to is Some(_) {
    bits = bits | 0x0200
  }
  if properties.expiration is Some(_) {
    bits = bits | 0x0100
  }
  if properties.message_id is Some(_) {
    bits = bits | 0x0080
  }
  if properties.timestamp is Some(_) {
    bits = bits | 0x0040
  }
  if properties.message_type is Some(_) {
    bits = bits | 0x0020
  }
  if properties.user_id is Some(_) {
    bits = bits | 0x0010
  }
  if properties.app_id is Some(_) {
    bits = bits | 0x0008
  }
  if properties.cluster_id is Some(_) {
    bits = bits | 0x0004
  }
  bits.to_uint16()
}

///|
/// Encode a Basic content-header payload, excluding its frame envelope.
pub fn encode_content_header(
  header : ContentHeader,
) -> Result[Bytes, AmqpError] {
  if header.class_id != UInt16(60) {
    return Err(InvalidContentClass)
  }
  if header.weight != UInt16(0) {
    return Err(InvalidContentWeight)
  }
  let properties = header.properties
  let output : Array[Byte] = []
  append_content_bytes(output, encode_u16(header.class_id))
  append_content_bytes(output, encode_u16(header.weight))
  append_content_bytes(output, encode_u64(header.body_size))
  append_content_bytes(output, encode_u16(content_property_flags(properties)))
  match append_optional_short_string(output, properties.content_type) {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  match append_optional_short_string(output, properties.content_encoding) {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  match append_optional_table(output, properties.headers) {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  append_optional_byte(output, properties.delivery_mode)
  append_optional_byte(output, properties.priority)
  match append_optional_short_string(output, properties.correlation_id) {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  match append_optional_short_string(output, properties.reply_to) {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  match append_optional_short_string(output, properties.expiration) {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  match append_optional_short_string(output, properties.message_id) {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  append_optional_u64(output, properties.timestamp)
  match append_optional_short_string(output, properties.message_type) {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  match append_optional_short_string(output, properties.user_id) {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  match append_optional_short_string(output, properties.app_id) {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  match append_optional_short_string(output, properties.cluster_id) {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  Ok(Bytes::from_array(output[:]))
}

///|
/// Decode a complete Basic content-header payload with exact consumption.
pub fn decode_content_header(input : Bytes) -> Result[ContentHeader, AmqpError] {
  if input.length() < 14 {
    return Err(TruncatedInput)
  }
  let (class_id, _) = match read_u16_at(input, 0) {
    Ok(value) => value
    Err(error) => return Err(error)
  }
  if class_id != UInt16(60) {
    return Err(InvalidContentClass)
  }
  let (weight, _) = match read_u16_at(input, 2) {
    Ok(value) => value
    Err(error) => return Err(error)
  }
  if weight != UInt16(0) {
    return Err(InvalidContentWeight)
  }
  let (body_size, _) = match read_u64_at(input, 4) {
    Ok(value) => value
    Err(error) => return Err(error)
  }
  let (property_flags, offset_value) = match read_u16_at(input, 12) {
    Ok(value) => value
    Err(error) => return Err(error)
  }
  let mut offset = offset_value
  let flags = property_flags.to_int()
  if (flags & 0x0003) != 0 {
    return Err(InvalidPropertyFlags)
  }
  let mut properties = BasicProperties::new()
  if (flags & 0x8000) != 0 {
    let (value, next) = match read_short_string_at(input, offset) {
      Ok(value) => value
      Err(error) => return Err(error)
    }
    properties = properties.with_content_type(value)
    offset = next
  }
  if (flags & 0x4000) != 0 {
    let (value, next) = match read_short_string_at(input, offset) {
      Ok(value) => value
      Err(error) => return Err(error)
    }
    properties = properties.with_content_encoding(value)
    offset = next
  }
  if (flags & 0x2000) != 0 {
    let (value, next) = match read_table_at(input, offset) {
      Ok(value) => value
      Err(error) => return Err(error)
    }
    properties = properties.with_headers(value)
    offset = next
  }
  if (flags & 0x1000) != 0 {
    let (value, next) = match read_byte_at(input, offset) {
      Ok(value) => value
      Err(error) => return Err(error)
    }
    properties = properties.with_delivery_mode(value)
    offset = next
  }
  if (flags & 0x0800) != 0 {
    let (value, next) = match read_byte_at(input, offset) {
      Ok(value) => value
      Err(error) => return Err(error)
    }
    properties = properties.with_priority(value)
    offset = next
  }
  if (flags & 0x0400) != 0 {
    let (value, next) = match read_short_string_at(input, offset) {
      Ok(value) => value
      Err(error) => return Err(error)
    }
    properties = properties.with_correlation_id(value)
    offset = next
  }
  if (flags & 0x0200) != 0 {
    let (value, next) = match read_short_string_at(input, offset) {
      Ok(value) => value
      Err(error) => return Err(error)
    }
    properties = properties.with_reply_to(value)
    offset = next
  }
  if (flags & 0x0100) != 0 {
    let (value, next) = match read_short_string_at(input, offset) {
      Ok(value) => value
      Err(error) => return Err(error)
    }
    properties = properties.with_expiration(value)
    offset = next
  }
  if (flags & 0x0080) != 0 {
    let (value, next) = match read_short_string_at(input, offset) {
      Ok(value) => value
      Err(error) => return Err(error)
    }
    properties = properties.with_message_id(value)
    offset = next
  }
  if (flags & 0x0040) != 0 {
    let (value, next) = match read_u64_at(input, offset) {
      Ok(value) => value
      Err(error) => return Err(error)
    }
    properties = properties.with_timestamp(value)
    offset = next
  }
  if (flags & 0x0020) != 0 {
    let (value, next) = match read_short_string_at(input, offset) {
      Ok(value) => value
      Err(error) => return Err(error)
    }
    properties = properties.with_message_type(value)
    offset = next
  }
  if (flags & 0x0010) != 0 {
    let (value, next) = match read_short_string_at(input, offset) {
      Ok(value) => value
      Err(error) => return Err(error)
    }
    properties = properties.with_user_id(value)
    offset = next
  }
  if (flags & 0x0008) != 0 {
    let (value, next) = match read_short_string_at(input, offset) {
      Ok(value) => value
      Err(error) => return Err(error)
    }
    properties = properties.with_app_id(value)
    offset = next
  }
  if (flags & 0x0004) != 0 {
    let (value, next) = match read_short_string_at(input, offset) {
      Ok(value) => value
      Err(error) => return Err(error)
    }
    properties = properties.with_cluster_id(value)
    offset = next
  }
  match require_end(input, offset) {
    Ok(_) => Ok(ContentHeader::new(class_id~, weight~, body_size~, properties~))
    Err(error) => Err(error)
  }
}

///|
pub fn ContentAssembler::push_body(
  self : ContentAssembler,
  payload : Bytes,
) -> Result[Unit, AmqpError] {
  let declared = self.header.body_size.to_int()
  if declared < 0 || self.body.length() > self.max_body_size - payload.length() {
    return Err(ContentBodyTooLarge)
  }
  if self.body.length() + payload.length() > declared {
    return Err(ContentBodyTooLarge)
  }
  for byte in payload {
    self.body.push(byte)
  }
  Ok(())
}

///|
pub fn ContentAssembler::finish(
  self : ContentAssembler,
) -> Result[ContentMessage, AmqpError] {
  let declared = self.header.body_size.to_int()
  if declared < 0 || declared > self.max_body_size {
    return Err(ContentBodyTooLarge)
  }
  if self.body.length() != declared {
    return Err(ContentSizeMismatch)
  }
  Ok({ header: self.header, body: Bytes::from_array(self.body[:]), })
}
