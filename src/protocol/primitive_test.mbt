///|
/// The protocol header is fixed by AMQP 0-9-1 and is the first observable
/// contract of the codec.
test "protocol header encodes AMQP 0-9-1" {
  let encoded = encode_protocol_header()
  assert_eq(encoded, b"AMQP\x00\x00\x09\x01")
  assert_true(decode_protocol_header(encoded) is Ok(_))
}

///|
/// A header for another protocol revision must be rejected explicitly.
test "protocol header rejects unsupported revision" {
  assert_true(decode_protocol_header(b"AMQP\x00\x00\x09\x00") is Err(_))
}

///|
test "unsigned integers use network byte order" {
  assert_eq(encode_u16(UInt16(0x1234)), b"\x12\x34")
  assert_eq(encode_u32(0x12345678U), b"\x12\x34\x56\x78")
  match decode_u16(b"\x12\x34") {
    Ok(value) => assert_eq(value, UInt16(0x1234))
    Err(_) => fail("u16 decode unexpectedly failed")
  }
  assert_true(decode_u32(b"\x12\x34\x56\x78") is Ok(0x12345678U))
}

///|
test "short strings carry a UTF-8 byte length" {
  assert_true(encode_short_string("hello") is Ok(b"\x05hello"))
  assert_true(decode_short_string(b"\x05hello") is Ok("hello"))
}

///|
test "primitive decoders reject truncated input" {
  assert_true(decode_u16(b"\x01") is Err(_))
  assert_true(decode_u32(b"\x01\x02\x03") is Err(_))
  assert_true(decode_short_string(b"\x04abc") is Err(_))
}
