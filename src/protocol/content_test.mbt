///|
test "empty basic content header has zero properties" {
  let header = ContentHeader::new(
    class_id=UInt16(60),
    weight=UInt16(0),
    body_size=UInt64(0),
    properties=BasicProperties::new(),
  )
  let expected = Bytes::from_array([
    b'\x00', b'\x3C', b'\x00', b'\x00', b'\x00', b'\x00', b'\x00', b'\x00', b'\x00',
    b'\x00', b'\x00', b'\x00', b'\x00', b'\x00',
  ])
  match encode_content_header(header) {
    Ok(bytes) => assert_eq(bytes, expected)
    Err(_) => fail("empty content header encoding unexpectedly failed")
  }
  match decode_content_header(expected) {
    Ok(decoded) => assert_eq(decoded, header)
    Err(_) => fail("empty content header decoding unexpectedly failed")
  }
}

///|
test "basic property flags encode in AMQP wire order" {
  let properties = BasicProperties::new()
    .with_content_type("application/json")
    .with_delivery_mode(2)
    .with_priority(5)
    .with_timestamp(UInt64(123))
  let header = ContentHeader::new(
    class_id=UInt16(60),
    weight=UInt16(0),
    body_size=UInt64(3),
    properties~,
  )
  let expected = b"\x00\x3C\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03\x98\x40\x10application/json\x02\x05\x00\x00\x00\x00\x00\x00\x00\x7B"
  match encode_content_header(header) {
    Ok(bytes) => assert_eq(bytes, expected)
    Err(_) => fail("property content header encoding unexpectedly failed")
  }
  match decode_content_header(expected) {
    Ok(decoded) => assert_eq(decoded, header)
    Err(_) => fail("property content header decoding unexpectedly failed")
  }
}

///|
test "content assembler joins body frames and enforces declared size" {
  let header = ContentHeader::new(
    class_id=UInt16(60),
    weight=UInt16(0),
    body_size=UInt64(5),
    properties=BasicProperties::new(),
  )
  let assembler = ContentAssembler::new(header, 16)
  assert_true(assembler.push_body(b"he") is Ok(_))
  assert_true(assembler.push_body(b"llo") is Ok(_))
  match assembler.finish() {
    Ok(message) => assert_eq(message.body(), b"hello")
    Err(_) => fail("complete content unexpectedly failed")
  }

  let overrun = ContentAssembler::new(header, 16)
  assert_true(overrun.push_body(b"hello!") is Err(ContentBodyTooLarge))
  let underrun = ContentAssembler::new(header, 16)
  assert_true(underrun.push_body(b"hel") is Ok(_))
  assert_true(underrun.finish() is Err(ContentSizeMismatch))
}

///|
test "content header rejects unsupported class and malformed properties" {
  let unsupported = ContentHeader::new(
    class_id=UInt16(10),
    weight=UInt16(0),
    body_size=UInt64(0),
    properties=BasicProperties::new(),
  )
  assert_true(encode_content_header(unsupported) is Err(InvalidContentClass))
  assert_true(
    decode_content_header(
      Bytes::from_array([
        b'\x00', b'\x3C', b'\x00', b'\x00', b'\x00', b'\x00', b'\x00', b'\x00', b'\x00',
        b'\x00', b'\x00', b'\x00', b'\x00', b'\x01',
      ]),
    )
    is Err(InvalidPropertyFlags),
  )
  assert_true(decode_content_header(b"\x00\x3C") is Err(TruncatedInput))
}
