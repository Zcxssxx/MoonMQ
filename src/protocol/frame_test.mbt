///|
test "method frame encodes and decodes the AMQP envelope" {
  let frame = Frame::new(FrameKind::FrameMethod, UInt16(1), b"\x01\x02\x03\x04")
  let expected = b"\x01\x00\x01\x00\x00\x00\x04\x01\x02\x03\x04\xCE"
  match encode_frame(frame) {
    Ok(bytes) => assert_eq(bytes, expected)
    Err(_) => fail("frame encoding unexpectedly failed")
  }
  match decode_frame(expected) {
    Ok(decoded) => {
      assert_eq(decoded.kind(), FrameKind::FrameMethod)
      assert_eq(decoded.channel(), UInt16(1))
      assert_eq(decoded.payload(), b"\x01\x02\x03\x04")
    }
    Err(_) => fail("frame decoding unexpectedly failed")
  }
}

///|
test "heartbeat frame has an empty payload" {
  let frame = Frame::new(FrameKind::FrameHeartbeat, UInt16(0), b"")
  match encode_frame(frame) {
    Ok(bytes) => assert_eq(bytes, b"\x08\x00\x00\x00\x00\x00\x00\xCE")
    Err(_) => fail("heartbeat encoding unexpectedly failed")
  }
}

///|
test "frame decoder rejects bad terminator and trailing bytes" {
  assert_true(decode_frame(b"\x08\x00\x00\x00\x00\x00\x00\x00") is Err(_))
  assert_true(decode_frame(b"\x08\x00\x00\x00\x00\x00\x00\xCE\x00") is Err(_))
  assert_true(decode_frame(b"\x04\x00\x00\x00\x00\x00\x00\xCE") is Err(_))
  assert_true(decode_frame(b"\x01\x00\x00\x00\x00\x00\x02\x00\xCE") is Err(_))
  assert_true(decode_frame(b"\x01\x00\x00\x00\x00\x00\x02\x01\xCE") is Err(_))
}

///|
test "frame payload respects frame-max overhead" {
  let too_large = Bytes::make(131065, b'\x00')
  let frame = Frame::new(FrameKind::FrameBody, UInt16(1), too_large)
  assert_true(encode_frame(frame) is Err(_))
}

///|
test "frame kinds enforce channel and payload invariants" {
  assert_true(
    encode_frame(Frame::new(FrameKind::FrameHeartbeat, UInt16(1), b""))
    is Err(_),
  )
  assert_true(
    encode_frame(Frame::new(FrameKind::FrameHeartbeat, UInt16(0), b"x"))
    is Err(_),
  )
  assert_true(
    encode_frame(Frame::new(FrameKind::FrameBody, UInt16(0), b"x")) is Err(_),
  )
  assert_true(decode_frame(b"\x08\x00\x01\x00\x00\x00\x00\xCE") is Err(_))
}

///|
test "frame validator protects callers that already have a Frame" {
  assert_true(
    validate_frame(Frame::new(FrameKind::FrameHeartbeat, UInt16(1), b""))
    is Err(_),
  )
  assert_true(
    validate_frame(Frame::new(FrameKind::FrameHeartbeat, UInt16(0), b"x"))
    is Err(_),
  )
}

///|
test "method frames require class and method identifiers" {
  let malformed = Frame::new(FrameKind::FrameMethod, UInt16(1), b"\x01")
  assert_true(encode_frame(malformed) is Err(_))
  assert_true(decode_frame(b"\x01\x00\x01\x00\x00\x00\x01\x01\xCE") is Err(_))
}

///|
test "caller can use a negotiated lower frame-max" {
  let frame = Frame::new(FrameKind::FrameBody, UInt16(1), b"1234")
  match encode_frame_with_max(frame, 11) {
    Ok(_) => fail("frame should exceed the negotiated limit")
    Err(_) => ()
  }
  let encoded = match encode_frame_with_max(frame, 12) {
    Ok(bytes) => bytes
    Err(_) => fail("frame should fit the negotiated limit")
  }
  assert_true(decode_frame_with_max(encoded, 12) is Ok(_))
}

///|
test "frame-max zero means no specific negotiated limit" {
  let frame = Frame::new(FrameKind::FrameBody, UInt16(1), b"unlimited")
  match encode_frame_with_max(frame, 0) {
    Ok(encoded) => assert_true(decode_frame_with_max(encoded, 0) is Ok(_))
    Err(_) => fail("frame-max zero should permit the default bounded limit")
  }
}
