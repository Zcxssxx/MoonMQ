///|
test "method frame encodes and decodes the AMQP envelope" {
  let frame = Frame::new(FrameKind::FrameMethod, UInt16(1), b"\x01\x02")
  let expected = b"\x01\x00\x01\x00\x00\x00\x02\x01\x02\xCE"
  match encode_frame(frame) {
    Ok(bytes) => assert_eq(bytes, expected)
    Err(_) => fail("frame encoding unexpectedly failed")
  }
  match decode_frame(expected) {
    Ok(decoded) => {
      assert_eq(decoded.kind(), FrameKind::FrameMethod)
      assert_eq(decoded.channel(), UInt16(1))
      assert_eq(decoded.payload(), b"\x01\x02")
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
}
