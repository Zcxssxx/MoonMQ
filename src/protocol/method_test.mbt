///|
test "basic.publish encodes its class, method, strings, and flags" {
  let wire_method = Method::BasicPublish(
    ticket=UInt16(0),
    exchange="events",
    routing_key="hello",
    mandatory=true,
    immediate=false,
  )
  let expected = b"\x00\x3C\x00\x28\x00\x00\x06events\x05hello\x01"
  match encode_method(wire_method) {
    Ok(bytes) => assert_eq(bytes, expected)
    Err(_) => fail("basic.publish encoding unexpectedly failed")
  }
}

///|
test "basic.ack encodes a delivery tag and multiple bit" {
  let wire_method = Method::BasicAck(delivery_tag=UInt64(42), multiple=true)
  let expected = b"\x00\x3C\x00\x50\x00\x00\x00\x00\x00\x00\x00\x2A\x01"
  match encode_method(wire_method) {
    Ok(bytes) => assert_eq(bytes, expected)
    Err(_) => fail("basic.ack encoding unexpectedly failed")
  }
}

///|
test "method decoder round trips basic.publish" {
  let payload = b"\x00\x3C\x00\x28\x00\x00\x06events\x05hello\x01"
  match decode_method(payload) {
    Ok(BasicPublish(ticket~, exchange~, routing_key~, mandatory~, immediate~)) => {
      assert_eq(ticket, UInt16(0))
      assert_eq(exchange, "events")
      assert_eq(routing_key, "hello")
      assert_true(mandatory)
      assert_true(!immediate)
    }
    Ok(_) => fail("decoded a different method")
    Err(_) => fail("basic.publish decoding unexpectedly failed")
  }
}

///|
test "method decoder rejects truncated arguments" {
  assert_true(decode_method(b"\x00\x3C\x00\x28\x00") is Err(_))
}
