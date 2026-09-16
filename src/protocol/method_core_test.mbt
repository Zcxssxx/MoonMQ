///|
test "connection.start uses AMQP long strings and an empty property table" {
  let value = Method::ConnectionStart(
    version_major=0,
    version_minor=9,
    server_properties=FieldTable::new([]),
    mechanisms=b"PLAIN",
    locales=b"en_US",
  )
  let expected = b"\x00\x0A\x00\x0A\x00\x09\x00\x00\x00\x00\x00\x00\x00\x05PLAIN\x00\x00\x00\x05en_US"
  match encode_method(value) {
    Ok(bytes) => assert_eq(bytes, expected)
    Err(_) => fail("method encoding unexpectedly failed")
  }
  match decode_method(expected) {
    Ok(decoded) => assert_eq(decoded, value)
    Err(_) => fail("method decoding unexpectedly failed")
  }
}

///|
test "connection.start-ok encodes properties, mechanism, response, and locale" {
  let value = Method::ConnectionStartOk(
    client_properties=FieldTable::new([]),
    mechanism="PLAIN",
    response=b"\x00guest\x00guest",
    locale="en_US",
  )
  let expected = b"\x00\x0A\x00\x0B\x00\x00\x00\x00\x05PLAIN\x00\x00\x00\x0C\x00guest\x00guest\x05en_US"
  match encode_method(value) {
    Ok(bytes) => assert_eq(bytes, expected)
    Err(_) => fail("method encoding unexpectedly failed")
  }
  match decode_method(expected) {
    Ok(decoded) => assert_eq(decoded, value)
    Err(_) => fail("method decoding unexpectedly failed")
  }
}

///|
test "connection.tune preserves unsigned frame-max and heartbeat" {
  let value = Method::ConnectionTune(
    channel_max=UInt16(16),
    frame_max=131072U,
    heartbeat=UInt16(10),
  )
  let expected = b"\x00\x0A\x00\x1E\x00\x10\x00\x02\x00\x00\x00\x0A"
  match encode_method(value) {
    Ok(bytes) => assert_eq(bytes, expected)
    Err(_) => fail("method encoding unexpectedly failed")
  }
  match decode_method(expected) {
    Ok(decoded) => assert_eq(decoded, value)
    Err(_) => fail("method decoding unexpectedly failed")
  }
}

///|
test "exchange.declare packs flags and arguments" {
  let value = Method::ExchangeDeclare(
    ticket=UInt16(0),
    exchange="events",
    kind="direct",
    passive=false,
    durable=true,
    auto_delete=false,
    internal=false,
    nowait=false,
    arguments=FieldTable::new([]),
  )
  let expected = b"\x00\x28\x00\x0A\x00\x00\x06events\x06direct\x02\x00\x00\x00\x00"
  match encode_method(value) {
    Ok(bytes) => assert_eq(bytes, expected)
    Err(_) => fail("method encoding unexpectedly failed")
  }
  match decode_method(expected) {
    Ok(decoded) => assert_eq(decoded, value)
    Err(_) => fail("method decoding unexpectedly failed")
  }
}

///|
test "basic.consume and basic.deliver round trip" {
  let consume = Method::BasicConsume(
    ticket=UInt16(0),
    queue="jobs",
    consumer_tag="worker",
    no_local=false,
    no_ack=false,
    exclusive=false,
    nowait=false,
    arguments=FieldTable::new([]),
  )
  let consume_wire = b"\x00\x3C\x00\x14\x00\x00\x04jobs\x06worker\x00\x00\x00\x00\x00"
  match encode_method(consume) {
    Ok(bytes) => assert_eq(bytes, consume_wire)
    Err(_) => fail("basic.consume encoding unexpectedly failed")
  }
  match decode_method(consume_wire) {
    Ok(decoded) => assert_eq(decoded, consume)
    Err(_) => fail("basic.consume decoding unexpectedly failed")
  }
  let deliver = Method::BasicDeliver(
    consumer_tag="worker",
    delivery_tag=UInt64(42),
    redelivered=true,
    exchange="events",
    routing_key="jobs",
  )
  let deliver_wire = b"\x00\x3C\x00\x3C\x06worker\x00\x00\x00\x00\x00\x00\x00\x2A\x01\x06events\x04jobs"
  match encode_method(deliver) {
    Ok(bytes) => assert_eq(bytes, deliver_wire)
    Err(_) => fail("basic.deliver encoding unexpectedly failed")
  }
  match decode_method(deliver_wire) {
    Ok(decoded) => assert_eq(decoded, deliver)
    Err(_) => fail("basic.deliver decoding unexpectedly failed")
  }
}

///|
test "long strings preserve arbitrary bytes and reject truncation" {
  let encoded = encode_long_string(b"\x00\xFFpayload")
  assert_true(encoded is Ok(b"\x00\x00\x00\x09\x00\xFFpayload"))
  assert_true(
    decode_long_string(b"\x00\x00\x00\x09\x00\xFFpayload")
    is Ok(b"\x00\xFFpayload"),
  )
  assert_true(decode_long_string(b"\x00\x00\x00\x04abc") is Err(_))
}
