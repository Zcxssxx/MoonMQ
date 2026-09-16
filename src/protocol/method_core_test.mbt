///|
test "connection.start uses AMQP long strings and an empty property table" {
  let value = ConnectionStart(
    version_major=0,
    version_minor=9,
    server_properties=FieldTable::new([]),
    mechanisms=b"PLAIN",
    locales=b"en_US",
  )
  let expected = b"\x00\x0A\x00\x0A\x00\x09\x00\x01\x00\x00\x00\x00\x00\x00\x00\x05PLAIN\x00\x00\x00\x05en_US"
  assert_true(encode_method(value) is Ok(expected))
  assert_true(decode_method(expected) is Ok(value))
}

///|
test "connection.start-ok encodes properties, mechanism, response, and locale" {
  let value = ConnectionStartOk(
    client_properties=FieldTable::new([]),
    mechanism="PLAIN",
    response=b"\x00guest\x00guest",
    locale="en_US",
  )
  let expected = b"\x00\x0A\x00\x0B\x00\x00\x00\x00\x05PLAIN\x00\x00\x00\x0C\x00guest\x00guest\x05en_US"
  assert_true(encode_method(value) is Ok(expected))
  assert_true(decode_method(expected) is Ok(value))
}

///|
test "connection.tune preserves unsigned frame-max and heartbeat" {
  let value = ConnectionTune(
    channel_max=UInt16(16),
    frame_max=131072U,
    heartbeat=UInt16(10),
  )
  let expected = b"\x00\x0A\x00\x1E\x00\x10\x00\x02\x00\x00\x00\x0A"
  assert_true(encode_method(value) is Ok(expected))
  assert_true(decode_method(expected) is Ok(value))
}

///|
test "exchange.declare packs flags and arguments" {
  let value = ExchangeDeclare(
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
  assert_true(encode_method(value) is Ok(expected))
  assert_true(decode_method(expected) is Ok(value))
}

///|
test "basic.consume and basic.deliver round trip" {
  let consume = BasicConsume(
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
  assert_true(encode_method(consume) is Ok(consume_wire))
  assert_true(decode_method(consume_wire) is Ok(consume))
  let deliver = BasicDeliver(
    consumer_tag="worker",
    delivery_tag=UInt64(42),
    redelivered=true,
    exchange="events",
    routing_key="jobs",
  )
  let deliver_wire = b"\x00\x3C\x00\x3C\x06worker\x00\x00\x00\x00\x00\x00\x00\x2A\x01\x00\x06events\x04jobs"
  assert_true(encode_method(deliver) is Ok(deliver_wire))
  assert_true(decode_method(deliver_wire) is Ok(deliver))
}

///|
test "long strings preserve arbitrary bytes and reject truncation" {
  let encoded = encode_long_string(b"\x00\xFFpayload")
  assert_true(encoded is Ok(b"\x00\x00\x00\x09\x00\xFFpayload"))
  assert_true(decode_long_string(b"\x00\x00\x00\x09\x00\xFFpayload") is Ok(b"\x00\xFFpayload"))
  assert_true(decode_long_string(b"\x00\x00\x00\x04abc") is Err(_))
}
