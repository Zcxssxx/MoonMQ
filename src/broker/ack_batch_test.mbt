///|
test "batch acknowledgement restores prefetch across affected queues" {
  let broker = Broker::new()
  ignore(assert_ok(broker.declare_queue("first")))
  ignore(assert_ok(broker.declare_queue("second")))
  let first = assert_ok(
    broker.consume_with_owner("first", "worker-1", "owner", 1),
  )
  let second = assert_ok(
    broker.consume_with_owner("second", "worker-2", "owner", 1),
  )
  ignore(assert_ok(broker.publish("", "first", b"first-a")))
  ignore(assert_ok(broker.publish("", "first", b"first-b")))
  ignore(assert_ok(broker.publish("", "second", b"second-a")))
  ignore(assert_ok(broker.publish("", "second", b"second-b")))
  let first_delivery = match broker.poll(first) {
    Ok(Some(value)) => value
    _ => fail("expected first queue delivery")
  }
  let second_delivery = match broker.poll(second) {
    Ok(Some(value)) => value
    _ => fail("expected second queue delivery")
  }
  assert_ok(
    broker.ack_many_with_owner("owner", [
      first_delivery.tag(),
      second_delivery.tag(),
    ]),
  )
  match broker.poll(first) {
    Ok(Some(value)) => assert_eq(value.body(), b"first-b")
    _ => fail("first queue should resume after batch acknowledgement")
  }
  match broker.poll(second) {
    Ok(Some(value)) => assert_eq(value.body(), b"second-b")
    _ => fail("second queue should resume after batch acknowledgement")
  }
}

///|
test "batch acknowledgement restores both slots for prefetch two" {
  let broker = Broker::new()
  let queue = assert_ok(broker.declare_queue("prefetch-two"))
  let consumer = assert_ok(
    broker.consume_with_owner("prefetch-two", "worker", "owner", 2),
  )
  for body in [b"one", b"two", b"three", b"four"] {
    ignore(assert_ok(broker.publish("", queue, body)))
  }
  let first = match broker.poll(consumer) {
    Ok(Some(value)) => value
    _ => fail("expected first delivery")
  }
  let second = match broker.poll(consumer) {
    Ok(Some(value)) => value
    _ => fail("expected second delivery")
  }
  assert_true(broker.poll(consumer) is Ok(None))
  assert_ok(broker.ack_many_with_owner("owner", [first.tag(), second.tag()]))
  match broker.poll(consumer) {
    Ok(Some(value)) => assert_eq(value.body(), b"three")
    _ => fail("first released prefetch slot should dispatch the third message")
  }
  match broker.poll(consumer) {
    Ok(Some(value)) => assert_eq(value.body(), b"four")
    _ =>
      fail("second released prefetch slot should dispatch the fourth message")
  }
}

///|
test "batch acknowledgement validates every owner and tag before mutation" {
  let broker = Broker::new()
  let queue = assert_ok(broker.declare_queue("atomic"))
  ignore(assert_ok(broker.publish("", queue, b"one")))
  let delivery = match broker.get_with_owner(queue, "owner-1") {
    Ok(Some(value)) => value
    _ => fail("expected one unsettled delivery")
  }
  assert_true(
    broker.ack_many_with_owner("owner-1", [delivery.tag(), UInt64(999)])
    is Err(UnknownDelivery),
  )
  assert_true(
    broker.ack_many_with_owner("owner-2", [delivery.tag()])
    is Err(UnknownDelivery),
  )
  assert_ok(broker.ack_with_owner("owner-1", delivery.tag()))
  assert_ok(broker.ack_many_with_owner("owner-1", []))
}
