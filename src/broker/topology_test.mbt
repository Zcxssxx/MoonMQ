///|
test "deleting an exchange removes its bindings and preserves rejected state" {
  let broker = Broker::new()
  assert_true(
    broker.delete_exchange("", false) is Err(CannotDeleteDefaultExchange),
  )
  assert_true(broker.delete_exchange("missing", false) is Err(UnknownExchange))
  assert_ok(broker.declare_exchange("events", ExchangeKind::ExchangeDirect))
  let first = assert_ok(broker.declare_queue("first"))
  let second = assert_ok(broker.declare_queue("second"))
  assert_ok(broker.bind_queue("events", first, "created"))
  assert_ok(broker.bind_queue("events", second, "deleted"))
  assert_true(broker.delete_exchange("events", true) is Err(ExchangeInUse))
  assert_true(
    broker.bind_queue("events", first, "created") is Err(DuplicateBinding),
  )
  assert_ok(broker.delete_exchange("events", false))
  assert_ok(broker.declare_exchange("events", ExchangeKind::ExchangeFanout))
  assert_eq(assert_ok(broker.publish("events", "created", b"x")), 0)
}

///|
test "queue unbind removes only the exact binding" {
  let broker = Broker::new()
  assert_ok(broker.declare_exchange("events", ExchangeKind::ExchangeDirect))
  let queue = assert_ok(broker.declare_queue("jobs"))
  assert_ok(broker.bind_queue("events", queue, "created"))
  assert_ok(broker.bind_queue("events", queue, "deleted"))
  assert_ok(broker.unbind_queue("events", queue, "created"))
  assert_eq(assert_ok(broker.publish("events", "created", b"x")), 0)
  assert_eq(assert_ok(broker.publish("events", "deleted", b"y")), 1)
  assert_true(
    broker.unbind_queue("events", queue, "missing") is Err(UnknownBinding),
  )
  assert_true(
    broker.unbind_queue("missing", queue, "created") is Err(UnknownExchange),
  )
  assert_true(
    broker.unbind_queue("events", "missing", "created") is Err(UnknownQueue),
  )
}

///|
test "deleting an idle queue returns ready count and removes its bindings" {
  let broker = Broker::new()
  assert_ok(broker.declare_exchange("events", ExchangeKind::ExchangeDirect))
  let queue = assert_ok(broker.declare_queue("jobs"))
  assert_ok(broker.bind_queue("events", queue, "created"))
  assert_eq(assert_ok(broker.publish("events", "created", b"one")), 1)
  assert_eq(assert_ok(broker.publish("events", "created", b"two")), 1)
  assert_true(broker.delete_queue(queue, false, false) is Ok(2))
  assert_true(broker.queue_stats(queue) is Err(UnknownQueue))
  ignore(assert_ok(broker.declare_queue(queue)))
  assert_eq(assert_ok(broker.publish("events", "created", b"three")), 0)
}

///|
test "queue delete conditions reject without removing messages or consumers" {
  let broker = Broker::new()
  let active = assert_ok(broker.declare_queue("active"))
  ignore(assert_ok(broker.consume(active, "worker", 0)))
  assert_true(broker.delete_queue(active, true, false) is Err(QueueInUse))
  assert_true(
    broker.delete_queue(active, false, false)
    is Err(QueueDeleteWithConsumersUnsupported),
  )
  assert_eq(broker.queue_stats(active).unwrap().consumer_count(), 1)
  let nonempty = assert_ok(broker.declare_queue("nonempty"))
  assert_eq(assert_ok(broker.publish("", nonempty, b"message")), 1)
  assert_true(broker.delete_queue(nonempty, false, true) is Err(QueueNotEmpty))
  assert_eq(broker.queue_stats(nonempty).unwrap().message_count(), 1)
}

///|
test "queue delete rejects unsettled get deliveries until acknowledged" {
  let broker = Broker::new()
  let queue = assert_ok(broker.declare_queue("unsettled"))
  assert_eq(assert_ok(broker.publish("", queue, b"message")), 1)
  let delivery = match broker.get_with_owner(queue, "session-2") {
    Ok(Some(value)) => value
    _ => fail("expected one unsettled delivery")
  }
  assert_true(
    broker.delete_queue(queue, false, false)
    is Err(QueueDeleteWithUnackedDeliveriesUnsupported),
  )
  assert_ok(broker.ack_with_owner("session-2", delivery.tag()))
  assert_true(broker.delete_queue(queue, false, false) is Ok(0))
}
