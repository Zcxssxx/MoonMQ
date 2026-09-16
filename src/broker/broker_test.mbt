///|
test "direct exchange routes exact keys and get returns a delivery" {
  let broker = Broker::new()
  assert_ok(broker.declare_exchange("events", ExchangeKind::ExchangeDirect))
  let queue = assert_ok(broker.declare_queue("audit"))
  assert_ok(broker.bind_queue("events", queue, "created"))
  assert_eq(assert_ok(broker.publish("events", "created", b"one")), 1)
  assert_eq(assert_ok(broker.publish("events", "deleted", b"two")), 0)
  match broker.get("audit") {
    Ok(Some(delivery)) => {
      assert_eq(delivery.body(), b"one")
      assert_eq(delivery.routing_key(), "created")
      assert_true(!delivery.redelivered())
    }
    _ => fail("expected one direct delivery")
  }
  assert_true(broker.get("audit") is Ok(None))
}

///|
test "fanout and topic exchanges route to matching queues" {
  let broker = Broker::new()
  assert_ok(broker.declare_exchange("fanout", ExchangeKind::ExchangeFanout))
  assert_ok(broker.declare_exchange("topic", ExchangeKind::ExchangeTopic))
  let first = assert_ok(broker.declare_queue("first"))
  let second = assert_ok(broker.declare_queue("second"))
  assert_ok(broker.bind_queue("fanout", first, "ignored"))
  assert_ok(broker.bind_queue("fanout", second, "also-ignored"))
  assert_ok(broker.bind_queue("topic", first, "logs.*"))
  assert_ok(broker.bind_queue("topic", second, "logs.#"))
  assert_eq(assert_ok(broker.publish("fanout", "anything", b"fan")), 2)
  assert_eq(assert_ok(broker.publish("topic", "logs.info", b"topic")), 2)
  assert_eq(assert_ok(broker.publish("topic", "metrics.info", b"no")), 0)
  assert_true(broker.get("first") is Ok(Some(_)))
  assert_true(broker.get("first") is Ok(Some(_)))
  assert_true(broker.get("second") is Ok(Some(_)))
  assert_true(broker.get("second") is Ok(Some(_)))
}

///|
test "reject with requeue redelivers and ack removes in-flight state" {
  let broker = Broker::new()
  let queue = assert_ok(broker.declare_queue("jobs"))
  ignore(assert_ok(broker.publish("", queue, b"retry")))
  let first = match broker.get(queue) {
    Ok(Some(delivery)) => delivery
    _ => fail("expected first delivery")
  }
  assert_ok(broker.reject(first.tag(), true))
  let second = match broker.get(queue) {
    Ok(Some(delivery)) => delivery
    _ => fail("expected requeued delivery")
  }
  assert_true(second.redelivered())
  assert_ok(broker.ack(second.tag()))
  assert_true(broker.get(queue) is Ok(None))
}

///|
test "consumer polling honors prefetch one" {
  let broker = Broker::new()
  let queue = assert_ok(broker.declare_queue("work"))
  let consumer = assert_ok(broker.consume(queue, "worker", 1))
  ignore(assert_ok(broker.publish("", queue, b"a")))
  ignore(assert_ok(broker.publish("", queue, b"b")))
  assert_true(broker.poll(consumer) is Ok(Some(_)))
  assert_true(broker.poll(consumer) is Ok(None))
}

///|
test "ack releases prefetch and pumps the next queued message" {
  let broker = Broker::new()
  let queue = assert_ok(broker.declare_queue("ack-pump"))
  let consumer = assert_ok(broker.consume(queue, "worker", 1))
  ignore(assert_ok(broker.publish("", queue, b"a")))
  ignore(assert_ok(broker.publish("", queue, b"b")))
  let first = match broker.poll(consumer) {
    Ok(Some(delivery)) => delivery
    _ => fail("expected first consumer delivery")
  }
  assert_ok(broker.ack(first.tag()))
  match broker.poll(consumer) {
    Ok(Some(delivery)) => assert_eq(delivery.body(), b"b")
    _ => fail("ack should pump the next delivery")
  }
}

///|
test "consumer delivery is deterministic round robin" {
  let broker = Broker::new()
  let queue = assert_ok(broker.declare_queue("round-robin"))
  let first = assert_ok(broker.consume(queue, "first", 0))
  let second = assert_ok(broker.consume(queue, "second", 0))
  ignore(assert_ok(broker.publish("", queue, b"one")))
  ignore(assert_ok(broker.publish("", queue, b"two")))
  match broker.poll(first) {
    Ok(Some(delivery)) => assert_eq(delivery.body(), b"one")
    _ => fail("first consumer should receive the first message")
  }
  match broker.poll(second) {
    Ok(Some(delivery)) => assert_eq(delivery.body(), b"two")
    _ => fail("second consumer should receive the second message")
  }
}

///|
test "canceling a middle consumer preserves round robin position" {
  let broker = Broker::new()
  let queue = assert_ok(broker.declare_queue("round-robin-cancel"))
  let first = assert_ok(broker.consume(queue, "first", 0))
  let middle = assert_ok(broker.consume(queue, "middle", 0))
  let last = assert_ok(broker.consume(queue, "last", 0))
  ignore(assert_ok(broker.publish("", queue, b"one")))
  ignore(assert_ok(broker.publish("", queue, b"two")))
  let first_delivery = match broker.poll(first) {
    Ok(Some(delivery)) => delivery
    _ => fail("first consumer should receive the first message")
  }
  let middle_delivery = match broker.poll(middle) {
    Ok(Some(delivery)) => delivery
    _ => fail("middle consumer should receive the second message")
  }
  assert_ok(broker.ack(first_delivery.tag()))
  assert_ok(broker.ack(middle_delivery.tag()))
  assert_ok(broker.cancel_consumer(middle))
  ignore(assert_ok(broker.publish("", queue, b"three")))
  match broker.poll(last) {
    Ok(Some(delivery)) => assert_eq(delivery.body(), b"three")
    _ => fail("the next active consumer should keep the round-robin position")
  }
  assert_true(broker.poll(first) is Ok(None))
}

///|
test "canceling the next consumer advances to its successor" {
  let broker = Broker::new()
  let queue = assert_ok(broker.declare_queue("round-robin-next-cancel"))
  let first = assert_ok(broker.consume(queue, "first", 0))
  let middle = assert_ok(broker.consume(queue, "middle", 0))
  let last = assert_ok(broker.consume(queue, "last", 0))
  ignore(assert_ok(broker.publish("", queue, b"one")))
  let delivery = match broker.poll(first) {
    Ok(Some(delivery)) => delivery
    _ => fail("first consumer should receive the first message")
  }
  assert_ok(broker.ack(delivery.tag()))
  assert_ok(broker.cancel_consumer(middle))
  ignore(assert_ok(broker.publish("", queue, b"two")))
  match broker.poll(last) {
    Ok(Some(delivery)) => assert_eq(delivery.body(), b"two")
    _ => fail("canceling the next consumer should advance to its successor")
  }
}

///|
test "canceling a later consumer keeps the earlier cursor" {
  let broker = Broker::new()
  let queue = assert_ok(broker.declare_queue("round-robin-earlier-cancel"))
  let first = assert_ok(broker.consume(queue, "first", 0))
  let middle = assert_ok(broker.consume(queue, "middle", 0))
  ignore(assert_ok(broker.consume(queue, "last", 0)))
  assert_ok(broker.cancel_consumer(middle))
  ignore(assert_ok(broker.publish("", queue, b"one")))
  match broker.poll(first) {
    Ok(Some(delivery)) => assert_eq(delivery.body(), b"one")
    _ => fail("canceling a later consumer should keep the earlier cursor")
  }
}

///|
test "cancel requeues unacknowledged push deliveries" {
  let broker = Broker::new()
  let queue = assert_ok(broker.declare_queue("cancel"))
  let consumer = assert_ok(broker.consume(queue, "worker", 0))
  ignore(assert_ok(broker.publish("", queue, b"cancel-me")))
  assert_true(broker.poll(consumer) is Ok(Some(_)))
  assert_ok(broker.cancel_consumer(consumer))
  match broker.get(queue) {
    Ok(Some(delivery)) => {
      assert_eq(delivery.body(), b"cancel-me")
      assert_true(delivery.redelivered())
    }
    _ => fail("cancel should requeue the in-flight message")
  }
}

///|
test "anonymous queue names never alias an existing named queue" {
  let broker = Broker::new()
  let first = assert_ok(broker.declare_queue(""))
  assert_eq(first, "amq.gen-1")
  ignore(assert_ok(broker.declare_queue("amq.gen-2")))
  let second = assert_ok(broker.declare_queue(""))
  assert_true(second != "amq.gen-2")
}

///|
test "broker preserves non-ASCII UTF-8 payload bytes" {
  let broker = Broker::new()
  let queue = assert_ok(broker.declare_queue("utf8"))
  let payload = b"\xE5\xB5\x8C\xE5\x85\xa5"
  ignore(assert_ok(broker.publish("", queue, payload)))
  match broker.get(queue) {
    Ok(Some(delivery)) => assert_eq(delivery.body(), payload)
    _ => fail("expected UTF-8 payload")
  }
}

///|
fn[T] assert_ok(result : Result[T, BrokerError]) -> T raise {
  match result {
    Ok(value) => value
    Err(_) => fail("unexpected broker error")
  }
}
