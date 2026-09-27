///|
fn topology_declare_exchange(session : Session, exchange : String) -> Unit {
  ignore(
    session
    .receive_frame(
      method_frame(
        UInt16(1),
        @protocol.Method::ExchangeDeclare(
          ticket=UInt16(0),
          exchange~,
          kind="direct",
          passive=false,
          durable=false,
          auto_delete=false,
          internal=false,
          nowait=false,
          arguments=@protocol.FieldTable::new([]),
        ),
      ),
    )
    .unwrap(),
  )
}

///|
fn topology_declare_queue(session : Session, queue : String) -> Unit {
  ignore(
    session
    .receive_frame(
      method_frame(
        UInt16(1),
        @protocol.Method::QueueDeclare(
          ticket=UInt16(0),
          queue~,
          passive=false,
          durable=false,
          exclusive=false,
          auto_delete=false,
          nowait=false,
          arguments=@protocol.FieldTable::new([]),
        ),
      ),
    )
    .unwrap(),
  )
}

///|
fn topology_bind_queue(
  session : Session,
  queue : String,
  exchange : String,
  routing_key : String,
) -> Unit {
  ignore(
    session
    .receive_frame(
      method_frame(
        UInt16(1),
        @protocol.Method::QueueBind(
          ticket=UInt16(0),
          queue~,
          exchange~,
          routing_key~,
          nowait=false,
          arguments=@protocol.FieldTable::new([]),
        ),
      ),
    )
    .unwrap(),
  )
}

///|
fn topology_queue_delete(
  session : Session,
  queue : String,
  if_unused : Bool,
  if_empty : Bool,
  nowait : Bool,
) -> Result[Array[@protocol.Frame], SessionError] {
  session.receive_frame(
    method_frame(
      UInt16(1),
      @protocol.Method::QueueDelete(
        ticket=UInt16(0),
        queue~,
        if_unused~,
        if_empty~,
        nowait~,
      ),
    ),
  )
}

///|
test "session deletes a named exchange and removes its bindings" {
  let broker = @broker.Broker::new()
  let session = opened_session_with(broker)
  let empty_table = @protocol.FieldTable::new([])
  ignore(
    session
    .receive_frame(
      method_frame(
        UInt16(1),
        @protocol.Method::ExchangeDeclare(
          ticket=UInt16(0),
          exchange="events",
          kind="direct",
          passive=false,
          durable=false,
          auto_delete=false,
          internal=false,
          nowait=false,
          arguments=empty_table,
        ),
      ),
    )
    .unwrap(),
  )
  ignore(
    session
    .receive_frame(
      method_frame(
        UInt16(1),
        @protocol.Method::QueueDeclare(
          ticket=UInt16(0),
          queue="jobs",
          passive=false,
          durable=false,
          exclusive=false,
          auto_delete=false,
          nowait=false,
          arguments=empty_table,
        ),
      ),
    )
    .unwrap(),
  )
  ignore(
    session
    .receive_frame(
      method_frame(
        UInt16(1),
        @protocol.Method::QueueBind(
          ticket=UInt16(0),
          queue="jobs",
          exchange="events",
          routing_key="created",
          nowait=false,
          arguments=empty_table,
        ),
      ),
    )
    .unwrap(),
  )
  let deleted = session.receive_frame(
    method_frame(
      UInt16(1),
      @protocol.Method::ExchangeDelete(
        ticket=UInt16(0),
        exchange="events",
        if_unused=false,
        nowait=false,
      ),
    ),
  )
  assert_true(deleted is Ok(_))
  let responses = deleted.unwrap()
  assert_eq(responses.length(), 1)
  assert_true(method_value(responses[0]) is @protocol.Method::ExchangeDeleteOk)
  let redeclared = session
    .receive_frame(
      method_frame(
        UInt16(1),
        @protocol.Method::ExchangeDeclare(
          ticket=UInt16(0),
          exchange="events",
          kind="fanout",
          passive=false,
          durable=false,
          auto_delete=false,
          internal=false,
          nowait=false,
          arguments=empty_table,
        ),
      ),
    )
    .unwrap()
  assert_true(
    method_value(redeclared[0]) is @protocol.Method::ExchangeDeclareOk,
  )
}

///|
test "session rejects exchange deletion when if-unused has bindings" {
  let session = opened_session()
  let empty_table = @protocol.FieldTable::new([])
  ignore(
    session
    .receive_frame(
      method_frame(
        UInt16(1),
        @protocol.Method::ExchangeDeclare(
          ticket=UInt16(0),
          exchange="busy",
          kind="direct",
          passive=false,
          durable=false,
          auto_delete=false,
          internal=false,
          nowait=false,
          arguments=empty_table,
        ),
      ),
    )
    .unwrap(),
  )
  ignore(
    session
    .receive_frame(
      method_frame(
        UInt16(1),
        @protocol.Method::QueueDeclare(
          ticket=UInt16(0),
          queue="busy-queue",
          passive=false,
          durable=false,
          exclusive=false,
          auto_delete=false,
          nowait=false,
          arguments=empty_table,
        ),
      ),
    )
    .unwrap(),
  )
  ignore(
    session
    .receive_frame(
      method_frame(
        UInt16(1),
        @protocol.Method::QueueBind(
          ticket=UInt16(0),
          queue="busy-queue",
          exchange="busy",
          routing_key="key",
          nowait=false,
          arguments=empty_table,
        ),
      ),
    )
    .unwrap(),
  )
  let result = session.receive_frame(
    method_frame(
      UInt16(1),
      @protocol.Method::ExchangeDelete(
        ticket=UInt16(0),
        exchange="busy",
        if_unused=true,
        nowait=false,
      ),
    ),
  )
  assert_true(result is Err(SessionError::BrokerError(_)))
}

///|
test "session unbinds an exact queue routing key" {
  let broker = @broker.Broker::new()
  let session = opened_session_with(broker)
  let empty_table = @protocol.FieldTable::new([])
  ignore(
    session
    .receive_frame(
      method_frame(
        UInt16(1),
        @protocol.Method::ExchangeDeclare(
          ticket=UInt16(0),
          exchange="events",
          kind="direct",
          passive=false,
          durable=false,
          auto_delete=false,
          internal=false,
          nowait=false,
          arguments=empty_table,
        ),
      ),
    )
    .unwrap(),
  )
  ignore(
    session
    .receive_frame(
      method_frame(
        UInt16(1),
        @protocol.Method::QueueDeclare(
          ticket=UInt16(0),
          queue="jobs",
          passive=false,
          durable=false,
          exclusive=false,
          auto_delete=false,
          nowait=false,
          arguments=empty_table,
        ),
      ),
    )
    .unwrap(),
  )
  ignore(
    session
    .receive_frame(
      method_frame(
        UInt16(1),
        @protocol.Method::QueueBind(
          ticket=UInt16(0),
          queue="jobs",
          exchange="events",
          routing_key="created",
          nowait=false,
          arguments=empty_table,
        ),
      ),
    )
    .unwrap(),
  )
  let result = session.receive_frame(
    method_frame(
      UInt16(1),
      @protocol.Method::QueueUnbind(
        ticket=UInt16(0),
        queue="jobs",
        exchange="events",
        routing_key="created",
        arguments=empty_table,
      ),
    ),
  )
  assert_true(result is Ok(_))
  let responses = result.unwrap()
  assert_eq(responses.length(), 1)
  assert_true(method_value(responses[0]) is @protocol.Method::QueueUnbindOk)
  let published = broker.publish("events", "created", b"message").unwrap()
  assert_eq(published, 0)
}

///|
test "exchange delete nowait does not send a response frame" {
  let session = opened_session()
  ignore(
    session
    .receive_frame(
      method_frame(
        UInt16(1),
        @protocol.Method::ExchangeDeclare(
          ticket=UInt16(0),
          exchange="events",
          kind="direct",
          passive=false,
          durable=false,
          auto_delete=false,
          internal=false,
          nowait=false,
          arguments=@protocol.FieldTable::new([]),
        ),
      ),
    )
    .unwrap(),
  )
  let responses = session
    .receive_frame(
      method_frame(
        UInt16(1),
        @protocol.Method::ExchangeDelete(
          ticket=UInt16(0),
          exchange="events",
          if_unused=false,
          nowait=true,
        ),
      ),
    )
    .unwrap()
  assert_eq(responses.length(), 0)
}

///|
test "queue unbind rejects nonempty arguments explicitly" {
  let session = opened_session()
  let arguments = @protocol.FieldTable::new([
    ("unsupported", @protocol.FieldValue::FieldBoolean(true)),
  ])
  let result = session.receive_frame(
    method_frame(
      UInt16(1),
      @protocol.Method::QueueUnbind(
        ticket=UInt16(0),
        queue="jobs",
        exchange="events",
        routing_key="created",
        arguments~,
      ),
    ),
  )
  assert_true(result is Err(SessionError::UnsupportedOperation))
}

///|
test "session deletes an idle queue and returns the ready message count" {
  let broker = @broker.Broker::new()
  let session = opened_session_with(broker)
  topology_declare_exchange(session, "events")
  topology_declare_queue(session, "jobs")
  topology_bind_queue(session, "jobs", "events", "created")
  ignore(broker.publish("events", "created", b"one").unwrap())
  ignore(broker.publish("events", "created", b"two").unwrap())
  let result = topology_queue_delete(session, "jobs", false, false, false)
  assert_true(result is Ok(_))
  let responses = result.unwrap()
  assert_eq(responses.length(), 1)
  assert_true(
    method_value(responses[0])
    is @protocol.Method::QueueDeleteOk(message_count=2U),
  )
  assert_true(
    broker.queue_stats("jobs") is Err(@broker.BrokerError::UnknownQueue),
  )
  assert_eq(broker.publish("events", "created", b"three").unwrap(), 0)
}

///|
test "queue delete nowait does not send a response frame" {
  let session = opened_session()
  topology_declare_queue(session, "idle")
  let result = topology_queue_delete(session, "idle", false, false, true)
  assert_true(result is Ok(_))
  let responses = result.unwrap()
  assert_eq(responses.length(), 0)
}

///|
test "queue delete honors if-unused and rejects active consumer deletion" {
  let broker = @broker.Broker::new()
  let deleter = opened_session_with(broker)
  let consumer_session = opened_session_with(broker)
  topology_declare_queue(deleter, "shared")
  topology_declare_queue(consumer_session, "shared")
  ignore(
    consumer_session
    .receive_frame(
      method_frame(
        UInt16(1),
        @protocol.Method::BasicConsume(
          ticket=UInt16(0),
          queue="shared",
          consumer_tag="worker",
          no_local=false,
          no_ack=true,
          exclusive=false,
          nowait=false,
          arguments=@protocol.FieldTable::new([]),
        ),
      ),
    )
    .unwrap(),
  )
  assert_true(
    topology_queue_delete(deleter, "shared", true, false, false)
    is Err(SessionError::BrokerError(@broker.BrokerError::QueueInUse)),
  )
  assert_eq(broker.queue_stats("shared").unwrap().consumer_count(), 1)
  assert_true(
    topology_queue_delete(deleter, "shared", false, false, false)
    is Err(
      SessionError::BrokerError(
        @broker.BrokerError::QueueDeleteWithConsumersUnsupported
      )
    ),
  )
  assert_eq(broker.publish("", "shared", b"message").unwrap(), 1)
  let cancelled = consumer_session
    .receive_frame(
      method_frame(
        UInt16(1),
        @protocol.Method::BasicCancel(consumer_tag="worker", nowait=false),
      ),
    )
    .unwrap()
  assert_true(method_value(cancelled[0]) is @protocol.Method::BasicCancelOk(..))
  assert_eq(broker.queue_stats("shared").unwrap().message_count(), 1)
}

///|
test "queue delete honors if-empty and retains ready messages on rejection" {
  let broker = @broker.Broker::new()
  let session = opened_session_with(broker)
  topology_declare_queue(session, "nonempty")
  ignore(broker.publish("", "nonempty", b"message").unwrap())
  assert_true(
    topology_queue_delete(session, "nonempty", false, true, false)
    is Err(SessionError::BrokerError(@broker.BrokerError::QueueNotEmpty)),
  )
  assert_eq(broker.queue_stats("nonempty").unwrap().message_count(), 1)
}

///|
test "queue delete refuses a queue with an unacknowledged get" {
  let broker = @broker.Broker::new()
  let session = opened_session_with(broker)
  topology_declare_queue(session, "in-flight")
  ignore(broker.publish("", "in-flight", b"message").unwrap())
  let get = session
    .receive_frame(
      method_frame(
        UInt16(1),
        @protocol.Method::BasicGet(
          ticket=UInt16(0),
          queue="in-flight",
          no_ack=false,
        ),
      ),
    )
    .unwrap()
  let tag = match method_value(get[0]) {
    @protocol.Method::BasicGetOk(delivery_tag~, ..) => delivery_tag
    _ => fail("expected one unacknowledged get")
  }
  assert_true(
    topology_queue_delete(session, "in-flight", false, false, false)
    is Err(
      SessionError::BrokerError(
        @broker.BrokerError::QueueDeleteWithUnackedDeliveriesUnsupported
      )
    ),
  )
  ignore(
    session
    .receive_frame(
      method_frame(
        UInt16(1),
        @protocol.Method::BasicAck(delivery_tag=tag, multiple=false),
      ),
    )
    .unwrap(),
  )
  assert_eq(broker.queue_stats("in-flight").unwrap().message_count(), 0)
}
