///|
fn ack_test_open_channel(session : Session, channel : UInt16) -> Unit {
  ignore(
    session
    .receive_frame(
      method_frame(channel, @protocol.Method::ChannelOpen(out_of_band="")),
    )
    .unwrap(),
  )
}

///|
fn ack_test_get_tag(
  session : Session,
  channel : UInt16,
  queue : String,
) -> Result[UInt64, String] {
  let responses = session
    .receive_frame(
      method_frame(
        channel,
        @protocol.Method::BasicGet(ticket=UInt16(0), queue~, no_ack=false),
      ),
    )
    .unwrap()
  match method_value(responses[0]) {
    @protocol.Method::BasicGetOk(delivery_tag~, ..) => Ok(delivery_tag)
    _ => Err("expected one basic.get-ok")
  }
}

///|
test "multiple ack acknowledges through its tag on the current channel only" {
  let broker = @broker.Broker::new()
  let session = opened_session_with(broker)
  ack_test_open_channel(session, UInt16(2))
  ignore(
    session
    .receive_frame(
      method_frame(
        UInt16(1),
        @protocol.Method::QueueDeclare(
          ticket=UInt16(0),
          queue="ack-range",
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
  ignore(broker.publish("", "ack-range", b"one").unwrap())
  ignore(broker.publish("", "ack-range", b"two").unwrap())
  ignore(broker.publish("", "ack-range", b"three").unwrap())
  let first = ack_test_get_tag(session, UInt16(1), "ack-range").unwrap()
  let second = ack_test_get_tag(session, UInt16(1), "ack-range").unwrap()
  let other_channel = ack_test_get_tag(session, UInt16(2), "ack-range").unwrap()
  let result = session.receive_frame(
    method_frame(
      UInt16(1),
      @protocol.Method::BasicAck(delivery_tag=second, multiple=true),
    ),
  )
  assert_true(result is Ok(_))
  assert_eq(result.unwrap().length(), 0)
  assert_true(
    session.receive_frame(
      method_frame(
        UInt16(2),
        @protocol.Method::BasicAck(delivery_tag=other_channel, multiple=false),
      ),
    )
    is Ok(_),
  )
  assert_true(
    session.receive_frame(
      method_frame(
        UInt16(1),
        @protocol.Method::BasicAck(delivery_tag=first, multiple=false),
      ),
    )
    is Err(SessionError::InvalidChannel),
  )
}

///|
test "multiple ack tag zero acknowledges all current channel deliveries" {
  let broker = @broker.Broker::new()
  let session = opened_session_with(broker)
  ack_test_open_channel(session, UInt16(2))
  ignore(
    session
    .receive_frame(
      method_frame(
        UInt16(1),
        @protocol.Method::QueueDeclare(
          ticket=UInt16(0),
          queue="ack-zero",
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
  ignore(broker.publish("", "ack-zero", b"one").unwrap())
  ignore(broker.publish("", "ack-zero", b"two").unwrap())
  ignore(ack_test_get_tag(session, UInt16(1), "ack-zero").unwrap())
  let other_channel = ack_test_get_tag(session, UInt16(2), "ack-zero").unwrap()
  let result = session.receive_frame(
    method_frame(
      UInt16(1),
      @protocol.Method::BasicAck(delivery_tag=UInt64(0), multiple=true),
    ),
  )
  assert_true(result is Ok(_))
  assert_eq(result.unwrap().length(), 0)
  assert_true(
    session.receive_frame(
      method_frame(
        UInt16(2),
        @protocol.Method::BasicAck(delivery_tag=other_channel, multiple=false),
      ),
    )
    is Ok(_),
  )
}

///|
test "multiple ack broker failure preserves session delivery records" {
  let broker = @broker.Broker::new()
  let session = opened_session_with(broker)
  ignore(
    session
    .receive_frame(
      method_frame(
        UInt16(1),
        @protocol.Method::QueueDeclare(
          ticket=UInt16(0),
          queue="ack-atomic",
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
  ignore(broker.publish("", "ack-atomic", b"one").unwrap())
  ignore(broker.publish("", "ack-atomic", b"two").unwrap())
  let first = ack_test_get_tag(session, UInt16(1), "ack-atomic").unwrap()
  let second = ack_test_get_tag(session, UInt16(1), "ack-atomic").unwrap()

  // Desynchronize one tag as if another Broker operation invalidated it.
  assert_true(broker.ack_with_owner(session.consumer_owner, second) is Ok(_))
  assert_eq(session.deliveries.length(), 2)
  let result = session.receive_frame(
    method_frame(
      UInt16(1),
      @protocol.Method::BasicAck(delivery_tag=second, multiple=true),
    ),
  )
  assert_true(
    result
    is Err(SessionError::BrokerError(@broker.BrokerError::UnknownDelivery)),
  )
  assert_eq(session.deliveries.length(), 2)
  assert_true(broker.ack_with_owner(session.consumer_owner, first) is Ok(_))
}
