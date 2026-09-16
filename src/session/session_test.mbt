///|
fn method_frame(channel : UInt16, value : @protocol.Method) -> @protocol.Frame {
  let payload = @protocol.encode_method(value).unwrap()
  @protocol.Frame::new(@protocol.FrameMethod, channel, payload)
}

///|
fn method_value(frame : @protocol.Frame) -> @protocol.Method {
  @protocol.decode_method(frame.payload()).unwrap()
}

///|
fn opened_session() -> Session {
  let session = Session::new(@broker.Broker::new())
  let response = session
    .receive_protocol_header(@protocol.encode_protocol_header())
    .unwrap()
  ignore(response)
  ignore(
    session.receive_frame(
      method_frame(
        UInt16(0),
        @protocol.Method::ConnectionStartOk(
          client_properties=@protocol.FieldTable::new([]),
          mechanism="PLAIN",
          response=b"\x00guest\x00guest",
          locale="en_US",
        ),
      ),
    ),
  )
  ignore(
    session.receive_frame(
      method_frame(
        UInt16(0),
        @protocol.Method::ConnectionTuneOk(
          channel_max=UInt16(0),
          frame_max=131072U,
          heartbeat=UInt16(0),
        ),
      ),
    ),
  )
  let responses = session.receive_frame(
    method_frame(
      UInt16(0),
      @protocol.Method::ConnectionOpen(
        virtual_host="/",
        capabilities="",
        insist=false,
      ),
    ),
  )
  ignore(responses)
  ignore(
    session
    .receive_frame(
      method_frame(UInt16(1), @protocol.Method::ChannelOpen(out_of_band="")),
    )
    .unwrap(),
  )
  session
}

///|
test "session performs protocol handshake and opens a channel" {
  let session = Session::new(@broker.Broker::new())
  assert_true(session.state() is SessionState::AwaitProtocolHeader)
  let response = session
    .receive_protocol_header(@protocol.encode_protocol_header())
    .unwrap()
  assert_eq(response, @protocol.encode_protocol_header())
  let start_frame = session.server_start_frame().unwrap()
  assert_true(
    method_value(start_frame) is @protocol.Method::ConnectionStart(..),
  )
  assert_true(session.state() is SessionState::AwaitStartOk)
  let start_responses = session
    .receive_frame(
      method_frame(
        UInt16(0),
        @protocol.Method::ConnectionStartOk(
          client_properties=@protocol.FieldTable::new([]),
          mechanism="PLAIN",
          response=b"\x00guest\x00guest",
          locale="en_US",
        ),
      ),
    )
    .unwrap()
  assert_eq(start_responses.length(), 1)
  assert_true(
    method_value(start_responses[0]) is @protocol.Method::ConnectionTune(..),
  )
  assert_true(session.state() is SessionState::AwaitTuneOk)
  let tune_responses = session
    .receive_frame(
      method_frame(
        UInt16(0),
        @protocol.Method::ConnectionTuneOk(
          channel_max=UInt16(0),
          frame_max=131072U,
          heartbeat=UInt16(0),
        ),
      ),
    )
    .unwrap()
  assert_eq(tune_responses.length(), 0)
  assert_true(session.state() is SessionState::AwaitOpen)
  let open_responses = session
    .receive_frame(
      method_frame(
        UInt16(0),
        @protocol.Method::ConnectionOpen(
          virtual_host="/",
          capabilities="",
          insist=false,
        ),
      ),
    )
    .unwrap()
  assert_eq(open_responses.length(), 1)
  assert_true(
    method_value(open_responses[0]) is @protocol.Method::ConnectionOpenOk,
  )
  assert_true(session.state() is SessionState::Open)
  let channel_responses = session
    .receive_frame(
      method_frame(UInt16(1), @protocol.Method::ChannelOpen(out_of_band="")),
    )
    .unwrap()
  assert_eq(channel_responses.length(), 1)
  assert_true(
    method_value(channel_responses[0]) is @protocol.Method::ChannelOpenOk,
  )
}

///|
test "session translates queue publish get and acknowledgement" {
  let session = opened_session()
  let queue_responses = match
    session.receive_frame(
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
          arguments=@protocol.FieldTable::new([]),
        ),
      ),
    ) {
    Ok(responses) => responses
    Err(_) => fail("queue.declare unexpectedly failed")
  }
  assert_true(
    method_value(queue_responses[0]) is @protocol.Method::QueueDeclareOk(..),
  )

  let publish_responses = match
    session.receive_frame(
      method_frame(
        UInt16(1),
        @protocol.Method::BasicPublish(
          ticket=UInt16(0),
          exchange="",
          routing_key="jobs",
          mandatory=false,
          immediate=false,
        ),
      ),
    ) {
    Ok(responses) => responses
    Err(_) => fail("basic.publish unexpectedly failed")
  }
  assert_eq(publish_responses.length(), 0)
  let header = @protocol.ContentHeader::new(
    class_id=UInt16(60),
    weight=UInt16(0),
    body_size=UInt64(5),
    properties=@protocol.BasicProperties::new(),
  )
  let header_payload = match @protocol.encode_content_header(header) {
    Ok(value) => value
    Err(_) => fail("test content header unexpectedly failed")
  }
  match
    session.receive_frame(
      @protocol.Frame::new(@protocol.FrameHeader, UInt16(1), header_payload),
    ) {
    Ok(_) => ()
    Err(_) => fail("content header unexpectedly failed")
  }
  match
    session.receive_frame(
      @protocol.Frame::new(@protocol.FrameBody, UInt16(1), b"hello"),
    ) {
    Ok(_) => ()
    Err(_) => fail("content body unexpectedly failed")
  }

  let get_responses = match
    session.receive_frame(
      method_frame(
        UInt16(1),
        @protocol.Method::BasicGet(ticket=UInt16(0), queue="jobs", no_ack=false),
      ),
    ) {
    Ok(responses) => responses
    Err(_) => fail("basic.get unexpectedly failed")
  }
  assert_eq(get_responses.length(), 3)
  let delivery_tag = match method_value(get_responses[0]) {
    @protocol.Method::BasicGetOk(delivery_tag~, ..) => delivery_tag
    _ => fail("expected basic.get-ok")
  }
  match @protocol.decode_content_header(get_responses[1].payload()) {
    Ok(decoded) => assert_eq(decoded.body_size(), UInt64(5))
    Err(_) => fail("get response header unexpectedly failed")
  }
  assert_eq(get_responses[2].payload(), b"hello")
  let ack_responses = match
    session.receive_frame(
      method_frame(
        UInt16(1),
        @protocol.Method::BasicAck(delivery_tag~, multiple=false),
      ),
    ) {
    Ok(responses) => responses
    Err(_) => fail("basic.ack unexpectedly failed")
  }
  assert_eq(ack_responses.length(), 0)
}

///|
test "session rejects frames before the handshake" {
  let session = Session::new(@broker.Broker::new())
  let result = session.receive_frame(
    method_frame(
      UInt16(0),
      @protocol.Method::ConnectionOpen(
        virtual_host="/",
        capabilities="",
        insist=false,
      ),
    ),
  )
  assert_true(result is Err(SessionError::InvalidState))
}

///|
test "session pushes deliveries after a publish and cancels consumers" {
  let session = opened_session()
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
          arguments=@protocol.FieldTable::new([]),
        ),
      ),
    )
    .unwrap(),
  )
  let consume_responses = session
    .receive_frame(
      method_frame(
        UInt16(1),
        @protocol.Method::BasicConsume(
          ticket=UInt16(0),
          queue="jobs",
          consumer_tag="worker",
          no_local=false,
          no_ack=false,
          exclusive=false,
          nowait=false,
          arguments=@protocol.FieldTable::new([]),
        ),
      ),
    )
    .unwrap()
  assert_eq(consume_responses.length(), 1)
  assert_true(
    method_value(consume_responses[0])
    is @protocol.Method::BasicConsumeOk(consumer_tag="worker"),
  )
  ignore(
    session
    .receive_frame(
      method_frame(
        UInt16(1),
        @protocol.Method::BasicPublish(
          ticket=UInt16(0),
          exchange="",
          routing_key="jobs",
          mandatory=false,
          immediate=false,
        ),
      ),
    )
    .unwrap(),
  )
  let header = @protocol.ContentHeader::new(
    class_id=UInt16(60),
    weight=UInt16(0),
    body_size=UInt64(2),
    properties=@protocol.BasicProperties::new(),
  )
  let header_payload = @protocol.encode_content_header(header).unwrap()
  ignore(
    session
    .receive_frame(
      @protocol.Frame::new(@protocol.FrameHeader, UInt16(1), header_payload),
    )
    .unwrap(),
  )
  let delivery_responses = session
    .receive_frame(@protocol.Frame::new(@protocol.FrameBody, UInt16(1), b"hi"))
    .unwrap()
  assert_eq(delivery_responses.length(), 3)
  assert_true(
    method_value(delivery_responses[0])
    is @protocol.Method::BasicDeliver(consumer_tag="worker", ..),
  )
  assert_eq(delivery_responses[2].payload(), b"hi")
  let cancel_responses = session
    .receive_frame(
      method_frame(
        UInt16(1),
        @protocol.Method::BasicCancel(consumer_tag="worker", nowait=false),
      ),
    )
    .unwrap()
  assert_eq(cancel_responses.length(), 1)
  assert_true(
    method_value(cancel_responses[0])
    is @protocol.Method::BasicCancelOk(consumer_tag="worker"),
  )
}

///|
test "session rejects content before publish and unknown channels" {
  let session = opened_session()
  assert_true(
    session.receive_frame(
      @protocol.Frame::new(@protocol.FrameBody, UInt16(1), b"orphan"),
    )
    is Err(SessionError::MissingContent),
  )
  assert_true(
    session.receive_frame(
      method_frame(
        UInt16(9),
        @protocol.Method::BasicPublish(
          ticket=UInt16(0),
          exchange="",
          routing_key="jobs",
          mandatory=false,
          immediate=false,
        ),
      ),
    )
    is Err(SessionError::InvalidChannel),
  )
}

///|
test "session preserves named exchange metadata in get delivery" {
  let session = opened_session()
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
  ignore(
    session
    .receive_frame(
      method_frame(
        UInt16(1),
        @protocol.Method::BasicPublish(
          ticket=UInt16(0),
          exchange="events",
          routing_key="created",
          mandatory=false,
          immediate=false,
        ),
      ),
    )
    .unwrap(),
  )
  let header = @protocol.ContentHeader::new(
    class_id=UInt16(60),
    weight=UInt16(0),
    body_size=UInt64(5),
    properties=@protocol.BasicProperties::new(),
  )
  ignore(
    session
    .receive_frame(
      @protocol.Frame::new(
        @protocol.FrameHeader,
        UInt16(1),
        @protocol.encode_content_header(header).unwrap(),
      ),
    )
    .unwrap(),
  )
  ignore(
    session
    .receive_frame(
      @protocol.Frame::new(@protocol.FrameBody, UInt16(1), b"hello"),
    )
    .unwrap(),
  )
  let responses = session
    .receive_frame(
      method_frame(
        UInt16(1),
        @protocol.Method::BasicGet(ticket=UInt16(0), queue="jobs", no_ack=false),
      ),
    )
    .unwrap()
  match method_value(responses[0]) {
    @protocol.Method::BasicGetOk(exchange~, ..) => assert_eq(exchange, "events")
    _ => fail("expected basic.get-ok from named exchange")
  }
}
