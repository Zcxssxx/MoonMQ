///|
fn method_frame(channel : UInt16, value : @protocol.Method) -> @protocol.Frame {
  @protocol.Frame::new(
    @protocol.FrameMethod,
    channel,
    @protocol.encode_method(value).unwrap(),
  )
}

///|
async fn read_frame(conn : @socket.Tcp) -> @protocol.Frame {
  let envelope = conn.read_exactly(7)
  let size = @protocol.decode_u32(envelope[3:7].to_owned())
    .unwrap()
    .reinterpret_as_int()
  let tail = conn.read_exactly(size + 1)
  let encoded : Array[Byte] = []
  for byte in envelope {
    encoded.push(byte)
  }
  for byte in tail {
    encoded.push(byte)
  }
  @protocol.decode_frame(Bytes::from_array(encoded[:])).unwrap()
}

///|
test "native connection accepts split protocol and frame input" {
  let connection = NativeConnection::new(@broker.Broker::new(), 131072)
  let header = @protocol.encode_protocol_header()
  let first = connection.feed(header[:3].to_owned()).unwrap()
  assert_eq(first.length(), 0)
  let start = connection.feed(header[3:].to_owned()).unwrap()
  assert_eq(start[:8].to_owned(), header)
  let start_frame = @protocol.decode_frame(start[8:].to_owned()).unwrap()
  assert_true(
    @protocol.decode_method(start_frame.payload()).unwrap()
    is @protocol.Method::ConnectionStart(..),
  )

  let start_ok = @protocol.encode_frame(
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
  let tune = connection.feed(start_ok.unwrap()).unwrap()
  let tune_frame = @protocol.decode_frame(tune).unwrap()
  assert_true(
    @protocol.decode_method(tune_frame.payload()).unwrap()
    is @protocol.Method::ConnectionTune(..),
  )
}

///|
test "native connection rejects malformed frame bytes" {
  let connection = NativeConnection::new(@broker.Broker::new(), 131072)
  ignore(connection.feed(@protocol.encode_protocol_header()).unwrap())
  assert_true(connection.feed(b"\x08\x00\x01\x00\x00\x00\x00\xCE") is Err(_))
}

///|
async test "native TCP server completes the AMQP handshake" {
  @async.with_task_group() <| root => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    let port = server.addr.port()
    root.spawn_bg(no_wait=true) <| () => {
      let (conn, _) = server.accept()
      defer conn.close()
      serve_connection(conn, @broker.Broker::new())
    }
    let client = @socket.Tcp::connect(@socket.Addr::parse("127.0.0.1:\{port}"))
    defer client.close()
    client.write(@protocol.encode_protocol_header())
    assert_eq(client.read_exactly(8), @protocol.encode_protocol_header())
    let start = read_frame(client)
    assert_true(
      @protocol.decode_method(start.payload()).unwrap()
      is @protocol.Method::ConnectionStart(..),
    )
    client.write(
      @protocol.encode_frame(
        method_frame(
          UInt16(0),
          @protocol.Method::ConnectionStartOk(
            client_properties=@protocol.FieldTable::new([]),
            mechanism="PLAIN",
            response=b"\x00guest\x00guest",
            locale="en_US",
          ),
        ),
      ).unwrap(),
    )
    let tune = read_frame(client)
    assert_true(
      @protocol.decode_method(tune.payload()).unwrap()
      is @protocol.Method::ConnectionTune(..),
    )
    client.write(
      @protocol.encode_frame(
        method_frame(
          UInt16(0),
          @protocol.Method::ConnectionTuneOk(
            channel_max=UInt16(0),
            frame_max=131072U,
            heartbeat=UInt16(0),
          ),
        ),
      ).unwrap(),
    )
    client.write(
      @protocol.encode_frame(
        method_frame(
          UInt16(0),
          @protocol.Method::ConnectionOpen(
            virtual_host="/",
            capabilities="",
            insist=false,
          ),
        ),
      ).unwrap(),
    )
    let open_ok = read_frame(client)
    assert_true(
      @protocol.decode_method(open_ok.payload()).unwrap()
      is @protocol.Method::ConnectionOpenOk,
    )
    client.write(
      @protocol.encode_frame(
        method_frame(
          UInt16(0),
          @protocol.Method::ConnectionClose(
            reply_code=UInt16(0),
            reply_text="",
            class_id=UInt16(0),
            method_id=UInt16(0),
          ),
        ),
      ).unwrap(),
    )
    let close_ok = read_frame(client)
    assert_true(
      @protocol.decode_method(close_ok.payload()).unwrap()
      is @protocol.Method::ConnectionCloseOk,
    )
  }
}
