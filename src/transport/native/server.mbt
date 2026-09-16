///|
/// Serve one bounded MoonMQ session per accepted TCP connection.
pub async fn serve_connection(
  conn : @socket.Tcp,
  broker : @broker.Broker,
) -> Unit {
  let connection = NativeConnection::new(broker, 131072)
  try {
    for ;; {
      let chunk = conn.read_some(max_len=131072)
      match chunk {
        None => break
        Some(bytes) => {
          let responses = match connection.feed(bytes) {
            Ok(value) => value
            Err(_) => break
          }
          if responses.length() > 0 {
            conn.write(responses)
          }
          if connection.session().state() is @session.SessionState::Closed {
            break
          }
        }
      }
    }
  } catch {
    _ => ()
  }
}

///|
/// Start a native AMQP listener on an address such as `127.0.0.1:5672`.
pub async fn serve(addr : String, broker : @broker.Broker) -> Unit {
  let server = @socket.TcpServer(@socket.Addr::parse(addr))
  server.run_forever() <| ((conn, _) => serve_connection(conn, broker))
}
