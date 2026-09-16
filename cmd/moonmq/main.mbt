///|
fn bytes_text(data : Bytes) -> String {
  try @utf8.decode(data[:]) catch {
    _ => "<invalid utf8>"
  } noraise {
    value => value
  }
}

///|
/// Run a no-service publish/consume demonstration.
fn main {
  let broker = @broker.Broker::new()
  let queue = match broker.declare_queue("demo") {
    Ok(value) => value
    Err(_) => {
      println("MoonMQ demo failed to declare its queue")
      return
    }
  }
  match broker.publish("", queue, b"hello from an embedded broker") {
    Ok(count) =>
      println("published to \{count} queue(s) through the default exchange")
    Err(_) => {
      println("MoonMQ demo failed to publish")
      return
    }
  }
  match broker.get(queue) {
    Ok(Some(delivery)) => {
      println("consumed: \{bytes_text(delivery.body())}")
      match broker.ack(delivery.tag()) {
        Ok(_) => println("acknowledged delivery \{delivery.tag()}")
        Err(_) => println("MoonMQ demo failed to acknowledge")
      }
    }
    Ok(None) => println("MoonMQ demo queue was unexpectedly empty")
    Err(_) => println("MoonMQ demo failed to consume")
  }
}
