///|
fn bytes_text(data : Bytes) -> String {
  try @utf8.decode(data[:]) catch {
    _ => "<invalid utf8>"
  } noraise {
    value => value
  }
}

///|
/// A library-oriented example with no external broker process.
fn main {
  let broker = @broker.Broker::new()
  let queue = match broker.declare_queue("embedded.jobs") {
    Ok(value) => value
    Err(_) => {
      println("could not declare embedded.jobs")
      return
    }
  }
  ignore(broker.publish("", queue, b"local job \xE5\xB5\x8C\xE5\x85\xa5"))
  match broker.get(queue) {
    Ok(Some(delivery)) => {
      println("embedded message: \{bytes_text(delivery.body())}")
      ignore(broker.ack(delivery.tag()))
    }
    _ => println("embedded queue is empty")
  }
}
