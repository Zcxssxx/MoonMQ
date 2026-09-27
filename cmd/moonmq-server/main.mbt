///|
/// Start the local native AMQP listener on the standard AMQP port.
async fn main {
  println("MoonMQ native server listening on 127.0.0.1:5672")
  @native.serve("127.0.0.1:5672", @broker.Broker::new()) catch {
    error => println("MoonMQ native server stopped: \{error}")
  }
}
