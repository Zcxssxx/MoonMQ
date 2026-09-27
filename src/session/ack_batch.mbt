///|
fn Session::handle_basic_ack(
  self : Session,
  channel : UInt16,
  delivery_tag : UInt64,
  multiple : Bool,
) -> Result[Array[@protocol.Frame], SessionError] {
  match self.check_channel(channel) {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  if !multiple {
    let delivery_index = match self.check_delivery(channel, delivery_tag) {
      Ok(value) => value
      Err(error) => return Err(error)
    }
    match self.broker.ack_with_owner(self.consumer_owner, delivery_tag) {
      Ok(_) => ignore(self.deliveries.remove(delivery_index))
      Err(error) => return Err(broker_error(error))
    }
  } else {
    let tags : Array[UInt64] = []
    for delivery in self.deliveries {
      if delivery.channel == channel &&
        (delivery_tag == UInt64(0) || delivery.tag <= delivery_tag) {
        tags.push(delivery.tag)
      }
    }
    match self.broker.ack_many_with_owner(self.consumer_owner, tags) {
      Ok(_) => {
        let mut index = self.deliveries.length() - 1
        while index >= 0 {
          let delivery = self.deliveries[index]
          if delivery.channel == channel &&
            (delivery_tag == UInt64(0) || delivery.tag <= delivery_tag) {
            ignore(self.deliveries.remove(index))
          }
          index -= 1
        }
      }
      Err(error) => return Err(broker_error(error))
    }
  }
  let output : Array[@protocol.Frame] = []
  match self.drain_consumers(output) {
    Ok(_) => Ok(output)
    Err(error) => Err(error)
  }
}
