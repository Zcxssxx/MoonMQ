///|
fn contains_delivery_tag(tags : Array[UInt64], target : UInt64) -> Bool {
  for tag in tags {
    if tag == target {
      return true
    }
  }
  false
}

///|
/// Acknowledge the requested deliveries atomically within one session owner.
pub fn Broker::ack_many_with_owner(
  self : Broker,
  owner : String,
  tags : Array[UInt64],
) -> Result[Unit, BrokerError] {
  let unique_tags : Array[UInt64] = []
  for tag in tags {
    if !contains_delivery_tag(unique_tags, tag) {
      unique_tags.push(tag)
    }
  }
  for tag in unique_tags {
    let mut found = false
    for delivery in self.in_flight {
      if delivery.owner == owner && delivery.tag == tag {
        found = true
        break
      }
    }
    if !found {
      return Err(UnknownDelivery)
    }
  }
  let affected_queues : Array[String] = []
  for tag in unique_tags {
    let delivery = match self.remove_in_flight(owner, tag) {
      Ok(value) => value
      Err(error) => return Err(error)
    }
    self.decrement_consumer(delivery.queue, delivery.consumer_tag)
    if !contains_string(affected_queues, delivery.queue) {
      affected_queues.push(delivery.queue)
    }
  }
  for queue in affected_queues {
    let queue_index = self.find_queue(queue)
    if queue_index >= 0 {
      self.pump_queue(queue_index)
    }
  }
  Ok(())
}
