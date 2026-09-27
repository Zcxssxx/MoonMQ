///|
/// Delete an exchange, optionally requiring it to have no queue bindings.
pub fn Broker::delete_exchange(
  self : Broker,
  name : String,
  if_unused : Bool,
) -> Result[Unit, BrokerError] {
  if name == "" {
    return Err(CannotDeleteDefaultExchange)
  }
  let exchange_index = self.find_exchange(name)
  if exchange_index < 0 {
    return Err(UnknownExchange)
  }
  if if_unused {
    for binding in self.bindings {
      if binding.exchange == name {
        return Err(ExchangeInUse)
      }
    }
  }
  ignore(self.exchanges.remove(exchange_index))
  let mut binding_index = 0
  while binding_index < self.bindings.length() {
    if self.bindings[binding_index].exchange == name {
      ignore(self.bindings.remove(binding_index))
    } else {
      binding_index += 1
    }
  }
  Ok(())
}

///|
/// Remove one exact queue binding from an existing exchange and queue.
pub fn Broker::unbind_queue(
  self : Broker,
  exchange : String,
  queue : String,
  pattern : String,
) -> Result[Unit, BrokerError] {
  if self.find_exchange(exchange) < 0 {
    return Err(UnknownExchange)
  }
  if self.find_queue(queue) < 0 {
    return Err(UnknownQueue)
  }
  for index = 0; index < self.bindings.length(); index = index + 1 {
    let binding = self.bindings[index]
    if binding.exchange == exchange &&
      binding.queue == queue &&
      binding.pattern == pattern {
      ignore(self.bindings.remove(index))
      return Ok(())
    }
  }
  Err(UnknownBinding)
}

///|
/// Delete a queue only when it has no consumer or unsettled delivery.
pub fn Broker::delete_queue(
  self : Broker,
  queue : String,
  if_unused : Bool,
  if_empty : Bool,
) -> Result[Int, BrokerError] {
  let queue_index = self.find_queue(queue)
  if queue_index < 0 {
    return Err(UnknownQueue)
  }
  let consumer_count = self.queues[queue_index].consumers.length()
  if if_unused && consumer_count > 0 {
    return Err(QueueInUse)
  }
  let message_count = self.queues[queue_index].messages.length()
  if if_empty && message_count > 0 {
    return Err(QueueNotEmpty)
  }
  if consumer_count > 0 {
    return Err(QueueDeleteWithConsumersUnsupported)
  }
  for delivery in self.in_flight {
    if delivery.queue == queue {
      return Err(QueueDeleteWithUnackedDeliveriesUnsupported)
    }
  }
  let mut binding_index = 0
  while binding_index < self.bindings.length() {
    if self.bindings[binding_index].queue == queue {
      ignore(self.bindings.remove(binding_index))
    } else {
      binding_index += 1
    }
  }
  ignore(self.queues.remove(queue_index))
  Ok(message_count)
}
