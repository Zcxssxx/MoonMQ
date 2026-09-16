///|
/// Create an isolated broker with the AMQP default exchange.
pub fn Broker::new() -> Broker {
  {
    exchanges: [{ name: "", kind: ExchangeDirect, }],
    queues: [],
    bindings: [],
    in_flight: [],
    next_tag: UInt64(1),
    next_queue_id: 1,
  }
}

///|
fn Broker::find_exchange(self : Broker, name : String) -> Int {
  for index = 0; index < self.exchanges.length(); index = index + 1 {
    if self.exchanges[index].name == name {
      return index
    }
  }
  -1
}

///|
fn Broker::find_queue(self : Broker, name : String) -> Int {
  for index = 0; index < self.queues.length(); index = index + 1 {
    if self.queues[index].name == name {
      return index
    }
  }
  -1
}

///|
fn contains_string(values : Array[String], value : String) -> Bool {
  for item in values {
    if item == value {
      return true
    }
  }
  false
}

///|
pub fn Broker::declare_exchange(
  self : Broker,
  name : String,
  kind : ExchangeKind,
) -> Result[Unit, BrokerError] {
  let existing = self.find_exchange(name)
  if existing >= 0 {
    if self.exchanges[existing].kind == kind {
      Ok(())
    } else {
      Err(ExchangeKindMismatch)
    }
  } else {
    self.exchanges.push({ name, kind, })
    Ok(())
  }
}

///|
pub fn Broker::declare_queue(
  self : Broker,
  requested_name : String,
) -> Result[String, BrokerError] {
  let name = if requested_name == "" {
    let generated = "amq.gen-{self.next_queue_id}"
    self.next_queue_id = self.next_queue_id + 1
    generated
  } else {
    requested_name
  }
  if self.find_queue(name) >= 0 {
    return Ok(name)
  }
  self.queues.push({ name, messages: [], consumers: [], next_consumer: 0, })
  Ok(name)
}

///|
pub fn Broker::bind_queue(
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
  for binding in self.bindings {
    if binding.exchange == exchange &&
      binding.queue == queue &&
      binding.pattern == pattern {
      return Err(DuplicateBinding)
    }
  }
  self.bindings.push({ exchange, queue, pattern, })
  Ok(())
}

///|
fn binding_matches(
  binding : Binding,
  exchange : ExchangeState,
  routing_key : String,
) -> Bool {
  if exchange.kind == ExchangeFanout {
    true
  } else if exchange.kind == ExchangeDirect {
    binding.pattern == routing_key
  } else {
    topic_matches(binding.pattern, routing_key)
  }
}

///|
fn Broker::next_delivery_tag(self : Broker) -> UInt64 {
  let tag = self.next_tag
  self.next_tag = self.next_tag + UInt64(1)
  tag
}

///|
fn Broker::enqueue(
  self : Broker,
  queue_name : String,
  message : Message,
) -> Unit {
  let queue_index = self.find_queue(queue_name)
  if queue_index >= 0 {
    self.queues[queue_index].messages.push(message)
    self.pump_queue(queue_index)
  }
}

///|
fn Broker::available_consumer(self : Broker, queue_index : Int) -> Int {
  let queue = self.queues[queue_index]
  let count = queue.consumers.length()
  if count == 0 {
    return -1
  }
  for offset = 0; offset < count; offset = offset + 1 {
    let index = (queue.next_consumer + offset) % count
    let consumer = queue.consumers[index]
    if consumer.prefetch == 0 || consumer.unacked < consumer.prefetch {
      return index
    }
  }
  -1
}

///|
fn Broker::pump_queue(self : Broker, queue_index : Int) -> Unit {
  while self.queues[queue_index].messages.length() > 0 {
    let consumer_index = self.available_consumer(queue_index)
    if consumer_index < 0 {
      return
    }
    let message = self.queues[queue_index].messages.remove(0)
    let queue_name = self.queues[queue_index].name
    let consumer_tag = self.queues[queue_index].consumers[consumer_index].tag
    let tag = self.next_delivery_tag()
    let delivery = Delivery::{ tag, queue: queue_name, consumer_tag, message, }
    self.queues[queue_index].consumers[consumer_index].inbox.push(delivery)
    self.queues[queue_index].consumers[consumer_index].unacked += 1
    self.in_flight.push({ tag, queue: queue_name, consumer_tag, message, })
    let count = self.queues[queue_index].consumers.length()
    self.queues[queue_index].next_consumer = (consumer_index + 1) % count
  }
}

///|
/// Publish a message and return the number of queues that accepted it.
pub fn Broker::publish(
  self : Broker,
  exchange : String,
  routing_key : String,
  body : Bytes,
) -> Result[Int, BrokerError] {
  let exchange_index = self.find_exchange(exchange)
  if exchange_index < 0 {
    return Err(UnknownExchange)
  }
  let exchange_state = self.exchanges[exchange_index]
  let targets : Array[String] = []
  if exchange == "" {
    if self.find_queue(routing_key) >= 0 {
      targets.push(routing_key)
    }
  } else {
    for binding in self.bindings {
      if binding.exchange == exchange &&
        binding_matches(binding, exchange_state, routing_key) &&
        !contains_string(targets, binding.queue) {
        targets.push(binding.queue)
      }
    }
  }
  for queue in targets {
    self.enqueue(queue, Message::new(body, routing_key))
  }
  targets.length() |> Ok
}

///|
/// Pull one message from a queue and track it for acknowledgement.
pub fn Broker::get(
  self : Broker,
  queue : String,
) -> Result[Delivery?, BrokerError] {
  let queue_index = self.find_queue(queue)
  if queue_index < 0 {
    return Err(UnknownQueue)
  }
  if self.queues[queue_index].messages.length() == 0 {
    return Ok(None)
  }
  let message = self.queues[queue_index].messages.remove(0)
  let tag = self.next_delivery_tag()
  let delivery = Delivery::{ tag, queue, consumer_tag: "", message, }
  self.in_flight.push({ tag, queue, consumer_tag: "", message, })
  Ok(Some(delivery))
}

///|
pub fn Broker::consume(
  self : Broker,
  queue : String,
  tag : String,
  prefetch : Int,
) -> Result[ConsumerHandle, BrokerError] {
  if prefetch < 0 {
    return Err(InvalidPrefetch)
  }
  let queue_index = self.find_queue(queue)
  if queue_index < 0 {
    return Err(UnknownQueue)
  }
  for consumer in self.queues[queue_index].consumers {
    if consumer.tag == tag {
      return Err(DuplicateConsumer)
    }
  }
  self.queues[queue_index].consumers.push({
    tag,
    prefetch,
    unacked: 0,
    inbox: [],
  })
  self.pump_queue(queue_index)
  Ok({ queue, tag, })
}

///|
/// Poll the next already-accepted push delivery for a consumer.
pub fn Broker::poll(
  self : Broker,
  handle : ConsumerHandle,
) -> Result[Delivery?, BrokerError] {
  let queue_index = self.find_queue(handle.queue)
  if queue_index < 0 {
    return Err(UnknownQueue)
  }
  let mut consumer_index = -1
  for index = 0
      index < self.queues[queue_index].consumers.length()
      index = index + 1 {
    if self.queues[queue_index].consumers[index].tag == handle.tag {
      consumer_index = index
      break
    }
  }
  if consumer_index < 0 {
    return Err(UnknownConsumer)
  }
  if self.queues[queue_index].consumers[consumer_index].inbox.length() == 0 {
    return Ok(None)
  }
  Ok(Some(self.queues[queue_index].consumers[consumer_index].inbox.remove(0)))
}

///|
fn Broker::remove_in_flight(
  self : Broker,
  tag : UInt64,
) -> Result[InFlight, BrokerError] {
  for index = 0; index < self.in_flight.length(); index = index + 1 {
    if self.in_flight[index].tag == tag {
      return Ok(self.in_flight.remove(index))
    }
  }
  Err(UnknownDelivery)
}

///|
fn Broker::decrement_consumer(
  self : Broker,
  queue : String,
  tag : String,
) -> Unit {
  if tag == "" {
    return
  }
  let queue_index = self.find_queue(queue)
  if queue_index >= 0 {
    for index = 0
        index < self.queues[queue_index].consumers.length()
        index = index + 1 {
      if self.queues[queue_index].consumers[index].tag == tag {
        if self.queues[queue_index].consumers[index].unacked > 0 {
          self.queues[queue_index].consumers[index].unacked -= 1
        }
        return
      }
    }
  }
}

///|
/// Acknowledge one delivery.
pub fn Broker::ack(self : Broker, tag : UInt64) -> Result[Unit, BrokerError] {
  let in_flight = match self.remove_in_flight(tag) {
    Ok(value) => value
    Err(error) => return Err(error)
  }
  self.decrement_consumer(in_flight.queue, in_flight.consumer_tag)
  Ok(())
}

///|
/// Reject one delivery, optionally putting it back at the front of its queue.
pub fn Broker::reject(
  self : Broker,
  tag : UInt64,
  requeue : Bool,
) -> Result[Unit, BrokerError] {
  let in_flight = match self.remove_in_flight(tag) {
    Ok(value) => value
    Err(error) => return Err(error)
  }
  self.decrement_consumer(in_flight.queue, in_flight.consumer_tag)
  if requeue {
    let queue_index = self.find_queue(in_flight.queue)
    if queue_index >= 0 {
      let message = {
        body: in_flight.message.body(),
        routing_key: in_flight.message.routing_key(),
        redelivered: true,
      }
      let messages : Array[Message] = [message]
      for item in self.queues[queue_index].messages {
        messages.push(item)
      }
      self.queues[queue_index].messages = messages
      self.pump_queue(queue_index)
    }
  }
  Ok(())
}
