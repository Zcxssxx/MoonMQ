///|
fn Session::handle_exchange_delete(
  self : Session,
  channel : UInt16,
  exchange : String,
  if_unused : Bool,
  nowait : Bool,
) -> Result[Array[@protocol.Frame], SessionError] {
  match self.check_channel(channel) {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  match self.broker.delete_exchange(exchange, if_unused) {
    Ok(_) => ()
    Err(error) => return Err(broker_error(error))
  }
  let output : Array[@protocol.Frame] = []
  if !nowait {
    match
      push_method_frame(output, channel, @protocol.Method::ExchangeDeleteOk) {
      Ok(_) => ()
      Err(error) => return Err(error)
    }
  }
  Ok(output)
}

///|
fn Session::handle_queue_unbind(
  self : Session,
  channel : UInt16,
  queue : String,
  exchange : String,
  routing_key : String,
  arguments : @protocol.FieldTable,
) -> Result[Array[@protocol.Frame], SessionError] {
  match self.check_channel(channel) {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  if arguments.entries().length() > 0 {
    return Err(SessionError::UnsupportedOperation)
  }
  match self.broker.unbind_queue(exchange, queue, routing_key) {
    Ok(_) => ()
    Err(error) => return Err(broker_error(error))
  }
  let output : Array[@protocol.Frame] = []
  match push_method_frame(output, channel, @protocol.Method::QueueUnbindOk) {
    Ok(_) => Ok(output)
    Err(error) => Err(error)
  }
}

///|
fn Session::handle_queue_delete(
  self : Session,
  channel : UInt16,
  queue : String,
  if_unused : Bool,
  if_empty : Bool,
  nowait : Bool,
) -> Result[Array[@protocol.Frame], SessionError] {
  match self.check_channel(channel) {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  let message_count = match
    self.broker.delete_queue(queue, if_unused, if_empty) {
    Ok(value) => value
    Err(error) => return Err(broker_error(error))
  }
  let output : Array[@protocol.Frame] = []
  if !nowait {
    match
      push_method_frame(
        output,
        channel,
        @protocol.Method::QueueDeleteOk(
          message_count=message_count.reinterpret_as_uint(),
        ),
      ) {
      Ok(_) => ()
      Err(error) => return Err(error)
    }
  }
  Ok(output)
}
