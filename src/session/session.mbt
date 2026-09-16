///|
fn protocol_error(error : @protocol.AmqpError) -> SessionError {
  SessionError::ProtocolError(error)
}

///|
fn broker_error(error : @broker.BrokerError) -> SessionError {
  SessionError::BrokerError(error)
}

///|
fn push_method_frame(
  output : Array[@protocol.Frame],
  channel : UInt16,
  value : @protocol.Method,
) -> Result[Unit, SessionError] {
  match @protocol.encode_method(value) {
    Ok(payload) => {
      output.push(@protocol.Frame::new(@protocol.FrameMethod, channel, payload))
      Ok(())
    }
    Err(error) => Err(protocol_error(error))
  }
}

///|
fn exchange_kind(value : String) -> Result[@broker.ExchangeKind, SessionError] {
  match value {
    "direct" => Ok(@broker.ExchangeKind::ExchangeDirect)
    "fanout" => Ok(@broker.ExchangeKind::ExchangeFanout)
    "topic" => Ok(@broker.ExchangeKind::ExchangeTopic)
    _ => Err(SessionError::UnsupportedOperation)
  }
}

///|
pub fn Session::new(broker : @broker.Broker) -> Session {
  {
    state: SessionState::AwaitProtocolHeader,
    broker,
    channels: [],
    consumers: [],
    pending_publish: None,
    pending_content: None,
  }
}

///|
pub fn Session::state(self : Session) -> SessionState {
  self.state
}

///|
/// Return the server's initial `connection.start` frame.
pub fn Session::server_start_frame(
  self : Session,
) -> Result[@protocol.Frame, SessionError] {
  if self.state != SessionState::AwaitStartOk {
    return Err(SessionError::InvalidState)
  }
  let value = @protocol.Method::ConnectionStart(
    version_major=0,
    version_minor=9,
    server_properties=@protocol.FieldTable::new([]),
    mechanisms=b"PLAIN",
    locales=b"en_US",
  )
  let output : Array[@protocol.Frame] = []
  match push_method_frame(output, UInt16(0), value) {
    Ok(_) => Ok(output[0])
    Err(error) => Err(error)
  }
}

///|
pub fn Session::receive_protocol_header(
  self : Session,
  input : Bytes,
) -> Result[Bytes, SessionError] {
  if self.state != SessionState::AwaitProtocolHeader {
    return Err(SessionError::InvalidState)
  }
  match @protocol.decode_protocol_header(input) {
    Ok(_) => {
      self.state = SessionState::AwaitStartOk
      Ok(@protocol.encode_protocol_header())
    }
    Err(error) => Err(protocol_error(error))
  }
}

///|
fn Session::find_channel(self : Session, id : UInt16) -> Int {
  for index = 0; index < self.channels.length(); index = index + 1 {
    if self.channels[index].id == id {
      return index
    }
  }
  -1
}

///|
fn Session::require_channel(
  self : Session,
  id : UInt16,
) -> Result[Int, SessionError] {
  if id == UInt16(0) {
    return Err(SessionError::InvalidChannel)
  }
  let index = self.find_channel(id)
  if index < 0 || !self.channels[index].open {
    Err(SessionError::InvalidChannel)
  } else {
    Ok(index)
  }
}

///|
fn Session::channel_prefetch(
  self : Session,
  id : UInt16,
) -> Result[Int, SessionError] {
  match self.require_channel(id) {
    Ok(index) => Ok(self.channels[index].prefetch.to_int())
    Err(error) => Err(error)
  }
}

///|
fn Session::check_channel(
  self : Session,
  id : UInt16,
) -> Result[Unit, SessionError] {
  match self.require_channel(id) {
    Ok(_) => Ok(())
    Err(error) => Err(error)
  }
}

///|
fn Session::receive_method_frame(
  self : Session,
  frame : @protocol.Frame,
) -> Result[Array[@protocol.Frame], SessionError] {
  let value = match @protocol.decode_method(frame.payload()) {
    Ok(value) => value
    Err(error) => return Err(protocol_error(error))
  }
  match self.state {
    SessionState::AwaitStartOk => {
      if frame.channel() != UInt16(0) {
        return Err(SessionError::InvalidChannel)
      }
      match value {
        @protocol.Method::ConnectionStartOk(..) => {
          self.state = SessionState::AwaitTuneOk
          let output : Array[@protocol.Frame] = []
          match
            push_method_frame(
              output,
              UInt16(0),
              @protocol.Method::ConnectionTune(
                channel_max=UInt16(0),
                frame_max=131072U,
                heartbeat=UInt16(0),
              ),
            ) {
            Ok(_) => Ok(output)
            Err(error) => Err(error)
          }
        }
        _ => Err(SessionError::InvalidState)
      }
    }
    SessionState::AwaitTuneOk => {
      if frame.channel() != UInt16(0) {
        return Err(SessionError::InvalidChannel)
      }
      match value {
        @protocol.Method::ConnectionTuneOk(..) => {
          self.state = SessionState::AwaitOpen
          Ok([])
        }
        _ => Err(SessionError::InvalidState)
      }
    }
    SessionState::AwaitOpen => {
      if frame.channel() != UInt16(0) {
        return Err(SessionError::InvalidChannel)
      }
      match value {
        @protocol.Method::ConnectionOpen(..) => {
          self.state = SessionState::Open
          let output : Array[@protocol.Frame] = []
          match
            push_method_frame(
              output,
              UInt16(0),
              @protocol.Method::ConnectionOpenOk,
            ) {
            Ok(_) => Ok(output)
            Err(error) => Err(error)
          }
        }
        _ => Err(SessionError::InvalidState)
      }
    }
    SessionState::Open => self.dispatch_open(frame.channel(), value)
    SessionState::AwaitProtocolHeader | SessionState::Closed =>
      Err(SessionError::InvalidState)
  }
}

///|
fn append_delivery_frames(
  output : Array[@protocol.Frame],
  channel : UInt16,
  delivery : @broker.Delivery,
) -> Result[Unit, SessionError] {
  match
    push_method_frame(
      output,
      channel,
      @protocol.Method::BasicDeliver(
        consumer_tag=delivery.consumer_tag(),
        delivery_tag=delivery.tag(),
        redelivered=delivery.redelivered(),
        exchange=delivery.exchange(),
        routing_key=delivery.routing_key(),
      ),
    ) {
    Ok(_) => ()
    Err(error) => return Err(error)
  }
  append_content_frames(output, channel, delivery.body())
}

///|
fn append_content_frames(
  output : Array[@protocol.Frame],
  channel : UInt16,
  body : Bytes,
) -> Result[Unit, SessionError] {
  let header = @protocol.ContentHeader::new(
    class_id=UInt16(60),
    weight=UInt16(0),
    body_size=body.length().to_uint64(),
    properties=@protocol.BasicProperties::new(),
  )
  let header_payload = match @protocol.encode_content_header(header) {
    Ok(value) => value
    Err(error) => return Err(protocol_error(error))
  }
  output.push(
    @protocol.Frame::new(@protocol.FrameHeader, channel, header_payload),
  )
  if body.length() > 0 {
    output.push(@protocol.Frame::new(@protocol.FrameBody, channel, body))
  }
  Ok(())
}

///|
fn Session::drain_consumers(
  self : Session,
  output : Array[@protocol.Frame],
) -> Result[Unit, SessionError] {
  let mut index = 0
  while index < self.consumers.length() {
    let consumer = self.consumers[index]
    match self.broker.poll(consumer.handle) {
      Ok(Some(delivery)) => {
        match append_delivery_frames(output, consumer.channel, delivery) {
          Ok(_) => ()
          Err(error) => return Err(error)
        }
        if consumer.no_ack {
          match self.broker.ack(delivery.tag()) {
            Ok(_) => ()
            Err(error) => return Err(broker_error(error))
          }
        }
      }
      Ok(None) => index = index + 1
      Err(error) => return Err(broker_error(error))
    }
  }
  Ok(())
}

///|
fn Session::finish_pending_content(
  self : Session,
) -> Result[Array[@protocol.Frame], SessionError] {
  let pending = match self.pending_content {
    Some(value) => value
    None => return Err(SessionError::MissingContent)
  }
  let message = match pending.assembler.finish() {
    Ok(value) => value
    Err(@protocol.ContentSizeMismatch) => return Ok([])
    Err(error) => return Err(protocol_error(error))
  }
  self.pending_content = None
  self.pending_publish = None
  match
    self.broker.publish(pending.exchange, pending.routing_key, message.body()) {
    Ok(_) => ()
    Err(error) => return Err(broker_error(error))
  }
  let output : Array[@protocol.Frame] = []
  match self.drain_consumers(output) {
    Ok(_) => Ok(output)
    Err(error) => Err(error)
  }
}

///|
fn Session::receive_content_frame(
  self : Session,
  frame : @protocol.Frame,
) -> Result[Array[@protocol.Frame], SessionError] {
  if self.state != SessionState::Open {
    return Err(SessionError::InvalidState)
  }
  match frame.kind() {
    @protocol.FrameHeader => {
      let publish = match self.pending_publish {
        Some(value) => value
        None => return Err(SessionError::MissingContent)
      }
      if publish.channel != frame.channel() {
        return Err(SessionError::InvalidChannel)
      }
      if self.pending_content is Some(_) {
        return Err(SessionError::PendingContent)
      }
      let header = match @protocol.decode_content_header(frame.payload()) {
        Ok(value) => value
        Err(error) => return Err(protocol_error(error))
      }
      self.pending_content = Some({
        channel: frame.channel(),
        exchange: publish.exchange,
        routing_key: publish.routing_key,
        assembler: @protocol.ContentAssembler::new(header, 0),
      })
      if header.body_size() == UInt64(0) {
        self.finish_pending_content()
      } else {
        Ok([])
      }
    }
    @protocol.FrameBody => {
      let pending = match self.pending_content {
        Some(value) => value
        None => return Err(SessionError::MissingContent)
      }
      if pending.channel != frame.channel() {
        return Err(SessionError::InvalidChannel)
      }
      match pending.assembler.push_body(frame.payload()) {
        Ok(_) => self.finish_pending_content()
        Err(error) => Err(protocol_error(error))
      }
    }
    _ => Err(SessionError::InvalidFrameKind)
  }
}

///|
fn Session::remove_channel_consumers(
  self : Session,
  channel : UInt16,
) -> Result[Unit, SessionError] {
  let mut index = 0
  while index < self.consumers.length() {
    if self.consumers[index].channel == channel {
      let handle = self.consumers[index].handle
      match self.broker.cancel_consumer(handle) {
        Ok(_) => ()
        Err(error) => return Err(broker_error(error))
      }
      ignore(self.consumers.remove(index))
    } else {
      index = index + 1
    }
  }
  Ok(())
}

///|
fn Session::remove_all_consumers(self : Session) -> Result[Unit, SessionError] {
  while self.consumers.length() > 0 {
    let handle = self.consumers[0].handle
    match self.broker.cancel_consumer(handle) {
      Ok(_) => ()
      Err(error) => return Err(broker_error(error))
    }
    ignore(self.consumers.remove(0))
  }
  Ok(())
}

///|
fn Session::dispatch_open(
  self : Session,
  channel : UInt16,
  value : @protocol.Method,
) -> Result[Array[@protocol.Frame], SessionError] {
  match value {
    @protocol.Method::ChannelOpen(out_of_band~) => {
      ignore(out_of_band)
      if channel == UInt16(0) || self.find_channel(channel) >= 0 {
        return Err(SessionError::InvalidChannel)
      }
      self.channels.push({ id: channel, prefetch: UInt16(0), open: true, })
      let output : Array[@protocol.Frame] = []
      match
        push_method_frame(output, channel, @protocol.Method::ChannelOpenOk) {
        Ok(_) => Ok(output)
        Err(error) => Err(error)
      }
    }
    @protocol.Method::ChannelClose(..) => {
      match self.require_channel(channel) {
        Ok(index) => self.channels[index].open = false
        Err(error) => return Err(error)
      }
      match self.remove_channel_consumers(channel) {
        Ok(_) => ()
        Err(error) => return Err(error)
      }
      let output : Array[@protocol.Frame] = []
      match
        push_method_frame(output, channel, @protocol.Method::ChannelCloseOk) {
        Ok(_) =>
          match self.drain_consumers(output) {
            Ok(_) => Ok(output)
            Err(error) => Err(error)
          }
        Err(error) => Err(error)
      }
    }
    @protocol.Method::ConnectionClose(..) => {
      if channel != UInt16(0) {
        return Err(SessionError::InvalidChannel)
      }
      match self.remove_all_consumers() {
        Ok(_) => ()
        Err(error) => return Err(error)
      }
      self.state = SessionState::Closed
      let output : Array[@protocol.Frame] = []
      match
        push_method_frame(
          output,
          UInt16(0),
          @protocol.Method::ConnectionCloseOk,
        ) {
        Ok(_) => Ok(output)
        Err(error) => Err(error)
      }
    }
    @protocol.Method::BasicPublish(exchange~, routing_key~, ..) => {
      match self.check_channel(channel) {
        Ok(_) => ()
        Err(error) => return Err(error)
      }
      if self.pending_publish is Some(_) || self.pending_content is Some(_) {
        return Err(SessionError::PendingContent)
      }
      self.pending_publish = Some({ channel, exchange, routing_key, })
      Ok([])
    }
    @protocol.Method::ExchangeDeclare(exchange~, kind~, nowait~, ..) => {
      match self.check_channel(channel) {
        Ok(_) => ()
        Err(error) => return Err(error)
      }
      let exchange_kind = match exchange_kind(kind) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      match self.broker.declare_exchange(exchange, exchange_kind) {
        Ok(_) => ()
        Err(error) => return Err(broker_error(error))
      }
      let output : Array[@protocol.Frame] = []
      if !nowait {
        match
          push_method_frame(
            output,
            channel,
            @protocol.Method::ExchangeDeclareOk,
          ) {
          Ok(_) => ()
          Err(error) => return Err(error)
        }
      }
      Ok(output)
    }
    @protocol.Method::QueueDeclare(queue~, nowait~, ..) => {
      match self.check_channel(channel) {
        Ok(_) => ()
        Err(error) => return Err(error)
      }
      let declared_name = match self.broker.declare_queue(queue) {
        Ok(value) => value
        Err(error) => return Err(broker_error(error))
      }
      let output : Array[@protocol.Frame] = []
      if !nowait {
        match
          push_method_frame(
            output,
            channel,
            @protocol.Method::QueueDeclareOk(
              queue=declared_name,
              message_count=0U,
              consumer_count=0U,
            ),
          ) {
          Ok(_) => ()
          Err(error) => return Err(error)
        }
      }
      Ok(output)
    }
    @protocol.Method::QueueBind(queue~, exchange~, routing_key~, nowait~, ..) => {
      match self.check_channel(channel) {
        Ok(_) => ()
        Err(error) => return Err(error)
      }
      match self.broker.bind_queue(exchange, queue, routing_key) {
        Ok(_) => ()
        Err(error) => return Err(broker_error(error))
      }
      let output : Array[@protocol.Frame] = []
      if !nowait {
        match
          push_method_frame(output, channel, @protocol.Method::QueueBindOk) {
          Ok(_) => ()
          Err(error) => return Err(error)
        }
      }
      Ok(output)
    }
    @protocol.Method::BasicQos(prefetch_count~, global~, ..) => {
      ignore(global)
      let index = match self.require_channel(channel) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      self.channels[index].prefetch = prefetch_count
      let output : Array[@protocol.Frame] = []
      match push_method_frame(output, channel, @protocol.Method::BasicQosOk) {
        Ok(_) => Ok(output)
        Err(error) => Err(error)
      }
    }
    @protocol.Method::BasicConsume(queue~, consumer_tag~, no_ack~, nowait~, ..) => {
      let prefetch = match self.channel_prefetch(channel) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let handle = match self.broker.consume(queue, consumer_tag, prefetch) {
        Ok(value) => value
        Err(error) => return Err(broker_error(error))
      }
      self.consumers.push({ channel, tag: consumer_tag, no_ack, handle, })
      let output : Array[@protocol.Frame] = []
      if !nowait {
        match
          push_method_frame(
            output,
            channel,
            @protocol.Method::BasicConsumeOk(consumer_tag~),
          ) {
          Ok(_) => ()
          Err(error) => return Err(error)
        }
      }
      match self.drain_consumers(output) {
        Ok(_) => Ok(output)
        Err(error) => Err(error)
      }
    }
    @protocol.Method::BasicCancel(consumer_tag~, nowait~) => {
      let mut found = -1
      for index = 0; index < self.consumers.length(); index = index + 1 {
        if self.consumers[index].channel == channel &&
          self.consumers[index].tag == consumer_tag {
          found = index
          break
        }
      }
      if found < 0 {
        return Err(SessionError::InvalidChannel)
      }
      let handle = self.consumers[found].handle
      match self.broker.cancel_consumer(handle) {
        Ok(_) => ()
        Err(error) => return Err(broker_error(error))
      }
      ignore(self.consumers.remove(found))
      let output : Array[@protocol.Frame] = []
      if !nowait {
        match
          push_method_frame(
            output,
            channel,
            @protocol.Method::BasicCancelOk(consumer_tag~),
          ) {
          Ok(_) => ()
          Err(error) => return Err(error)
        }
      }
      match self.drain_consumers(output) {
        Ok(_) => Ok(output)
        Err(error) => Err(error)
      }
    }
    @protocol.Method::BasicGet(queue~, no_ack~, ..) => {
      match self.check_channel(channel) {
        Ok(_) => ()
        Err(error) => return Err(error)
      }
      let output : Array[@protocol.Frame] = []
      let delivery = match self.broker.get(queue) {
        Ok(Some(value)) => value
        Ok(None) =>
          match
            push_method_frame(
              output,
              channel,
              @protocol.Method::BasicGetEmpty(cluster_id=""),
            ) {
            Ok(_) => return Ok(output)
            Err(error) => return Err(error)
          }
        Err(error) => return Err(broker_error(error))
      }
      match
        push_method_frame(
          output,
          channel,
          @protocol.Method::BasicGetOk(
            delivery_tag=delivery.tag(),
            redelivered=delivery.redelivered(),
            exchange=delivery.exchange(),
            routing_key=delivery.routing_key(),
            message_count=0U,
          ),
        ) {
        Ok(_) => ()
        Err(error) => return Err(error)
      }
      match append_content_frames(output, channel, delivery.body()) {
        Ok(_) => ()
        Err(error) => return Err(error)
      }
      if no_ack {
        match self.broker.ack(delivery.tag()) {
          Ok(_) => ()
          Err(error) => return Err(broker_error(error))
        }
      }
      Ok(output)
    }
    @protocol.Method::BasicAck(delivery_tag~, multiple~) => {
      match self.check_channel(channel) {
        Ok(_) => ()
        Err(error) => return Err(error)
      }
      if multiple {
        return Err(SessionError::UnsupportedOperation)
      }
      match self.broker.ack(delivery_tag) {
        Ok(_) => Ok([])
        Err(error) => Err(broker_error(error))
      }
    }
    @protocol.Method::BasicReject(delivery_tag~, requeue~) => {
      match self.check_channel(channel) {
        Ok(_) => ()
        Err(error) => return Err(error)
      }
      match self.broker.reject(delivery_tag, requeue) {
        Ok(_) => {
          let output : Array[@protocol.Frame] = []
          match self.drain_consumers(output) {
            Ok(_) => Ok(output)
            Err(error) => Err(error)
          }
        }
        Err(error) => Err(broker_error(error))
      }
    }
    @protocol.Method::ExchangeDelete(..)
    | @protocol.Method::QueueDelete(..)
    | @protocol.Method::QueueUnbind(..)
    | @protocol.Method::BasicGetEmpty(..)
    | @protocol.Method::BasicDeliver(..)
    | @protocol.Method::BasicConsumeOk(..)
    | @protocol.Method::BasicCancelOk(..)
    | @protocol.Method::BasicQosOk
    | @protocol.Method::ExchangeDeclareOk
    | @protocol.Method::QueueDeclareOk(..)
    | @protocol.Method::QueueBindOk
    | @protocol.Method::QueueUnbindOk
    | @protocol.Method::ExchangeDeleteOk
    | @protocol.Method::QueueDeleteOk(..)
    | @protocol.Method::BasicGetOk(..)
    | @protocol.Method::ConnectionStart(..)
    | @protocol.Method::ConnectionStartOk(..)
    | @protocol.Method::ConnectionTune(..)
    | @protocol.Method::ConnectionTuneOk(..)
    | @protocol.Method::ConnectionOpen(..)
    | @protocol.Method::ConnectionOpenOk
    | @protocol.Method::ConnectionCloseOk
    | @protocol.Method::ChannelOpenOk
    | @protocol.Method::ChannelCloseOk =>
      Err(SessionError::UnsupportedOperation)
  }
}

///|
pub fn Session::receive_frame(
  self : Session,
  frame : @protocol.Frame,
) -> Result[Array[@protocol.Frame], SessionError] {
  match self.state {
    SessionState::AwaitProtocolHeader | SessionState::Closed =>
      Err(SessionError::InvalidState)
    _ =>
      match frame.kind() {
        @protocol.FrameMethod => self.receive_method_frame(frame)
        @protocol.FrameHeader | @protocol.FrameBody =>
          self.receive_content_frame(frame)
        @protocol.FrameHeartbeat => Ok([])
      }
  }
}
