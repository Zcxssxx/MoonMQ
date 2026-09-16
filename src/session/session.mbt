///|
let next_consumer_owner_id : Array[Int] = [1]

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
  let consumer_owner = "moonmq-session-\{next_consumer_owner_id[0]}"
  next_consumer_owner_id[0] = next_consumer_owner_id[0] + 1
  {
    state: SessionState::AwaitProtocolHeader,
    broker,
    consumer_owner,
    channels: [],
    consumers: [],
    pending_publishes: [],
    pending_contents: [],
    deliveries: [],
    negotiated_channel_max: UInt16(0),
    negotiated_frame_max: 131072,
  }
}

///|
pub fn Session::state(self : Session) -> SessionState {
  self.state
}

///|
/// Return the effective local frame limit after connection tuning.
pub fn Session::frame_max(self : Session) -> Int {
  self.negotiated_frame_max
}

///|
fn Session::channel_allowed(self : Session, channel : UInt16) -> Bool {
  self.negotiated_channel_max == UInt16(0) ||
  channel.to_int() <= self.negotiated_channel_max.to_int()
}

///|
fn Session::find_pending_publish(self : Session, channel : UInt16) -> Int {
  for index = 0; index < self.pending_publishes.length(); index = index + 1 {
    if self.pending_publishes[index].channel == channel {
      return index
    }
  }
  -1
}

///|
fn Session::find_pending_content(self : Session, channel : UInt16) -> Int {
  for index = 0; index < self.pending_contents.length(); index = index + 1 {
    if self.pending_contents[index].channel == channel {
      return index
    }
  }
  -1
}

///|
fn Session::find_delivery(self : Session, tag : UInt64) -> Int {
  for index = 0; index < self.deliveries.length(); index = index + 1 {
    if self.deliveries[index].tag == tag {
      return index
    }
  }
  -1
}

///|
fn Session::check_delivery(
  self : Session,
  channel : UInt16,
  tag : UInt64,
) -> Result[Int, SessionError] {
  let index = self.find_delivery(tag)
  if index < 0 || self.deliveries[index].channel != channel {
    Err(SessionError::InvalidChannel)
  } else {
    Ok(index)
  }
}

///|
fn Session::remember_delivery(
  self : Session,
  channel : UInt16,
  delivery : @broker.Delivery,
) -> Unit {
  self.deliveries.push({
    channel,
    tag: delivery.tag(),
    consumer_tag: delivery.consumer_tag(),
  })
}

///|
fn Session::forget_delivery(self : Session, tag : UInt64) -> Unit {
  let index = self.find_delivery(tag)
  if index >= 0 {
    ignore(self.deliveries.remove(index))
  }
}

///|
fn Session::forget_consumer_deliveries(
  self : Session,
  channel : UInt16,
  consumer_tag : String,
) -> Unit {
  let mut index = 0
  while index < self.deliveries.length() {
    let delivery = self.deliveries[index]
    if delivery.channel == channel && delivery.consumer_tag == consumer_tag {
      ignore(self.deliveries.remove(index))
    } else {
      index = index + 1
    }
  }
}

///|
fn Session::requeue_get_deliveries(
  self : Session,
  channel : UInt16?,
) -> Result[Unit, SessionError] {
  let mut index = 0
  while index < self.deliveries.length() {
    let delivery = self.deliveries[index]
    let selected_channel = match channel {
      Some(value) => delivery.channel == value
      None => true
    }
    let selected = delivery.consumer_tag == "" && selected_channel
    if selected {
      match
        self.broker.reject_with_owner(self.consumer_owner, delivery.tag, true) {
        Ok(_) => ignore(self.deliveries.remove(index))
        Err(error) => return Err(broker_error(error))
      }
    } else {
      index = index + 1
    }
  }
  Ok(())
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
        @protocol.Method::ConnectionStartOk(mechanism~, response~, ..) => {
          if mechanism != "PLAIN" || response != b"\x00guest\x00guest" {
            return Err(SessionError::AuthenticationFailed)
          }
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
        @protocol.Method::ConnectionTuneOk(channel_max~, frame_max~, heartbeat~) => {
          ignore(heartbeat)
          if channel_max != UInt16(0) &&
            self.negotiated_channel_max != UInt16(0) &&
            channel_max > self.negotiated_channel_max {
            return Err(SessionError::InvalidChannel)
          }
          let selected_frame_max = frame_max.reinterpret_as_int()
          if selected_frame_max != 0 &&
            (selected_frame_max < 8 || selected_frame_max > 131072) {
            return Err(SessionError::InvalidFrameSize)
          }
          self.negotiated_channel_max = channel_max
          self.negotiated_frame_max = if selected_frame_max == 0 {
            131072
          } else {
            selected_frame_max
          }
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
        @protocol.Method::ConnectionOpen(virtual_host~, ..) => {
          if virtual_host != "/" {
            return Err(SessionError::InvalidVirtualHost)
          }
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
fn Session::append_delivery_frames(
  self : Session,
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
  self.append_content_frames(
    output,
    channel,
    delivery.properties(),
    delivery.body(),
  )
}

///|
fn Session::append_content_frames(
  self : Session,
  output : Array[@protocol.Frame],
  channel : UInt16,
  properties : @protocol.BasicProperties,
  body : Bytes,
) -> Result[Unit, SessionError] {
  let header = @protocol.ContentHeader::new(
    class_id=UInt16(60),
    weight=UInt16(0),
    body_size=body.length().to_uint64(),
    properties~,
  )
  let header_payload = match @protocol.encode_content_header(header) {
    Ok(value) => value
    Err(error) => return Err(protocol_error(error))
  }
  output.push(
    @protocol.Frame::new(@protocol.FrameHeader, channel, header_payload),
  )
  let chunk_size = self.negotiated_frame_max - 8
  if body.length() > 0 && chunk_size <= 0 {
    return Err(SessionError::InvalidFrameSize)
  }
  let mut offset = 0
  while offset < body.length() {
    let end = if offset + chunk_size < body.length() {
      offset + chunk_size
    } else {
      body.length()
    }
    output.push(
      @protocol.Frame::new(
        @protocol.FrameBody,
        channel,
        body[offset:end].to_owned(),
      ),
    )
    offset = end
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
    let mut polling = true
    while polling {
      match self.broker.poll(consumer.handle) {
        Ok(Some(delivery)) => {
          self.remember_delivery(consumer.channel, delivery)
          match
            self.append_delivery_frames(output, consumer.channel, delivery) {
            Ok(_) => ()
            Err(error) => return Err(error)
          }
          if consumer.no_ack {
            match
              self.broker.ack_with_owner(self.consumer_owner, delivery.tag()) {
              Ok(_) => ()
              Err(error) => return Err(broker_error(error))
            }
            self.forget_delivery(delivery.tag())
          }
        }
        Ok(None) => polling = false
        Err(error) => return Err(broker_error(error))
      }
    }
    index = index + 1
  }
  Ok(())
}

///|
fn Session::finish_pending_content(
  self : Session,
  channel : UInt16,
) -> Result[Array[@protocol.Frame], SessionError] {
  let index = self.find_pending_content(channel)
  if index < 0 {
    return Err(SessionError::MissingContent)
  }
  let pending = self.pending_contents[index]
  let message = match pending.assembler.finish() {
    Ok(value) => value
    Err(@protocol.ContentSizeMismatch) => return Ok([])
    Err(error) => return Err(protocol_error(error))
  }
  ignore(self.pending_contents.remove(index))
  let routed = match
    self.broker.publish_with_properties(
      pending.exchange,
      pending.routing_key,
      message.body(),
      message.header().properties(),
    ) {
    Ok(value) => value
    Err(error) => return Err(broker_error(error))
  }
  let output : Array[@protocol.Frame] = []
  if routed == 0 && pending.mandatory {
    match
      push_method_frame(
        output,
        channel,
        @protocol.Method::BasicReturn(
          reply_code=UInt16(312),
          reply_text="NO_ROUTE",
          exchange=pending.exchange,
          routing_key=pending.routing_key,
        ),
      ) {
      Ok(_) => ()
      Err(error) => return Err(error)
    }
    match
      self.append_content_frames(
        output,
        channel,
        message.header().properties(),
        message.body(),
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
      let publish_index = self.find_pending_publish(frame.channel())
      if publish_index < 0 {
        if self.pending_publishes.length() > 0 {
          return Err(SessionError::InvalidChannel)
        }
        return Err(SessionError::MissingContent)
      }
      if self.find_pending_content(frame.channel()) >= 0 {
        return Err(SessionError::PendingContent)
      }
      let publish = self.pending_publishes[publish_index]
      let header = match @protocol.decode_content_header(frame.payload()) {
        Ok(value) => value
        Err(error) => return Err(protocol_error(error))
      }
      ignore(self.pending_publishes.remove(publish_index))
      self.pending_contents.push({
        channel: frame.channel(),
        exchange: publish.exchange,
        routing_key: publish.routing_key,
        mandatory: publish.mandatory,
        assembler: @protocol.ContentAssembler::new(header, 0),
      })
      if header.body_size() == UInt64(0) {
        self.finish_pending_content(frame.channel())
      } else {
        Ok([])
      }
    }
    @protocol.FrameBody => {
      let content_index = self.find_pending_content(frame.channel())
      if content_index < 0 {
        if self.pending_contents.length() > 0 {
          return Err(SessionError::InvalidChannel)
        }
        return Err(SessionError::MissingContent)
      }
      let pending = self.pending_contents[content_index]
      match pending.assembler.push_body(frame.payload()) {
        Ok(_) => self.finish_pending_content(frame.channel())
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
      self.forget_consumer_deliveries(channel, self.consumers[index].tag)
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
    let consumer_tag = self.consumers[0].tag
    self.forget_consumer_deliveries(self.consumers[0].channel, consumer_tag)
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
      if out_of_band != "" {
        return Err(SessionError::UnsupportedOperation)
      }
      if channel == UInt16(0) ||
        !self.channel_allowed(channel) ||
        self.find_channel(channel) >= 0 {
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
      match self.requeue_get_deliveries(Some(channel)) {
        Ok(_) => ()
        Err(error) => return Err(error)
      }
      let mut pending_index = 0
      while pending_index < self.pending_publishes.length() {
        if self.pending_publishes[pending_index].channel == channel {
          ignore(self.pending_publishes.remove(pending_index))
        } else {
          pending_index = pending_index + 1
        }
      }
      pending_index = 0
      while pending_index < self.pending_contents.length() {
        if self.pending_contents[pending_index].channel == channel {
          ignore(self.pending_contents.remove(pending_index))
        } else {
          pending_index = pending_index + 1
        }
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
      match self.requeue_get_deliveries(None) {
        Ok(_) => ()
        Err(error) => return Err(error)
      }
      self.pending_publishes = []
      self.pending_contents = []
      self.deliveries = []
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
    @protocol.Method::BasicPublish(
      exchange~,
      routing_key~,
      mandatory~,
      immediate~,
      ..
    ) => {
      match self.check_channel(channel) {
        Ok(_) => ()
        Err(error) => return Err(error)
      }
      if immediate {
        return Err(SessionError::UnsupportedOperation)
      }
      if self.find_pending_publish(channel) >= 0 ||
        self.find_pending_content(channel) >= 0 {
        return Err(SessionError::PendingContent)
      }
      self.pending_publishes.push({ channel, exchange, routing_key, mandatory, })
      Ok([])
    }
    @protocol.Method::ExchangeDeclare(
      exchange~,
      kind~,
      passive~,
      durable~,
      auto_delete~,
      internal~,
      nowait~,
      arguments~,
      ..
    ) => {
      match self.check_channel(channel) {
        Ok(_) => ()
        Err(error) => return Err(error)
      }
      if passive ||
        durable ||
        auto_delete ||
        internal ||
        arguments.entries().length() > 0 {
        return Err(SessionError::UnsupportedOperation)
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
    @protocol.Method::QueueDeclare(
      queue~,
      passive~,
      durable~,
      exclusive~,
      auto_delete~,
      nowait~,
      arguments~,
      ..
    ) => {
      match self.check_channel(channel) {
        Ok(_) => ()
        Err(error) => return Err(error)
      }
      if passive ||
        durable ||
        exclusive ||
        auto_delete ||
        arguments.entries().length() > 0 {
        return Err(SessionError::UnsupportedOperation)
      }
      let declared_name = match self.broker.declare_queue(queue) {
        Ok(value) => value
        Err(error) => return Err(broker_error(error))
      }
      let stats = match self.broker.queue_stats(declared_name) {
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
              message_count=stats.message_count().reinterpret_as_uint(),
              consumer_count=stats.consumer_count().reinterpret_as_uint(),
            ),
          ) {
          Ok(_) => ()
          Err(error) => return Err(error)
        }
      }
      Ok(output)
    }
    @protocol.Method::QueueBind(
      ticket~,
      queue~,
      exchange~,
      routing_key~,
      nowait~,
      arguments~
    ) => {
      ignore(ticket)
      match self.check_channel(channel) {
        Ok(_) => ()
        Err(error) => return Err(error)
      }
      if arguments.entries().length() > 0 {
        return Err(SessionError::UnsupportedOperation)
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
    @protocol.Method::BasicQos(prefetch_size~, prefetch_count~, global~) => {
      if prefetch_size != UInt(0) || global {
        return Err(SessionError::UnsupportedOperation)
      }
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
    @protocol.Method::BasicConsume(
      queue~,
      consumer_tag~,
      no_local~,
      no_ack~,
      exclusive~,
      nowait~,
      arguments~,
      ..
    ) => {
      if no_local || exclusive || arguments.entries().length() > 0 {
        return Err(SessionError::UnsupportedOperation)
      }
      let prefetch = match self.channel_prefetch(channel) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      let handle = match
        self.broker.consume_with_owner(
          queue,
          consumer_tag,
          self.consumer_owner,
          prefetch,
        ) {
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
      let delivery = match
        self.broker.get_with_owner(queue, self.consumer_owner) {
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
      let stats = match self.broker.queue_stats(queue) {
        Ok(value) => value
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
            message_count=stats.message_count().reinterpret_as_uint(),
          ),
        ) {
        Ok(_) => ()
        Err(error) => return Err(error)
      }
      match
        self.append_content_frames(
          output,
          channel,
          delivery.properties(),
          delivery.body(),
        ) {
        Ok(_) => ()
        Err(error) => return Err(error)
      }
      if no_ack {
        match self.broker.ack_with_owner(self.consumer_owner, delivery.tag()) {
          Ok(_) => ()
          Err(error) => return Err(broker_error(error))
        }
      } else {
        self.remember_delivery(channel, delivery)
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
      let delivery_index = match self.check_delivery(channel, delivery_tag) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      match self.broker.ack_with_owner(self.consumer_owner, delivery_tag) {
        Ok(_) => {
          ignore(self.deliveries.remove(delivery_index))
          let output : Array[@protocol.Frame] = []
          match self.drain_consumers(output) {
            Ok(_) => Ok(output)
            Err(error) => Err(error)
          }
        }
        Err(error) => Err(broker_error(error))
      }
    }
    @protocol.Method::BasicReject(delivery_tag~, requeue~) => {
      match self.check_channel(channel) {
        Ok(_) => ()
        Err(error) => return Err(error)
      }
      let delivery_index = match self.check_delivery(channel, delivery_tag) {
        Ok(value) => value
        Err(error) => return Err(error)
      }
      match
        self.broker.reject_with_owner(
          self.consumer_owner,
          delivery_tag,
          requeue,
        ) {
        Ok(_) => {
          ignore(self.deliveries.remove(delivery_index))
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
    | @protocol.Method::BasicReturn(..)
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
fn Session::abort(self : Session) -> Unit {
  ignore(self.remove_all_consumers())
  ignore(self.requeue_get_deliveries(None))
  self.pending_publishes = []
  self.pending_contents = []
  self.deliveries = []
  self.state = SessionState::Closed
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
      match
        @protocol.validate_frame_with_max(frame, self.negotiated_frame_max) {
        Ok(_) => {
          let result = match frame.kind() {
            @protocol.FrameMethod => self.receive_method_frame(frame)
            @protocol.FrameHeader | @protocol.FrameBody =>
              self.receive_content_frame(frame)
            @protocol.FrameHeartbeat => Ok([])
          }
          match result {
            Err(SessionError::ProtocolError(error)) => {
              self.abort()
              Err(SessionError::ProtocolError(error))
            }
            _ => result
          }
        }
        Err(error) => {
          self.state = SessionState::Closed
          Err(protocol_error(error))
        }
      }
  }
}
