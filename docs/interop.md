# Interoperability boundary

MoonMQ ships a portable AMQP 0-9-1 wire codec, a portable session boundary, an embedded in-memory Broker, and a native TCP listener. The embedded demonstration remains entirely local and does not require RabbitMQ, Docker, or another service.

## What is testable today

- Encode and decode the AMQP 0-9-1 protocol header.
- Encode and decode bounded method, header, body, and heartbeat frame envelopes.
- Encode and decode the typed `basic.publish`, `basic.ack`, and `basic.reject` methods.
- Route bytes through the embedded Broker using default, direct, fanout, and topic exchanges.
- Exercise pull and push delivery, prefetch, acknowledgements, reject/requeue, and consumer cancellation.
- Drive a fragmented byte stream through the native connection adapter.
- Run a real native TCP handshake against the listener using `moonbitlang/async/socket`.

Run the local paths from the repository root:

```text
moon test --target all
moon run cmd/moonmq
moon run examples/embedded
moon test --target native src/transport/native
moon run --target native cmd/moonmq-server
```

## Compatibility policy

Interoperability claims are limited to the methods, content properties, session behavior, and frame handling covered by the tests and [`protocol-profile.md`](protocol-profile.md). The native server accepts only the documented local profile (`PLAIN` with `guest`/`guest`, virtual host `/`) and uses a pure in-memory Broker; it is not a production authentication, persistence, or multi-tenant service. A full RabbitMQ client/server compatibility claim is not made.

The native adapter closes a connection when the portable session or frame decoder returns an error. The current Core Profile does not yet expose exchange/queue deletion, queue unbind, multiple acknowledgements, persistence, TLS, or external authentication.
