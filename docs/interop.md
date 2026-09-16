# Interoperability boundary

MoonMQ currently ships a portable AMQP 0-9-1 wire-codec slice and an embedded in-memory Broker. It does not yet expose a TCP listener, so the default demonstration is entirely local and does not require RabbitMQ, Docker, or another service.

## What is testable today

- Encode and decode the AMQP 0-9-1 protocol header.
- Encode and decode bounded method, header, body, and heartbeat frame envelopes.
- Encode and decode the typed `basic.publish`, `basic.ack`, and `basic.reject` methods.
- Route bytes through the embedded Broker using default, direct, fanout, and topic exchanges.
- Exercise pull and push delivery, prefetch, acknowledgements, reject/requeue, and consumer cancellation.

Run the local paths from the repository root:

```text
moon test --target all
moon run cmd/moonmq
moon run examples/embedded
```

## Compatibility policy

Interoperability claims are limited to the methods and frame behavior covered by the tests and [`protocol-profile.md`](protocol-profile.md). The project does not claim compatibility with a complete RabbitMQ server or client library yet. A future native adapter will add a loopback smoke test and a separately documented external-client matrix.
