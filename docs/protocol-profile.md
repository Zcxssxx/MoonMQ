# MoonMQ AMQP 0-9-1 Core Profile

This document is the compatibility contract for the first release. It is narrower than the entire RabbitMQ feature set and will be expanded only through a recorded design change.

## Current implemented surface

| Area | Methods / behavior |
| --- | --- |
| Framing | protocol header, method/header/body/heartbeat envelopes, `0xCE` terminator, exact-size and bounded frame validation |
| Primitives | network-byte-order `u16`/`u32`/`u64`, UTF-8 short strings, exact-width and malformed-input checks |
| Methods | typed `basic.publish`, `basic.ack`, and `basic.reject` payloads with field validation |
| Broker | in-memory default/direct/fanout/topic routing, named/generated queues, push/pull delivery, prefetch, ack/reject/requeue, cancellation cleanup |
| Transport | no socket dependency in the current release; embedded use is the supported execution path |

## Deferred profile expansion

Connection/channel handshake, exchange/queue method frames, content-header properties, multi-frame content assembly, and the native TCP/session adapter are planned follow-up slices. They are intentionally not claimed as implemented interoperability in this release.

## Explicit non-goals

AMQP 1.0 and 0-10, persistence/recovery, replication, clustering, transactions, publisher confirms, alternate exchanges, dead-lettering, priorities, plugins, TLS, authentication/ACL, and full RabbitMQ extension compatibility are not first-release promises.

## Interoperability position

The wire codec aims to be spec-driven and bounds-checked. Interoperability claims will be limited to the methods and content properties covered by tests. The embedded demo is the default verification path and requires no external broker.
