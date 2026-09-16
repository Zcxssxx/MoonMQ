# MoonMQ AMQP 0-9-1 Core Profile

This document is the compatibility contract for the first release. It is narrower than the entire RabbitMQ feature set and will be expanded only through a recorded design change.

## Current implemented surface

| Area | Methods / behavior |
| --- | --- |
| Framing | protocol header, method/header/body/heartbeat envelopes, `0xCE` terminator, exact-size and bounded frame validation |
| Primitives | network-byte-order `u16`/`u32`/`u64`, UTF-8 short strings, bounded arbitrary-byte long strings, exact-width and malformed-input checks |
| Methods | typed connection/channel handshake, exchange/queue topology, Basic QoS/consume/get/deliver/publish/ack/reject/cancel methods with field validation |
| Content | Basic class content properties, header property flags, and bounded multi-frame body assembly with exact declared-size checks |
| Broker | in-memory default/direct/fanout/topic routing, named/generated queues, push/pull delivery, prefetch, ack/reject/requeue, cancellation cleanup |
| Transport | no socket dependency in the current release; embedded use is the supported execution path |

## Deferred profile expansion

The portable session and native TCP adapter remain planned follow-up slices. The protocol package now covers method and content sequencing primitives, but end-to-end network interoperability is not claimed until session/transport tests land.

## Explicit non-goals

AMQP 1.0 and 0-10, persistence/recovery, replication, clustering, transactions, publisher confirms, alternate exchanges, dead-lettering, priorities, plugins, TLS, authentication/ACL, and full RabbitMQ extension compatibility are not first-release promises.

## Interoperability position

The wire codec aims to be spec-driven and bounds-checked. Interoperability claims will be limited to the methods and content properties covered by tests. The embedded demo is the default verification path and requires no external broker.
