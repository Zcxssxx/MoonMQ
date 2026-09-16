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
| Session | portable handshake, channel lifecycle, topology, publish/content assembly, get, consume/deliver, cancel, ack, reject, and heartbeat translation to the embedded Broker |
| Transport | no socket dependency in the current release; embedded/session use is the supported execution path |

## Deferred profile expansion

The native TCP adapter remains a planned follow-up slice. The portable session is covered by fake-frame tests; end-to-end network interoperability is not claimed until the native transport tests and an external-client smoke test land.

## Explicit non-goals

AMQP 1.0 and 0-10, persistence/recovery, replication, clustering, transactions, publisher confirms, alternate exchanges, dead-lettering, priorities, plugins, TLS, authentication/ACL, and full RabbitMQ extension compatibility are not first-release promises.

## Session limitations

The session currently exposes the broker's deterministic in-memory semantics. Exchange/queue deletion, queue unbind, multiple acknowledgements, authentication, and negotiated per-connection limits are codec-complete but return an explicit unsupported-operation result at the session boundary until the Broker and transport contracts grow to support them. Published Basic content properties are decoded and validated by the protocol layer; the current Broker message model carries body/routing metadata only, so property-preserving delivery is not yet part of the session interoperability claim.

## Interoperability position

The wire codec aims to be spec-driven and bounds-checked. Interoperability claims will be limited to the methods and content properties covered by tests. The embedded demo is the default verification path and requires no external broker.
