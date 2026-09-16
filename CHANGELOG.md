# Changelog

All notable changes to MoonMQ are recorded here. The project is currently pre-1.0 and follows the September 2026 MoonBit Hackathon development period.

## Unreleased

- Defined the AMQP 0-9-1 Core Profile and embedded-broker architecture.
- Added persistent planning, provenance, and update-record documents.
- Scaffolded the MoonBit module and repository governance files.
- Added bounded protocol-header, integer, short-string, and AMQP frame codecs.
- Added typed `basic.publish`, `basic.ack`, and `basic.reject` method payload codecs with malformed-input checks.
- Added AMQP frame-shape validation, negotiated frame limits, exact-width decoding, and explicit truncated-input errors.
- Added delivery-tag validation for the AMQP zero-tag acknowledgement rule.
- Added a deterministic in-memory Broker with direct, fanout, topic, and default-exchange routing plus pull/push delivery, prefetch, ack, reject/requeue, and consumer cancellation.
- Added local embedded-Broker demos that exercise UTF-8 payload bytes.
- Preserved deterministic round-robin position when a middle consumer is cancelled.
- Added bounded AMQP field-table/array codecs for Boolean, Int, longstr, nested table/array, timestamp, and void values.
- Added typed AMQP connection, channel, exchange, queue, and Basic lifecycle method codecs with exact-length readers and reserved-bit validation.
- Added AMQP long-string helpers that preserve arbitrary bytes within a bounded 1 MiB host-side limit.
- Added Basic content properties, content-header codecs, and bounded multi-frame body assembly with strict declared-size checks.
- Added a portable session state machine that drives the tested AMQP handshake, channel, topology, publish/content, get, consume/deliver, cancel, acknowledgement, reject, and heartbeat paths against the embedded Broker.
- Added `Basic.Return`/mandatory publish handling, per-channel content sequencing, channel/session delivery ownership, tune-bound frame output, queue statistics, and explicit rejection of unsupported non-default topology/consumer options.
- Added exchange/property-preserving Broker messages and session deliveries, including session-scoped consumer namespaces for shared embedded Brokers.
- Added a native-only byte-stream connection adapter, `moonbitlang/async@0.20.2` TCP server loop, and native executable entrypoint with a real loopback handshake test.
