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
