# MoonMQ Core Profile Expansion Design

## Goal

Expand MoonMQ from the initial frame and three-Basic-method slice into a usable, bounded AMQP 0-9-1 Core Profile while preserving the portable in-memory Broker. The expansion must remain narrower than a RabbitMQ clone and must be testable without a socket or external service.

## Scope

The protocol package will add these method families:

- `connection.start`, `start-ok`, `tune`, `tune-ok`, `open`, `open-ok`, `close`, `close-ok`;
- `channel.open`, `open-ok`, `close`, `close-ok`;
- exchange declare/delete and their `*-ok` responses;
- queue declare/delete/bind/unbind and their `*-ok` responses;
- `basic.qos`, `publish`, `consume`, `cancel`, `deliver`, `get`, `get-ok`, `get-empty`, `ack`, and `reject`.

Method payloads use typed MoonBit variants and a shared bounded reader/writer. Unknown method identifiers return `UnsupportedMethod`; malformed lengths, invalid bit fields, and invalid field values return typed errors.

The content layer will add AMQP field tables/long strings and Basic content properties, then expose a content assembler that accepts one content-header frame followed by body frames until the declared byte count is complete. It will reject body-before-header, overrun, underrun at finalization, and oversized content.

## Architecture

1. `src/protocol` remains backend-independent. It owns wire values, method identifiers, field tables, content headers, frame envelopes, and pure content assembly.
2. `src/broker` remains the deterministic in-memory state machine. It is not coupled to sockets or async tasks.
3. A portable session package translates typed protocol commands into Broker operations using an explicit connection/channel state machine. It can be tested with arrays of frames.
4. `src/transport/native` will wrap the session in the current `moonbitlang/async/socket` API. Native-only imports stay outside the portable packages; if the runtime API is unstable, the adapter remains separately scoped and documented.

## Wire and API decisions

- All decoded input is length-checked before indexing or allocation.
- AMQP short strings remain UTF-8, NUL-free, and limited to 255 bytes.
- Field-table keys are bounded short strings; the first implementation supports the scalar types needed by the Core Profile and preserves unknown values as a typed unsupported-value error.
- Basic properties use optional fields and AMQP property flags in wire order. Empty properties are a valid zero-flag header.
- Body chunks are byte-preserving and may be split across any number of body frames, subject to the configured frame limit.
- Session methods are channel-scoped after connection negotiation. Protocol errors close the affected session/channel through typed events instead of panicking.

## Testing

- Start every protocol family with a failing golden-wire or malformed-input test.
- Test round trips, exact declared sizes, invalid flags, truncated tables, unknown method IDs, content-header property flags, multi-frame body assembly, zero-length bodies, and negotiated frame limits.
- Test session handshake and Broker translation with a fake frame array before adding native sockets.
- Keep `moon check --target all` and `moon test --target all` green for portable packages. Native transport tests may be target-gated only where the official async runtime requires native.

## Non-goals

AMQP 1.0, AMQP 0-10, RabbitMQ extensions, persistence, clustering, transactions, publisher confirms, TLS, authentication/ACL, and production-scale performance remain out of scope. External-client compatibility will only be claimed for method and content paths covered by tests.
