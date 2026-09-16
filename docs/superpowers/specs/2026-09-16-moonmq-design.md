# MoonMQ Design Specification

## 1. Goal

MoonMQ is a MoonBit-native implementation of a deliberately documented AMQP 0-9-1 Core Profile plus a pure in-memory embedded Broker. It should let a developer run a complete publish/route/consume/ack flow locally without installing RabbitMQ, while also exposing a native TCP adapter for interoperability checks.

The project is scoped for the September 2026 MoonBit Hackathon acceptance deadline. It must be real, testable and maintainable, not a claim of full RabbitMQ compatibility.

## 2. Value and differentiation

The MoonBit ecosystem has a published MQTT 3.1.1 packet codec, but that package explicitly stops before transport, client runtime and Broker sessions. The existing task-queue and async-queue packages are useful adjacent primitives, not AMQP implementations. MoonMQ therefore fills a protocol and infrastructure gap without replacing those projects.

The differentiators are:

1. AMQP 0-9-1 wire-level types and framing written in MoonBit.
2. A deterministic, dependency-light in-memory Broker usable inside tests and examples.
3. A clear separation between portable protocol/core packages and native network transport.
4. An explicit compatibility matrix and golden/differential tests instead of an unbounded “RabbitMQ clone” promise.

## 3. Supported Core Profile

### Protocol

- Protocol header and frame parsing/serialization.
- Method, content-header, content-body and heartbeat frames.
- AMQP primitive values needed by the profile: short strings, long strings, octets, integers, booleans, arrays and field tables.
- Connection negotiation: `connection.start`, `start-ok`, `tune`, `tune-ok`, `open`, `open-ok`, `close`, `close-ok`.
- Channel lifecycle: `channel.open`, `open-ok`, `close`, `close-ok`.
- Exchange operations: declare/delete for direct, fanout and topic exchanges.
- Queue operations: declare/delete, bind/unbind.
- Basic operations: qos, publish, consume, cancel, deliver, get, get-ok, get-empty, ack and reject.
- Heartbeat frames and clean connection shutdown.

### Broker semantics

- One in-memory virtual host by default.
- Direct, fanout and topic routing.
- Default exchange behavior for direct queue publishing.
- Named and server-generated queues.
- Multiple consumers per queue with deterministic round-robin dispatch.
- Push delivery and pull/get delivery.
- Explicit acknowledgement, reject with optional requeue, and requeue on consumer/connection cleanup.
- Channel-level prefetch limit.
- Clear errors for invalid declarations, missing entities, duplicate consumers and unsupported operations.

## 4. Non-goals

- AMQP 1.0 or AMQP 0-10.
- RabbitMQ-specific extensions beyond the documented Core Profile.
- Durable disk storage, crash recovery and replication.
- Clustering, leader election or distributed consensus.
- Transactions, publisher confirms, alternate exchanges, dead-lettering, priority queues and plugins.
- TLS, production authentication, authorization and multi-vhost administration.
- A promise of complete RabbitMQ conformance or production-scale performance.

Unsupported methods must fail with a defined protocol error or a clear library error; they must not silently pretend to work.

## 5. Architecture

The repository will use one MoonBit module with focused packages:

- `protocol`: public wire types, primitive codecs, frame codecs and method/content-header codecs. This package has no network dependency.
- `broker`: pure in-memory entities, routing, queue state, consumer state and delivery acknowledgement logic. This package consumes protocol-independent domain types.
- `transport/native`: native-only TCP connection, channel multiplexing, handshake, frame loop and conversion between protocol messages and Broker actions.
- `cmd/moonmq`: small native executable that starts a Broker server from command-line options.
- `examples/embedded`: an in-process demonstration using the Broker API without external services.
- `tests` or package-local `*_test.mbt`: black-box unit, property, golden and integration tests.
- `docs`: compatibility matrix, protocol notes, architecture decisions, development log and release notes.

The dependency direction is one-way: transport depends on protocol and broker; broker may use protocol-independent message/domain types; protocol depends only on the standard library. This keeps deterministic tests fast and allows the core to be checked on additional backends where the current toolchain permits it.

## 6. Data flow

1. A transport connection reads bytes from a TCP stream.
2. The frame decoder validates frame size, channel number and frame end marker.
3. The method/content decoder produces a typed protocol event.
4. The connection/session layer validates lifecycle and channel state.
5. A broker command mutates the in-memory topology or produces deliveries.
6. Deliveries become protocol method/header/body frames and are written to the correct channel.
7. Acknowledgements update the broker delivery ledger and either finalize or requeue messages.

The embedded API skips steps 1–3 and 6–7's wire conversion, but uses the same broker state machine as the TCP adapter.

## 7. Error handling

- Malformed bytes return typed decode errors with offset/context.
- Invalid protocol state results in connection- or channel-scoped protocol errors.
- Broker declaration conflicts and missing names are explicit domain errors.
- Transport failures close the affected connection and requeue unacknowledged deliveries.
- Resource limits are bounded and documented; the Broker must not grow unbounded delivery state for a rejected or closed consumer.

## 8. Testing strategy

- Unit tests for every primitive and frame boundary condition.
- Golden tests for representative AMQP frames and method payloads.
- Broker tests for direct/fanout/topic routing, default exchange, round-robin, prefetch, ack, reject/requeue and cleanup.
- Property/fuzz-style tests where practical for encode/decode round trips and truncated frames.
- Native loopback integration test for handshake, declaration, publish, consume and ack.
- Optional external-client smoke test documented separately; it must not be required for the portable test suite.
- CI runs formatting, check, test and native integration on the supported runner.

## 9. Public demo

The primary demo starts an embedded Broker in the same process, declares an exchange and queue, publishes a JSON payload, consumes it and acknowledges it. The README will show the exact command and expected output. The native server demo will expose the same topology over TCP and report the supported Core Profile.

## 10. Open-source and provenance

MoonMQ will use Apache-2.0. The implementation will be written from the public AMQP 0-9-1 specification and compatibility references, without copying RabbitMQ server/client implementation code. Any generated constants or fixtures will identify their source and license in `NOTICE`/`THIRD_PARTY.md` and in the relevant test file.

## 11. Development records

- Every feature begins with a GitHub Issue.
- Each milestone is developed on a branch and merged through a PR.
- Commits are small but meaningful: protocol, broker, transport, tests, docs and CI changes remain distinguishable.
- `CHANGELOG.md`, compatibility notes and test reports record user-visible progress.
- The default branch contains all milestone merges and is the branch submitted to the contest.

## 12. Acceptance criteria

The design is successful when:

1. `moon check --target all` passes for portable packages.
2. `moon test --target all` passes for portable packages.
3. Native transport tests pass on the supported CI runner.
4. The embedded example runs without RabbitMQ or another external service.
5. The README explains supported methods, non-goals, commands and limitations.
6. The repository contains an OSI-compatible license, CI, changelog, tests, examples and traceable history.
7. The project can be published as a MoonBit package without exposing unfinished APIs as supported behavior.
