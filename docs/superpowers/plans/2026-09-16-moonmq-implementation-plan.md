# MoonMQ Implementation Plan

## Goal

Deliver a public, reproducible MoonBit project that implements a documented AMQP 0-9-1 Core Profile, a pure in-memory embedded Broker, and a testable session/transport boundary. The project must be understandable from its README, runnable locally without external services, tested by `moon test`, and developed through traceable Issues, pull requests, meaningful commits, and an update log.

## Architecture and technology

- MoonBit is the primary implementation language.
- `src/protocol` is a backend-independent AMQP 0-9-1 codec and method-profile package.
- `src/broker` is a backend-independent in-memory broker package and depends only on `src/protocol` for message/method data types.
- `src/transport/native` is an optional native TCP adapter. It is isolated because the current MoonBit async I/O surface is native/LLVM oriented.
- `cmd/moonmq` provides a local CLI demo; `examples/embedded` is a small library-facing example.
- Tests live beside each package and use public behavior rather than implementation details.
- CI runs formatting/check/test on the supported native target and records the MoonBit toolchain version.
- Project license is Apache-2.0. AMQP 0-9-1 terminology and wire behavior are implemented from the public protocol specification; the repository will include provenance notes and links rather than copying third-party implementation code.

## Global constraints

- Apply test-driven development: each production capability starts with a failing test and is made green before the next capability.
- Keep the first release deliberately scoped. Do not silently promise AMQP 1.0, clustering, disk persistence, replication, transactions, publisher confirms, TLS, authentication/ACL, or complete RabbitMQ extension compatibility.
- Keep protocol encoding deterministic and bounds-checked. Malformed frames and invalid method fields must return explicit errors rather than panic.
- Keep broker behavior deterministic for tests: one default virtual host, direct/fanout/topic exchanges, default exchange, generated or named queues, round-robin delivery, push and pull consumption, acknowledgements, reject/requeue, and per-channel prefetch.
- Avoid a transport dependency in the core packages. A protocol or broker test must run without a listening socket or external service.
- Every completed milestone updates `CHANGELOG.md` and `docs/updates.md`, closes its issue through its pull request, and leaves a meaningful commit history.
- Public APIs must have package-level and exported symbol documentation sufficient for a newcomer to build the embedded demo.

## Phase 0 — Repository and project scaffold

1. Generate the MoonBit module with module path `Zcxssxx/moonmq`; confirm the generated layout and current compiler conventions with `moon check`.
2. Add the repository metadata:
   - `README.md` with value proposition, scope, quick start, architecture, limitations, and a local demo transcript.
   - `LICENSE` with Apache-2.0 text.
   - `NOTICE.md` and `THIRD_PARTY.md` documenting AMQP specification provenance and links.
   - `.gitignore`, `.editorconfig`, `CHANGELOG.md`, `docs/updates.md`.
   - `.github/workflows/ci.yml` with MoonBit setup/version reporting and check/test commands.
   - `.github/ISSUE_TEMPLATE/feature.yml` and `.github/pull_request_template.md` to preserve the development record.
3. Create the initial GitHub public repository under the already authenticated `Zcxssxx` account only after confirming that the exact repository name is available. Do not inspect or use historical GitHub accounts.
4. Open milestone Issues for scaffold, protocol codec, broker core, native transport/demo, and release polish. Each implementation branch references one issue.

Verification for Phase 0:

```text
moon check
moon test
git diff --check
```

## Phase 1 — Protocol primitives and frame codec

Target package: `src/protocol`.

Files:

- `src/protocol/moon.pkg.json` — package declaration and test configuration.
- `src/protocol/types.mbt` — `AmqpError`, unsigned integer aliases/helpers, `FrameType`, `Frame`, `FramePayload`, `ProtocolHeader`.
- `src/protocol/bytes.mbt` — bounds-checked reader/writer for octets, short strings, long strings, short/long integers, bit fields, and tables/arrays needed by the profile.
- `src/protocol/frame.mbt` — encode/decode AMQP protocol header, method, content-header, body, heartbeat, frame terminator, and maximum-frame-size checks.
- `src/protocol/primitive_test.mbt` — red/green tests for primitive round trips and malformed input.
- `src/protocol/frame_test.mbt` — golden bytes and negative tests for all frame kinds.

Public behavior:

- `encode_protocol_header` and `decode_protocol_header` accept/reject `AMQP\\x00\\x00\\x09\\x01`.
- `encode_frame` and `decode_frame` preserve type, channel, payload, and terminator.
- Decoding rejects truncated input, unsupported frame type, invalid terminator, oversized payload, and trailing bytes where a single frame is required.
- The codec never owns a socket and never performs I/O.

TDD order:

1. Add failing tests for byte order, short/long strings, bit fields, and frame terminator.
2. Implement the smallest reader/writer needed to pass them.
3. Add failing frame-header/body/heartbeat tests and implement frame encoding.
4. Add malformed-input tests and enforce all bounds.
5. Run `moon test src/protocol` and `moon check` before opening the protocol PR.

## Phase 2 — AMQP method and content profile

Target package: `src/protocol`.

Files:

- `src/protocol/methods.mbt` — typed method variants for the selected connection, channel, exchange, queue, basic, and heartbeat methods.
- `src/protocol/method_codec.mbt` — method class/id encoding and decoding, field-table/field-array support, and content-header property flags.
- `src/protocol/method_test.mbt` — method round trips and representative wire vectors.
- `src/protocol/content_test.mbt` — content-header/body sequencing and property tests.
- `docs/protocol-profile.md` — exact supported methods, intentional omissions, and interoperability notes.

The initial profile includes connection start/start-ok/tune/tune-ok/open/open-ok/close/close-ok; channel open/open-ok/close/close-ok; exchange declare/delete; queue declare/delete/bind/unbind; basic qos/publish/consume/cancel/deliver/get/get-ok/get-empty/ack/reject; and heartbeat. Each typed method records only fields needed by the profile, while unknown methods produce a structured `UnsupportedMethod` error.

Content handling must expose a clear sequence API so a caller cannot accidentally treat a content body frame as a method frame. Tests cover multi-frame bodies, zero-length bodies, property flags, and frame-size boundaries.

## Phase 3 — Pure in-memory embedded Broker

Target package: `src/broker`.

Files:

- `src/broker/moon.pkg.json` — package declaration.
- `src/broker/model.mbt` — `Broker`, `Exchange`, `Queue`, `Binding`, `Message`, `Delivery`, `Consumer`, `ChannelState`, and public configuration/error types.
- `src/broker/broker.mbt` — construction, exchange/queue declaration and deletion, binding operations, publish routing, and lifecycle cleanup.
- `src/broker/topic.mbt` — AMQP topic matching for `*` and `#`, including empty and multi-segment routing keys.
- `src/broker/consumer.mbt` — push consumers, pull/get, deterministic round-robin, prefetch accounting, ack, reject, and requeue.
- `src/broker/broker_test.mbt` — isolated behavior tests for declarations and lifecycle.
- `src/broker/routing_test.mbt` — direct/fanout/topic/default-exchange routing tests.
- `src/broker/ack_test.mbt` — ack/reject/requeue/prefetch/consumer-cancel tests.
- `examples/embedded/moon.pkg.json` and `examples/embedded/main.mbt` — a local publish/consume walkthrough with no external service.

Public API shape:

- `Broker::new(config)` creates an isolated broker.
- `declare_exchange`, `delete_exchange`, `declare_queue`, `delete_queue`, `bind_queue`, and `unbind_queue` manage the in-memory topology.
- `publish(exchange, routing_key, body, properties)` returns routed count and does not silently drop unroutable messages; the caller receives an explicit result.
- `consume(queue, consumer_config)` registers a consumer and returns a consumer handle.
- `get(queue)` returns `Delivery` or an empty result.
- `ack`, `reject(requeue)`, and `cancel_consumer` update delivery state.

Semantics to lock down with tests:

- The default exchange routes directly to a queue named by the routing key.
- Direct exchange matches exact routing keys; fanout ignores the key; topic implements segment matching.
- A named queue is idempotent only when its durable/exclusive/auto-delete attributes agree; generated names are deterministic within a test broker.
- Delivery tags are monotonic per channel. Prefetch limits unacknowledged deliveries. Requeue puts a rejected message back in a deterministic position.
- Closing a consumer requeues its unacknowledged messages according to the documented policy.

## Phase 4 — Native TCP adapter and local protocol demo

Target packages/files:

- `src/transport/native/moon.pkg.json` — native-only package declaration.
- `src/transport/native/tcp.mbt` — listener/connection loop using the current MoonBit native async/socket APIs discovered from `moon ide doc`.
- `src/transport/native/session.mbt` — protocol handshake, channel map, content assembly, method dispatch to `Broker`, and graceful close/heartbeat handling.
- `src/transport/native/transport_test.mbt` — loopback smoke tests where supported; skip only with a documented platform condition.
- `cmd/moonmq/moon.pkg.json` and `cmd/moonmq/main.mbt` — `moonmq demo` starts an in-process broker and exercises publish/consume; `moonmq serve` starts the native adapter.
- `docs/interop.md` — commands for an AMQP 0-9-1 client, expected supported subset, and platform limitations.

TDD order:

1. Start with a fake byte-stream/session test that proves handshake and frame dispatch without a socket.
2. Add a native loopback adapter around the fake stream.
3. Add the CLI demo and only then the real listener.
4. Run native checks/tests on the supported CI runner; on Windows, retain the pure-core and embedded-demo verification if native async is unavailable.

The adapter must not expand the core API or copy routing logic. It translates frames to broker calls and broker results back to frames, with a bounded maximum frame size and connection shutdown on protocol errors.

## Phase 5 — Documentation, CI, interoperability, and release record

Files:

- `README.md` — update with real commands, tested toolchain, feature matrix, architecture diagram, and a 60–90 second demo path.
- `docs/protocol-profile.md` — supported/unsupported method table and wire-level caveats.
- `docs/interop.md` — optional RabbitMQ `amqplib`/other-client smoke test instructions; no external service is required for the default demo.
- `docs/updates.md` and `CHANGELOG.md` — dated milestone entries linked to issues and PRs.
- `.github/workflows/ci.yml` — formatting, check, tests, and artifact/log capture.
- `examples/embedded/README.md` — copy-paste local walkthrough.
- `SECURITY.md` — scope of the in-memory demo and how to report protocol/parser issues.

Final verification commands:

```text
moon fmt
moon check
moon test
moon info
git diff --check
gh issue list --state all
gh pr list --state all
```

Before claiming completion, inspect the resulting GitHub default branch, confirm every milestone PR is merged or explicitly documented, confirm the repository is public and the README example works from a clean checkout, and record the exact MoonBit version in `docs/updates.md`.

## Commit and PR sequence

Use small, reviewable commits and branches with the `codex/` prefix:

1. `docs: define MoonMQ AMQP core profile` — design/spec/plan/findings/progress.
2. `chore: scaffold MoonMQ module and project governance` — module metadata, license, CI templates, README skeleton.
3. `feat(protocol): add bounded AMQP primitives and frames` — closes protocol codec issue.
4. `feat(protocol): add AMQP core method profile` — closes method/profile issue.
5. `feat(broker): add deterministic in-memory routing and delivery` — closes broker issue.
6. `feat(transport): add native session adapter and embedded demo` — closes transport/demo issue.
7. `docs: publish interoperability and release records` — closes release issue.

Each implementation commit is preceded by red tests in the same branch, each PR description includes verification output and limitations, and each merged PR updates the changelog/update log.

## Acceptance criteria

- A fresh clone can run the embedded example without RabbitMQ, Redis, or a network service.
- Protocol tests prove valid round trips plus malformed-input rejection for the documented profile.
- Broker tests prove direct/fanout/topic/default-exchange routing, pull/push delivery, acknowledgements, reject/requeue, prefetch, and cleanup.
- The native adapter is clearly marked by backend/platform and does not contaminate the portable core.
- README and docs state the project’s differentiation from MoonBit MQTT and adjacent task queues, while acknowledging the old Go project with the same `moonmq` name as a naming/search collision.
- GitHub history contains meaningful commits, Issues, PRs, and dated updates under the `Zcxssxx` account, with no use of historical cached account identities.
