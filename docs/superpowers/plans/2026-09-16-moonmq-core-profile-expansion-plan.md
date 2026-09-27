# MoonMQ Core Profile Expansion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend the existing MoonMQ portable MVP into a bounded AMQP 0-9-1 Core Profile with content sequencing, a portable session boundary, and a separately isolated native adapter.

**Architecture:** Keep `src/protocol` pure and backend-independent. Add a typed method/content layer there, then translate protocol events through a portable session state machine into the existing deterministic `src/broker` API. Put all socket and async imports in `src/transport/native`; no core test may need a network service.

**Tech Stack:** MoonBit `0.1.20260915`, `moon` packages, standard `Bytes`/`Array`, `moonbitlang/async@0.20.2` only in the native adapter, GitHub Actions, and package-local TDD tests.

## Global Constraints

- AMQP 0-9-1 only; no AMQP 1.0/0-10 or full RabbitMQ compatibility claim.
- Every production behavior starts with a red test and reaches green before the next behavior.
- Every decoder validates lengths before indexing or allocating and returns typed errors rather than panicking.
- Portable protocol/Broker/session tests run without sockets; native-only code is target-gated.
- Each task updates `CHANGELOG.md`, `docs/updates.md`, `progress.md`, and its GitHub Issue/PR record.
- Public interfaces are regenerated with `moon info` and reviewed before commits.

---

### Task 1: AMQP field values and bounded readers

**Files:**
- Create: `src/protocol/field_table.mbt`
- Create: `src/protocol/field_table_test.mbt`
- Modify: `src/protocol/header.mbt`
- Modify: `src/protocol/moon.pkg`

**Interfaces:**
- Produces `FieldValue`, `encode_field_table`, and `decode_field_table` for later method/content codecs.
- `FieldValue` supports Boolean, signed Int, UInt64 Long, UTF-8 short string, raw long string, nested table, and nested array values.
- `decode_field_table` requires exact consumption of the declared table length and rejects unknown tags, invalid UTF-8, NUL short strings, and overrun input.

- [ ] Write failing tests for empty tables, scalar values, nested tables/arrays, malformed tags, and declared-size truncation.
- [ ] Run `moon test src/protocol --filter 'field'` and confirm the new tests fail for missing symbols or incomplete behavior.
- [ ] Implement a bounded byte reader and deterministic table encoder/decoder with a local maximum table size.
- [ ] Run `moon check src/protocol` and `moon test src/protocol`; expect all existing tests plus field-table tests to pass.
- [ ] Run `moon fmt`, `moon info`, inspect `src/protocol/pkg.generated.mbti`, and commit `feat(protocol): add bounded AMQP field tables`.

### Task 2: Connection, channel, topology, and Basic lifecycle methods

**Files:**
- Modify: `src/protocol/methods.mbt`
- Modify: `src/protocol/method_codec.mbt`
- Modify: `src/protocol/header.mbt`
- Modify: `src/protocol/method_test.mbt`

**Interfaces:**
- Extend `Method` with typed variants for connection start/start-ok/tune/tune-ok/open/open-ok/close/close-ok; channel open/open-ok/close/close-ok; exchange declare/delete and responses; queue declare/delete/bind/unbind and responses; Basic qos/consume/cancel/deliver/get/get-ok/get-empty.
- Preserve the existing `BasicPublish`, `BasicAck`, and `BasicReject` wire IDs and validation behavior.
- Use `FieldTable` for AMQP table arguments and `Bytes` for challenge/response and long strings where a text interpretation is not guaranteed.

- [ ] Add golden-wire tests for one request and response in each method family, including bit-field packing and empty-table encoding.
- [ ] Add malformed-input tests for unknown IDs, truncated short/long strings, invalid reserved bits, and trailing method bytes.
- [ ] Run `moon test src/protocol` and confirm the new tests fail before production changes.
- [ ] Implement class/method dispatch and exact argument readers/writers with explicit `UnsupportedMethod`/`InvalidFrameShape` errors.
- [ ] Run `moon check --target all` and `moon test --target all`; expect no regression in the existing 36-test suite.
- [ ] Regenerate the interface and commit `feat(protocol): expand AMQP Core method profile`.

### Task 3: Content headers and multi-frame body assembly

**Files:**
- Create: `src/protocol/content.mbt`
- Create: `src/protocol/content_test.mbt`
- Modify: `src/protocol/frame.mbt`
- Modify: `src/protocol/header.mbt`
- Modify: `docs/protocol-profile.md`

**Interfaces:**
- Add `BasicProperties` with optional AMQP Basic property fields and `ContentHeader` with class ID, weight, body size, and properties.
- Add `encode_content_header` and `decode_content_header` for a header-frame payload.
- Add `ContentAssembler::new(header, max_body_size)`, `push_body(frame_payload)`, and `finish()`; it returns a byte-preserving `ContentMessage` only when the declared body size is complete.

- [ ] Add failing tests for zero-body content, property-flag round trips, split body frames, body overrun, body underrun, and body-before-header.
- [ ] Run the focused content tests and confirm they fail for the missing API.
- [ ] Implement property-flag packing in AMQP wire order and bounded body accumulation.
- [ ] Run `moon check src/protocol`, `moon test src/protocol`, and all-target tests.
- [ ] Commit `feat(protocol): add AMQP content headers and body assembly`.

### Task 4: Portable session state machine

**Files:**
- Create: `src/session/moon.pkg`
- Create: `src/session/model.mbt`
- Create: `src/session/session.mbt`
- Create: `src/session/session_test.mbt`
- Modify: `src/broker/model.mbt` only if a protocol-independent delivery/property bridge is required

**Interfaces:**
- `Session::new(broker : Broker) -> Session` starts in `AwaitProtocolHeader`.
- `Session::receive_protocol_header(input : Bytes) -> Result[Bytes, SessionError]` validates the header and returns the server header.
- `Session::receive_frame(frame : Frame) -> Result[Array[Frame], SessionError]` advances handshake/channel state and translates declared topology, publish, get, consume, ack, and reject operations to the Broker.
- Session errors distinguish protocol state, channel, and Broker failures; no socket type appears in this package.

- [ ] Add fake-frame tests for header/start/tune/open handshake, channel open/close, queue declaration, publish/get/ack, and invalid state transitions.
- [ ] Run the session tests and observe failures before implementation.
- [ ] Implement the smallest state machine that emits deterministic response frames and preserves channel IDs.
- [ ] Run all portable checks/tests and a local session demonstration using only byte arrays.
- [ ] Commit `feat(session): translate AMQP frames to embedded Broker actions`.

### Task 5: Native TCP adapter

**Files:**
- Create: `src/transport/native/moon.pkg`
- Create: `src/transport/native/tcp.mbt`
- Create: `src/transport/native/transport_test.mbt`
- Modify: `moon.mod`
- Modify: `cmd/moonmq/moon.pkg`
- Modify: `cmd/moonmq/main.mbt`
- Modify: `docs/interop.md`

**Interfaces:**
- Native transport owns `moonbitlang/async/socket` and exposes `serve(addr, broker)` plus a bounded connection loop around the portable `Session`.
- The adapter reads the exact protocol header and frame envelope, closes on typed protocol errors, and requeues unacknowledged Broker deliveries through session cleanup.
- The portable packages remain compilable for wasm/wasm-gc/js.

- [ ] Add a fake stream loopback test around `Session` before using sockets.
- [ ] Run the native transport test and confirm it fails before the dependency/API implementation exists.
- [ ] Add `moonbitlang/async@0.20.2` and inspect current `socket` signatures with `moon ide doc`.
- [ ] Implement native listener/accept/read/write/close with a bounded per-connection frame buffer.
- [ ] Run native check/test and retain portable target checks; document any platform condition in `docs/interop.md`.
- [ ] Commit `feat(transport): add native AMQP session adapter`.

### Task 6: Release record and final audit

**Files:**
- Modify: `README.md`
- Modify: `docs/protocol-profile.md`
- Modify: `docs/interop.md`
- Modify: `CHANGELOG.md`
- Modify: `docs/updates.md`
- Modify: `progress.md`
- Modify: `task_plan.md`

- [ ] Update the method matrix from deferred to implemented only where tests prove it.
- [ ] Add exact native/server commands and limitations; do not claim unsupported RabbitMQ extensions.
- [ ] Run `moon fmt --check`, `moon check --target all`, `moon test --target all`, `moon info`, both local demos, and `git diff --check` from `main`.
- [ ] Inspect `gh issue list --state all`, `gh pr list --state all`, repository visibility, default branch, and merged milestone PRs.
- [ ] Commit `docs: publish expanded Core Profile release record` and close only Issues whose acceptance criteria are actually met.
