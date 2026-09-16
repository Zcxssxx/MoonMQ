# MoonMQ development updates

This log is intentionally chronological. Each implementation milestone links its GitHub Issue and, once opened, its pull request.

## 2026-09-16 — project and design baseline

- Confirmed the project direction: a MoonBit AMQP 0-9-1 Core Profile plus a pure in-memory embedded Broker.
- Researched the adjacent MoonBit MQTT codec package, async I/O constraints, task-queue packages, RabbitMQ AMQP concepts, and the public AMQP 0-9-1 specification archive.
- Updated and verified the local MoonBit toolchain: `moon 0.1.20260915`, `moonc v0.10.13+cbb11c36f`, `moonrun 0.1.20260915`.
- Committed the design and implementation plan as `a4eba1c` (`docs: define MoonMQ AMQP core profile`).
- GitHub CLI identity verified as `Zcxssxx`; no historical account list was read.

## 2026-09-16 — protocol codec milestone ([#2](https://github.com/Zcxssxx/MoonMQ/issues/2), [#3](https://github.com/Zcxssxx/MoonMQ/issues/3), [PR #7](https://github.com/Zcxssxx/MoonMQ/pull/7), [PR #8](https://github.com/Zcxssxx/MoonMQ/pull/8))

- Added red-first tests for AMQP protocol headers, network-byte-order integers, UTF-8 short strings, frame envelopes, and typed method payloads.
- Implemented bounded frame decoding with size, terminator, type, and truncation checks.
- Implemented typed `basic.publish`, `basic.ack`, and `basic.reject` payload encode/decode paths.
- Reviewer follow-up: frame-max now reserves the 8-byte envelope overhead; fixed-width integer decoders reject trailing bytes; u64, BasicAck/BasicReject, unsupported-type, declared-size, and oversized-payload cases are covered.
- Added frame-shape validation for heartbeat/channel rules and method identifiers, AMQP field validation for `basic.publish`, and explicit truncation errors for short protocol headers.
- AMQP `frame-max=0` is treated as “no specific negotiated limit” while retaining MoonMQ's bounded default codec limit of 131072 bytes.
- Verification: `moon check src/protocol`, `moon test src/protocol` — 22 tests passed; `moon fmt` and `git diff --check` clean.
- Follow-up: zero delivery-tags are rejected for ordinary ACKs and all REJECTs; zero remains valid only for the AMQP multiple-acknowledgement form. The local `Int` frame limit is documented as a bounded host-side cap, separate from the wire-level unsigned field.
- Verification: `moon check --target all`, `moon test --target all` — 23 tests passed on wasm, wasm-gc, js, and native.
- Commits: `91774d0`, `803371b`, `1a4252f`, `0179106`, `7e990fa`, `f9d0b6f`, `2c1e75b`, `28de167`, `55cb641`, `8a3d7df`, `19c60a0`, `e623d54`, `a0f4edb`.

## 2026-09-16 — embedded Broker milestone ([#4](https://github.com/Zcxssxx/MoonMQ/issues/4), [#5](https://github.com/Zcxssxx/MoonMQ/issues/5), [PR #9](https://github.com/Zcxssxx/MoonMQ/pull/9))

- Added deterministic in-memory direct, fanout, topic, and default-exchange routing with duplicate-binding protection.
- Added pull delivery, push consumers, prefetch, round-robin dispatch, acknowledgements, reject/requeue, and consumer cancellation cleanup.
- Fixed acknowledgement/requeue pumping and generated queue-name collisions; added UTF-8 payload and local demo fixtures.
- Verification: `moon check --target all`, `moon test --target all` — 33 tests passed on wasm, wasm-gc, js, and native; both local demos publish and consume successfully.
- Commits: `cb3f385`, `6dbda72`, `0b62d55`, `ffb5961`, `316ed95`, `683981e`, `6332689`, `2aa7c77`, `5e43469`.
- Scope note: the embedded demo portion of #5 is delivered; the native TCP/session adapter remains a separate follow-up and is not included in the interoperability claim.

## 2026-09-16 — portable MVP submission boundary

- PR #7 (protocol), PR #8 (protocol correctness), and PR #9 (embedded Broker) are merged into `main`.
- Final local verification before the follow-up: 34 tests passed on wasm, wasm-gc, js, and native; both demos passed; formatting and generated public interfaces were checked.
- The submission boundary is the portable AMQP Core Profile plus deterministic embedded Broker. Native TCP, external-client interoperability, and Mooncakes publication remain explicitly tracked follow-ups rather than unverified claims.

## 2026-09-16 — Broker review follow-up ([#10](https://github.com/Zcxssxx/MoonMQ/issues/10), [PR #11](https://github.com/Zcxssxx/MoonMQ/pull/11))

- A review found that removing a middle consumer could leave the round-robin cursor pointing at the wrong active consumer.
- Added a red regression test for the A/B/C cancellation sequence and adjusted the cursor when the consumer array shrinks.
- Verification: Broker package 13/13 and full project 36/36 passed on wasm, wasm-gc, js, and native; both demos passed.
- Commits: `3821c5f`, `282844b`, `5a49672`.

## 2026-09-16 — final merged-main verification ([PR #11](https://github.com/Zcxssxx/MoonMQ/pull/11))

- PR #11 is merged into `main`; Issues #1–#4 and #10 are closed, while #5 and #6 remain explicit follow-up work.
- The merged tree is the portable MVP boundary: AMQP Core Profile codec plus deterministic embedded Broker, with no external service required for the demos.
- Final verification is recorded after the merge: 36/36 tests pass on wasm, wasm-gc, js, and native; both local demos pass; format and working-tree checks are clean.

## Record format for future entries

Each milestone entry will include its date, Issue/PR links, user-visible behavior, verification commands, compatibility limitations, and commit summary.

## 2026-09-16 — Core Profile expansion: field tables ([#12](https://github.com/Zcxssxx/MoonMQ/issues/12))

- Added deterministic, bounded AMQP field-table and field-array encoding/decoding with nested values, long strings, timestamps, and explicit malformed-value errors.
- Added red-first tests for empty tables, nested round trips, unknown tags, truncation, and declared-size mismatches.
- Verification: `moon check --target all`, `moon test --target all` — 39 tests passed on wasm, wasm-gc, js, and native; `moon fmt --check` and `moon info` completed.
- Commit: `e3a3020`.

## 2026-09-16 — Core Profile expansion: method families ([#12](https://github.com/Zcxssxx/MoonMQ/issues/12))

- Added typed `connection`, `channel`, `exchange`, `queue`, and Basic lifecycle method variants, including topology arguments, delivery metadata, and response methods.
- Added bounded long-string helpers for arbitrary challenge/response bytes and exact method readers that reject truncation, trailing bytes, and reserved flag bits.
- Added red-first golden-wire and round-trip tests for connection start/start-ok/tune, exchange declare, queue/consumer flows, and long strings.
- Verification: `moon check --target all`, `moon test --target all` — 45 tests passed on wasm, wasm-gc, js, and native; `moon fmt --check`, `moon info`, and `git diff --check` completed.
- Commits: `e3a3020`, `45b12b3`, and `fcade5b`.

## 2026-09-16 — Core Profile expansion: content sequencing ([#12](https://github.com/Zcxssxx/MoonMQ/issues/12))

- Added all AMQP Basic content properties with fluent optional-value construction and property-flag encoding in wire order.
- Added Basic content-header encode/decode and a bounded assembler that joins body frames only when the declared body size is complete.
- Added red-first tests for zero-body headers, property flags, split bodies, body overrun/underrun, unsupported classes, and malformed flags.
- Verification: `moon check --target all`, `moon test --target all` — 49 tests passed on wasm, wasm-gc, js, and native; `moon fmt --check`, `moon info`, and `git diff --check` completed.
- Commit: pending for this milestone.

## 2026-09-16 — Core Profile expansion: portable session ([#12](https://github.com/Zcxssxx/MoonMQ/issues/12), [PR #13](https://github.com/Zcxssxx/MoonMQ/pull/13))

- Added a portable AMQP session state machine with protocol-header validation, connection handshake, channel lifecycle, topology declarations, publish/content sequencing, pull and push delivery, cancellation cleanup, acknowledgement/reject handling, and heartbeat acceptance.
- Added red-first fake-frame tests for handshake, queue publish/get/ack, consumer delivery/cancel, invalid state, missing content, and unknown channels.
- The session deliberately keeps native sockets out of the portable package. Deletion, unbind, multiple acknowledgements, authentication, negotiated connection limits, and property-preserving Broker delivery remain explicit follow-ups.
- Verification: `moon check --target all`, `moon test --target all` — 55 tests passed on wasm, wasm-gc, js, and native; `moon fmt --check`, `moon info`, and `git diff --check` completed.
- Commit: pending for this milestone.
