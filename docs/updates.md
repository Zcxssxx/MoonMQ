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
- Final local verification on the merged tree: 33 tests passed on wasm, wasm-gc, js, and native; both demos passed; formatting and generated public interfaces were checked.
- The submission boundary is the portable AMQP Core Profile plus deterministic embedded Broker. Native TCP, external-client interoperability, and Mooncakes publication remain explicitly tracked follow-ups rather than unverified claims.

## Record format for future entries

Each milestone entry will include its date, Issue/PR links, user-visible behavior, verification commands, compatibility limitations, and commit summary.
