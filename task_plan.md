# Task Plan: MoonMQ AMQP 0-9-1 Core Profile

## Goal

交付一个以 MoonBit 为主要实现语言的 MoonMQ：包含 AMQP 0-9-1 Core Profile 协议编解码器、纯内存嵌入式 Broker、native TCP 适配器、本地演示、测试、CI、文档和可追踪的 GitHub 开发记录。

## Current Phase

Phase 6: Core Profile expansion

## Phases

### Phase 1: Requirements & Discovery

- [x] Confirm contest and project scope
- [x] Verify current GitHub identity is `Zcxssxx`
- [x] Verify MoonBit toolchain
- [x] Research MoonBit ecosystem overlap
- **Status:** complete

### Phase 2: Planning & Structure

- [x] Write and commit design specification
- [x] Write and self-review implementation plan
- [x] Create public GitHub repository and issue workflow
- **Status:** complete

### Phase 3: Implementation

- [x] Add protocol value and frame codecs with failing tests first
- [x] Add AMQP method codecs and Core Profile dispatch
- [x] Add pure in-memory Broker topology and delivery semantics
- [ ] Add native TCP connection/channel adapter
- [x] Add embedded and local smoke-test demos
- **Status:** complete for the portable MVP; native TCP is deferred as a follow-up issue

### Phase 4: Testing & Verification

- [x] Run `moon check --target all`
- [x] Run `moon test --target all`
- [x] Run native core checks and demo smoke tests
- [x] Run `moon fmt --check` or the current equivalent
- [x] Run `moon info` and review public interface changes
- **Status:** complete for the portable Core Profile and embedded Broker scope

### Phase 5: Delivery

- [x] Finish README, changelog, license and compatibility notes
- [x] Configure GitHub Actions CI
- [x] Publish meaningful commits, Issues and PRs on the default branch
- [ ] Prepare Mooncakes publication metadata and acceptance checklist
- **Status:** in_progress; portable MVP is ready, publication metadata remains

### Phase 6: Core Profile expansion

- [ ] Add AMQP field-table and long-string codecs with bounded readers
- [ ] Add connection, channel, exchange, queue, and Basic lifecycle method variants
- [ ] Add Basic content properties and content-header/body sequencing
- [ ] Add a portable session state machine that translates protocol events to Broker actions
- [ ] Add native TCP adapter behind the session interface where the current async runtime supports it
- [ ] Add loopback/session tests and update the interoperability matrix
- **Status:** planned; tracked by Issue #12 and the existing transport Issue #5

## Key Questions

1. What is the smallest AMQP 0-9-1 profile that proves real interoperability without claiming full RabbitMQ compatibility?
2. Which Broker semantics belong in the pure, deterministic core and which belong in the native transport adapter?
3. How can the public development record show real progress without meaningless commit splitting?

## Decisions Made

| Decision | Rationale |
|----------|-----------|
| Use a MoonBit-native AMQP 0-9-1 Core Profile | The ecosystem gap is real, while full RabbitMQ compatibility is too large for the deadline |
| Separate pure protocol/core from native TCP transport | Keeps codec and embedded Broker testable and portable; async TCP is currently native-oriented |
| Implement an in-memory embedded Broker first | Enables a fully local demo and deterministic tests without requiring RabbitMQ |
| Use Apache-2.0 for the project | Clear OSI-compatible license for a new ecosystem library; third-party sources will be documented separately |
| Use meaningful Issues, branches, PRs and changelog entries | Matches the hackathon requirement for traceable development history |

## Errors Encountered

| Error | Attempt | Resolution |
|-------|---------|------------|
| `gh api user` could not read GitHub CLI config inside sandbox | 1 | Re-ran the read-only current-user query with escalation; result was `Zcxssxx` |
| `moon upgrade` failed with `not a terminal` | 1 | Re-ran in an interactive terminal and completed the update |

## Notes

- Current workspace started as an empty Git repository on `master`.
- Do not use or inspect historical GitHub CLI accounts.
- The current contest deadline is 2026-09-24; scope must prioritize a runnable, tested core over feature breadth.
