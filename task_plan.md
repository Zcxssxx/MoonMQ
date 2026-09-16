# Task Plan: MoonMQ AMQP 0-9-1 Core Profile

## Goal

交付一个以 MoonBit 为主要实现语言的 MoonMQ：包含 AMQP 0-9-1 Core Profile 协议编解码器、纯内存嵌入式 Broker、native TCP 适配器、本地演示、测试、CI、文档和可追踪的 GitHub 开发记录。

## Current Phase

Phase 1: Requirements & Discovery

## Phases

### Phase 1: Requirements & Discovery

- [x] Confirm contest and project scope
- [x] Verify current GitHub identity is `Zcxssxx`
- [x] Verify MoonBit toolchain
- [x] Research MoonBit ecosystem overlap
- **Status:** complete

### Phase 2: Planning & Structure

- [ ] Write and commit design specification
- [ ] Write and self-review implementation plan
- [ ] Create public GitHub repository and issue workflow
- **Status:** in_progress

### Phase 3: Implementation

- [ ] Add protocol value and frame codecs with failing tests first
- [ ] Add AMQP method codecs and Core Profile dispatch
- [ ] Add pure in-memory Broker topology and delivery semantics
- [ ] Add native TCP connection/channel adapter
- [ ] Add embedded and interoperability demos
- **Status:** pending

### Phase 4: Testing & Verification

- [ ] Run `moon check --target all`
- [ ] Run `moon test --target all`
- [ ] Run native integration and demo smoke tests
- [ ] Run `moon fmt --check` or the current equivalent
- [ ] Run `moon info` and review public interface changes
- **Status:** pending

### Phase 5: Delivery

- [ ] Finish README, changelog, license and compatibility notes
- [ ] Configure GitHub Actions CI
- [ ] Publish meaningful commits, Issues and PRs on the default branch
- [ ] Prepare Mooncakes publication metadata and acceptance checklist
- **Status:** pending

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
