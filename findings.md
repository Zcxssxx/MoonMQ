# Findings & Decisions

## Requirements

- Build a new MoonBit project named MoonMQ.
- Implement an AMQP 0-9-1 protocol library and an embeddable Broker.
- Keep the project public and develop visibly with meaningful commits, Issues, PRs and update records.
- Use the authenticated GitHub account `Zcxssxx` only.
- Finish with a runnable local demonstration, tests, CI and clear documentation.

## Research Findings

- The September MoonBit Hackathon officially supports new projects and active maintenance projects, with project acceptance on 2026-09-24.
- The official directions include Web/network infrastructure and data/distributed systems, so AMQP plus a Broker is a strong thematic fit.
- RabbitMQ documents AMQP 0-9-1 as a real broker protocol built around exchanges, queues, bindings, consumers, acknowledgements, channels and virtual hosts.
- `zbhzs1/moonbit-mqtt` is a MoonBit MQTT 3.1.1 packet codec. Its README explicitly says it does not implement network transport, client runtime, broker sessions, TLS or MQTT 5.0.
- `nexacodes/moontask` provides a task-queue contract with memory/Redis backends, but it is not an AMQP protocol implementation or AMQP Broker.
- `moonbitlang/async` currently documents native/LLVM-oriented support for asynchronous I/O, so the TCP adapter should be isolated from the portable protocol and Broker core and validated on Linux CI.
- No confirmed MoonBit AMQP 0-9-1 library or Broker was found in the bounded Mooncakes/GitHub searches. This is evidence of a likely ecosystem gap, not proof that no unindexed project exists.
- An older Go project named `siddontang/moonmq` exists. This creates name/search ambiguity but not a functional MoonBit ecosystem overlap.
- The AMQP 0-9-1 spec archive provides machine-readable XML and reference material. MoonMQ should reference the specification and avoid copying RabbitMQ implementation code; any vendored/generated material must carry explicit provenance and license notes.

## Technical Decisions

| Decision | Rationale |
|----------|-----------|
| Project title: `MoonMQ — MoonBit AMQP 0-9-1 Core Profile & Embedded Broker` | Preserves the user's name while making the compatibility boundary explicit |
| Protocol profile: connection/channel handshake, frames, exchanges, queues, bindings, publish, consume, get, delivery, ack/reject, qos and heartbeat | Enough to demonstrate a usable message path and meaningful AMQP semantics |
| Broker core: in-memory, deterministic, direct/fanout/topic routing | Strong local demo, testability and feasible deadline |
| Transport: native TCP adapter separated from the core | Avoids coupling all packages to the current async backend limitation |
| Non-goals: AMQP 1.0, clustering, persistence, transactions, full RabbitMQ extensions, complex auth/ACL and TLS | Prevents an unfinishable promise |
| Root license: Apache-2.0 | Clear OSI-compatible project license |

## Issues Encountered

| Issue | Resolution |
|-------|------------|
| The prior workspace contained no MoonBit files or commits | Treat the repository as a clean new project and establish the design/record first |
| Full AMQP compatibility is much broader than the hackathon window | Define and document a Core Profile and test the supported boundary explicitly |

## Resources

- September Hackathon: https://moonbitlang.github.io/Hackathon2026/
- Formal charter: https://bxup9uklfcb.feishu.cn/wiki/Dx4Bwd6D1i3GfHkajQCcF7SznEd
- RabbitMQ AMQP model: https://www.rabbitmq.com/tutorials/amqp-concepts
- RabbitMQ AMQP 0-9-1 reference: https://github.com/rabbitmq/amqp-0.9.1-spec
- MoonBit MQTT package: https://mooncakes.io/docs/zbhzs1/moonbit-mqtt
- MoonBit async package: https://mooncakes.io/docs/moonbitlang/async
- Existing Go moonmq name: https://github.com/siddontang/moonmq

## Visual/Browser Findings

- The official September Hackathon page states: 9 月第一周—24 日集中开发, 9 月 24 日项目验收, public continuous commits/Issues/PRs, README, runnable example and tests.
- The page defines two quarterly-pool paths: monthly new project and community maintenance project.
- The Feishu document title is `2026 9月 MoonBit 黑客松大赛章程` and shows a 2026-09-03 update date; its visible content aligns with the official site's MoonBit/open-source/ecosystem positioning.
