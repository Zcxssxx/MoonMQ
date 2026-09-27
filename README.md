# MoonMQ

MoonMQ is an open-source MoonBit implementation of a focused AMQP 0-9-1 protocol core and a deterministic, in-memory embedded broker.

一个 MoonBit 原生 AMQP 0-9-1 Core Profile 与嵌入式 Broker。

It targets a practical gap in the MoonBit ecosystem: the existing MQTT package provides MQTT packet codecs, while MoonMQ explores AMQP-compatible interoperability and a broker that can run inside a local MoonBit program without RabbitMQ or another service.

## Status

This repository is in active development for the 2026 MoonBit September Hackathon. The first release is intentionally a Core Profile, not a claim of complete RabbitMQ compatibility.

Current milestone:

- bounded AMQP protocol-header, frame, field-table, long-string, typed method, and Basic content-header codecs;
- a documented Core Profile covering connection/channel lifecycle, exchange/queue topology, and Basic delivery methods;
- pure in-memory default, direct, fanout, and topic routing;
- exchange deletion, exact queue unbind, idle-queue deletion with explicit busy-queue restrictions, and channel-scoped multiple acknowledgements;
- pull and push consumption with acknowledgements, reject/requeue, prefetch, and cancellation cleanup;
- a portable session state machine covering the tested handshake, channel, topology, publish, get, consume, cancel, ack, and reject paths;
- a native TCP adapter built on `moonbitlang/async@0.20.2`, with fragmented-input handling and a real loopback handshake test;
- an embedded local demo that runs without RabbitMQ or any other service.

Queue deletion is intentionally limited to queues without active consumers or unsettled deliveries. Cross-session cancellation notifications and full RabbitMQ compatibility remain out of scope. Next milestone: expand the external-client interoperability matrix without widening the compatibility claim.

Out of scope for the first release: AMQP 1.0, persistence/recovery, clustering, replication, transactions, publisher confirms, TLS, external authentication/ACL, and the full RabbitMQ extension surface.

## Development

The project currently records the tested toolchain in [`docs/updates.md`](docs/updates.md). From the repository root:

```text
moon check --deny-warn --target all
moon test --deny-warn --target all
moon fmt --check
moon build --target native cmd/moonmq-server
```

The self-contained demos are available as:

```text
moon run cmd/moonmq
moon run examples/embedded
moon run --target native cmd/moonmq-server
```

See [`docs/protocol-profile.md`](docs/protocol-profile.md) for the compatibility boundary, [`docs/interop.md`](docs/interop.md) for the current interoperability policy, and [`docs/updates.md`](docs/updates.md) for the development record.

## Project records

Public milestones are tracked through Issues, a focused branch, pull requests, meaningful commits, and dated entries in [`docs/updates.md`](docs/updates.md). The project is being built for the [2026 MoonBit September Hackathon](https://moonbitlang.github.io/Hackathon2026/).

## License

MoonMQ is released under the Apache License 2.0. See [`LICENSE`](LICENSE) and [`THIRD_PARTY.md`](THIRD_PARTY.md).
