# MoonMQ

MoonMQ is an open-source MoonBit implementation of a focused AMQP 0-9-1 protocol core and a deterministic, in-memory embedded broker.

It targets a practical gap in the MoonBit ecosystem: the existing MQTT package provides MQTT packet codecs, while MoonMQ explores AMQP-compatible interoperability and a broker that can run inside a local MoonBit program without RabbitMQ or another service.

## Status

This repository is in active development for the 2026 MoonBit September Hackathon. The first release is intentionally a Core Profile, not a claim of complete RabbitMQ compatibility.

Planned first-release capabilities:

- bounded AMQP protocol-header, frame, method, content-header, and body codecs;
- connection/channel handshake and a documented AMQP 0-9-1 method subset;
- pure in-memory direct, fanout, and topic routing;
- pull and push consumption with acknowledgements, reject/requeue, and prefetch;
- an embedded local demo, with the native TCP adapter kept separate from the portable core.

Out of scope for the first release: AMQP 1.0, persistence/recovery, clustering, replication, transactions, publisher confirms, TLS, authentication/ACL, and the full RabbitMQ extension surface.

## Development

The project currently records the tested toolchain in [`docs/updates.md`](docs/updates.md). From the repository root:

```text
moon check
moon test
moon fmt --check
```

The self-contained demo will be available as:

```text
moon run cmd/moonmq
```

See [`docs/protocol-profile.md`](docs/protocol-profile.md) for the compatibility boundary and [`docs/superpowers/specs/2026-09-16-moonmq-design.md`](docs/superpowers/specs/2026-09-16-moonmq-design.md) for the design rationale.

## Project records

Every milestone is developed through an Issue, a focused branch and pull request, meaningful commits, and a dated entry in [`docs/updates.md`](docs/updates.md). The project is being built for the [2026 MoonBit September Hackathon](https://moonbitlang.github.io/Hackathon2026/).

## License

MoonMQ is released under the Apache License 2.0. See [`LICENSE`](LICENSE) and [`THIRD_PARTY.md`](THIRD_PARTY.md).
