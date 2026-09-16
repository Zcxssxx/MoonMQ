# MoonMQ development updates

This log is intentionally chronological. Each implementation milestone will link its GitHub Issue and pull request after the public repository is created.

## 2026-09-16 — project and design baseline

- Confirmed the project direction: a MoonBit AMQP 0-9-1 Core Profile plus a pure in-memory embedded Broker.
- Researched the adjacent MoonBit MQTT codec package, async I/O constraints, task-queue packages, RabbitMQ AMQP concepts, and the public AMQP 0-9-1 specification archive.
- Updated and verified the local MoonBit toolchain: `moon 0.1.20260915`, `moonc v0.10.13+cbb11c36f`, `moonrun 0.1.20260915`.
- Committed the design and implementation plan as `a4eba1c` (`docs: define MoonMQ AMQP core profile`).
- GitHub CLI identity verified as `Zcxssxx`; no historical account list was read.

## Record format for future entries

Each milestone entry will include its date, Issue/PR links, user-visible behavior, verification commands, compatibility limitations, and commit summary.
