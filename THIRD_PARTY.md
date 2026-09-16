# Third-party provenance

MoonMQ uses the following public references while implementing its documented AMQP 0-9-1 Core Profile:

- AMQP 0-9-1 specification archive: <https://github.com/rabbitmq/amqp-0.9.1-spec>
- RabbitMQ AMQP concepts: <https://www.rabbitmq.com/tutorials/amqp-concepts>
- MoonBit MQTT package used as an ecosystem comparison, not a dependency: <https://mooncakes.io/docs/zbhzs1/moonbit-mqtt>
- `moonbitlang/async@0.20.2` for the native TCP adapter; its upstream source and Apache-2.0 terms are published at <https://github.com/moonbitlang/async>.

MoonMQ does not copy source code from these references. The implementation and tests in this repository are authored for MoonBit and the supported profile documented in `docs/protocol-profile.md`.
