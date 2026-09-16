# MoonMQ AMQP 0-9-1 Core Profile

This document is the compatibility contract for the first release. It is narrower than the entire RabbitMQ feature set and will be expanded only through a recorded design change.

## Planned supported surface

| Area | Methods / behavior |
| --- | --- |
| Framing | protocol header, method, content-header, body, heartbeat frames; frame terminator and size validation |
| Connection | `start`, `start-ok`, `tune`, `tune-ok`, `open`, `open-ok`, `close`, `close-ok` |
| Channel | `open`, `open-ok`, `close`, `close-ok` |
| Exchange | declare/delete for direct, fanout, and topic exchanges |
| Queue | declare/delete/bind/unbind; default exchange; generated queue names |
| Basic | qos, publish, consume, cancel, deliver, get, get-ok, get-empty, ack, reject |
| Broker | in-memory topology, deterministic routing, push/pull delivery, prefetch, ack/requeue |
| Transport | portable core first; native TCP adapter is isolated and documented by platform |

## Explicit non-goals

AMQP 1.0 and 0-10, persistence/recovery, replication, clustering, transactions, publisher confirms, alternate exchanges, dead-lettering, priorities, plugins, TLS, authentication/ACL, and full RabbitMQ extension compatibility are not first-release promises.

## Interoperability position

The wire codec aims to be spec-driven and bounds-checked. Interoperability claims will be limited to the methods and content properties covered by tests. The embedded demo is the default verification path and requires no external broker.
