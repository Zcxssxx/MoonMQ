# Embedded demo

This example constructs a broker in the current MoonBit process, publishes a byte payload through the default exchange, pulls it from a queue, and acknowledges the delivery.

```text
moon run examples/embedded
```

It does not start RabbitMQ, open a network port, or require another service.
