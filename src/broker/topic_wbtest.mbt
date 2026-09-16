///|
test "topic matching follows AMQP star and hash segments" {
  assert_true(topic_matches("logs.*", "logs.info"))
  assert_true(!topic_matches("logs.*", "logs.info.detail"))
  assert_true(topic_matches("logs.#", "logs.info.detail"))
  assert_true(topic_matches("#", ""))
  assert_true(!topic_matches("metrics.#", "logs.info"))
}
