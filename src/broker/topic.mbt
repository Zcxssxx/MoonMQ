///|
fn topic_matches(pattern : String, routing_key : String) -> Bool {
  let pattern_parts = pattern.split(".").map(x => x.to_owned()).to_array()
  let key_parts = routing_key.split(".").map(x => x.to_owned()).to_array()
  topic_matches_at(pattern_parts, 0, key_parts, 0)
}

///|
fn topic_matches_at(
  pattern : Array[String],
  pattern_index : Int,
  key : Array[String],
  key_index : Int,
) -> Bool {
  if pattern_index == pattern.length() {
    return key_index == key.length()
  }
  let current = pattern[pattern_index]
  if current == "#" {
    topic_matches_at(pattern, pattern_index + 1, key, key_index) ||
    (
      key_index < key.length() &&
      topic_matches_at(pattern, pattern_index, key, key_index + 1)
    )
  } else if key_index < key.length() &&
    (current == "*" || current == key[key_index]) {
    topic_matches_at(pattern, pattern_index + 1, key, key_index + 1)
  } else {
    false
  }
}
