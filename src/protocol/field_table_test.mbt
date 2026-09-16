///|
test "field tables round trip supported scalar and nested values" {
  let expected = FieldTable::new([
    ("enabled", FieldBoolean(true)),
    ("name", FieldLongString(b"moon")),
    ("payload", FieldLongString(b"\x00\xFF")),
    ("nested", FieldTableValue(FieldTable::new([("count", FieldInt(42))]))),
    ("items", FieldArray([FieldBoolean(false), FieldVoid])),
  ])
  match encode_field_table(expected) {
    Ok(encoded) =>
      match decode_field_table(encoded) {
        Ok(actual) => assert_eq(actual, expected)
        Err(_) => fail("field table decoding unexpectedly failed")
      }
    Err(_) => fail("field table encoding unexpectedly failed")
  }
}

///|
test "empty field tables use a zero length prefix" {
  let empty = FieldTable::new([])
  match encode_field_table(empty) {
    Ok(encoded) => {
      assert_eq(encoded, b"\x00\x00\x00\x00")
      assert_true(decode_field_table(encoded) is Ok(_))
    }
    Err(_) => fail("empty field table encoding unexpectedly failed")
  }
}

///|
test "field table decoder rejects malformed and unknown values" {
  assert_true(decode_field_table(b"\x00\x00\x00\x05\x01aS\x00") is Err(_))
  assert_true(decode_field_table(b"\x00\x00\x00\x03\x01aX") is Err(_))
  assert_true(decode_field_table(b"\x00\x00\x00\x02\x01a") is Err(_))
  assert_true(
    decode_field_table(b"\x00\x00\x00\x03\x01aS\x00\x00\x00") is Err(_),
  )
}
