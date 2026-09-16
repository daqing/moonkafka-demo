///|
test "CLI defaults" {
  assert_eq(parse_port([], 0), 9092)
  assert_true(parse_start_from(None) is @moonkafka.StartFrom::Earliest)
  assert_true(parse_start_from(Some("latest")) is @moonkafka.StartFrom::Latest)
}

///|
test "take_max_messages" {
  let (none_limit, positionals) = take_max_messages([
    "prog", "consume", "events",
  ])
  assert_true(none_limit is None)
  assert_eq(positionals, ["prog", "consume", "events"])
  let (limit, positionals) = take_max_messages([
    "prog", "consume", "events", "127.0.0.1", "9092", "latest", "--max-messages",
    "2",
  ])
  assert_true(limit is Some(2))
  assert_eq(positionals, [
    "prog", "consume", "events", "127.0.0.1", "9092", "latest",
  ])
  let (limit, positionals) = take_max_messages([
    "prog", "consume", "--max-messages=5", "events",
  ])
  assert_true(limit is Some(5))
  assert_eq(positionals, ["prog", "consume", "events"])
}
