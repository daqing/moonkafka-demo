///|
/// A deterministic `Options` value for the pure unit tests below:
/// `parse_options` reads the real process arguments and cannot be used here.
fn sample_options(compose_bin : String) -> Options {
  {
    runtime: "auto",
    compose_bin,
    compose_file: "docker-compose.kafka.yml",
    run_mode: "auto",
    image: "",
    container: "moonkafka-demo-kafka",
    host: "127.0.0.1",
    port: 9092,
    repo: ".",
    target: "native",
    cli: "",
    topic: "itest-1",
    topic_live: "itest-1-live",
    topic_given: false,
    partitions: 1,
    timeout_secs: 120,
    live_delay_ms: 2000,
    no_container: false,
    keep: false,
    dry_run: false,
  }
}

///|
test "container runtime candidates" {
  assert_eq(runtime_candidates("auto"), ["podman", "docker"])
  assert_eq(runtime_candidates("docker"), ["docker"])
  assert_eq(runtime_candidates("/usr/bin/podman"), ["/usr/bin/podman"])
}

///|
test "compose step shape" {
  assert_eq(
    render_step(
      compose_step(
        sample_options(""),
        { program: "podman", words: ["compose"], },
        ["up", "-d"],
      ),
    ),
    "podman compose -f docker-compose.kafka.yml up -d",
  )
  assert_eq(
    render_step(
      compose_step(
        sample_options("podman-compose"),
        { program: "podman-compose", words: [], },
        ["down"],
      ),
    ),
    "podman-compose -f docker-compose.kafka.yml down",
  )
}

///|
test "topic creation quotes nothing but passes the documented argv" {
  assert_eq(
    render_step(create_topic_step(sample_options(""), "podman", "itest-1")),
    "podman exec moonkafka-demo-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --create --if-not-exists --topic itest-1 --partitions 1 --replication-factor 1",
  )
}

///|
test "cli steps drive the built binary with the documented argv" {
  let opt = sample_options("")
  let cli = "/tmp/main.exe"
  assert_eq(
    render_step(produce_step(opt, cli, "events", "hello", "k")),
    "/tmp/main.exe produce events hello k 127.0.0.1 9092",
  )
  assert_eq(
    render_step(consume_step(opt, cli, "events", "earliest", 1)),
    "/tmp/main.exe consume events 127.0.0.1 9092 earliest --max-messages 1",
  )
}

///|
test "default cli path follows the toolchain layout" {
  assert_eq(
    default_cli_path(sample_options("")),
    "_build/native/debug/build/cmd/main/main.exe",
  )
  assert_eq(
    render_step(produce_step(sample_options(""), "", "t", "v", "k")),
    "_build/native/debug/build/cmd/main/main.exe produce t v k 127.0.0.1 9092",
  )
}

///|
test "render_step quotes arguments containing spaces" {
  let step = new_step("probe", "podman", ["exec", "my container", "--list"])
  assert_eq(render_step(step), "podman exec \"my container\" --list")
}

///|
test "parse_nonneg_int" {
  assert_eq(parse_nonneg_int("0"), Some(0))
  assert_eq(parse_nonneg_int("42"), Some(42))
  assert_eq(parse_nonneg_int("007"), Some(7))
  assert_eq(parse_nonneg_int(""), None)
  assert_eq(parse_nonneg_int("1x"), None)
  assert_eq(parse_nonneg_int("-1"), None)
}

///|
test "parse_sent_offset" {
  assert_eq(
    parse_sent_offset("sent to topic \"events\" at offset 0\n"),
    Some(0),
  )
  assert_eq(
    parse_sent_offset("sent to topic \"events\" at offset 17\n"),
    Some(17),
  )
  assert_eq(parse_sent_offset("TransportError.ConnectionClosed"), None)
}

///|
test "parse_records keeps the whole value, spaces included" {
  let stdout = "consuming topic \"events\" from 127.0.0.1:9092; press Ctrl-C to stop\noffset=3 timestamp=1757933012345 key=k1 value=hello world\n"
  let records = parse_records(stdout)
  assert_eq(records.length(), 1)
  assert_eq(records[0].offset, 3)
  assert_eq(records[0].key, "k1")
  assert_eq(records[0].value, "hello world")
  assert_eq(offset_of(records, "k1", "hello world"), Some(3))
  assert_true(records_contain(records, "k1", "hello world"))
  assert_false(records_contain(records, "k1", "something else"))
}

///|
test "parse_records tolerates tombstones and other lines" {
  let stdout =
    #|usage: consume <topic> [host] [port] [earliest|latest]
    #|offset=0 timestamp=1 key=<null> value=<tombstone>
  let records = parse_records(stdout)
  assert_eq(records.length(), 1)
  assert_eq(records[0].key, "<null>")
  assert_eq(records[0].value, "<tombstone>")
}
