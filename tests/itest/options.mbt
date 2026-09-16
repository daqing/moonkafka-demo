///|
/// Everything the harness needs, with defaults that match the compose file and
/// the CLI shipped in this repository so that a bare run works.
struct Options {
  /// `auto`, a runtime name (`podman`/`docker`) or a path to one.
  runtime : String
  /// Explicit compose binary; empty means `<runtime> compose`.
  compose_bin : String
  compose_file : String
  /// How Kafka is managed: `auto`, `compose` or `run`.
  run_mode : String
  /// Image tag for the direct `run` path; empty reads it from the compose file.
  image : String
  container : String
  host : String
  port : Int
  /// Repository root; `moon` and the compose file are resolved from here.
  repo : String
  /// Build target whose artifacts the harness executes.
  target : String
  /// CLI binary to drive; empty means the native debug build of cmd/main.
  cli : String
  /// Topic for the sequential phase.
  topic : String
  /// Topic for the live-delivery phase.
  topic_live : String
  /// Whether the caller named the topic, or it was generated per run.
  topic_given : Bool
  partitions : Int
  timeout_secs : Int
  live_delay_ms : Int
  no_container : Bool
  keep : Bool
  dry_run : Bool
}

///|
fn option(
  matches : @argparse.Matches,
  key : String,
  fallback : String,
) -> String {
  match matches.values.get(key) {
    Some([value, ..]) => value
    _ => fallback
  }
}

///|
fn int_option(matches : @argparse.Matches, key : String) -> Int raise {
  @string.parse_int(option(matches, key, "0"))
}

///|
fn bool_flag(matches : @argparse.Matches, key : String) -> Bool {
  matches.flags.get(key).unwrap_or(false)
}

///|
fn parse_options() -> Options raise {
  let matches = @argparse.parse(
    @argparse.Command(
      "itest",
      about="End-to-end integration test: start Kafka in a container, then drive the demo CLI against it.",
      flags=[
        @argparse.FlagArg(
          "no-container",
          long="no-container",
          about="Use an already running Kafka: skip container up/down and topic creation.",
        ),
        @argparse.FlagArg(
          "keep",
          long="keep",
          about="Leave Kafka running when the test finishes.",
        ),
        @argparse.FlagArg(
          "dry-run",
          long="dry-run",
          about="Print the plan and exit without running anything.",
        ),
      ],
      options=[
        @argparse.OptionArg(
          "runtime",
          long="runtime",
          default_values=["auto"],
          about="Container runtime: auto, podman, docker or a path.",
        ),
        @argparse.OptionArg(
          "compose-bin",
          long="compose-bin",
          default_values=[""],
          about="Compose binary; default is '<runtime> compose'.",
        ),
        @argparse.OptionArg(
          "compose-file",
          long="compose-file",
          default_values=["docker-compose.kafka.yml"],
          about="Compose file used to start Kafka.",
        ),
        @argparse.OptionArg(
          "run-mode",
          long="run-mode",
          default_values=["auto"],
          about="How to manage Kafka: auto, compose or run.",
        ),
        @argparse.OptionArg(
          "image",
          long="image",
          default_values=[""],
          about="Container image for the direct run path; default is read from the compose file.",
        ),
        @argparse.OptionArg(
          "container",
          long="container",
          default_values=["moonkafka-demo-kafka"],
          about="Name of the Kafka container.",
        ),
        @argparse.OptionArg(
          "host",
          long="host",
          default_values=["127.0.0.1"],
          about="Broker host the CLI connects to.",
        ),
        @argparse.OptionArg(
          "port",
          long="port",
          default_values=["9092"],
          about="Broker port the CLI connects to.",
        ),
        @argparse.OptionArg(
          "target",
          long="target",
          default_values=["native"],
          about="Build target whose artifacts are executed.",
        ),
        @argparse.OptionArg(
          "cli",
          long="cli",
          default_values=[""],
          about="CLI binary to drive; default is the native debug build.",
        ),
        @argparse.OptionArg(
          "repo",
          long="repo",
          default_values=["."],
          about="Repository root (where moon.mod lives).",
        ),
        @argparse.OptionArg(
          "topic",
          long="topic",
          default_values=[""],
          about="Topic name; default is a unique name per run.",
        ),
        @argparse.OptionArg(
          "partitions",
          long="partitions",
          default_values=["1"],
          about="Partitions for the topics this harness creates.",
        ),
        @argparse.OptionArg(
          "timeout-secs",
          long="timeout-secs",
          default_values=["120"],
          about="Budget for waiting until Kafka is ready.",
        ),
        @argparse.OptionArg(
          "live-delay-ms",
          long="live-delay-ms",
          default_values=["2000"],
          about="Delay before producing in the live-delivery phase.",
        ),
      ],
    ),
  )
  let given = option(matches, "topic", "")
  let topic = if given.is_blank() { "itest-\{@async.now()}" } else { given }
  {
    runtime: option(matches, "runtime", "auto"),
    compose_bin: option(matches, "compose-bin", ""),
    compose_file: option(matches, "compose-file", "docker-compose.kafka.yml"),
    run_mode: option(matches, "run-mode", "auto"),
    image: option(matches, "image", ""),
    container: option(matches, "container", "moonkafka-demo-kafka"),
    host: option(matches, "host", "127.0.0.1"),
    port: int_option(matches, "port"),
    repo: option(matches, "repo", "."),
    target: option(matches, "target", "native"),
    cli: option(matches, "cli", ""),
    topic,
    topic_live: topic + "-live",
    topic_given: !given.is_blank(),
    partitions: int_option(matches, "partitions"),
    timeout_secs: int_option(matches, "timeout-secs"),
    live_delay_ms: int_option(matches, "live-delay-ms"),
    no_container: bool_flag(matches, "no-container"),
    keep: bool_flag(matches, "keep"),
    dry_run: bool_flag(matches, "dry-run"),
  }
}
