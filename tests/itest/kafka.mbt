///|
/// Candidate container runtimes, in preference order.
fn runtime_candidates(runtime : String) -> Array[String] {
  if runtime == "auto" {
    ["podman", "docker"]
  } else {
    [runtime]
  }
}

///|
/// Picks a container engine that actually works.
///
/// `docker version` and `podman version` both fail when the engine or its VM is
/// not running, so probing with them avoids picking an installed-but-unusable
/// engine and then failing later with a confusing error.
async fn detect_engine(opt : Options, runner : Runner) -> String {
  for candidate in runtime_candidates(opt.runtime) {
    let probe = new_step("probe container runtime", candidate, ["version"])
    announce(probe)
    if step_output(probe, opt.repo, 60_000).exit_code == 0 {
      println("  -> container runtime: \{candidate}")
      return candidate
    }
    println("  ... \{candidate} is not usable")
  }
  runner.fail(
    "no working container runtime (tried \{[..runtime_candidates(opt.runtime)].join(", ")}); install podman or docker, or run with --no-container",
  )
  ""
}

///|
/// A compose provider: the program to run plus the words that precede the compose
/// arguments. `podman compose` and `docker compose` are plugins and carry
/// `["compose"]`; the standalone `podman-compose` / `docker-compose` binaries
/// carry nothing.
struct ComposeProvider {
  program : String
  words : Array[String]
}

///|
async fn probe_provider(opt : Options, provider : ComposeProvider) -> Bool {
  let probe = new_step(
    "probe compose provider",
    provider.program,
    [..provider.words, "version"],
  )
  announce(probe)
  step_output(probe, opt.repo, 60_000).exit_code == 0
}

///|
/// Finds the compose provider for `engine`, or `None` when it has none.
///
/// A podman installation without `podman-compose` answers neither probe, and that
/// is exactly when the harness switches to driving `podman run` itself.
async fn detect_provider(opt : Options, engine : String) -> ComposeProvider? {
  if !opt.compose_bin.is_blank() {
    let forced : ComposeProvider = { program: opt.compose_bin, words: [], }
    return if probe_provider(opt, forced) { Some(forced) } else { None }
  }
  let candidates : Array[ComposeProvider] = [
    { program: engine, words: ["compose"], },
    { program: engine + "-compose", words: [], },
  ]
  for provider in candidates {
    if probe_provider(opt, provider) {
      return Some(provider)
    }
  }
  None
}

///|
/// The compose command for a provider: `<program> <words...> -f <file> <action...>`.
fn compose_step(
  opt : Options,
  provider : ComposeProvider,
  action : Array[String],
) -> Step {
  new_step(
    "compose \{[..action].join(" ")}",
    provider.program,
    [..provider.words, "-f", opt.compose_file, ..action],
  )
}

///|
/// Starts Kafka with `<engine> run`, for engines that have no compose provider.
///
/// These settings mirror the single-broker `kafka:` service in
/// docker-compose.kafka.yml: the compose file is what a reader consults, this is
/// what runs when compose is unavailable (a common podman setup, where
/// `podman compose` needs `podman-compose`).
fn direct_run_step(opt : Options, engine : String, image : String) -> Step {
  new_step("run Kafka directly (\{engine} has no compose provider)", engine, [
    "run",
    "-d",
    "--name",
    opt.container,
    "-p",
    "\{opt.port}:\{opt.port}",
    "-e",
    "KAFKA_NODE_ID=1",
    "-e",
    "KAFKA_PROCESS_ROLES=broker,controller",
    "-e",
    "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
    "-e",
    "KAFKA_LISTENERS=CONTROLLER://:9093,PLAINTEXT://:\{opt.port}",
    "-e",
    "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
    "-e",
    "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
    "-e",
    "KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT",
    "-e",
    "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:\{opt.port}",
    "-e",
    "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
    "-e",
    "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
    "-e",
    "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
    "-e",
    "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
    image,
  ])
}

///|
fn direct_remove_step(opt : Options, engine : String) -> Step {
  new_step("remove the Kafka container", engine, ["rm", "-f", opt.container])
}

///|
fn direct_inspect_step(opt : Options, engine : String) -> Step {
  new_step("inspect the Kafka container", engine, [
    "inspect",
    "--format",
    "{{.State.Running}}",
    opt.container,
  ])
}

///|
/// Reads the image tag out of the compose file, the way upstream's cluster script
/// does, so the compose and direct paths cannot drift apart.
async fn image_from_compose_file(opt : Options) -> String? {
  let text = @fs.read_file("\{opt.repo}/\{opt.compose_file}").text()
  for line in text.split("\n") {
    guard line.split_once("image:") is Some((prefix, rest)) else { continue }
    if !prefix.trim().is_blank() {
      continue
    }
    let image = rest.trim()
    if !image.is_blank() {
      return Some(image.to_owned())
    }
  }
  None
}

///|
/// The image tag a direct `run` would use: `--image`, else the compose file.
async fn plan_image(opt : Options) -> String? {
  if !opt.image.is_blank() {
    return Some(opt.image)
  }
  image_from_compose_file(opt) catch {
    _ => None
  }
}

///|
/// How Kafka is started and stopped. Resolved once, so the bring-up, the teardown
/// and the dry-run plan all agree.
struct Cluster {
  engine : String
  /// `compose` or `run`; used for reporting.
  mode : String
  up : Step
  down : Step
}

///|
/// Works out how Kafka will be managed for this run.
async fn plan_cluster(opt : Options, runner : Runner) -> Cluster? {
  if opt.run_mode != "auto" &&
    opt.run_mode != "compose" &&
    opt.run_mode != "run" {
    runner.fail("--run-mode must be auto, compose or run (got \{opt.run_mode})")
    return None
  }
  let engine = detect_engine(opt, runner)
  if engine.is_blank() {
    return None
  }
  let provider = if opt.run_mode == "run" {
    None
  } else {
    detect_provider(opt, engine)
  }
  match provider {
    Some(provider) => {
      println(
        "  -> compose provider: \{provider.program} \{[..provider.words].join(" ")}",
      )
      Some({
        engine,
        mode: "compose",
        up: compose_step(opt, provider, ["up", "-d"]),
        down: compose_step(opt, provider, ["down"]),
      })
    }
    None => {
      if opt.run_mode == "compose" {
        runner.fail(
          "--run-mode compose, but no compose provider answers for \{engine}",
        )
        return None
      }
      match plan_image(opt) {
        Some(image) => {
          println(
            "  -> no compose provider; driving \{engine} run with \{image}",
          )
          Some({
            engine,
            mode: "run",
            up: direct_run_step(opt, engine, image),
            down: direct_remove_step(opt, engine),
          })
        }
        None => {
          runner.fail(
            "\{engine} has no compose provider and no image tag is known; pass --image <tag> or install a compose provider",
          )
          None
        }
      }
    }
  }
}

///|
/// Brings the cluster up. With compose this is one command; without a provider the
/// container is replaced first, unless an equivalent one is already running.
async fn cluster_up(opt : Options, runner : Runner, cluster : Cluster) -> Bool {
  if cluster.mode == "compose" {
    return run_required(
      runner,
      cluster.up,
      opt.repo,
      600_000,
      "start the Kafka container",
    )
  }
  let inspect = direct_inspect_step(opt, cluster.engine)
  announce(inspect)
  let state = step_output(inspect, opt.repo, 60_000)
  if state.exit_code == 0 && state.stdout.trim() == "true" {
    println("  ... \{opt.container} is already running; reusing it")
    return true
  }
  println("  # removing any stale \{opt.container}")
  ignore(step_output(direct_remove_step(opt, cluster.engine), opt.repo, 60_000))
  run_required(
    runner,
    cluster.up,
    opt.repo,
    900_000,
    "start the Kafka container (this may have to pull the image)",
  )
}

///|
/// Kafka's own command line tools, which the image ships at this path (the
/// compose healthcheck uses the very same one).
const KAFKA_TOPICS_SH : String = "/opt/kafka/bin/kafka-topics.sh"

///|
/// The in-container bootstrap address. The bundled compose file publishes
/// `opt.port` on the host and listens on the same port inside the container.
fn bootstrap(opt : Options) -> String {
  "\{opt.host}:\{opt.port}"
}

///|
/// Runs a Kafka tool inside the running container.
fn exec_step(opt : Options, runtime : String, argv : Array[String]) -> Step {
  new_step(
    "kafka tool in \{opt.container}",
    runtime,
    ["exec", opt.container, ..argv],
  )
}

///|
/// Polls the broker until `kafka-topics.sh --list` succeeds, mirroring the
/// healthcheck in docker-compose.kafka.yml. This is the one wait in the harness
/// that is bounded only by a deadline, and the deadline is always enforced.
async fn wait_until_ready(
  opt : Options,
  runtime : String,
  runner : Runner,
) -> Bool {
  let deadline = @async.now() + opt.timeout_secs.to_int64() * 1000
  let list = exec_step(opt, runtime, [
    KAFKA_TOPICS_SH,
    "--bootstrap-server",
    bootstrap(opt),
    "--list",
  ])
  announce(list)
  let mut ready = false
  while @async.now() < deadline {
    if step_output(list, opt.repo, 60_000).exit_code == 0 {
      ready = true
      break
    }
    @async.sleep(2_000)
  }
  if ready {
    println("  -> broker is ready")
  } else {
    runner.fail("Kafka was not ready within \{opt.timeout_secs}s")
  }
  ready
}

///|
/// Creates a topic idempotently, the way the README tells a user to.
fn create_topic_step(opt : Options, runtime : String, topic : String) -> Step {
  exec_step(opt, runtime, [
    KAFKA_TOPICS_SH,
    "--bootstrap-server",
    bootstrap(opt),
    "--create",
    "--if-not-exists",
    "--topic",
    topic,
    "--partitions",
    "\{opt.partitions}",
    "--replication-factor",
    "1",
  ])
}

///|
/// `kafka-topics.sh --describe --topic <topic>`: used to confirm the partitions
/// have elected leaders.
fn describe_topic_step(opt : Options, engine : String, topic : String) -> Step {
  exec_step(opt, engine, [
    KAFKA_TOPICS_SH,
    "--bootstrap-server",
    bootstrap(opt),
    "--describe",
    "--topic",
    topic,
  ])
}

///|
/// Waits until every partition of `topic` has a leader.
///
/// A topic can report itself created while its partitions still say `Leader: -1`,
/// and a produce sent in that window is rejected immediately. Polling `--describe`
/// removes that race without an arbitrary sleep.
async fn wait_topic_ready(
  opt : Options,
  engine : String,
  topic : String,
  runner : Runner,
) -> Bool {
  let deadline = @async.now() + 60_000
  let describe = describe_topic_step(opt, engine, topic)
  announce(describe)
  let mut ready = false
  while @async.now() < deadline {
    let result = step_output(describe, opt.repo, 60_000)
    let described = result.exit_code == 0 && result.stdout.contains("Leader: ")
    let elected = !result.stdout.contains("Leader: -1")
    if described && elected {
      ready = true
      break
    }
    @async.sleep(1_000)
  }
  if ready {
    println("  -> topic \{topic} has a partition leader")
  } else {
    runner.fail("topic \{topic} did not get a partition leader within 60s")
  }
  ready
}
