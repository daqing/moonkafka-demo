///|
/// End-to-end integration test for the moonkafka demo.
///
/// It starts Kafka in a container (podman or docker), creates fresh topics, then
/// drives the real `cmd/main` CLI through `moon run`: a produce/consume pair, and
/// a consumer that is already polling while a message is produced.
///
/// This is deliberately **not** part of `moon test`: it needs a container runtime
/// and a broker, so it only runs when asked for. See `make itest`.
async fn main {
  let opt = parse_options()
  let runner = new_runner()
  banner(opt)
  if opt.dry_run {
    print_plan(opt)
    return
  }

  // The harness executes the built CLI, so build it here: this is where a compile
  // error surfaces, and no timed phase then pays for a build.
  ignore(
    run_required(
      runner,
      new_step("build the demo CLI before any timed phase", "moon", [
        "build",
        "--target",
        opt.target,
      ]),
      opt.repo,
      900_000,
      "build the demo CLI",
    ),
  )

  // Unique payloads per run, so nothing a previous run left behind can satisfy
  // an assertion.
  let stamp = @async.now()
  let key = "itest-key-\{stamp}"
  let value_sequential = "itest-value-a-\{stamp}"
  let value_live = "itest-value-b-\{stamp}"

  let cli = if runner.aborted { "" } else { resolve_cli(opt, runner) }

  let mut cluster : Cluster? = None
  if opt.no_container {
    if !opt.topic_given {
      runner.fail(
        "--no-container cannot create a topic; pass --topic <a fresh, empty topic>",
      )
    }
  } else {
    cluster = plan_cluster(opt, runner)
  }

  if !runner.aborted && !opt.no_container {
    phase_header("bring up Kafka")
    match cluster {
      Some(cluster) =>
        if cluster_up(opt, runner, cluster) {
          if wait_until_ready(opt, cluster.engine, runner) {
            for topic in [opt.topic, opt.topic_live] {
              if !run_required(
                  runner,
                  create_topic_step(opt, cluster.engine, topic),
                  opt.repo,
                  120_000,
                  "create topic \{topic}",
                ) {
                break
              }
              if !wait_topic_ready(opt, cluster.engine, topic, runner) {
                break
              }
            }
          }
        }
      None => ()
    }
  }

  if !runner.aborted {
    phase_sequential(opt, runner, cli, key, value_sequential)
  }
  if !runner.aborted {
    if opt.no_container {
      println("")
      println(
        "== phase 2 (live delivery) SKIPPED: --no-container cannot create the empty topic it needs",
      )
    } else {
      phase_live(opt, runner, cli, key, value_live)
    }
  }

  match cluster {
    Some(cluster) => {
      println("")
      if opt.keep {
        println("(leaving the Kafka container running: --keep)")
      } else {
        let stopped = run_observed(cluster.down, opt.repo, 300_000)
        runner.check(stopped.exit_code == 0, "the Kafka container is stopped")
      }
    }
    None => ()
  }

  println("")
  println("---")
  println("checks=\{runner.checks} failures=\{runner.failures}")
  if runner.failures > 0 {
    raise ItestError::Aborted(
      "\{runner.failures} of \{runner.checks} checks failed",
    )
  }
  println("integration test passed")
}

///|
fn banner(opt : Options) -> Unit {
  println("moonkafka demo - integration test")
  println("  broker     \{opt.host}:\{opt.port}")
  println("  topic      \{opt.topic}")
  println("  partitions \{opt.partitions}")
  println("  repo       \{opt.repo}")
  if opt.no_container {
    println("  container  (not managed: --no-container)")
  } else {
    println("  container  \{opt.container}")
    println("  runtime    \{opt.runtime}")
  }
}

///|
/// Phase 1: a message is produced first and read back from the beginning of the
/// topic, so nothing in this phase depends on timing.
async fn phase_sequential(
  opt : Options,
  runner : Runner,
  cli : String,
  key : String,
  value : String,
) -> Unit {
  phase_header("phase 1: produce into \{opt.topic}, then consume from earliest")
  let produced = run_until_success(
    produce_step(opt, cli, opt.topic, value, key),
    opt.repo,
    120_000,
    5,
  )
  runner.check(produced.exit_code == 0, "the producer exits 0")
  let offset = parse_sent_offset(produced.stdout)
  runner.check(offset is Some(_), "the producer reports the offset it wrote to")
  if opt.partitions == 1 {
    match offset {
      Some(parsed) =>
        runner.check(
          parsed == 0,
          "the first record of a fresh 1-partition topic is at offset 0",
        )
      None => ()
    }
  }
  let consumed = run_observed(
    consume_step(opt, cli, opt.topic, "earliest", 1),
    opt.repo,
    120_000,
  )
  runner.check(
    consumed.exit_code == 0,
    "the consumer exits 0 after --max-messages 1",
  )
  let records = parse_records(consumed.stdout)
  let consumed_offset = offset_of(records, key, value)
  runner.check(consumed_offset is Some(_), "the consumer read back key=\{key}")
  if consumed_offset is None {
    print_indented("!", [consumed.stdout, consumed.stderr])
  }
  match (offset, consumed_offset) {
    (Some(written), Some(read)) =>
      runner.check(
        read == written,
        "the consumed record is the produced one (offset \{written})",
      )
    _ => ()
  }
}

///|
/// Phase 2: a consumer is already polling `opt.topic_live` when the producer sends
/// to it.
///
/// The topic is empty when the run starts, so a consumer that wins the race reads
/// the record on its next poll and one that loses it reads it on its first fetch:
/// both orderings satisfy the assertions. `--live-delay-ms` makes the common case
/// a genuinely live delivery rather than a race.
async fn phase_live(
  opt : Options,
  runner : Runner,
  cli : String,
  key : String,
  value : String,
) -> Unit {
  phase_header("phase 2: consume \{opt.topic_live} while it is produced to")
  let consumer = consume_step(opt, cli, opt.topic_live, "earliest", 1)
  let produced = produce_step(opt, cli, opt.topic_live, value, key)
  announce(consumer)
  announce(produced)
  @async.with_task_group(group => {
    let consumer_task = group.spawn(() => {
      step_output(consumer, opt.repo, 120_000)
    })
    @async.sleep(opt.live_delay_ms)
    let producer_output = step_output(produced, opt.repo, 120_000)
    runner.check(producer_output.exit_code == 0, "the producer exits 0")
    let consumer_output = consumer_task.wait()
    runner.check(
      consumer_output.exit_code == 0,
      "the consumer exits 0 after --max-messages 1",
    )
    let matched = records_contain(
      parse_records(consumer_output.stdout),
      key,
      value,
    )
    runner.check(matched, "the consumer received the record while polling")
    if !matched {
      print_indented("!", [consumer_output.stdout, consumer_output.stderr])
    }
  })
}

///|
/// Renders every command a real run would execute, in order. The steps come from
/// the same builders the run itself uses, so this cannot drift from reality.
async fn print_plan(opt : Options) -> Unit {
  // `auto` is resolved at run time, so the plan shows a placeholder rather than
  // a program name that would never be executed.
  let engine = if opt.runtime == "auto" { "<engine>" } else { opt.runtime }
  println("")
  println("plan (--dry-run: nothing below is executed)")
  let mut step_no = 1
  if opt.no_container {
    println(
      "  \{step_no}. reuse the Kafka you already run; nothing is started or created",
    )
  } else {
    println("  \{step_no}. pick a working container runtime")
    for candidate in runtime_candidates(opt.runtime) {
      println("       - \{candidate} version")
    }
    step_no = step_no + 1
    println("  \{step_no}. start Kafka and wait for the broker")
    if opt.run_mode != "run" {
      println("     if it has a compose provider:")
      println(
        "       $ \{render_step(compose_step(opt, { program: engine, words: ["compose"] }, ["up", "-d"]))}",
      )
    }
    if opt.run_mode != "compose" {
      match plan_image(opt) {
        Some(image) => {
          println("     otherwise (\{engine} run, single broker only):")
          println(
            "       $ \{render_step(direct_run_step(opt, engine, image))}",
          )
        }
        None =>
          println(
            "     otherwise: \{engine} run with the tag from --image (none readable from \{opt.compose_file})",
          )
      }
    }
    println(
      "     then poll: $ \{render_step(exec_step(opt, engine, [KAFKA_TOPICS_SH, "--bootstrap-server", bootstrap(opt), "--list"]))}   (up to \{opt.timeout_secs}s)",
    )
    step_no = step_no + 1
    println("  \{step_no}. create the topics")
    for topic in [opt.topic, opt.topic_live] {
      println("       $ \{render_step(create_topic_step(opt, engine, topic))}")
    }
  }
  step_no = step_no + 1
  println("  \{step_no}. phase 1: produce, then consume \{opt.topic}")
  println(
    "       $ \{render_step(produce_step(opt, "", opt.topic, "<value>", "<key>"))}",
  )
  println(
    "       $ \{render_step(consume_step(opt, "", opt.topic, "earliest", 1))}",
  )
  if !opt.no_container {
    step_no = step_no + 1
    println(
      "  \{step_no}. phase 2: consume \{opt.topic_live} while producing to it",
    )
    println(
      "       $ \{render_step(consume_step(opt, "", opt.topic_live, "earliest", 1))}   (started first)",
    )
    println(
      "       $ \{render_step(produce_step(opt, "", opt.topic_live, "<value>", "<key>"))}   (sent after \{opt.live_delay_ms}ms)",
    )
  }
  if !opt.no_container && !opt.keep {
    step_no = step_no + 1
    println("  \{step_no}. stop Kafka")
    if opt.run_mode != "run" {
      println(
        "       $ \{render_step(compose_step(opt, { program: engine, words: ["compose"] }, ["down"]))}",
      )
    }
    if opt.run_mode != "compose" {
      println("       $ \{render_step(direct_remove_step(opt, engine))}")
    }
  }
  if opt.runtime == "auto" {
    println("")
    println(
      "note: '<engine>' is resolved at run time to the first of podman, docker that answers;",
    )
    println(
      "      the compose lines apply only when that engine has a compose provider.",
    )
  }
}
