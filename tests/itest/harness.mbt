///|
/// The harness aborts through this error: it is raised once at the very end of
/// `main`, after every external resource has been released, so that the process
/// exits non-zero with a readable message.
suberror ItestError {
  Aborted(String)
}

///|
/// One external command the harness may run.
///
/// Steps are plain values built by shared helpers, so the `--dry-run` output and
/// the real run can never disagree about what would be executed.
struct Step {
  purpose : String
  program : String
  args : Array[String]
}

///|
fn new_step(purpose : String, program : String, args : Array[String]) -> Step {
  { purpose, program, args, }
}

///|
/// Renders a step roughly the way a shell user would type it.
fn render_step(step : Step) -> String {
  [step.program, ..step.args]
  .map(arg => {
    if arg.is_blank() || arg.contains(" ") {
      "\"\{arg}\""
    } else {
      arg
    }
  })
  .join(" ")
}

///|
/// What a step produced: a small stand-in for `@shell.Output` that also covers the
/// case where the program could not be spawned at all (a compose provider that is
/// not installed, say). For the harness that is an ordinary failed step, not an
/// exception.
struct StepResult {
  exit_code : Int
  stdout : String
  stderr : String
}

///|
/// Runs the step with a hard timeout and captured output. `timeout_ms` kills the
/// child rather than hanging the harness.
async fn step_output(step : Step, cwd : String, timeout_ms : Int) -> StepResult {
  let output = @shell.Cmd(step.program, step.args, cwd~, env={ "NO_COLOR": "1" }).output(
    timeout_ms~,
  ) catch {
    error =>
      return {
        exit_code: -1,
        stdout: "",
        stderr: "\{step.program} could not be run: \{repr(error)}",
      }
  }
  {
    exit_code: output.exit_code(),
    stdout: output.stdout(),
    stderr: output.stderr(),
  }
}

///|
/// Prints the step before running it, so a failing CI log shows the exact command
/// that produced the failure.
fn announce(step : Step) -> Unit {
  println("  # \{step.purpose}")
  println("  $ \{render_step(step)}")
}

///|
struct Runner {
  mut checks : Int
  mut failures : Int
  /// Set when a prerequisite failed; later phases are then skipped, because
  /// their results could not mean anything.
  mut aborted : Bool
}

///|
fn new_runner() -> Runner {
  { checks: 0, failures: 0, aborted: false, }
}

///|
fn Runner::check(self : Runner, ok : Bool, what : String) -> Unit {
  self.checks = self.checks + 1
  if ok {
    println("  PASS  \{what}")
  } else {
    self.failures = self.failures + 1
    println("  FAIL  \{what}")
  }
}

///|
fn Runner::fail(self : Runner, what : String) -> Unit {
  self.checks = self.checks + 1
  self.failures = self.failures + 1
  self.aborted = true
  println("  FAIL  \{what}")
}

///|
fn phase_header(text : String) -> Unit {
  println("")
  println("== \{text}")
}

///|
/// Prints captured output under a marker, skipping blank lines.
fn print_indented(marker : String, texts : Array[String]) -> Unit {
  for text in texts {
    for line in text.split("\n") {
      if !line.trim().is_blank() {
        println("    \{marker} \{line}")
      }
    }
  }
}

///|
/// Runs a step the harness depends on. Returns `false` (and aborts the run) when
/// the command fails, because every later phase would be meaningless.
async fn run_required(
  runner : Runner,
  step : Step,
  cwd : String,
  timeout_ms : Int,
  what : String,
) -> Bool {
  announce(step)
  let output = step_output(step, cwd, timeout_ms)
  if output.exit_code == 0 {
    return true
  }
  println("  FAIL  \{what} (exit=\{output.exit_code})")
  print_indented("!", [output.stderr, output.stdout])
  runner.failures = runner.failures + 1
  runner.aborted = true
  false
}

///|
/// Runs a step whose outcome is itself part of the test.
async fn run_observed(
  step : Step,
  cwd : String,
  timeout_ms : Int,
) -> StepResult {
  announce(step)
  let started = @async.now()
  let result = step_output(step, cwd, timeout_ms)
  let elapsed = @async.now() - started
  if result.exit_code == 0 {
    println("  ... exit=0 in \{elapsed}ms")
  } else {
    println("  ... exit=\{result.exit_code} after \{elapsed}ms")
    print_indented("!", [result.stderr, result.stdout])
  }
  result
}

///|
/// Runs a step, retrying while the broker refuses it, and returns the first
/// successful result (or the last failure).
///
/// A Kafka broker that has just started answers admin commands and accepts topic
/// creation well before it can serve a produce: the very first produce then fails
/// fast with moonkafka's ProtocolError and the next one succeeds. Retrying here
/// keeps that startup race out of the assertions, while every attempt is still
/// announced and timed in the log.
async fn run_until_success(
  step : Step,
  cwd : String,
  timeout_ms : Int,
  attempts : Int,
) -> StepResult {
  let mut result = run_observed(step, cwd, timeout_ms)
  let mut attempt = 1
  while result.exit_code != 0 && attempt < attempts {
    attempt = attempt + 1
    println(
      "  ... retrying (\{attempt}/\{attempts}): the previous attempt was refused",
    )
    @async.sleep(1_000)
    result = run_observed(step, cwd, timeout_ms)
  }
  result
}
