///|
/// One invocation of the built demo CLI, executed directly rather than through
/// moon run: a moon run spawned by a program that moon run itself started can
/// block on the build lock, and every phase here runs under a timeout.
fn cli_step(opt : Options, cli : String, args : Array[String]) -> Step {
  let program = if cli.is_blank() { default_cli_path(opt) } else { cli }
  new_step("moonkafka cli", program, args)
}

///|
fn produce_step(
  opt : Options,
  cli : String,
  topic : String,
  value : String,
  key : String,
) -> Step {
  cli_step(opt, cli, ["produce", topic, value, key, opt.host, "\{opt.port}"])
}

///|
fn consume_step(
  opt : Options,
  cli : String,
  topic : String,
  start : String,
  limit : Int,
) -> Step {
  cli_step(opt, cli, [
    "consume",
    topic,
    opt.host,
    "\{opt.port}",
    start,
    "--max-messages",
    "\{limit}",
  ])
}

///|
/// Parses a non-negative decimal integer, returning `None` for anything else.
/// Deliberately dependency-free so the parsing rules can be unit-tested without
/// a broker.
fn parse_nonneg_int(text : StringView) -> Int? {
  let mut value = 0
  let mut index = 0
  while index < text.length() {
    let digit = text[index]
    if digit < '0' || digit > '9' {
      return None
    }
    value = value * 10 + (digit - '0').to_int()
    index = index + 1
  }
  if index == 0 {
    return None
  }
  Some(value)
}

///|
/// Reads the offset out of a `sent to topic "<topic>" at offset <n>` line.
fn parse_sent_offset(stdout : String) -> Int? {
  for line in stdout.split("\n") {
    guard line.split_once(" at offset ") is Some((_, tail)) else { continue }
    match parse_nonneg_int(tail.trim()) {
      Some(offset) => return Some(offset)
      None => continue
    }
  }
  None
}

///|
/// One record line as the consumer prints it.
struct ConsumerRecord {
  offset : Int
  key : String
  value : String
}

///|
/// Parses the consumer's record lines:
/// `offset=<n> timestamp=<n> key=<k> value=<v>`.
///
/// `value` is taken as the remainder of the line, so a value containing spaces
/// (or even ` key=`) round-trips intact.
fn parse_records(stdout : String) -> Array[ConsumerRecord] {
  let records : Array[ConsumerRecord] = []
  for line in stdout.split("\n") {
    guard line.split_once(" timestamp=") is Some((head, rest)) else { continue }
    guard head.split_once("offset=") is Some((prefix, offset_text)) else {
      continue
    }
    if !prefix.trim().is_blank() {
      continue
    }
    guard rest.split_once(" key=") is Some((_, key_and_value)) else { continue }
    guard key_and_value.split_once(" value=") is Some((key, value)) else {
      continue
    }
    guard parse_nonneg_int(offset_text.trim()) is Some(offset) else { continue }
    records.push({ offset, key: key.to_owned(), value: value.to_owned(), })
  }
  records
}

///|
fn records_contain(
  records : Array[ConsumerRecord],
  key : String,
  value : String,
) -> Bool {
  let mut found = false
  for record in records {
    if record.key == key && record.value == value {
      found = true
    }
  }
  found
}

///|
fn offset_of(
  records : Array[ConsumerRecord],
  key : String,
  value : String,
) -> Int? {
  let mut found : Int? = None
  for record in records {
    if record.key == key && record.value == value {
      found = Some(record.offset)
    }
  }
  found
}

///|
/// Where the current toolchain writes the CLI binary:
/// _build/<target>/debug/build/<package path>/<name>.exe. The --cli option
/// overrides it, and the default matches what moon build --target TARGET
/// produces.
fn default_cli_path(opt : Options) -> String {
  if !opt.cli.is_blank() {
    return opt.cli
  }
  "_build/\{opt.target}/debug/build/cmd/main/main.exe"
}

///|
/// Confirms the CLI binary exists, so a missing build is reported as such instead
/// of as a mysterious spawn failure.
async fn resolve_cli(opt : Options, runner : Runner) -> String {
  let cli = default_cli_path(opt)
  if @fs.exists(cli) {
    return cli
  }
  runner.fail(
    "the CLI binary is missing at \{cli}; build it first (moon build --target \{opt.target}) or pass --cli PATH",
  )
  ""
}
