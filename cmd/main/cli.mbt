///|
fn print_help() -> Unit {
  println(
    (
      $|moonkafka demo
      $|
      $|Usage:
      $|  moon run --target native cmd/main -- produce <topic> <value> [key] [host] [port]
      $|  moon run --target native cmd/main -- consume <topic> [host] [port] [earliest|latest]
      $|    [--max-messages <n>]
      $|
      $|Defaults:
      $|  host       127.0.0.1
      $|  port       9092
      $|  start      earliest
      $|
      $|--max-messages <n> stops the consumer after it has printed n records,
      $|which is what scripts and tests use. Omit it to poll until Ctrl-C.
      $|
      $|The topic must already exist. Press Ctrl-C to stop the consumer.
    ),
  )
}

///|
fn parse_port(args : Array[String], index : Int) -> Int {
  match args.get(index) {
    Some(value) => {
      let port = @string.parse_int(value) catch {
        _ => {
          println("invalid port: \{value}")
          abort("")
        }
      }
      if port < 1 || port > 65535 {
        println("port must be between 1 and 65535: \{value}")
        abort("")
      }
      port
    }
    None => 9092
  }
}

///|
fn parse_start_from(value : String?) -> @moonkafka.StartFrom {
  match value {
    Some("latest") => @moonkafka.StartFrom::Latest
    Some("earliest") | None => @moonkafka.StartFrom::Earliest
    Some(other) => {
      println("invalid start position: \{other} (expected earliest or latest)")
      abort("")
    }
  }
}

///|
/// Pulls `--max-messages <n>` (or `--max-messages=<n>`) out of `args` and
/// returns the limit together with the remaining positional arguments, in
/// order. Returns `None` when the flag is absent, in which case the consumer
/// polls until it is interrupted.
fn take_max_messages(args : Array[String]) -> (Int?, Array[String]) {
  let mut limit : Int? = None
  let positionals : Array[String] = []
  let mut index = 0
  while index < args.length() {
    let arg = args[index]
    let inline = arg.has_prefix("--max-messages=")
    if arg == "--max-messages" || inline {
      let text = if inline {
        arg[15:].to_owned()
      } else {
        match args.get(index + 1) {
          Some(value) => {
            index = index + 1
            value
          }
          None => {
            println("--max-messages needs a value")
            abort("")
          }
        }
      }
      let value : Int = @string.parse_int(text) catch {
        _ => {
          println("invalid --max-messages value: \{text}")
          abort("")
        }
      }
      if value < 1 {
        println("--max-messages must be at least 1: \{text}")
        abort("")
      }
      limit = Some(value)
    } else {
      positionals.push(arg)
    }
    index = index + 1
  }
  (limit, positionals)
}
