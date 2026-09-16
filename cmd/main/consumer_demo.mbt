///|
async fn run_consumer(args : Array[String]) -> Unit {
  let (max_messages, args) = take_max_messages(args)
  guard args.get(2) is Some(topic) else {
    println(
      "usage: consume <topic> [host] [port] [earliest|latest] [--max-messages <n>]",
    )
    return
  }
  let host = args.get(3).unwrap_or("127.0.0.1")
  let port = parse_port(args, 4)
  let start_from = parse_start_from(args.get(5))

  @async.with_task_group(group => {
    let consumer = @moonkafka.Consumer::connect(
      group~,
      host~,
      port~,
      topic~,
      start_from~,
    )
    defer consumer.close()
    println(
      "consuming topic \"\{topic}\" from \{host}:\{port}; press Ctrl-C to stop",
    )

    let mut received = 0
    for ;; {
      for record in consumer.poll() {
        let key = record.key_utf8().unwrap_or("<null>")
        let value = record.value_utf8().unwrap_or("<tombstone>")
        println(
          "offset=\{record.offset} timestamp=\{record.timestamp} key=\{key} value=\{value}",
        )
        received = received + 1
      }
      if max_messages is Some(limit) && received >= limit {
        break
      }
    }
  })
}
