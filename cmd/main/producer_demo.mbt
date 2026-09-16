///|
async fn run_producer(args : Array[String]) -> Unit {
  guard args.get(2) is Some(topic) && args.get(3) is Some(value) else {
    println("usage: produce <topic> <value> [key] [host] [port]")
    return
  }
  let key = args.get(4).map(text => @utf8.encode(text))
  let host = args.get(5).unwrap_or("127.0.0.1")
  let port = parse_port(args, 6)

  @async.with_task_group(group => {
    let producer = @moonkafka.Producer::connect(
      group~,
      host~,
      port~,
      topic~,
      acks=-1,
    )
    defer producer.close()

    let offset = producer.send(key?, value=@utf8.encode(value))
    println("sent to topic \"\{topic}\" at offset \{offset}")
  })
}
