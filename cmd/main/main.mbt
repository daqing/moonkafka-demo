///|
async fn main {
  let args = @env.args()
  match args.get(1) {
    Some("produce") => run_producer(args)
    Some("consume") => run_consumer(args)
    Some("-h") | Some("--help") | None => print_help()
    Some(command) => {
      println("unknown command: \{command}\n")
      print_help()
    }
  }
}
