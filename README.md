# moonkafka demo

A standalone MoonBit command-line demo that uses the open-source
[`daqing/moonkafka`](https://github.com/daqing/moonkafka) library to connect to Apache Kafka,
produce messages, and consume them.

📖 **Documentation site (English and Chinese): <https://daqing.github.io/moonkafka-demo/>**

The project pulls in `daqing/moonkafka@0.3.4` through the MoonBit package manager. Upstream calls
that release documentation-only — no library code changed — so the API baseline is still the
`v0.3.3` tag, commit `6e0f45f18ae9e5d9b16e30ddf9ac0533a7cdd602`. `0.3.4` lives on mooncakes.io
only; it carries no tag of its own.

`cmd/main` holds nothing but command-line argument parsing, UTF-8 text conversion, and result
display. Broker connections, metadata handling, the Kafka protocol, message production, and
message fetching all come from `moonkafka`'s `Producer`, `Consumer`, and related public APIs — no
custom Kafka client logic is implemented here.

## Prerequisites

- A recent stable MoonBit toolchain
- A native C toolchain and zlib
- An Apache Kafka 4.x KRaft cluster

`moonkafka`'s socket implementation supports the MoonBit `native` target only, so every command
below passes `--target native` explicitly.

## Starting Kafka locally

The project ships the same single-node Kafka 4.3 KRaft configuration as the `moonkafka` main
repository:

```sh
docker compose -f docker-compose.kafka.yml up -d
```

Once the container is healthy, create the demo topic:

```sh
docker exec moonkafka-demo-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic events \
  --partitions 3 \
  --replication-factor 1
```

## Build

```sh
moon build --target native
```

## Consuming messages

Start the consumer in terminal A first:

```sh
moon run --target native cmd/main -- consume events
```

The consumer connects to `127.0.0.1:9092` by default and reads from the earliest available
offset. Both the address and the starting position can be overridden:

```sh
moon run --target native cmd/main -- consume events kafka.example.com 9092 latest
```

The consumer calls `moonkafka.Consumer::poll()` in a loop; press `Ctrl-C` to stop it.

## Producing messages

In terminal B, send a message with the producer:

```sh
moon run --target native cmd/main -- produce events "Hello from MoonBit" demo-key
```

The message key is optional, and you can point at a remote broker:

```sh
moon run --target native cmd/main -- produce events "hello" demo-key kafka.example.com 9092
```

The producer uses `acks=-1` and waits for `moonkafka.Producer::send()` to return the message
offset.

## Integration tests

`tests/itest` is an end-to-end integration test: it starts Kafka in a container (podman or
docker), creates a fresh topic, and then drives the real command-line program through `moon run`,
covering the whole produce → consume path. It needs a container runtime and a live broker, so it
is **not part of `moon test`** and runs only when invoked explicitly:

```sh
make itest
```

The container engine is auto-detected: `podman` first, then `docker` (confirmed to actually work
via `<engine> version`, not merely installed). When that engine has a compose provider
(`<engine> compose` or `<engine>-compose`), Kafka starts through `docker-compose.kafka.yml`; when
there is **no** provider — common on a podman-only install — the test instead launches a
single-node broker with `podman run`, using the same arguments as the `kafka` service in the
compose file. Use `--run-mode compose|run` to force one of the two paths, and `--image` to
override the image tag (read from the compose file by default, so the two paths never each carry
their own copy). A failed probe prints a line such as `<engine> is not usable` — for instance
`... podman is not usable` when podman is not installed — which is an intermediate result from
trying the candidates in turn, not an error.

`make itest` already pins `--runtime docker` by default (override with `ITEST_RUNTIME`), so it
skips the auto-detection above and never prints that probe line. Running
`moon run ... tests/itest` directly still auto-detects.

The test runs two phases:

1. Produce one message, then read it back from `earliest` with `--max-messages 1`, asserting that
   the key, the value, and the offset all match (the first message on a new single-partition topic
   has offset 0).
2. Let the consumer poll an empty topic first, then produce a message, and assert that the
   consumer received it while polling.

Every run uses a unique topic name and message payload, so leftover data from an earlier run never
gets in the way. Kafka is stopped when the run finishes by default; `ITEST_FLAGS` changes that:

```sh
make itest ITEST_FLAGS="--keep"       # keep the container around for debugging
make itest ITEST_FLAGS="--dry-run"    # print the commands that would run, without running them
make itest-plan                       # same as above
make itest ITEST_RUNTIME=podman       # use podman instead (docker is the default)
```

If you started Kafka yourself, you can skip the container lifecycle and topic creation (naming an
existing, empty topic):

```sh
moon run --target native tests/itest -- --no-container --topic <existing-empty-topic>
```

For `--runtime`, `--compose-bin`, `--run-mode`, `--image`, `--cli`, `--target`, `--host`, `--port`,
`--partitions`, `--timeout-secs`, `--live-delay-ms` and the rest, see
`moon run --target native tests/itest -- --help`. When debugging by hand, `make kafka-up` and
`make kafka-down` start and stop Kafka on their own (the engine comes from the `RUNTIME`
variable, auto-detected by default).

The harness compiles the CLI first and then executes that binary directly (no extra `moon run`
layer), so each phase is fast; the log prints every command, exit code, and elapsed time. One
startup quirk is worth knowing: a broker that has just come up accepts admin commands and topic
creation, but **the very first produce may be rejected outright**
(`daqing/moonkafka.ProtocolError`) — the second one succeeds. The harness therefore retries
produce with a bounded limit (5 attempts at most) and prints every retry faithfully.

## Continuous integration

`.github/workflows/ci.yml` runs on every push to `main` or `develop` and on every pull request:
type-check with warnings as errors, build the native CLI, run the unit tests, and verify
formatting — the same steps `make ci` runs locally. The integration test is not part of it: it
needs a container runtime and a live broker.

## Command line

```text
produce <topic> <value> [key] [host] [port]
consume <topic> [host] [port] [earliest|latest] [--max-messages <n>]
```

`consume` polls until `Ctrl-C` by default; adding `--max-messages <n>` exits cleanly after
printing n records, which is handy for scripts and tests.

Show the built-in help:

```sh
moon run --target native cmd/main -- --help
```

Stop the local Kafka:

```sh
docker compose -f docker-compose.kafka.yml down
```

## License

[MIT](LICENSE)

English | [中文](README.zh-CN.md)
