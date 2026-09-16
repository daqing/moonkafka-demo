# moonkafka demo

这是一个独立的 MoonBit 命令行演示项目，使用开源
[`daqing/moonkafka`](https://github.com/daqing/moonkafka) 库连接 Apache Kafka、
发送消息并读取消息。

📖 **在线文档（中英双语）：<https://daqing.github.io/moonkafka-demo/>**

本项目以 `moonkafka` 的 `v0.2.1` 标签提交
`a9c93da25b47f008659b34a334f8d3758c47f285` 为 API 基线；该提交的模块版本为
`0.2.1`，本项目通过 MoonBit 包管理器引用 `daqing/moonkafka@0.2.1`。

`cmd/main` 中只有命令行参数解析、UTF-8 文本转换和结果展示。Broker 连接、元数据
处理、Kafka 协议、消息发送和消息拉取均由 `moonkafka` 的 `Producer`、`Consumer`
以及相关公开 API 完成，没有实现任何自定义 Kafka 客户端逻辑。

## 前置条件

- 最新稳定版 MoonBit 工具链；
- 原生 C 编译环境和 zlib；
- Apache Kafka 4.x KRaft 集群。

`moonkafka` 的 socket 实现只支持 MoonBit `native` 目标，因此下文命令都显式使用
`--target native`。

## 启动本地 Kafka

项目附带与 `moonkafka` 主仓库一致的单节点 Kafka 4.3 KRaft 配置：

```sh
docker compose -f docker-compose.kafka.yml up -d
```

等待容器健康后创建演示主题：

```sh
docker exec moonkafka-demo-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic events \
  --partitions 3 \
  --replication-factor 1
```

## 构建

```sh
moon build --target native
```

## 读取消息

先在终端 A 启动 Consumer：

```sh
moon run --target native cmd/main -- consume events
```

Consumer 默认连接 `127.0.0.1:9092`，并从最早可用偏移量读取。可以覆盖连接地址和
起始位置：

```sh
moon run --target native cmd/main -- consume events kafka.example.com 9092 latest
```

Consumer 会持续调用 `moonkafka.Consumer::poll()`，使用 `Ctrl-C` 停止。

## 发送消息

在终端 B 通过 Producer 发送消息：

```sh
moon run --target native cmd/main -- produce events "Hello from MoonBit" demo-key
```

消息 key 可省略；也可以指定远程 Broker：

```sh
moon run --target native cmd/main -- produce events "hello" demo-key kafka.example.com 9092
```

Producer 使用 `acks=-1`，并等待 `moonkafka.Producer::send()` 返回消息偏移量。

## 集成测试

`tests/itest` 是端到端集成测试：用容器（podman 或 docker）启动 Kafka、创建全新主题，再
通过 `moon run` 驱动真实的命令行程序，完成「发送消息 → 消费消息」的完整链路。它需要容器
运行时和一个真实 broker，因此**不属于 `moon test`**，只在被显式调用时运行：

```sh
make itest
```

容器引擎会自动探测：优先 `podman`，其次 `docker`（用 `<engine> version` 确认它真的可用，
而不只是安装了）。如果该引擎有 compose provider（`<engine> compose` 或 `<engine>-compose`），
就用 `docker-compose.kafka.yml` 启动；**没有** provider 时（常见的纯 podman 安装）改为直接用
`podman run` 启动单节点 broker，参数与 compose 文件里的 `kafka` 服务一致。可以用
`--run-mode compose|run` 强制其中一条路径；`--image` 覆盖镜像标签（默认从 compose 文件读取，
因此两条路径不会各写一份）。探测失败时会打印一行 `<engine> is not usable`（例如没有安装
podman 时的 `... podman is not usable`），这只是逐个候选尝试的中间结果，不是错误。

`make itest` 默认已经固定 `--runtime docker`（用 `ITEST_RUNTIME` 可覆盖），因此不会走上面
的自动探测，也不会打印那行探测日志。直接运行 `moon run ... tests/itest` 时仍为自动探测。

测试依次执行两个阶段：

1. 先发送一条消息，再用 `--max-messages 1` 从 `earliest` 读回，断言 key、value 与偏移量
   都一致（单分区新主题的第一条消息偏移量为 0）；
2. 先让 Consumer 在空主题上轮询，再发送消息，断言 Consumer 在轮询过程中收到了这条消息。

每次运行都使用唯一的主题名和消息内容，因此不会与上一次运行的残留数据混淆。测试结束默认
停止 Kafka，用 `ITEST_FLAGS` 可以改变行为：

```sh
make itest ITEST_FLAGS="--keep"       # 保留容器，便于排查
make itest ITEST_FLAGS="--dry-run"    # 只打印将要执行的命令
make itest-plan                       # 同上
make itest ITEST_RUNTIME=podman       # 改用 podman（默认 docker）
```

如果 Kafka 由你自己启动，可以跳过容器生命周期与建主题（需要指定一个已存在且为空的主题）：

```sh
moon run --target native tests/itest -- --no-container --topic <已存在且为空的主题>
```

`--runtime`、`--compose-bin`、`--run-mode`、`--image`、`--cli`、`--target`、`--host`、`--port`、`--partitions`、
`--timeout-secs`、`--live-delay-ms` 等选项见
`moon run --target native tests/itest -- --help`。手工排查时可以用 `make kafka-up` /
`make kafka-down` 单独开关 Kafka（引擎由 `RUNTIME` 变量选择，默认自动探测）。

harness 先编译 CLI 再直接执行该二进制（不再套一层 `moon run`），因此每个阶段都很快；日志会
逐条打印命令、退出码与耗时。有一个启动期的现象需要注意：刚起来的 broker 会先接受管理命令与建
主题，但**第一个 produce 可能被立即拒绝**（`daqing/moonkafka.ProtocolError`），第二次即可成功。
harness 因此对 produce 做有界重试（最多 5 次），并在日志里如实打印每一次重试。

## 命令行

```text
produce <topic> <value> [key] [host] [port]
consume <topic> [host] [port] [earliest|latest] [--max-messages <n>]
```

`consume` 默认一直轮询到 `Ctrl-C`；加上 `--max-messages <n>` 会在打印 n 条记录后正常退出，
便于脚本和测试使用。

查看内置帮助：

```sh
moon run --target native cmd/main -- --help
```

停止本地 Kafka：

```sh
docker compose -f docker-compose.kafka.yml down
```
