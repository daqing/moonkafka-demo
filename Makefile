# Developer commands for the moonkafka demo.
#
# moonkafka's socket layer supports the `native` target only, so every
# MoonBit invocation below pins `--target native`.
#
# Run `make` or `make help` for the target list.

MOON   ?= moon
TARGET ?= native
PKG    ?= cmd/main

# Demo defaults; override on the command line, e.g. `make run-producer VALUE=hi`.
TOPIC ?= events
HOST  ?= 127.0.0.1
PORT  ?= 9092
KEY   ?= demo-key
START ?= earliest

# Container runtime for the kafka-* helpers; tests/itest detects its own.
RUNTIME ?= $(shell command -v podman >/dev/null 2>&1 && echo podman || echo docker)

# Container engine for `make itest`. Pinned instead of left to the harness's
# auto-detection: that probe tries candidates in order and prints a
# "podman is not usable" line for every miss, which is pure noise where podman
# is not installed. Override with ITEST_RUNTIME=podman if that ever changes.
ITEST_RUNTIME ?= docker

# Extra flags for `make itest`, e.g. ITEST_FLAGS="--keep".
ITEST_FLAGS ?=

.DEFAULT_GOAL := help
.PHONY: help check check-strict test build fmt fmt-check ci itest itest-plan clean run-producer run-consumer kafka-up kafka-down

# Every target drives one `moon` process sharing `_build`; keep this
# invocation serial even under `make -j`.
.NOTPARALLEL:

# Type-check the module without code generation; this is the fast feedback loop.
check:
	$(MOON) check --target $(TARGET)

# Same as `check` but treats warnings as errors; what CI should gate on.
check-strict:
	$(MOON) check --target $(TARGET) --deny-warn

# Run the test suite.
test:
	$(MOON) test --target $(TARGET)

# Produce the native binary.
build:
	$(MOON) build --target $(TARGET)

# Rewrite MoonBit sources into canonical form.
fmt:
	$(MOON) fmt

# Verify formatting without writing; fails if any file would change.
fmt-check:
	$(MOON) fmt --check

# Everything CI should run.
ci: check-strict test fmt-check

# End-to-end test: start Kafka in a container, then drive the real CLI against it.
itest: build
	$(MOON) run --target $(TARGET) tests/itest -- --target $(TARGET) --runtime $(ITEST_RUNTIME) $(ITEST_FLAGS)

# Print what `make itest` would execute, without starting anything.
itest-plan:
	$(MOON) run --target $(TARGET) tests/itest -- --target $(TARGET) --runtime $(ITEST_RUNTIME) --dry-run

# Kafka lifecycle helpers for manual runs and for cleaning up after --keep.
# `make itest` manages its own container and does not use these.
kafka-up:
	$(RUNTIME) compose -f docker-compose.kafka.yml up -d

kafka-down:
	$(RUNTIME) compose -f docker-compose.kafka.yml down

# Send one message: make run-producer VALUE="hello" [TOPIC=events] [KEY=demo-key]
run-producer:
	@test -n "$(VALUE)" || { echo 'usage: make run-producer VALUE="hello" [TOPIC=events] [KEY=demo-key] [HOST=127.0.0.1] [PORT=9092]'; exit 2; }
	$(MOON) run --target $(TARGET) $(PKG) -- produce "$(TOPIC)" "$(VALUE)" "$(KEY)" "$(HOST)" "$(PORT)"

# Consume a topic until Ctrl-C: make run-consumer [TOPIC=events] [START=latest]
run-consumer:
	$(MOON) run --target $(TARGET) $(PKG) -- consume "$(TOPIC)" "$(HOST)" "$(PORT)" "$(START)"

# Remove build artifacts.
clean:
	$(MOON) clean

# List the available targets. This is the default goal.
help:
	@echo 'moonkafka demo - make targets'
	@echo ''
	@echo '  make check         type-check the module (native target)'
	@echo '  make check-strict  type-check with warnings as errors'
	@echo '  make test          run the test suite (native target)'
	@echo '  make build         build the native CLI'
	@echo '  make fmt           format MoonBit sources in place'
	@echo '  make fmt-check     verify formatting (no writes)'
	@echo '  make ci            check-strict + test + fmt-check'
	@echo '  make itest         end-to-end test against Kafka (ITEST_RUNTIME=$(ITEST_RUNTIME))'
	@echo '  make itest-plan    print the itest plan without running it'
	@echo '  make kafka-up      start Kafka via compose (RUNTIME=$(RUNTIME))'
	@echo '  make kafka-down    stop that Kafka'
	@echo '  make run-consumer  consume TOPIC ($(TOPIC)) from $(HOST):$(PORT)'
	@echo '  make run-producer  send VALUE to TOPIC ($(TOPIC))'
	@echo '  make clean         remove build artifacts'
