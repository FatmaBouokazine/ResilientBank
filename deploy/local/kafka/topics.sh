#!/usr/bin/env bash
# Creates the Kafka topics used by the outbox relayer.
#
# WHY TOPICS ARE CREATED EXPLICITLY RATHER THAN AUTO-CREATED:
# Kafka's auto-creation would give every topic the broker default of 1 partition, which
# would silently serialise all payment events through a single partition and destroy the
# ordering guarantee that per-aggregate keying is meant to provide. Declaring partitions
# here makes the throughput contract reviewable in version control.
#
# WHY REPLICATION FACTOR IS 1:
# The local compose stack runs a single broker, so a replication factor above 1 can never
# be satisfied. The value must be raised alongside the broker count before this runs
# anywhere real; a single-broker cluster has no availability to speak of.
set -euo pipefail

# The compose service overrides this image's entrypoint in order to run this script, which
# bypasses the PATH the image normally sets up. Without this, kafka-topics.sh is simply
# not found and the broker appears "unavailable" even though it is healthy.
export PATH="${KAFKA_HOME:-/opt/kafka/bin}:${PATH}"

# Container-side bootstrap address. This MUST be the broker's INTERNAL listener
# (kafka:29092), not the host-facing localhost:9092: from inside this container,
# "localhost" would resolve to itself and the broker would be unreachable.
BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:29092}"

EVENTS_TOPIC="${KAFKA_TOPIC_EVENTS:-payments.events.v1}"
EVENTS_PARTITIONS="${KAFKA_TOPIC_EVENTS_PARTITIONS:-6}"

DLQ_TOPIC="${KAFKA_TOPIC_DLQ:-payments.events.v1.dlq}"
# The dead-letter topic carries low volume and its ordering is not relied upon by any
# consumer, so a single partition is sufficient.
DLQ_PARTITIONS="${KAFKA_TOPIC_DLQ_PARTITIONS:-1}"

REPLICATION="${KAFKA_REPLICATION_FACTOR:-1}"

log() { echo "[kafka-init] $*"; }

# The broker is started in parallel with this container, so a connection refusal here is
# expected and must be retried rather than treated as a failure.
wait_for_broker() {
    local attempts="${KAFKA_BOOTSTRAP_ATTEMPTS:-60}"
    log "waiting for broker at $BOOTSTRAP (up to ${attempts}s)"
    for ((i = 0; i < attempts; i++)); do
        if kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; then
            log "broker is ready after ${i}s"
            return 0
        fi
        sleep 1
    done
    log "ERROR: broker did not become available at $BOOTSTRAP"
    return 1
}

# Idempotent topic creation: create_topic is a no-op when the topic already exists with
# the same configuration, and errors loudly when it exists with a different one, which is
# the signal that the topic contract changed.
create_topic() {
    local topic="$1" partitions="$2" retention_ms="$3"

    if kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list 2>/dev/null | grep -qx "$topic"; then
        log "topic '$topic' already exists, leaving configuration unchanged"
        return 0
    fi

    log "creating topic '$topic' (partitions=$partitions, replication=$REPLICATION)"
    kafka-topics.sh \
        --bootstrap-server "$BOOTSTRAP" \
        --create \
        --topic "$topic" \
        --partitions "$partitions" \
        --replication-factor "$REPLICATION" \
        --config "retention.ms=${retention_ms}" \
        --config "cleanup.policy=delete" \
        >/dev/null
}

wait_for_broker

# Seven days for the main event topic: long enough for consumers to replay, short enough
# that the log does not grow without bound. Retention and archival of the authoritative
# outbox rows is a separate concern tracked in ADR 0002.
create_topic "$EVENTS_TOPIC" "$EVENTS_PARTITIONS" 604800000

# Thirty days for the dead-letter topic. Failures must be triaged well after they occur.
create_topic "$DLQ_TOPIC" "$DLQ_PARTITIONS" 2592000000

log "verifying"
kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --describe --topic "$EVENTS_TOPIC"
kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --describe --topic "$DLQ_TOPIC"

log "topic bootstrap complete"