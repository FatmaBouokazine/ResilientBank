#!/usr/bin/env bash
set -euo pipefail

# scripts/run-tlc.sh
# Executes the TLC model checker against the formal TLA+ Raft consensus specification.
# Used locally via `make tlc` and as an automated verification gate in CI/CD pipelines.

echo "=== ResilientBank Formal Verification Gate (TLA+ / TLC) ==="

# 1. Verify Java runtime is available
if ! command -v java >/dev/null 2>&1; then
    echo "ERROR: Java Runtime Environment (JRE/JDK 11+) is required to run TLC." >&2
    echo "Please install Java (e.g. OpenJDK 11+) and ensure 'java' is in PATH." >&2
    exit 1
fi

JAVA_VERSION=$(java -version 2>&1 | head -n 1)
echo "Java detected: $JAVA_VERSION"

# 2. Locate or download tla2tools.jar
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CACHE_DIR="${REPO_ROOT}/tools"
JAR_PATH="${TLA2TOOLS_JAR:-${CACHE_DIR}/tla2tools.jar}"
TLC_RELEASE_URL="https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar"

if [ ! -f "$JAR_PATH" ]; then
    echo "tla2tools.jar not found at $JAR_PATH. Downloading from official release..."
    mkdir -p "$CACHE_DIR"
    curl -fsSL -o "$JAR_PATH" "$TLC_RELEASE_URL" || {
        echo "ERROR: Failed to download tla2tools.jar from $TLC_RELEASE_URL" >&2
        exit 1
    }
    echo "Downloaded tla2tools.jar successfully to $JAR_PATH."
fi

# 3. Model checking execution
SPEC_DIR="${REPO_ROOT}/formal/raft"
SPEC_FILE="Raft.tla"
CONFIG_FILE="Raft.cfg"

if [ ! -f "${SPEC_DIR}/${SPEC_FILE}" ] || [ ! -f "${SPEC_DIR}/${CONFIG_FILE}" ]; then
    echo "ERROR: Specification or config missing in ${SPEC_DIR}" >&2
    exit 1
fi

echo "Running TLC on ${SPEC_DIR}/${SPEC_FILE} with config ${CONFIG_FILE}..."
echo "Workers: auto, Parallel GC enabled."

# Execute TLC from within the spec directory so module resolution works reliably
cd "$SPEC_DIR"

set +e
TLC_OUTPUT=$(java -XX:+UseParallelGC -cp "$JAR_PATH" tlc2.TLC -cleanup -workers auto -config "$CONFIG_FILE" "$SPEC_FILE" 2>&1)
TLC_STATUS=$?
set -e

echo "$TLC_OUTPUT"

# 4. Invariant and exit code verification
if [ $TLC_STATUS -ne 0 ]; then
    echo "=========================================================="
    echo "FAIL: TLC model checker exited with status $TLC_STATUS"
    echo "=========================================================="
    exit 1
fi

if echo "$TLC_OUTPUT" | grep -qi "Error:"; then
    echo "=========================================================="
    echo "FAIL: Invariant violation or semantic error detected by TLC"
    echo "=========================================================="
    exit 1
fi

if ! echo "$TLC_OUTPUT" | grep -qi "Model checking completed. No error has been found"; then
    echo "=========================================================="
    echo "FAIL: TLC did not complete state exploration cleanly"
    echo "=========================================================="
    exit 1
fi

echo "=========================================================="
echo "PASS: Raft Consensus Formal Verification Gate PASSED"
echo "Safety Invariants Verified: ElectionSafety, LogMatching, LeaderCompleteness, CommitIndexBounded, TypeOK"
echo "=========================================================="
exit 0
