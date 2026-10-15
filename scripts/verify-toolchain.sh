#!/usr/bin/env bash
# Verifies that the locally installed build toolchain matches the versions pinned in
# tools/go.mod.
#
# WHY THIS EXISTS:
# Generated protobuf stubs embed the generator version in a header comment, and `buf
# generate` output can differ between releases. If one developer generates with a
# different buf than another, every regeneration produces a spurious diff and CI's
# `buf breaking` gate compares against a different descriptor set than the one that
# was actually reviewed. Pinning is only useful if drift is detected.
#
# Run `make tools` to install the pinned versions, then `make verify` to check.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TOOLS_DIR="$REPO_ROOT/tools"

GO_MIN_MAJOR=1
GO_MIN_MINOR=24

failures=0

# Reads the version pinned for a module by parsing tools/go.mod directly.
#
# This deliberately avoids `go list -m`, which resolves through the module graph and
# silently fails on a version that does not exist. That produced a fail-open bug: a
# bogus pin reported "SKIP" and the whole verification passed. Parsing the file is
# offline, deterministic, and always reports whatever is actually written down.
pinned_version() {
    local module="$1"
    awk -v mod="$module" '
        $1 == mod && $2 ~ /^v/ { print $2; exit }
        $1 == "require" && $2 == mod && $3 ~ /^v/ { print $3; exit }
    ' "$TOOLS_DIR/go.mod"
}

# check_tool <binary> <module> <probe command...>
#
# NOTE: <module> must be a MODULE path as it appears in tools/go.mod, not a package path.
# "google.golang.org/protobuf" is a module; "google.golang.org/protobuf/cmd/protoc-gen-go"
# is only a package inside it, and `go list -m` rejects it.
check_tool() {
    local name="$1" module="$2"
    shift 2
    local probe=("$@")

    local want
    want="$(pinned_version "$module")"
    if [[ -z "$want" ]]; then
        printf '  %-20s SKIP     not pinned in tools/go.mod\n' "$name"
        return
    fi

    if ! command -v "$name" >/dev/null 2>&1; then
        printf '  %-20s MISSING  run: make tools\n' "$name"
        failures=1
        return
    fi

    local got
    got="$("${probe[@]}" 2>&1 || true)"

    # Tools report versions inconsistently: some print "1.5.1", others "v1.5.1".
    # Normalise the leading "v" on both sides before comparing.
    local want_norm="${want#v}"
    if [[ "${got// /}" == *"$want_norm"* ]]; then
        printf '  %-20s OK       %s\n' "$name" "$want"
    else
        printf '  %-20s MISMATCH want %s, got: %s\n' "$name" "$want" "${got//$'\n'/ }"
        failures=1
    fi
}

echo "Verifying toolchain against pins in tools/go.mod"

if ! command -v go >/dev/null 2>&1; then
    printf '  %-20s MISSING  Go is not on PATH (install Go %s.%s+)\n' "go" "$GO_MIN_MAJOR" "$GO_MIN_MINOR"
    failures=1
else
    go_version="$(go env GOVERSION 2>/dev/null || echo unknown)"
    go_min="${GO_MIN_MAJOR}.${GO_MIN_MINOR}"
    if [[ "$go_version" =~ ^go([0-9]+)\.([0-9]+) ]] &&
        { (( BASH_REMATCH[1] < GO_MIN_MAJOR )) ||
          { (( BASH_REMATCH[1] == GO_MIN_MAJOR )) && (( BASH_REMATCH[2] < GO_MIN_MINOR )); }; }; then
        printf '  %-20s TOO OLD  %s (need >= %s)\n' "go" "$go_version" "$go_min"
        failures=1
    else
        printf '  %-20s OK       %s\n' "go" "$go_version"
    fi
fi

check_tool "buf"                "github.com/bufbuild/buf"                 buf --version
check_tool "protoc-gen-go"      "google.golang.org/protobuf"              protoc-gen-go --version
check_tool "protoc-gen-go-grpc" "google.golang.org/grpc/cmd/protoc-gen-go-grpc" protoc-gen-go-grpc --version

# The Makefile repeats these versions so it can call `go install pkg@version`. Two
# copies can silently drift, which would make `make tools` install a different generator
# than the one CI verifies against. Check them against each other.
echo
echo "Checking Makefile pins against tools/go.mod"
# Makefile assignments look like "BUF_VERSION := v1.50.0", so the version is in $2
# (with "=" or ":=" as the operator) or in $3 for the space-separated form.
make_pin() {
    awk -v key="$1" '
        $1 == key {
            op = $2; gsub(/^:?=?$/, "", op)
            if (op ~ /^v/) { print op; exit }
            if ($3 ~ /^v/) { print $3; exit }
        }
    ' "$REPO_ROOT/Makefile"
}

for entry in \
    "BUF_VERSION:github.com/bufbuild/buf" \
    "PROTOC_GEN_GO_VERSION:google.golang.org/protobuf" \
    "PROTOC_GEN_GRPC_VERSION:google.golang.org/grpc/cmd/protoc-gen-go-grpc"
do
    key="${entry%%:*}"
    module="${entry#*:}"

    from_make="$(make_pin "$key")"
    from_gomod="$(pinned_version "$module")"

    if [[ -z "$from_make" ]]; then
        printf '  %-22s NOT FOUND in Makefile\n' "$key"
        failures=1
    elif [[ "$from_make" == "$from_gomod" ]]; then
        printf '  %-22s OK       %s\n' "$key" "$from_make"
    else
        printf '  %-22s MISMATCH Makefile=%s go.mod=%s\n' "$key" "$from_make" "$from_gomod"
        failures=1
    fi
done

echo
if (( failures )); then
    echo "Toolchain verification FAILED."
    echo "Fix with: make tools"
    exit 1
fi

echo "Toolchain verification passed."