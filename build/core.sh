#! /bin/sh

BINARY=core
SERVER=$1
if [ -z "$SERVER" ]; then
    SERVERFILE=build/host
    if [ -e "$SERVERFILE" ]; then
        SERVER=$(cat "$SERVERFILE")
    fi
fi

BUILD=$PWD/build
INSTALL=$BUILD/install

if [ -e $INSTALL ]; then
    rm -fr $INSTALL
fi
mkdir $INSTALL


echo "======== CORE ========"
CLIPKG=github.com/netstar-labs/core/internal/cli
VERSION="$(git describe --tags --always --dirty 2>/dev/null || echo dev)"
REVISION="$(git rev-parse --short=12 HEAD 2>/dev/null || echo unknown)"

echo "VERSION $VERSION $REVISION"

# Each embedded notice must match its source. BSD clause 2 (3-Clause and 2-Clause alike)
# requires the notice to accompany a BINARY redistribution, and a binary is all a
# recipient ever gets — so a stale copy here is a compliance gap, not a formatting nit.
# Fail the build rather than ship a binary whose notices have drifted.
NOTICES="internal/mmap/LICENSE:cmd/core/notices/golang.org-x-exp-mmap.LICENSE internal/xxh3/LICENSE:cmd/core/notices/github.com-zeebo-xxh3.LICENSE"
for pair in $NOTICES; do
    NOTICE_SRC=${pair%%:*}
    NOTICE_EMBED=${pair##*:}
    if ! cmp -s "$NOTICE_SRC" "$NOTICE_EMBED"; then
        echo "ERROR: $NOTICE_EMBED is stale — re-copy it from $NOTICE_SRC" >&2
        diff "$NOTICE_SRC" "$NOTICE_EMBED" >&2 || true
        exit 1
    fi
    echo "NOTICES in step with $NOTICE_SRC"
done

# build the binaries
echo "BUILD core"

# core: build the whole package (cmd/core has main.go, plan.go, prep.go, pack.go,
# push.go) — building a single .go file would leave cmdPlan/cmdPartition/... undefined.
GOOS=linux GOARCH=amd64 go build -trimpath \
  -ldflags "-X '$CLIPKG.Version=$VERSION' -X '$CLIPKG.Build=$REVISION'" \
  -o $INSTALL/$BINARY ./cmd/$BINARY

# Emit the notices beside the binary too. The binary can always print them itself
# (`core notices`), which is what makes the obligation inseparable from the artifact;
# this file covers the case where the delivery is a package a human reads.
echo "NOTICES to $INSTALL/THIRD-PARTY-NOTICES"
{
    echo "core — third-party notices"
    echo "=========================="
    echo
    echo "core itself is licensed under the Apache License, Version 2.0 (see LICENSE)."
    echo "The notices below cover third-party code compiled into this binary, whose"
    echo "licenses require them to accompany it."
    echo
    echo "--- golang.org/x/exp/mmap ---"
    echo
    cat internal/mmap/LICENSE
    echo
    echo "--- github.com/zeebo/xxh3 ---"
    echo
    cat internal/xxh3/LICENSE
} > $INSTALL/THIRD-PARTY-NOTICES


if [ -n "$SERVER" ]; then
    echo "TRANSMIT to $SERVER"
    scp $INSTALL/* $SERVER:  # binary AND the notices that must accompany it

    echo "RUN $SERVER $INSTALLER"
    ssh $SERVER "sudo mv $BINARY /usr/local/bin && sudo mkdir -p /usr/local/share/$BINARY && sudo mv THIRD-PARTY-NOTICES /usr/local/share/$BINARY/"

    echo "CLEANUP"
    rm -fr $INSTALL
fi

echo "bye"