#!/usr/bin/env bash
#
# Build the interpipe plugin from the current source tree, start gstd with the
# plugin on its path, run the cold-attach reproduction, and report the signal:
# the number of not-negotiated errors and the buffers that reached each node.
#
# Intended to run inside the interpipe-gstd image with the source bind-mounted
# at /src. Exit status is non-zero when not-negotiated is observed, so the same
# invocation is red on an unpatched tree and green on a patched one.
set -u

SCENARIO="${1:-ordered}"
SRC=/src
PLUGIN_DIR="$SRC/build/gst/interpipe"
GSTLOG=/tmp/gst.log

# Build the plugin (quietly; fail loudly).
meson setup "$SRC/build" -Denable-gtk-doc=false >/tmp/setup.log 2>&1 || true
ninja -C "$SRC/build" >/tmp/build.log 2>&1 || { echo "BUILD FAILED"; tail -20 /tmp/build.log; exit 99; }

export GST_PLUGIN_PATH_1_0="$PLUGIN_DIR"
export GST_DEBUG="2,interpipesink:6,basesrc:4"
# Route the GStreamer debug log to a file we can grep, independent of gstd's
# own daemon/foreground logging.
export GST_DEBUG_FILE="$GSTLOG"
export GST_DEBUG_NO_COLOR=1

# Start gstd, HTTP API on 35391.
pkill -x gstd 2>/dev/null
rm -f "$GSTLOG"
gstd --enable-http-protocol --http-port 35391 --http-address 127.0.0.1 \
     >/tmp/gstd.out 2>&1 &
GSTD_PID=$!

# Wait for the HTTP API to come up.
for i in $(seq 1 50); do
  curl -s "http://127.0.0.1:35391/pipelines" >/dev/null 2>&1 && break
  sleep 0.2
done

bash "$SRC/tests/gstd/cold_attach.sh" "$SCENARIO"

# Let final bus messages flush to the log.
sleep 1
kill "$GSTD_PID" 2>/dev/null

NN=$(grep -c "not-negotiated" "$GSTLOG" 2>/dev/null); NN=${NN:-0}
RAW=$(grep -c "on node ca-raw" "$GSTLOG" 2>/dev/null); RAW=${RAW:-0}
ENC=$(grep -c "on node ca-encoded" "$GSTLOG" 2>/dev/null); ENC=${ENC:-0}

echo "================ RESULT ($SCENARIO) ================"
echo "not-negotiated errors : $NN"
echo "raw-node buffers      : $RAW"
echo "encoded-node buffers  : $ENC"
echo "===================================================="

# Real signal: the encoded leg must carry buffers and post no negotiation error.
if [ "$NN" -gt 0 ] || [ "$ENC" -eq 0 ]; then
  echo "VERDICT: FAIL (race present or encoded leg starved)"
  exit 1
fi
echo "VERDICT: PASS"
exit 0
