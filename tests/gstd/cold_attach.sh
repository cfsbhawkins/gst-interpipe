#!/usr/bin/env bash
#
# gstd reproduction of the interpipe cold-attach negotiation race.
#
# Three independent pipelines bridged by interpipe, matching the deployment
# topology: a producer (ingest) feeds a raw node; a middle leg consumes the raw
# node and republishes to an encoded node; a delivery leg consumes the encoded
# node. The middle and delivery legs are live, compensate-ts consumers attached
# before the producer exists (cold attach).
#
# Two scenarios are exercised:
#   ordered : consumers played first, producer a couple seconds later
#   rapid   : all three legs played back-to-back with no settle time
#
# When the race is present, a cold-attached consumer's downstream errors with
# "not-negotiated" because its base source never renegotiates once caps finally
# arrive. The script counts not-negotiated occurrences in the GStreamer log and
# the buffers that reached the encoded node; a clean run is zero errors with
# buffers flowing through every leg.
#
# Only stock base-plugin elements are used so the reproduction needs no extra
# codecs. Set ENC to insert a heavier downstream encoder if desired.
set -u

G="http://127.0.0.1:35391"
SCENARIO="${1:-ordered}"
# Downstream of the middle leg. videoconvert alone forces real caps
# negotiation; override ENC for a stricter, real encoder that exposes the race,
# e.g. ENC="videoconvert ! x264enc tune=zerolatency" or the deployment's own
# encoder followed by its parser. A flexible converter renegotiates on late caps
# and hides the bug; a strict encoder does not.
ENC="${ENC:-videoconvert ! video/x-raw,format=RGB}"

st()   { curl -s "$G/pipelines/$1/state" | sed -n 's/.*"value" : "\([^"]*\)".*/\1/p' | head -1; }
mk()   { curl -s -G -X POST "$G/pipelines" --data-urlencode "name=$1" --data-urlencode "description=$2" >/dev/null; }
play() { curl -s -X PUT "$G/pipelines/$1/state?name=playing" >/dev/null; }
del()  { curl -s -X PUT "$G/pipelines/$1/state?name=null" >/dev/null 2>&1; curl -s -X DELETE "$G/pipelines?name=$1" >/dev/null 2>&1; }

for p in ca_delivery ca_encode ca_ingest; do del "$p"; done
sleep 1

ING="videotestsrc is-live=true ! video/x-raw,format=I420,width=1280,height=720,framerate=30/1 ! queue ! interpipesink name=ca-raw sync=true async=false"
ENCP="interpipesrc listen-to=ca-raw stream-sync=compensate-ts is-live=true format=time allow-renegotiation=false ! queue leaky=downstream max-size-buffers=2 ! ${ENC} ! interpipesink name=ca-encoded sync=false async=false"
# The delivery leg only needs to drain the encoded node, so it stays
# codec-agnostic (a bare fakesink accepts raw or any encoded format).
DEL="interpipesrc listen-to=ca-encoded stream-sync=compensate-ts is-live=true format=time allow-renegotiation=false ! queue leaky=downstream max-size-buffers=2 ! fakesink sync=false async=false"

if [ "$SCENARIO" = "rapid" ]; then
  echo "--- rapid: play all three back-to-back ---"
  mk ca_ingest "$ING"; mk ca_encode "$ENCP"; mk ca_delivery "$DEL"
  play ca_ingest; play ca_encode; play ca_delivery
else
  # Consumers attach while the producer node does not exist yet: the middle leg
  # registers as a listener for a node that appears only later.
  echo "--- ordered: consumers first, producer node created 2s later ---"
  mk ca_encode "$ENCP"; mk ca_delivery "$DEL"
  play ca_encode; play ca_delivery
  sleep 2
  echo "before producer: encode=$(st ca_encode) delivery=$(st ca_delivery)"
  mk ca_ingest "$ING"; play ca_ingest
fi

sleep 5
echo "FINAL states: ingest=$(st ca_ingest) encode=$(st ca_encode) delivery=$(st ca_delivery)"

for p in ca_delivery ca_encode ca_ingest; do del "$p"; done
