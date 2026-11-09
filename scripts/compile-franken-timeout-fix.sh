#!/bin/sh
# Compile only the Franken timeout fix on the Pod. Does not upload source maps,
# does not restart services, and does not replace unrelated dist files.
set -eu

root=${1:-/home/dac/free-sleep}
server=$root/server
src=$server/src/8sleep/frankenServer.ts

if [ ! -f "$src" ]; then
  echo "Missing $src"
  exit 1
fi
if ! grep -q 'Still waiting for this command only' "$src"; then
  echo "Refusing to compile: the slow-reply patch is not present in $src"
  exit 1
fi
if ! grep -q '"version": "3.0.3"' "$server/src/serverInfo.json"; then
  echo "Refusing to compile: serverInfo.json is not 3.0.3"
  exit 1
fi

if [ -x /home/dac/.volta/bin/node ]; then
  PATH="/home/dac/.volta/bin:$PATH"
  export PATH
fi

node_version=$(node -v)
case $node_version in
  v24.*) ;;
  *)
    echo "Refusing to compile with $node_version. The service runs Node 24.11.0."
    exit 1
    ;;
esac

out=$(mktemp -d /tmp/free-sleep-franken-build.XXXXXX)
trap 'rm -rf "$out"' EXIT

cd "$server"
npx tsc --pretty false --outDir "$out"

copy_output() {
  relative=$1
  if [ ! -f "$out/$relative" ]; then
    echo "tsc did not emit $relative"
    exit 1
  fi
  mkdir -p "$(dirname "$server/dist/$relative")"
  cp "$out/$relative" "$server/dist/$relative"
  if [ -f "$out/$relative.map" ]; then
    cp "$out/$relative.map" "$server/dist/$relative.map"
  fi
  echo "updated dist/$relative"
}

copy_output 8sleep/frankenServer.js
copy_output 8sleep/messageStream.js
copy_output 8sleep/unixSocketServer.js
copy_output 8sleep/deviceApi.js
copy_output 8sleep/frankenMonitor.js
copy_output server.js
copy_output jobs/alarmScheduler.js
copy_output routes/deviceStatus/deviceStatus.js
copy_output setupSentryTags.js

if ! grep -q 'The command was not sent' "$server/dist/8sleep/frankenServer.js"; then
  echo "Compiled frankenServer.js is missing the destroyed-stream guard"
  exit 1
fi

echo "Compiled with $node_version. Dist for unrelated files was left in place."
echo "Restart only free-sleep after reviewing the deploy instructions."
