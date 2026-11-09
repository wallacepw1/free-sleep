#!/bin/sh
# Confirm an installed Free Sleep 3.0.3 tree matches the source this patch was
# built against. There is no git metadata on the Pod. A version string alone is
# not enough: refuse to continue when any file the patch edits has drifted.
set -eu

root=${1:-/home/dac/free-sleep}
cd "$root"

fail=0

hash_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    openssl dgst -sha256 "$1" | awk '{print $NF}'
  fi
}

expect_hash() {
  file=$1
  expected=$2
  if [ ! -f "$file" ]; then
    echo "MISSING $file"
    fail=1
    return
  fi
  actual=$(hash_file "$file")
  if [ "$actual" != "$expected" ]; then
    echo "MISMATCH $file"
    echo "  expected $expected"
    echo "  actual   $actual"
    fail=1
    return
  fi
  echo "OK $file"
}

info=server/src/serverInfo.json
if [ ! -f "$info" ]; then
  echo "MISSING $info"
  exit 1
fi
if ! grep -q '"version": "3.0.3"' "$info" || ! grep -q '"branch": "main"' "$info"; then
  echo "REFUSING: $info is not version 3.0.3 on branch main"
  cat "$info"
  exit 1
fi
echo "OK $info version 3.0.3 main"

if grep -q 'Still waiting for this command only' server/src/8sleep/frankenServer.ts 2>/dev/null; then
  echo "REFUSING: frankenServer.ts already contains the patched slow-reply marker"
  exit 1
fi
if ! grep -q 'Closing stale socket' server/src/8sleep/frankenServer.ts 2>/dev/null; then
  echo "REFUSING: frankenServer.ts does not contain the 3.0.3 'Closing stale socket' path"
  exit 1
fi

# SHA-256 of e5172139874a274d1ced12c8da052ab2cbaa286d, which is the 3.0.3 tree
# whose serverInfo.json says version 3.0.3 / branch main.
expect_hash server/src/serverInfo.json e644be7572b4ae3a400c995ea54a1da471091f98d9078feebc740442664a9f32
expect_hash server/src/8sleep/frankenServer.ts 32ed22cef153b9a2ff511e29f4117d12129e5bede9327b83359ed558e5e7f9b9
expect_hash server/src/8sleep/messageStream.ts bb5cd81f9d0061e2d300febedb2a6c6e08f21f4947e15f2db07916821144a359
expect_hash server/src/8sleep/unixSocketServer.ts 501a4ddb85cf3404586094de4461f5ecfe33ba65cdd0252fc0edaae1dc944f1b
expect_hash server/src/8sleep/deviceApi.ts f4a65ce9ec7ed1c9510c0d43fd5ec10dc5f23acbb5aff46d6a0e0a90b1944878
expect_hash server/src/8sleep/frankenMonitor.ts 7f38a6f070656efddb08792070d3b18d66b1a8c35590c9d772a061ca14277cec
expect_hash server/src/server.ts 7a2e06de6868f91dd59552255525cd3d531e46842a15487b7a4667d122f677ed
expect_hash server/src/jobs/alarmScheduler.ts 939cdb76fe4cc1d25174dada0d6e30c6deb26422937ce529d3de4d8c9754206a
expect_hash server/src/routes/deviceStatus/deviceStatus.ts e71f8bf752233e7902a3b55f95ca6d53835419ccfcd73987fdf0277ff6111d0e
expect_hash server/src/setupSentryTags.ts 8a3bb8bd5dcb7279cf3559ffc956afeed20f078b653a46077d531b546bfd2045

if [ "$fail" -ne 0 ]; then
  echo "REFUSING: installed source does not match the 3.0.3 baseline this patch edits."
  echo "Do not apply the patch and do not replace the tree."
  exit 1
fi

echo "SOURCE MATCHES 3.0.3 baseline e5172139874a274d1ced12c8da052ab2cbaa286d"
echo "Dist bytes are not a gate. Compile the patched sources on this Pod."
exit 0
