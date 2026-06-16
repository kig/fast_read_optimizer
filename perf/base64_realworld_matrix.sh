#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "usage: $0 <raw-file> <base64-file>" >&2
  exit 1
fi

RAW_FILE="$1"
BASE64_FILE="$2"
RAW_SIZE="$(stat -c %s "$RAW_FILE")"

run_case() {
  local label="$1"
  local bytes="$2"
  shift 2
  local start end dt gib
  start="$(python - <<'PY'
import time
print(time.time())
PY
)"
  "$@"
  end="$(python - <<'PY'
import time
print(time.time())
PY
)"
  dt="$(python - <<PY
start=$start
end=$end
print(end-start)
PY
)"
  gib="$(python - <<PY
bytes_count=$bytes
dt=$dt
print(bytes_count/dt/(1024**3))
PY
)"
  printf '%s: %.3fs, %.2f GiB/s\n' "$label" "$dt" "$gib"
}

run_case "decode file->file" "$RAW_SIZE" bash -lc "./target/release/fro base64 -d '$BASE64_FILE' >/dev/null"
run_case "decode file->pipe" "$RAW_SIZE" bash -lc "./target/release/fro base64 -d '$BASE64_FILE' | bin/wc -c >/dev/null"
run_case "decode pipe->file" "$RAW_SIZE" bash -lc "bin/cat '$BASE64_FILE' | ./target/release/fro base64 -d >/dev/null"
run_case "decode pipe->pipe" "$RAW_SIZE" bash -lc "bin/cat '$BASE64_FILE' | ./target/release/fro base64 -d | bin/wc -c >/dev/null"
run_case "encode file->file" "$RAW_SIZE" bash -lc "./target/release/fro base64 -w 0 '$RAW_FILE' >/dev/null"
run_case "encode file->pipe" "$RAW_SIZE" bash -lc "./target/release/fro base64 -w 0 '$RAW_FILE' | bin/wc -c >/dev/null"
run_case "encode pipe->file" "$RAW_SIZE" bash -lc "bin/cat '$RAW_FILE' | ./target/release/fro base64 -w 0 >/dev/null"
run_case "encode pipe->pipe" "$RAW_SIZE" bash -lc "bin/cat '$RAW_FILE' | ./target/release/fro base64 -w 0 | bin/wc -c >/dev/null"
