#!/usr/bin/env bash
set -euo pipefail

cmd="$(basename "$0")"
if [[ -n "${FRO_CALL_LOG:-}" ]]; then
    printf '%s %s\n' "$cmd" "$*" >>"$FRO_CALL_LOG"
fi

exec -a "$cmd" /usr/local/bin/fro "$@"
