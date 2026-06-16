#!/usr/bin/env bash
set -euo pipefail

MODE="${BENCH_MODE:-baseline}"
OUTPUT_DIR="${BENCH_OUTPUT_DIR:-/bench-output}"
SUMMARY_FILE="${OUTPUT_DIR}/summary.env"
LOG_FILE="${OUTPUT_DIR}/bench.log"
SEED_DIR="${BENCH_SEED_DIR:-/seed/workload}"
WORK_DIR="${BENCH_WORK_DIR:-/work}"

mkdir -p "$OUTPUT_DIR"
: >"$LOG_FILE"

export LC_ALL=C.UTF-8
export LANG=C.UTF-8

if [[ "$MODE" == "fro" ]]; then
    export PATH="/opt/fro-coreutils/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
    export FRO_LOG_FALLBACKS="${FRO_LOG_FALLBACKS:-1}"
    export FRO_CALL_LOG="${OUTPUT_DIR}/fro-calls.log"
    export FRO_IPC_TIMING_LOG="${OUTPUT_DIR}/fro-ipc-timings.log"
    : >"$FRO_CALL_LOG"
    : >"$FRO_IPC_TIMING_LOG"
    /bin/rm -f /tmp/fro-ipc.sock
    /usr/local/bin/fro-ipc-server >/dev/null 2>>"$LOG_FILE" &
    FRO_IPC_SERVER_PID=$!
    trap 'kill "$FRO_IPC_SERVER_PID" 2>/dev/null || true' EXIT
    for _ in $(seq 1 100); do
        [[ -S /tmp/fro-ipc.sock ]] && break
        /bin/sleep 0.01
    done
    if [[ ! -S /tmp/fro-ipc.sock ]]; then
        echo "fro IPC server failed to start" >&2
        exit 1
    fi
else
    export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
    unset FRO_LOG_FALLBACKS || true
    unset FRO_CALL_LOG || true
    unset FRO_IPC_TIMING_LOG || true
fi

source /opt/fro-bench/shell-workload.sh

write_key() {
    printf '%s=%s\n' "$1" "$2" >>"$SUMMARY_FILE"
}

measure_phase() {
    local label="$1"
    shift
    local start end duration rc
    start="$(date +%s.%N)"
    if "$@" >>"$LOG_FILE" 2>&1; then
        rc=0
    else
        rc=$?
    fi
    end="$(date +%s.%N)"
    duration="$(awk -v start="$start" -v end="$end" 'BEGIN { printf "%.6f", end - start }')"
    write_key "${label}_seconds" "$duration"
    if [[ "$rc" -ne 0 ]]; then
        write_key failed_phase "$label"
        write_key exit_code "$rc"
        return "$rc"
    fi
}

/bin/rm -f "$SUMMARY_FILE"
write_key mode "$MODE"
write_key path "$PATH"
write_key seed_dir "$SEED_DIR"
write_key work_dir "$WORK_DIR"

if [[ "$MODE" == "seed" ]]; then
    generate_seed_workload "$SEED_DIR"
    write_key seed_file_count "$(/usr/bin/find "$SEED_DIR/project" -type f | /usr/bin/wc -l | tr -d ' ')"
    write_key seed_module_count "$(/usr/bin/find "$SEED_DIR/project/modules" -mindepth 1 -maxdepth 1 -type d | /usr/bin/wc -l | tr -d ' ')"
    write_key seed_large_file_count "$(/usr/bin/find "$SEED_DIR/project/assets/large" -maxdepth 1 -type f -name '*.bin' | /usr/bin/wc -l | tr -d ' ')"
    write_key seed_large_total_bytes "$(/usr/bin/du -sb "$SEED_DIR/project/assets/large" | /usr/bin/awk '{print $1}')"
    /bin/cat "$SUMMARY_FILE"
    exit 0
fi

if [[ ! -d "$SEED_DIR/project" ]]; then
    echo "Seed workload missing at $SEED_DIR/project; run BENCH_MODE=seed first." >&2
    exit 1
fi

overall_start="$(date +%s.%N)"
measure_phase prep prepare_workspace "$SEED_DIR" "$WORK_DIR"
measure_phase configure run_configure_slice "$WORK_DIR/project" "$WORK_DIR/build"
measure_phase build run_build_slice "$WORK_DIR/project" "$WORK_DIR/build"
measure_phase largeio run_large_io_slice "$WORK_DIR/project" "$WORK_DIR/build" "$WORK_DIR/package"
measure_phase package run_package_slice "$WORK_DIR/project" "$WORK_DIR/build" "$WORK_DIR/package"
measure_phase verify run_verify_slice "$WORK_DIR/package" "$WORK_DIR/verify"
measure_phase cleanup run_cleanup_slice "$WORK_DIR"
overall_end="$(date +%s.%N)"
overall_duration="$(awk -v start="$overall_start" -v end="$overall_end" 'BEGIN { printf "%.6f", end - start }')"
write_key total_seconds "$overall_duration"
write_key module_count "$(/usr/bin/wc -l < "$WORK_DIR/build/manifests/module-list.txt" | tr -d ' ')"
write_key archive_count "$(/usr/bin/find "$WORK_DIR/package/packages" -type f -name '*.tar' | /usr/bin/wc -l | tr -d ' ')"

fallback_lines=0
if [[ -n "${FRO_LOG_FALLBACKS:-}" ]]; then
    fallback_lines="$(/usr/bin/grep -c '^fro: fallback ' "$LOG_FILE" || true)"
fi
write_key fallback_lines "$fallback_lines"

fro_call_count=0
if [[ -n "${FRO_CALL_LOG:-}" ]]; then
    fro_call_count="$(/usr/bin/wc -l < "$FRO_CALL_LOG" | tr -d ' ')"
    awk '{ counts[$1]++ } END { for (cmd in counts) printf "%s %s\n", counts[cmd], cmd }' "$FRO_CALL_LOG" \
        | /usr/bin/sort -k2 > "${OUTPUT_DIR}/fro-call-counts.txt"
fi
write_key fro_call_count "$fro_call_count"

if [[ -n "${FRO_IPC_TIMING_LOG:-}" ]]; then
    fro_timing_count="$(/usr/bin/wc -l < "$FRO_IPC_TIMING_LOG" | tr -d ' ')"
    awk '
        {
            cmd = $1
            nanos = $2 + 0
            count[cmd]++
            total[cmd] += nanos
            if (nanos > max[cmd]) max[cmd] = nanos
        }
        END {
            for (cmd in count) {
                printf "%s %d %.6f %.6f %.6f\n",
                    cmd,
                    count[cmd],
                    total[cmd] / 1000000.0,
                    (total[cmd] / count[cmd]) / 1000000.0,
                    max[cmd] / 1000000.0
            }
        }
    ' "$FRO_IPC_TIMING_LOG" | /usr/bin/sort -k4,4nr > "${OUTPUT_DIR}/fro-call-timing-summary.txt"
    write_key fro_timing_count "$fro_timing_count"
fi

/bin/cat "$SUMMARY_FILE"
