#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_ROOT="${DATA_ROOT:-/data/fro-test/coreutils-shell-bench}"
IMAGE_TAG="${IMAGE_TAG:-fro-coreutils-shell-bench:latest}"
CONTAINER_ENGINE="${CONTAINER_ENGINE:-docker}"
CONTAINER_SUDO="${CONTAINER_SUDO:-0}"
CONTAINER_RUN_SECCOMP="${CONTAINER_RUN_SECCOMP:-unconfined}"
SKIP_BUILD="${SKIP_BUILD:-0}"
RUN_ID="${RUN_ID:-$(date +%Y%m%d-%H%M%S)}"
RUN_ROOT="${DATA_ROOT}/${RUN_ID}"

mkdir -p "$RUN_ROOT"

container_cmd() {
    if [[ "$CONTAINER_SUDO" == "1" ]]; then
        sudo "$CONTAINER_ENGINE" "$@"
    else
        "$CONTAINER_ENGINE" "$@"
    fi
}

container_run() {
    local -a args=(run)
    if [[ -n "$CONTAINER_RUN_SECCOMP" ]]; then
        args+=(--security-opt "seccomp=${CONTAINER_RUN_SECCOMP}")
    fi
    container_cmd "${args[@]}" "$@"
}

if ! container_cmd version >/dev/null 2>&1; then
    echo "Cannot access container engine '${CONTAINER_ENGINE}'. Try CONTAINER_SUDO=1 or a different CONTAINER_ENGINE." >&2
    exit 1
fi

cd "$ROOT_DIR"
if [[ "$SKIP_BUILD" != "1" ]]; then
    cargo +stable build --release --quiet
fi
container_cmd build -t "$IMAGE_TAG" -f docker/ubuntu-install-bench/Dockerfile .

seed_workload() {
    local seed_root="${RUN_ROOT}/seed"
    mkdir -p "${seed_root}/workload" "${seed_root}/bench-output"
    rm -rf "${seed_root}/workload"/* "${seed_root}/bench-output"/*

    container_run --rm \
        -e "BENCH_MODE=seed" \
        -e "BENCH_SEED_DIR=/seed/workload" \
        -e "BENCH_OUTPUT_DIR=/bench-output" \
        -v "${seed_root}/workload:/seed/workload" \
        -v "${seed_root}/bench-output:/bench-output" \
        "$IMAGE_TAG" \
        | tee "${seed_root}/container.stdout"
}

run_mode() {
    local mode="$1"
    local mode_root="${RUN_ROOT}/${mode}"
    mkdir -p "${mode_root}/work" "${mode_root}/bench-output"
    rm -rf "${mode_root}/work"/* "${mode_root}/bench-output"/*

    local -a env_args=(
        -e "BENCH_MODE=${mode}"
        -e "BENCH_SEED_DIR=/seed/workload"
        -e "BENCH_WORK_DIR=/work"
        -e "BENCH_OUTPUT_DIR=/bench-output"
    )
    if [[ "$mode" == "fro" ]]; then
        env_args+=( -e "FRO_LOG_FALLBACKS=1" )
    fi

    local host_start host_end host_seconds
    host_start="$(date +%s.%N)"
    container_run --rm \
        "${env_args[@]}" \
        -v "${mode_root}/work:/work" \
        -v "${mode_root}/bench-output:/bench-output" \
        -v "${RUN_ROOT}/seed/workload:/seed/workload:ro" \
        "$IMAGE_TAG" \
        | tee "${mode_root}/container.stdout"
    host_end="$(date +%s.%N)"
    host_seconds="$(awk -v start="$host_start" -v end="$host_end" 'BEGIN { printf "%.6f", end - start }')"
    printf '%s\n' "$host_seconds" >"${mode_root}/host_wall_seconds.txt"
}

summary_value() {
    local file="$1"
    local key="$2"
    awk -F= -v key="$key" '$1 == key { print substr($0, length(key) + 2) }' "$file"
}

seed_workload
run_mode baseline
run_mode fro

baseline_summary="${RUN_ROOT}/baseline/bench-output/summary.env"
fro_summary="${RUN_ROOT}/fro/bench-output/summary.env"

printf '\nResults for %s\n' "$RUN_ROOT"
printf '%-10s %-10s %-10s %-10s %-10s %-10s %-12s %-12s %-12s\n' \
    "mode" "prep_s" "config_s" "build_s" "pack_s" "verify_s" "total_s" "host_wall_s" "fro_calls"
for mode in baseline fro; do
    summary="${RUN_ROOT}/${mode}/bench-output/summary.env"
    printf '%-10s %-10s %-10s %-10s %-10s %-10s %-12s %-12s %-12s\n' \
        "$mode" \
        "$(summary_value "$summary" prep_seconds)" \
        "$(summary_value "$summary" configure_seconds)" \
        "$(summary_value "$summary" build_seconds)" \
        "$(summary_value "$summary" package_seconds)" \
        "$(summary_value "$summary" verify_seconds)" \
        "$(summary_value "$summary" total_seconds)" \
        "$(cat "${RUN_ROOT}/${mode}/host_wall_seconds.txt")" \
        "$(summary_value "$summary" fro_call_count)"
done
printf '\n%-10s %-12s %-12s\n' "mode" "fallbacks" "module_count"
for mode in baseline fro; do
    summary="${RUN_ROOT}/${mode}/bench-output/summary.env"
    printf '%-10s %-12s %-12s\n' \
        "$mode" \
        "$(summary_value "$summary" fallback_lines)" \
        "$(summary_value "$summary" module_count)"
done

if [[ -f "$baseline_summary" && -f "$fro_summary" ]]; then
    baseline_total="$(summary_value "$baseline_summary" total_seconds)"
    fro_total="$(summary_value "$fro_summary" total_seconds)"
    baseline_host="$(cat "${RUN_ROOT}/baseline/host_wall_seconds.txt")"
    fro_host="$(cat "${RUN_ROOT}/fro/host_wall_seconds.txt")"
    awk -v baseline="$baseline_total" -v fro="$fro_total" '
        BEGIN {
            diff = fro - baseline;
            pct = (baseline == 0) ? 0 : (diff / baseline) * 100;
            printf "\nInternal delta (fro - baseline): %.6f s (%+.2f%%)\n", diff, pct;
        }
    '
    awk -v baseline="$baseline_host" -v fro="$fro_host" '
        BEGIN {
            diff = fro - baseline;
            pct = (baseline == 0) ? 0 : (diff / baseline) * 100;
            printf "Host wall delta (fro - baseline): %.6f s (%+.2f%%)\n", diff, pct;
        }
    '
fi
