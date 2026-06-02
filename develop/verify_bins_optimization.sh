#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

cpu_count="$(getconf _NPROCESSORS_ONLN 2>/dev/null || nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 1)"
readonly cpu_count
readonly bin_targets=(
  temporal-server
  temporal-cassandra-tool
  temporal-sql-tool
  temporal-elasticsearch-tool
  tdbg
)

workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT

format_millis() {
  local millis="$1"
  printf '%d.%03ds' "$((millis / 1000))" "$((millis % 1000))"
}

run_build() {
  local name="$1"
  local jobs="$2"
  local trimpath="$3"
  local ldflags="$4"
  local outdir="$workdir/$name"
  local cache_dir="$workdir/cache-$name"
  local log_file="$workdir/$name.log"
  local start_ns
  local end_ns
  local duration_ms
  local bin

  mkdir -p "$outdir" "$cache_dir"
  make clean-bins >/dev/null

  start_ns="$(date +%s%N)"
  if ! GOCACHE="$cache_dir" make BUILD_JOBS="$jobs" BIN_GO_TRIMPATH="$trimpath" BIN_GO_LDFLAGS="$ldflags" bins >"$log_file" 2>&1; then
    cat "$log_file"
    return 1
  fi
  end_ns="$(date +%s%N)"
  duration_ms="$(((end_ns - start_ns) / 1000000))"

  for bin in "${bin_targets[@]}"; do
    cp "$bin" "$outdir/$bin"
  done

  printf '%s' "$duration_ms" >"$outdir/duration_ms"
}

verify_nm_state() {
  local binary_path="$1"
  local expect_symbols="$2"
  local nm_output="$workdir/$(basename "$binary_path").nm"

  if go tool nm "$binary_path" >"$nm_output" 2>&1; then
    if [[ "$expect_symbols" != "present" ]]; then
      cat "$nm_output"
      echo "expected stripped binary for $binary_path" >&2
      return 1
    fi
    return 0
  fi

  if [[ "$expect_symbols" == "present" ]]; then
    cat "$nm_output"
    echo "expected symbol table for $binary_path" >&2
    return 1
  fi

  if ! grep -Eq 'no symbol section|no symbols' "$nm_output"; then
    cat "$nm_output"
    echo "unexpected go tool nm output for $binary_path" >&2
    return 1
  fi
}

run_build baseline-serial 1 false ""
run_build optimized-serial 1 true "-s -w"
run_build optimized-parallel "$cpu_count" true "-s -w"

baseline_dir="$workdir/baseline-serial"
optimized_parallel_dir="$workdir/optimized-parallel"
optimized_serial_ms="$(cat "$workdir/optimized-serial/duration_ms")"
optimized_parallel_ms="$(cat "$optimized_parallel_dir/duration_ms")"
baseline_ms="$(cat "$baseline_dir/duration_ms")"

total_baseline_size=0
total_optimized_size=0

printf 'CPU cores detected: %s\n' "$cpu_count"
printf 'Baseline serial build: %s\n' "$(format_millis "$baseline_ms")"
printf 'Optimized serial build: %s\n' "$(format_millis "$optimized_serial_ms")"
printf 'Optimized parallel build: %s\n' "$(format_millis "$optimized_parallel_ms")"
printf '\n%-32s %14s %14s %12s\n' 'binary' 'baseline' 'optimized' 'delta'

for bin in "${bin_targets[@]}"; do
  baseline_size="$(stat -c %s "$baseline_dir/$bin")"
  optimized_size="$(stat -c %s "$optimized_parallel_dir/$bin")"
  delta_size="$((baseline_size - optimized_size))"

  if ((delta_size <= 0)); then
    echo "expected optimized binary to be smaller for $bin" >&2
    exit 1
  fi

  verify_nm_state "$baseline_dir/$bin" present
  verify_nm_state "$optimized_parallel_dir/$bin" stripped

  total_baseline_size="$((total_baseline_size + baseline_size))"
  total_optimized_size="$((total_optimized_size + optimized_size))"

  printf '%-32s %14s %14s %12s\n' "$bin" "$baseline_size" "$optimized_size" "$delta_size"
done

printf '\nTotal baseline size: %s\n' "$total_baseline_size"
printf 'Total optimized size: %s\n' "$total_optimized_size"
printf 'Total size reduction: %s\n' "$((total_baseline_size - total_optimized_size))"

if ((cpu_count > 1)); then
  printf 'Parallel speedup vs optimized serial: %s ms\n' "$((optimized_serial_ms - optimized_parallel_ms))"
else
  printf 'Parallel speedup vs optimized serial: single-core environment\n'
fi
