#!/usr/bin/env bash
#
# benchmark-build.sh - Quick build benchmarking script
# 
# Measures and compares build performance with/without optimizations
# Usage: ./scripts/benchmark-build.sh
#

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

BINARIES=("temporal-server" "tdbg" "temporal-cassandra-tool" "temporal-sql-tool" "temporal-elasticsearch-tool")

log() { echo -e "${BLUE}[BENCH]${NC} $1"; }
success() { echo -e "${GREEN}[PASS]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
error() { echo -e "${RED}[FAIL]${NC} $1"; }
section() { echo -e "\n${CYAN}=== $1 ===${NC}"; }

measure_build() {
    local label="$1"
    shift
    local make_args=("$@")
    
    log "Building: ${label}" >&2
    make clean-bins >/dev/null 2>&1
    
    local start_time
    start_time=$(date +%s%N)
    
    if make bins "${make_args[@]}" >/dev/null 2>&1; then
        local end_time
        end_time=$(date +%s%N)
        local duration_ms=$(( (end_time - start_time) / 1000000 ))
        # Only output the number, nothing else
        printf "%d" "${duration_ms}"
        return 0
    else
        printf "FAILED"
        return 1
    fi
}

measure_total_size() {
    local total=0
    for binary in "${BINARIES[@]}"; do
        if [[ -f "${binary}" ]]; then
            local size
            size=$(stat -c%s "${binary}")
            total=$((total + size))
        fi
    done
    echo "${total}"
}

check_symbols_stripped() {
    local binary="$1"
    if [[ ! -f "${binary}" ]]; then
        return 1
    fi
    
    # go tool nm should return very few symbols when stripped
    local symbol_count
    symbol_count=$(go tool nm "${binary}" 2>&1 | grep -c "^[0-9a-f]" 2>/dev/null || echo "0")
    # Ensure it's a clean number
    symbol_count=$(echo "${symbol_count}" | tr -d '[:space:]')
    
    if [[ ${symbol_count} -lt 50 ]]; then
        echo "STRIPPED (${symbol_count} symbols)"
        return 0
    else
        echo "NOT_STRIPPED (${symbol_count} symbols)"
        return 1
    fi
}

check_trimpath() {
    local binary="$1"
    if [[ ! -f "${binary}" ]]; then
        return 1
    fi
    
    # Check if build paths are embedded in binary
    if strings "${binary}" | grep -q "^/app/"; then
        echo "PATHS_FOUND"
        return 1
    else
        echo "PATHS_STRIPPED"
        return 0
    fi
}

main() {
    section "Build Optimization Benchmark"
    log "Go: $(go version)"
    log "CPUs: $(nproc)"
    log "Memory: $(free -h | grep Mem | awk '{print $2}')"
    
    # Test 1: Unoptimized sequential build
    section "Test 1: Unoptimized Sequential Build"
    local t1_start
    t1_start=$(date +%s%N)
    local t1_result
    t1_result=$(measure_build "unoptimized-sequential" "PARALLEL_BINS=0" "STRIP_SYMBOLS=0" "TRIM_PATH=0")
    local t1_end
    t1_end=$(date +%s%N)
    local t1_time=$(( (t1_end - t1_start) / 1000000 ))
    local t1_size
    t1_size=$(measure_total_size)
    log "Time: ${t1_time}ms, Size: $(awk "BEGIN {printf \"%.2f\", ${t1_size}/1048576}")MB"
    
    # Test 2: Optimized parallel build
    section "Test 2: Optimized Parallel Build"
    local t2_start
    t2_start=$(date +%s%N)
    local t2_result
    t2_result=$(measure_build "optimized-parallel" "PARALLEL_BINS=1" "STRIP_SYMBOLS=1" "TRIM_PATH=1")
    local t2_end
    t2_end=$(date +%s%N)
    local t2_time=$(( (t2_end - t2_start) / 1000000 ))
    local t2_size
    t2_size=$(measure_total_size)
    log "Time: ${t2_time}ms, Size: $(awk "BEGIN {printf \"%.2f\", ${t2_size}/1048576}")MB"
    
    # Results comparison
    section "Results Comparison"
    
    # Time comparison
    if [[ "${t1_result}" != "FAILED" && "${t2_result}" != "FAILED" ]]; then
        local speedup
        speedup=$(awk "BEGIN {printf \"%.2f\", ${t1_result}/${t2_result}}")
        local time_saved=$((t1_result - t2_result))
        local time_pct
        time_pct=$(awk "BEGIN {printf \"%.1f\", (${time_saved}*100)/${t1_result}}")
        
        printf "${GREEN}Build Time Improvement:${NC}\n"
        printf "  Sequential (unoptimized): %dms\n" "${t1_result}"
        printf "  Parallel (optimized):     %dms\n" "${t2_result}"
        printf "  Speedup:                  %sx faster (%s%% time saved)\n" "${speedup}" "${time_pct}"
    fi
    
    # Size comparison
    local size_saved=$((t1_size - t2_size))
    local size_pct
    size_pct=$(awk "BEGIN {printf \"%.1f\", (${size_saved}*100)/${t1_size}}")
    local size_saved_mb
    size_saved_mb=$(awk "BEGIN {printf \"%.2f\", ${size_saved}/1048576}")
    
    printf "${GREEN}Binary Size Reduction:${NC}\n"
    printf "  Unoptimized total: %sMB\n" "$(awk "BEGIN {printf \"%.2f\", ${t1_size}/1048576}")"
    printf "  Optimized total:   %sMB\n" "$(awk "BEGIN {printf \"%.2f\", ${t2_size}/1048576}")"
    printf "  Size saved:        %sMB (%s%% reduction)\n" "${size_saved_mb}" "${size_pct}"
    
    # Symbol stripping verification
    section "Symbol Stripping Verification"
    for binary in "${BINARIES[@]}"; do
        if [[ -f "${binary}" ]]; then
            local symbol_status
            symbol_status=$(check_symbols_stripped "${binary}")
            if [[ "${symbol_status}" == STRIPPED* ]]; then
                success "${binary}: ${symbol_status}"
            else
                warn "${binary}: ${symbol_status}"
            fi
        fi
    done
    
    # Trimpath verification
    section "Trimpath Verification"
    for binary in "${BINARIES[@]}"; do
        if [[ -f "${binary}" ]]; then
            local trimpath_status
            trimpath_status=$(check_trimpath "${binary}")
            if [[ "${trimpath_status}" == "PATHS_STRIPPED" ]]; then
                success "${binary}: ${trimpath_status}"
            else
                warn "${binary}: ${trimpath_status}"
            fi
        fi
    done
    
    section "Benchmark Complete"
}

main "$@"
