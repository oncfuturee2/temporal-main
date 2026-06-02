#!/usr/bin/env bash
#
# verify-build-optimization.sh
# 
# Validates build optimization effects:
# 1. Measures parallel vs sequential build times
# 2. Checks binary size reduction
# 3. Verifies symbol table stripping using go tool nm
# 4. Validates -trimpath effectiveness
#
# Usage: ./scripts/verify-build-optimization.sh
#

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
BINARIES=("temporal-server" "tdbg" "temporal-cassandra-tool" "temporal-sql-tool" "temporal-elasticsearch-tool")
BUILD_DIR="./build-verification"
RESULTS_FILE="${BUILD_DIR}/results.txt"

# Utility functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Initialize
setup() {
    log_info "Setting up verification environment..."
    mkdir -p "${BUILD_DIR}"
    > "${RESULTS_FILE}"
    
    # Record system info
    echo "=== System Information ===" >> "${RESULTS_FILE}"
    echo "Date: $(date)" >> "${RESULTS_FILE}"
    echo "Go Version: $(go version)" >> "${RESULTS_FILE}"
    echo "CPU Cores: $(nproc)" >> "${RESULTS_FILE}"
    echo "Memory: $(free -h | grep Mem | awk '{print $2}')" >> "${RESULTS_FILE}"
    echo "" >> "${RESULTS_FILE}"
}

# Build binaries with specified configuration
build_binaries() {
    local config_name="$1"
    shift
    local make_args=("$@")
    
    log_info "Building binaries: ${config_name}"
    
    # Clean previous builds
    make clean-bins >/dev/null 2>&1
    
    # Record start time
    local start_time
    start_time=$(date +%s%N)
    
    # Build
    if make bins "${make_args[@]}" >/dev/null 2>&1; then
        local end_time
        end_time=$(date +%s%N)
        local duration=$(( (end_time - start_time) / 1000000 )) # milliseconds
        
        log_success "Build completed: ${config_name} (${duration}ms)"
        echo "${config_name}: ${duration}ms" >> "${RESULTS_FILE}"
        return 0
    else
        log_error "Build failed: ${config_name}"
        return 1
    fi
}

# Measure binary sizes
measure_sizes() {
    local config_name="$1"
    local total_size=0
    
    echo "" >> "${RESULTS_FILE}"
    echo "=== Binary Sizes: ${config_name} ===" >> "${RESULTS_FILE}"
    
    printf "${BLUE}%-35s %15s %15s${NC}\n" "Binary" "Size (MB)" "Size (bytes)"
    printf "%-35s %15s %15s\n" "Binary" "Size (MB)" "Size (bytes)" >> "${RESULTS_FILE}"
    
    for binary in "${BINARIES[@]}"; do
        if [[ -f "${binary}" ]]; then
            local size_bytes
            size_bytes=$(stat -c%s "${binary}")
            local size_mb
            size_mb=$(echo "scale=2; ${size_bytes}/1048576" | bc)
            total_size=$((total_size + size_bytes))
            
            printf "${GREEN}%-35s %15s %15s${NC}\n" "${binary}" "${size_mb}" "${size_bytes}"
            printf "%-35s %15s %15s\n" "${binary}" "${size_mb}" "${size_bytes}" >> "${RESULTS_FILE}"
        else
            printf "${RED}%-35s %15s %15s${NC}\n" "${binary}" "MISSING" "N/A"
            printf "%-35s %15s %15s\n" "${binary}" "MISSING" "N/A" >> "${RESULTS_FILE}"
        fi
    done
    
    local total_mb
    total_mb=$(echo "scale=2; ${total_size}/1048576" | bc)
    printf "${YELLOW}%-35s %15s %15s${NC}\n" "TOTAL" "${total_mb}" "${total_size}"
    printf "%-35s %15s %15s\n" "TOTAL" "${total_mb}" "${total_size}" >> "${RESULTS_FILE}"
}

# Check symbol table stripping
check_symbol_stripping() {
    local binary="$1"
    
    if [[ ! -f "${binary}" ]]; then
        log_error "Binary not found: ${binary}"
        return 1
    fi
    
    log_info "Checking symbol stripping for: ${binary}"
    
    # Check for symbol table presence
    local nm_output
    nm_output=$(go tool nm "${binary}" 2>&1 || true)
    
    # Count symbols
    local symbol_count
    symbol_count=$(echo "${nm_output}" | grep -c "^[0-9a-f]" || echo "0")
    
    # Check for specific debug sections
    local has_gopclntab
    has_gopclntab=$(go tool nm "${binary}" 2>&1 | grep -c "runtime.pclntab" || echo "0")
    
    # Check for debug info
    local has_debug_info
    has_debug_info=$(objdump -h "${binary}" 2>/dev/null | grep -c "debug" || echo "0")
    
    echo "" >> "${RESULTS_FILE}"
    echo "=== Symbol Analysis: ${binary} ===" >> "${RESULTS_FILE}"
    echo "Symbol count: ${symbol_count}" >> "${RESULTS_FILE}"
    echo "Has pclntab: ${has_gopclntab}" >> "${RESULTS_FILE}"
    echo "Debug sections: ${has_debug_info}" >> "${RESULTS_FILE}"
    
    if [[ ${symbol_count} -lt 100 ]]; then
        log_success "${binary}: Symbols stripped (${symbol_count} symbols remaining)"
        return 0
    else
        log_warn "${binary}: ${symbol_count} symbols found (may not be fully stripped)"
        return 1
    fi
}

# Check trimpath effectiveness
check_trimpath() {
    local binary="$1"
    
    if [[ ! -f "${binary}" ]]; then
        log_error "Binary not found: ${binary}"
        return 1
    fi
    
    log_info "Checking -trimpath effectiveness for: ${binary}"
    
    # Look for file paths in binary
    local paths_found
    paths_found=$(strings "${binary}" | grep -E "^/app/" | head -5 || true)
    
    if [[ -z "${paths_found}" ]]; then
        log_success "${binary}: -trimpath effective (no /app/ paths found)"
        echo "${binary}: trimpath effective" >> "${RESULTS_FILE}"
        return 0
    else
        log_warn "${binary}: Found paths in binary:"
        echo "${paths_found}" | while read -r line; do
            log_warn "  ${line}"
        done
        echo "${binary}: trimpath may not be fully effective" >> "${RESULTS_FILE}"
        return 1
    fi
}

# Compare build configurations
compare_builds() {
    local sequential_time="$1"
    local parallel_time="$2"
    local unoptimized_size="$3"
    local optimized_size="$4"
    
    echo "" >> "${RESULTS_FILE}"
    echo "=== Build Comparison ===" >> "${RESULTS_FILE}"
    
    # Calculate speedup
    if [[ ${parallel_time} -gt 0 && ${sequential_time} -gt 0 ]]; then
        local speedup
        speedup=$(echo "scale=2; ${sequential_time}/${parallel_time}" | bc)
        local time_saved=$((sequential_time - parallel_time))
        local time_saved_pct
        time_saved_pct=$(echo "scale=1; (${time_saved}*100)/${sequential_time}" | bc)
        
        echo "Sequential build time: ${sequential_time}ms" >> "${RESULTS_FILE}"
        echo "Parallel build time: ${parallel_time}ms" >> "${RESULTS_FILE}"
        echo "Speedup: ${speedup}x (${time_saved_pct}% faster)" >> "${RESULTS_FILE}"
        
        printf "${GREEN}Parallel build: %.2fx faster (%dms vs %dms)${NC}\n" "${speedup}" "${sequential_time}" "${parallel_time}"
    fi
    
    # Calculate size reduction
    if [[ ${unoptimized_size} -gt 0 && ${optimized_size} -gt 0 ]]; then
        local size_saved=$((unoptimized_size - optimized_size))
        local size_saved_pct
        size_saved_pct=$(echo "scale=1; (${size_saved}*100)/${unoptimized_size}" | bc)
        
        echo "Unoptimized total size: ${unoptimized_size} bytes" >> "${RESULTS_FILE}"
        echo "Optimized total size: ${optimized_size} bytes" >> "${RESULTS_FILE}"
        echo "Size reduction: ${size_saved_pct}%" >> "${RESULTS_FILE}"
        
        printf "${GREEN}Size reduction: %s%% (%.2fMB saved)${NC}\n" "${size_saved_pct}" "$(echo "scale=2; ${size_saved}/1048576" | bc)"
    fi
}

# Main verification flow
main() {
    log_info "=== Build Optimization Verification ==="
    
    setup
    
    # Phase 1: Build with default optimizations (parallel + stripped)
    log_info "Phase 1: Building with optimizations (parallel + stripped + trimpath)"
    build_binaries "optimized-parallel" "PARALLEL_BINS=1"
    measure_sizes "optimized-parallel"
    
    # Check symbol stripping for main binary
    check_symbol_stripping "temporal-server"
    check_trimpath "temporal-server"
    
    # Phase 2: Build without optimizations for comparison
    log_info "Phase 2: Building without optimizations (sequential + no strip + no trimpath)"
    build_binaries "unoptimized-sequential" "PARALLEL_BINS=0" "STRIP_SYMBOLS=0" "TRIM_PATH=0"
    measure_sizes "unoptimized-sequential"
    
    # Phase 3: Build with optimizations but sequential
    log_info "Phase 3: Building with optimizations but sequential"
    build_binaries "optimized-sequential" "PARALLEL_BINS=0"
    measure_sizes "optimized-sequential"
    
    # Phase 4: Build without optimizations but parallel
    log_info "Phase 4: Building without optimizations but parallel"
    build_binaries "unoptimized-parallel" "PARALLEL_BINS=1" "STRIP_SYMBOLS=0" "TRIM_PATH=0"
    measure_sizes "unoptimized-parallel"
    
    # Final summary
    log_info "=== Verification Summary ==="
    log_info "Results saved to: ${RESULTS_FILE}"
    
    # Display results
    echo ""
    cat "${RESULTS_FILE}"
    
    log_success "Verification complete!"
}

main "$@"
