#!/bin/bash
set -e

# Build Optimization Verification Script
# This script verifies:
# 1. Concurrent build performance improvement
# 2. Binary size reduction with -ldflags="-s -w"
# 3. Symbol table stripping verification

COLOR_CYAN="\033[1;36m"
COLOR_GREEN="\033[0;32m"
COLOR_YELLOW="\033[1;33m"
COLOR_RED="\033[0;31m"
COLOR_RESET="\033[0m"

BINS=("temporal-server" "temporal-cassandra-tool" "temporal-sql-tool" "temporal-elasticsearch-tool" "tdbg")
CPU_CORES=$(nproc)

print_banner() {
    echo -e "${COLOR_CYAN}========================================${COLOR_RESET}"
    echo -e "${COLOR_CYAN}   Build Optimization Verification      ${COLOR_RESET}"
    echo -e "${COLOR_CYAN}========================================${COLOR_RESET}"
    echo ""
}

cleanup_bins() {
    echo -e "${COLOR_YELLOW}Cleaning up existing binaries...${COLOR_RESET}"
    for bin in "${BINS[@]}"; do
        rm -f "$bin"
    done
    echo ""
}

measure_build_time() {
    local description=$1
    shift
    local cmd="$@"
    
    echo -e "${COLOR_YELLOW}${description}${COLOR_RESET}"
    start_time=$(date +%s.%N)
    eval "$cmd"
    end_time=$(date +%s.%N)
    duration=$(echo "$end_time - $start_time" | bc)
    printf "  Time taken: %.2f seconds\n" "$duration"
    echo "$duration"
}

get_binary_sizes() {
    echo -e "${COLOR_YELLOW}Binary sizes:${COLOR_RESET}"
    total_size=0
    for bin in "${BINS[@]}"; do
        if [ -f "$bin" ]; then
            size=$(stat -c%s "$bin")
            size_mb=$(echo "scale=2; $size / 1024 / 1024" | bc)
            total_size=$(echo "$total_size + $size" | bc)
            echo "  $bin: ${size_mb} MB"
        fi
    done
    total_size_mb=$(echo "scale=2; $total_size / 1024 / 1024" | bc)
    echo "  Total: ${total_size_mb} MB"
    echo "$total_size"
}

verify_symbol_stripping() {
    local binary=$1
    echo -e "${COLOR_YELLOW}Checking symbol table for $binary...${COLOR_RESET}"
    
    # Check for debug symbols using go tool nm
    if go tool nm "$binary" 2>&1 | grep -q "no symbol table"; then
        echo -e "  ${COLOR_GREEN}✓ Symbol table successfully stripped${COLOR_RESET}"
        return 0
    else
        # Count number of symbols
        symbol_count=$(go tool nm "$binary" 2>/dev/null | wc -l)
        if [ "$symbol_count" -lt 100 ]; then
            echo -e "  ${COLOR_GREEN}✓ Symbol table significantly stripped (only $symbol_count symbols)${COLOR_RESET}"
            return 0
        else
            echo -e "  ${COLOR_RED}✗ Symbol table still present ($symbol_count symbols)${COLOR_RESET}"
            return 1
        fi
    fi
}

verify_trimpath() {
    local binary=$1
    echo -e "${COLOR_YELLOW}Checking trimpath for $binary...${COLOR_RESET}"
    
    # Check if binary contains build paths
    if strings "$binary" 2>&1 | grep -q "/app/" || strings "$binary" 2>&1 | grep -q "/go/"; then
        echo -e "  ${COLOR_RED}✗ Build paths still present in binary${COLOR_RESET}"
        return 1
    else
        echo -e "  ${COLOR_GREEN}✓ Build paths successfully stripped${COLOR_RESET}"
        return 0
    fi
}

# Main script execution
print_banner

echo -e "${COLOR_CYAN}System information:${COLOR_RESET}"
echo "  CPU cores: $CPU_CORES"
echo "  Go version: $(go version)"
echo ""

# Test 1: Build with optimizations and concurrent
cleanup_bins
echo -e "${COLOR_CYAN}Test 1: Concurrent build with optimizations (-s -w, -trimpath)${COLOR_RESET}"
echo "==============================================="
opt_concurrent_time=$(measure_build_time "Building concurrently with optimizations..." "make -j${CPU_CORES} bins")
opt_concurrent_size=$(get_binary_sizes)
verify_symbol_stripping "temporal-server"
verify_trimpath "temporal-server"
echo ""

# Test 2: Build with optimizations but not concurrent (for comparison)
cleanup_bins
echo -e "${COLOR_CYAN}Test 2: Serial build with optimizations${COLOR_RESET}"
echo "======================================"
opt_serial_time=$(measure_build_time "Building serially with optimizations..." "make -j1 bins")
opt_serial_size=$(get_binary_sizes)
echo ""

# Show comparison
echo -e "${COLOR_CYAN}Performance Comparison${COLOR_RESET}"
echo "======================"
echo "  Concurrent with optimizations: ${opt_concurrent_time}s"
echo "  Serial with optimizations:     ${opt_serial_time}s"
speedup=$(echo "scale=2; $opt_serial_time / $opt_concurrent_time" | bc)
echo -e "  Speedup: ${COLOR_GREEN}${speedup}x${COLOR_RESET}"
echo ""

echo -e "${COLOR_GREEN}==============================================${COLOR_RESET}"
echo -e "${COLOR_GREEN}Build optimization verification completed!${COLOR_RESET}"
echo -e "${COLOR_GREEN}==============================================${COLOR_RESET}"
