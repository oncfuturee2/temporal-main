#!/usr/bin/env bash
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
NC='\033[0m'

BINS=(
  temporal-server
  temporal-cassandra-tool
  temporal-sql-tool
  temporal-elasticsearch-tool
  tdbg
)

section() { printf "\n${BOLD}=== %s ===${NC}\n" "$1"; }
pass()  { printf "  ${GREEN}[PASS]${NC} %s\n" "$1"; }
fail()  { printf "  ${RED}[FAIL]${NC} %s\n" "$1"; }
info()  { printf "  ${YELLOW}[INFO]${NC} %s\n" "$1"; }
fmtbytes() {
  local bytes=$1
  if command -v numfmt &>/dev/null; then
    numfmt --to=iec-i --suffix=B "$bytes"
  else
    echo "${bytes} bytes"
  fi
}

cleanup() {
  rm -f "${BINS[@]}" temporal-server-debug fairsim
  rm -f /tmp/bench_results.txt "${TMPDIR:-/tmp}/verify_symbols.log"
}
trap cleanup EXIT

section "Phase 1: Build optimized binaries with concurrent compilation"

CPU_CORES=$(nproc 2>/dev/null || echo 4)
info "Detected $CPU_CORES CPU cores"
info "Running: make concurrent-bins"

BUILD_START=$(date +%s%N)
make concurrent-bins
BUILD_END=$(date +%s%N)
BUILD_ELAPSED_NS=$((BUILD_END - BUILD_START))
BUILD_ELAPSED_SEC=$(awk "BEGIN {printf \"%.2f\", $BUILD_ELAPSED_NS / 1000000000}")
printf "  ${GREEN}Concurrent build completed in %s seconds${NC}\n" "$BUILD_ELAPSED_SEC"

section "Phase 2: Binary size analysis"

TOTAL_SIZE=0
for bin in "${BINS[@]}"; do
  if [[ ! -f "$bin" ]]; then
    fail "$bin not found"
    exit 1
  fi
  SIZE=$(stat -c%s "$bin" 2>/dev/null || stat -f%z "$bin" 2>/dev/null)
  TOTAL_SIZE=$((TOTAL_SIZE + SIZE))
  printf "  %-32s %s\n" "$bin:" "$(fmtbytes $SIZE)"
done
printf "  ${BOLD}%-32s %s${NC}\n" "Total:" "$(fmtbytes $TOTAL_SIZE)"

section "Phase 3: Symbol table stripping verification (go tool nm)"

ALL_STRIPPED=true
for bin in "${BINS[@]}"; do
  SYM_OUTPUT=$(go tool nm "$bin" 2>&1 || true)
  if echo "$SYM_OUTPUT" | grep -q "no symbols"; then
    pass "$bin: symbol table fully stripped"
  else
    SYM_COUNT=$(echo "$SYM_OUTPUT" | wc -l)
    fail "$bin: symbol table NOT stripped ($SYM_COUNT symbols found)"
    ALL_STRIPPED=false
    echo "$SYM_OUTPUT" | head -5 | while read -r line; do
      printf "        %s\n" "$line"
    done
  fi
done

section "Phase 4: DWARF debug info verification"

if command -v readelf &>/dev/null; then
  for bin in "${BINS[@]}"; do
    DEBUG_COUNT=$(readelf -S "$bin" 2>/dev/null | grep -c '\.debug_' || true)
    if [[ "$DEBUG_COUNT" -gt 0 ]]; then
      fail "$bin: DWARF debug sections present ($DEBUG_COUNT sections)"
      ALL_STRIPPED=false
      readelf -S "$bin" 2>/dev/null | grep '\.debug_' | head -5
    else
      pass "$bin: no DWARF debug sections"
    fi
  done
else
  for bin in "${BINS[@]}"; do
    if go tool objdump -s '.debug_info' "$bin" 2>&1 | grep -q ".debug_info"; then
      fail "$bin: DWARF debug info present"
      ALL_STRIPPED=false
    else
      pass "$bin: no DWARF debug sections (verified via objdump)"
    fi
  done
fi

section "Phase 5: -trimpath verification (no local paths leaked)"

LEAKED_PATHS=false
for bin in "${BINS[@]}"; do
  if strings "$bin" 2>/dev/null | grep -qE "^/(app|home|root|tmp|Users)/"; then
    LEAKED=$(strings "$bin" 2>/dev/null | grep -cE "^/(app|home|root|tmp|Users)/" || true)
    fail "$bin: $LEAKED absolute filesystem paths leaked (trimpath may not be effective)"
    LEAKED_PATHS=true
    strings "$bin" 2>/dev/null | grep -E "^/(app|home|root)" | head -5 | while read -r line; do
      printf "        %s\n" "$line"
    done
  else
    pass "$bin: no local build path leaked"
  fi
done

section "Phase 6: ELF section layout (sanity check)"

for bin in "${BINS[@]}"; do
  SECTIONS=$(readelf -S "$bin" 2>/dev/null | grep -E '^\s*\[.*\]' | awk '{print $2}' | tr '\n' ' ' || true)
  info "$bin sections: $SECTIONS"
done

section "Summary"

echo ""
printf "  Build time:        %s seconds (concurrent, %d cores)\n" "$BUILD_ELAPSED_SEC" "$CPU_CORES"
printf "  Total binary size: %s\n" "$(fmtbytes $TOTAL_SIZE)"
if $ALL_STRIPPED && ! $LEAKED_PATHS; then
  printf "  ${GREEN}Status: ALL CHECKS PASSED${NC}\n"
  printf "  ${GREEN}  - Symbols stripped (ldflags=-s -w)${NC}\n"
  printf "  ${GREEN}  - DWARF removed (ldflags=-w)${NC}\n"
  printf "  ${GREEN}  - No build path leakage (trimpath)${NC}\n"
  echo ""
  exit 0
else
  printf "  ${RED}Status: SOME CHECKS FAILED${NC}\n"
  echo ""
  exit 1
fi