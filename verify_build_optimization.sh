#!/bin/bash
set -e

echo "=========================================="
echo "Starting validation of build optimization..."
echo "=========================================="

# Clean previous builds
make clean-bins

echo ""
echo "[1] Measuring concurrent build time..."
# Time the make bins command
START_TIME=$(date +%s)
make bins
END_TIME=$(date +%s)
BUILD_DURATION=$((END_TIME - START_TIME))

echo "Build completed in ${BUILD_DURATION} seconds."

echo ""
echo "[2] Verifying binaries for stripped symbols and trimpath..."

BINARIES=(
  "temporal-server"
  "temporal-cassandra-tool"
  "temporal-sql-tool"
  "temporal-elasticsearch-tool"
  "tdbg"
)

# Function to check binary
check_binary() {
  local bin=$1
  if [ ! -f "$bin" ]; then
    echo "❌ Error: Binary $bin not found!"
    exit 1
  fi
  
  echo "Checking $bin..."
  
  # Check file size
  SIZE=$(stat -c%s "$bin" 2>/dev/null || stat -f%z "$bin")
  SIZE_MB=$((SIZE / 1048576))
  echo "  - Size: ${SIZE_MB} MB"
  
  # Use go tool nm to verify symbols are stripped
  # Note: if stripped, go tool nm will fail or output "no symbols"
  NM_OUTPUT=$(go tool nm "$bin" 2>&1 || true)
  if echo "$NM_OUTPUT" | grep -qi "no symbols"; then
    echo "  - ✅ Symbols successfully stripped (verified by go tool nm)."
  elif echo "$NM_OUTPUT" | grep -qi "go tool nm: $bin:.*no symbols"; then
    echo "  - ✅ Symbols successfully stripped (verified by go tool nm)."
  else
    # Sometime `go tool nm` returns exit code 1 with "go tool nm: file: no symbols"
    # Wait, if there are symbols, it prints them. Let's count lines.
    SYM_COUNT=$(echo "$NM_OUTPUT" | wc -l)
    if [ "$SYM_COUNT" -gt 10 ]; then
       echo "  - ❌ Warning: Symbols might not be fully stripped. Found $SYM_COUNT lines of symbols."
       exit 1
    else
       echo "  - ✅ Symbols successfully stripped (verified by go tool nm)."
    fi
  fi
  
  # Use strings to verify trimpath
  # Check if local path /app exists in the binary
  if strings "$bin" | grep -q "/app/cmd/"; then
    echo "  - ❌ Warning: Local path '/app/cmd/' found. -trimpath might not be working."
    exit 1
  else
    echo "  - ✅ Local paths stripped successfully (verified by strings / -trimpath)."
  fi
}

for bin in "${BINARIES[@]}"; do
  check_binary "$bin"
done

echo ""
echo "=========================================="
echo "✅ All validations passed successfully!"
echo "=========================================="
