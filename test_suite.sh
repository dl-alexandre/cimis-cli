#!/bin/bash
# Comprehensive Test Suite for CIMIS Database
# Usage: ./test_suite.sh

set -e

echo "=========================================="
echo "CIMIS Database - Comprehensive Test Suite"
echo "=========================================="
echo ""

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Track results
PASSED=0
FAILED=0
SKIPPED=0

run_test() {
    local name="$1"
    local command="$2"
    
    echo -n "Testing $name... "
    if eval "$command" > /tmp/test_output.txt 2>&1; then
        echo -e "${GREEN}PASS${NC}"
        ((PASSED++))
        return 0
    else
        echo -e "${RED}FAIL${NC}"
        echo "  Output: $(head -1 /tmp/test_output.txt)"
        ((FAILED++))
        return 1
    fi
}

echo "1. Unit Tests"
echo "--------------"
run_test "Delta Encoding" "go test -run TestDeltaEncoding ./internal/storage/ -short"
run_test "RLE Encoding" "go test -run TestRunLengthEncoding ./internal/storage/ -short"
run_test "Bit Packing" "go test -run TestBitPacking ./internal/storage/ -short"
run_test "Varint Encoding" "go test -run TestVarintEncoding ./internal/storage/ -short"
run_test "Ultra Compression" "go test -run TestUltraCompression ./internal/storage/ -short"
run_test "Chunk Operations" "go test -run TestChunkOperations ./internal/storage/ -short"
run_test "Data Integrity" "go test -run TestDataIntegrity ./internal/storage/ -short"
run_test "Compression Stress" "go test -run TestCompressionStress ./internal/storage/ -short"

echo ""
echo "2. CLI Tests"
echo "--------------"
run_test "Build CLI" "make build-pure"
run_test "CLI Version" "./build/cimisdb version"
run_test "CLI Init" "rm -rf test_data && ./build/cimisdb init -data-dir test_data && test -f test_data/metadata.sqlite3"

echo ""
echo "3. Benchmarks"
echo "--------------"
echo "Running compression benchmarks..."
go test -bench=BenchmarkEncode -benchtime=100ms ./internal/storage/ -short > /tmp/bench.txt 2>&1 || true
echo "  Encode benchmark: $(grep -oP '\d+\.\d+ ns/op' /tmp/bench.txt | head -1 || echo 'N/A')"
go test -bench=BenchmarkCompress -benchtime=100ms ./internal/storage/ -short > /tmp/bench.txt 2>&1 || true
echo "  Compress speed: $(grep -oP '\d+\.\d+ MB/s' /tmp/bench.txt | head -1 || echo 'N/A')"

echo ""
echo "4. Statistics"
echo "--------------"
run_test "All Unit Tests" "go test -short ./internal/storage/"
run_test "All CLI Tests" "go test -short ./cmd/cimisdb/"

echo ""
echo "=========================================="
echo "Test Summary"
echo "=========================================="
echo -e "Passed: ${GREEN}${PASSED}${NC}"
echo -e "Failed: ${RED}${FAILED}${NC}"
echo -e "Skipped: ${YELLOW}${SKIPPED}${NC}"
echo ""

if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}All tests passed!${NC}"
    exit 0
else
    echo -e "${RED}Some tests failed.${NC}"
    exit 1
fi
