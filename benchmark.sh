#!/bin/bash
set -e

# Find executable
EXE=""
POSSIBLE_PATHS=(
    "./build/Release/tpch_query5.exe"
    "./build/Release/tpch_query5"
    "./build/Debug/tpch_query5.exe"
    "./build/Debug/tpch_query5"
    "./build/tpch_query5.exe"
    "./build/tpch_query5"
)

for path in "${POSSIBLE_PATHS[@]}"; do
    if [ -f "$path" ]; then
        EXE="$path"
        break
    fi
done

if [ -z "$EXE" ]; then
    echo "Error: Executable not found in build directories."
    exit 1
fi

DBGEN="./tpch-dbgen"
RES1="result_1.csv"
RES4="result_4.csv"

echo "Running Benchmark..."
echo "Executable: $EXE"
echo "Data Path: $DBGEN"

# Check if data exists
if [ ! -f "$DBGEN/lineitem.tbl" ]; then
    echo "Error: TPCH Data not found in $DBGEN"
    exit 1
fi

echo ""
echo "----------------------------------------"
echo "Running Single Threaded (Threads=1)..."
# Use 'time' to measure execution. 
# We capture the output of time which goes to stderr, so we redirect it.
{ time "$EXE" --r_name ASIA --start_date 1994-01-01 --end_date 1995-01-01 --threads 1 --table_path "$DBGEN" --result_path "$RES1"; } 2>&1

echo ""
echo "----------------------------------------"
echo "Running Multi Threaded (Threads=4)..."
{ time "$EXE" --r_name ASIA --start_date 1994-01-01 --end_date 1995-01-01 --threads 4 --table_path "$DBGEN" --result_path "$RES4"; } 2>&1

echo ""
echo "----------------------------------------"
echo "Comparing Results..."

# Diff returns 0 if files are same, 1 if different
if diff -q "$RES1" "$RES4" >/dev/null; then
    echo "Results Match! Verification Successful."
else
    echo "Results Differ! Check $RES1 and $RES4"
    exit 1
fi

echo ""
echo "Final Results (Top 5 lines of $RES4):"
head -n 5 "$RES4"
