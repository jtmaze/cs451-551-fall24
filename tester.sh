#!/bin/bash

# Define the output directory
OutputDir="tester_output"

# Create the output directory if it doesn't exist
if [ ! -d "$OutputDir" ]; then
  mkdir -p "$OutputDir"
fi

# Check if a test name was provided
if [ -z "$1" ]; then
  echo "Usage: $0 [test_name]"
  exit 1
fi

# Store the test name argument
Test="$1"
echo "Test name: $Test"

# Run the appropriate test based on the given argument
case "$Test" in
  "1") python3 ./m1_tester.py > "$OutputDir/m1_tester.txt" ;;
  "2") python3 ./m2_tester.py > "$OutputDir/m2_tester.txt" ;;
  "3") python3 ./m3_tester.py > "$OutputDir/m3_tester.txt" ;;
  "exam1") python3 ./exam_tester_m1.py > "$OutputDir/exam_tester_m1.txt" ;;
  "exam21") python3 ./exam_tester_m2_part1.py > "$OutputDir/exam_tester_m2_part1.txt" ;;
  "exam22") python3 ./exam_tester_m2_part2.py > "$OutputDir/exam_tester_m2_part2.txt" ;;
  "exam31") python3 ./exam_tester_m3_part1.py > "$OutputDir/exam_tester_m3_part1.txt" ;;
  "exam32") python3 ./exam_tester_m3_part2.py > "$OutputDir/exam_tester_m3_part2.txt" ;;
  *) 
    echo "Unknown command: $Test"
    exit 1
    ;;
esac

echo "Test completed and output written."
