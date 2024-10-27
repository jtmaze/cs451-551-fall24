# Runs a test with the given code and saves the output to ./tester_output/

param (
    [Parameter(Mandatory=$true)]
    [string]$Test
)

$OutputDir = "tester_output"

Write-Host "Test name: $Test"

# Create output directory
if (-not (Test-Path $OutputDir)) {
    New-Item -ItemType Directory -Path $OutputDir | Out-Null
}

# Run test based on given test name and save results
switch ($Test) {
    "main"      { py .\__main__.py > "$OutputDir/__main__.txt" }
    "1"      { py .\m1_tester.py > "$OutputDir/m1_tester.txt" }
    "2"      { py .\m2_tester.py > "$OutputDir/m2_tester.txt" }
    "3"      { py .\m3_tester.py > "$OutputDir/m3_tester.txt" }
    "exam1"  { py .\exam_tester_m1.py > "$OutputDir/exam_tester_m1.txt" }
    "exam21"  { py .\exam_tester_m2_part1.py > "$OutputDir/exam_tester_m2_part1.txt" }
    "exam22"  { py .\exam_tester_m2_part2.py > "$OutputDir/exam_tester_m2_part2.txt" }
    "exam31"  { py .\exam_tester_m3_part1.py > "$OutputDir/exam_tester_m3_part1.txt" }
    "exam32"  { py .\exam_tester_m3_part2.py > "$OutputDir/exam_tester_m3_part2.txt" }
    default  { Write-Host "Unknown command: $Test" }
}
