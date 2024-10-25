@echo off
setlocal enabledelayedexpansion

REM Define the output directory
set "OutputDir=tester_output"

REM Check if the output directory exists, if not, create it
if not exist "%OutputDir%" (
    mkdir "%OutputDir%"
)

REM Check if a test name was provided, show usage if not
if "%1"=="" (
    echo Usage: run_tests.bat [test_name]
    exit /b 1
)

REM Store the test name argument
set "Test=%1"
echo Test name: %Test%

REM Run the appropriate test based on the given argument
if "%Test%"=="1" (
    py .\m1_tester.py > "%OutputDir%\m1_tester.txt"
) else if "%Test%"=="2" (
    py .\m2_tester.py > "%OutputDir%\m2_tester.txt"
) else if "%Test%"=="3" (
    py .\m3_tester.py > "%OutputDir%\m3_tester.txt"
) else if "%Test%"=="exam1" (
    py .\exam_tester_m1.py > "%OutputDir%\exam_tester_m1.txt"
) else if "%Test%"=="exam21" (
    py .\exam_tester_m2_part1.py > "%OutputDir%\exam_tester_m2_part1.txt"
) else if "%Test%"=="exam22" (
    py .\exam_tester_m2_part2.py > "%OutputDir%\exam_tester_m2_part2.txt"
) else if "%Test%"=="exam31" (
    py .\exam_tester_m3_part1.py > "%OutputDir%\exam_tester_m3_part1.txt"
) else if "%Test%"=="exam32" (
    py .\exam_tester_m3_part2.py > "%OutputDir%\exam_tester_m3_part2.txt"
) else (
    echo Unknown command: %Test%
    exit /b 1
)

echo Test completed successfully.
exit /b 0
