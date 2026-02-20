@echo off
setlocal EnableExtensions EnableDelayedExpansion

if "%~1"=="" (
  echo Usage: %~nx0 ^<output_format^> [search_root]
  echo Example: %~nx0 svg .
  exit /b 1
)

set "OUTPUT_FORMAT=%~1"
if "%OUTPUT_FORMAT:~0,1%"=="." set "OUTPUT_FORMAT=%OUTPUT_FORMAT:~1%"
set "SEARCH_ROOT=%~2"

if "%SEARCH_ROOT%"=="" set "SEARCH_ROOT=."

if not exist "%SEARCH_ROOT%" (
  echo Error: search_root does not exist: "%SEARCH_ROOT%"
  exit /b 1
)

where mmdc >nul 2>&1
if errorlevel 1 (
  echo Error: mmdc not found in PATH. Install Mermaid CLI first.
  exit /b 1
)

set /a FOUND=0
set /a FAILED=0

for /r "%SEARCH_ROOT%" %%F in (*.mmd) do (
  set /a FOUND+=1
  set "INPUT=%%~fF"
  set "OUTPUT=%%~dpnF.!OUTPUT_FORMAT!"
  echo Rendering "!INPUT!" -^> "!OUTPUT!"
  call mmdc -i "!INPUT!" -o "!OUTPUT!"
  if errorlevel 1 (
    echo Error: failed to render "!INPUT!"
    set /a FAILED+=1
  )
)

if "!FOUND!"=="0" (
  echo No .mmd files found under "%SEARCH_ROOT%".
  exit /b 0
)

if not "!FAILED!"=="0" (
  echo Completed with !FAILED! failure^(s^) out of !FOUND! file^(s^).
  exit /b 2
)

echo Completed successfully: !FOUND! file^(s^) rendered to .!OUTPUT_FORMAT!.
exit /b 0
