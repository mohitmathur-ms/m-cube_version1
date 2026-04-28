@echo off
setlocal EnableDelayedExpansion
cd /d "%~dp0"

set "VENV_DIR=venv"
set "VENV_PY=%VENV_DIR%\Scripts\python.exe"
set "REQ_FILE=requirements.txt"
set "DEPS_MARKER=%VENV_DIR%\.deps_installed"
set "VERSION_CHECK=import sys; sys.exit(0 if sys.version_info >= (3,11) else 1)"

REM ===========================================================================
REM 1. Validate / recreate the venv (must be >= 3.11)
REM    Assumes "python" on PATH is >= 3.11.
REM ===========================================================================
if not exist "%VENV_PY%" goto CREATE_VENV

"%VENV_PY%" -c "%VERSION_CHECK%" >nul 2>&1
if errorlevel 1 (
    for /f "tokens=*" %%V in ('"%VENV_PY%" --version 2^>^&1') do set "VENV_VER=%%V"
    echo [setup] Existing venv is too old ^(!VENV_VER!^), need ^>=3.11. Recreating ...
    goto CREATE_VENV
)
goto INSTALL_DEPS

:CREATE_VENV
if exist "%VENV_DIR%" (
    echo [setup] Removing old venv ...
    rmdir /s /q "%VENV_DIR%"
)
echo [setup] Creating virtual environment ...
python -m venv "%VENV_DIR%"
if errorlevel 1 (
    echo [ERROR] Failed to create virtual environment. Make sure Python ^>=3.11 is installed and on PATH.
    pause
    exit /b 1
)
REM Force a fresh dependency install on a fresh venv
del /q "%DEPS_MARKER%" 2>nul

:INSTALL_DEPS
REM ===========================================================================
REM 2. Install / refresh dependencies
REM ===========================================================================
if exist "%DEPS_MARKER%" goto RUN_SERVER

echo [setup] Upgrading pip / setuptools / wheel ...
"%VENV_PY%" -m pip install --upgrade pip setuptools wheel
if errorlevel 1 (
    echo [ERROR] Failed to upgrade pip.
    pause
    exit /b 1
)
echo [setup] Installing dependencies from %REQ_FILE% (--prefer-binary) ...
"%VENV_PY%" -m pip install --prefer-binary -r "%REQ_FILE%"
if errorlevel 1 (
    echo.
    echo [ERROR] pip install failed. Common causes:
    echo   * Python version too old   ^(this script enforces ^>=3.11, so unlikely^)
    echo   * Network / proxy issue
    echo   * A package needs a C compiler and no wheel is published for your platform
    echo.
    pause
    exit /b 1
)
echo done> "%DEPS_MARKER%"

:RUN_SERVER
REM ===========================================================================
REM 3. Open browser after a short delay, then start the server
REM ===========================================================================
echo [run] Starting server on http://localhost:5000 ...
REM Poll the server in the background; open the browser only after it responds.
start "" powershell -NoProfile -WindowStyle Hidden -Command ^
  "$u='http://localhost:5000';" ^
  "for($i=0; $i -lt 120; $i++){" ^
    "try{ Invoke-WebRequest -Uri $u -UseBasicParsing -TimeoutSec 1 ^| Out-Null; Start-Process $u; break }" ^
    "catch{ Start-Sleep -Milliseconds 500 }" ^
  "}"
"%VENV_PY%" server.py
set "SERVER_EXIT=!ERRORLEVEL!"

echo.
echo Server stopped (exit code !SERVER_EXIT!).
pause
endlocal
exit /b !SERVER_EXIT!
