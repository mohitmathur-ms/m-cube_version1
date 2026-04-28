@echo off
setlocal EnableDelayedExpansion
cd /d "%~dp0"

set "VENV_DIR=venv"
set "VENV_PY=%VENV_DIR%\Scripts\python.exe"
set "REQ_FILE=requirements.txt"
set "DEPS_MARKER=%VENV_DIR%\.deps_installed"
set "FALLBACK_PY_VERSION=3.12.10"
set "VERSION_CHECK=import sys; sys.exit(0 if sys.version_info >= (3,11) else 1)"

REM ===========================================================================
REM 1. Locate ANY Python >= 3.11 (install 3.12.10 only if none is found)
REM ===========================================================================
set "PY_BOOT="

REM 1a. Prefer the py launcher's highest installed 3.x, validate it's >= 3.11
where py >nul 2>&1
if !ERRORLEVEL! EQU 0 (
    py -3 -c "%VERSION_CHECK%" >nul 2>&1
    if !ERRORLEVEL! EQU 0 set "PY_BOOT=py -3"
)

REM 1b. Otherwise, accept "python" on PATH if it's >= 3.11
if "!PY_BOOT!"=="" (
    where python >nul 2>&1
    if !ERRORLEVEL! EQU 0 (
        python -c "%VERSION_CHECK%" >nul 2>&1
        if !ERRORLEVEL! EQU 0 set "PY_BOOT=python"
    )
)

REM 1c. Otherwise, download + silent-install Python 3.12.10 per-user
if "!PY_BOOT!"=="" (
    set "PY_INSTALLER=python-%FALLBACK_PY_VERSION%-amd64.exe"
    set "PY_URL=https://www.python.org/ftp/python/%FALLBACK_PY_VERSION%/!PY_INSTALLER!"
    set "PY_INSTALLER_PATH=%TEMP%\!PY_INSTALLER!"
    echo [setup] No Python ^>=3.11 found. Downloading %FALLBACK_PY_VERSION% from python.org ...
    curl.exe -L --fail -o "!PY_INSTALLER_PATH!" "!PY_URL!"
    if !ERRORLEVEL! NEQ 0 (
        echo [ERROR] Failed to download Python installer from !PY_URL!
        echo Check your internet connection or install Python 3.11+ manually
        echo from https://www.python.org/downloads/ and re-run start.bat.
        pause
        exit /b 1
    )
    echo [setup] Installing Python %FALLBACK_PY_VERSION% silently (per-user, no admin) ...
    echo         A UAC prompt may appear briefly.
    "!PY_INSTALLER_PATH!" /quiet InstallAllUsers=0 PrependPath=1 Include_test=0 Include_launcher=1
    if !ERRORLEVEL! NEQ 0 (
        echo [ERROR] Python installer exited with code !ERRORLEVEL!.
        pause
        exit /b 1
    )
    del /q "!PY_INSTALLER_PATH!" 2>nul
    set "PY_USER_EXE=%LocalAppData%\Programs\Python\Python312\python.exe"
    if exist "!PY_USER_EXE!" (
        set "PY_BOOT=!PY_USER_EXE!"
    ) else (
        echo [ERROR] Python installed but interpreter not found at expected path.
        echo Open a new command window and re-run start.bat.
        pause
        exit /b 1
    )
)

for /f "tokens=*" %%V in ('!PY_BOOT! --version 2^>^&1') do set "BOOT_VER=%%V"
echo [setup] Using Python interpreter: !PY_BOOT!  ^(!BOOT_VER!^)

REM ===========================================================================
REM 2. Validate / recreate the venv (must also be >= 3.11)
REM ===========================================================================
set "RECREATE_VENV=0"
if exist "%VENV_PY%" (
    "%VENV_PY%" -c "%VERSION_CHECK%" >nul 2>&1
    if !ERRORLEVEL! NEQ 0 (
        for /f "tokens=*" %%V in ('"%VENV_PY%" --version 2^>^&1') do set "VENV_VER=%%V"
        echo [setup] Existing venv is too old ^(!VENV_VER!^), need ^>=3.11. Recreating ...
        set "RECREATE_VENV=1"
    )
) else (
    set "RECREATE_VENV=1"
)

if "!RECREATE_VENV!"=="1" (
    if exist "%VENV_DIR%" (
        echo [setup] Removing old venv ...
        rmdir /s /q "%VENV_DIR%"
    )
    echo [setup] Creating virtual environment ...
    !PY_BOOT! -m venv "%VENV_DIR%"
    if !ERRORLEVEL! NEQ 0 (
        echo [ERROR] Failed to create virtual environment.
        pause
        exit /b 1
    )
    REM Force a fresh dependency install on a fresh venv
    del /q "%DEPS_MARKER%" 2>nul
)

REM ===========================================================================
REM 3. Install / refresh dependencies
REM ===========================================================================
if not exist "%DEPS_MARKER%" (
    echo [setup] Upgrading pip / setuptools / wheel ...
    "%VENV_PY%" -m pip install --upgrade pip setuptools wheel
    if !ERRORLEVEL! NEQ 0 (
        echo [ERROR] Failed to upgrade pip.
        pause
        exit /b 1
    )
    echo [setup] Installing dependencies from %REQ_FILE% (--prefer-binary) ...
    "%VENV_PY%" -m pip install --prefer-binary -r "%REQ_FILE%"
    if !ERRORLEVEL! NEQ 0 (
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
)

REM ===========================================================================
REM 4. Open browser after a short delay, then start the server
REM ===========================================================================
echo [run] Starting server on http://localhost:5000 ...
start "" cmd /c "timeout /t 3 /nobreak >nul && start """" http://localhost:5000"
"%VENV_PY%" server.py

if %ERRORLEVEL% NEQ 0 (
    echo.
    echo Server exited with code %ERRORLEVEL%.
    pause
)
endlocal
