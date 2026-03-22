@echo off
REM git_checkin.bat — Initialise git repo and create the first commit.
REM Run once from the project root:   git_checkin.bat
REM
REM Prerequisites:
REM   - Git installed and on PATH  (https://git-scm.com/download/win)
REM   - Run from the project root  (d:\projects\cloud_agnostic_project)

echo.
echo ============================================================
echo  Git first-time checkin helper
echo ============================================================
echo.

REM ── 1. Verify git is available ──────────────────────────────
where git >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: git not found on PATH.
    echo        Download and install from https://git-scm.com/download/win
    echo        Then re-open this Command Prompt and try again.
    pause
    exit /b 1
)

REM ── 2. Init repo (safe to run on an existing repo) ──────────
git init
if %ERRORLEVEL% NEQ 0 ( echo ERROR: git init failed. & pause & exit /b 1 )

REM ── 3. Stage all non-ignored files ──────────────────────────
git add .
if %ERRORLEVEL% NEQ 0 ( echo ERROR: git add failed. & pause & exit /b 1 )

REM ── 4. Show what will be committed ──────────────────────────
echo.
echo Files staged for commit:
echo ---------------------------------------------------------------
git status --short
echo ---------------------------------------------------------------
echo.

REM ── 5. Confirm before committing ────────────────────────────
set /p CONFIRM="Proceed with initial commit? (Y/N): "
if /i "%CONFIRM%" NEQ "Y" (
    echo Aborted. No commit was made.
    pause
    exit /b 0
)

REM ── 6. Create the initial commit ────────────────────────────
git commit -m "Initial commit: cloud-agnostic Spark/Iceberg ETL pipeline"
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo TIP: If git says 'Author identity unknown', run:
    echo   git config --global user.email "you@example.com"
    echo   git config --global user.name  "Your Name"
    echo Then re-run this script.
    pause
    exit /b 1
)

echo.
echo ============================================================
echo  Done!  Next steps:
echo ============================================================
echo.
echo  Push to GitHub / Azure DevOps / GitLab:
echo    git remote add origin ^<your-repo-url^>
echo    git push -u origin main
echo.
echo  Or rename the default branch if needed:
echo    git branch -M main
echo.
pause
