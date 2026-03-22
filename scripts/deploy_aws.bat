@echo off
REM deploy_aws.bat — Build pipeline_deps.zip and upload all scripts to S3.
REM
REM Usage (from project root):
REM   set RAW_BUCKET=iceberg-pipeline-dev-raw-288316011614
REM   scripts\deploy_aws.bat
REM
REM Or let the script read it from Terraform output:
REM   scripts\deploy_aws.bat
REM
REM Requires: Python 3, AWS CLI v2 configured, Terraform applied.

setlocal

REM ── Resolve RAW_BUCKET ──────────────────────────────────────────────────────
if "%RAW_BUCKET%"=="" (
    echo RAW_BUCKET not set. Reading from Terraform output...
    pushd terraform\aws
    for /f "tokens=*" %%i in ('terraform output -raw raw_bucket') do set RAW_BUCKET=%%i
    popd
)

if "%RAW_BUCKET%"=="" (
    echo ERROR: Could not determine RAW_BUCKET. Run: set RAW_BUCKET=^<bucket-name^>
    exit /b 1
)

echo.
echo RAW_BUCKET : %RAW_BUCKET%
echo.

REM ── Build zip (Python ensures Unix path separators — Compress-Archive breaks on Glue) ──
echo Building pipeline_deps.zip...
python -c "
import zipfile, os, sys
with zipfile.ZipFile('pipeline_deps.zip', 'w', zipfile.ZIP_DEFLATED) as z:
    for f in ['config.py', 'spark_factory.py']:
        z.write(f, f)
        print('  +', f)
    for root, dirs, files in os.walk('utils'):
        dirs[:] = [d for d in dirs if d != '__pycache__']
        for fn in files:
            if not fn.endswith('.pyc'):
                full = os.path.join(root, fn)
                arcname = full.replace(os.sep, '/')
                z.write(full, arcname)
                print('  +', arcname)
print('Done: pipeline_deps.zip')
"
if %ERRORLEVEL% NEQ 0 ( echo ERROR: zip build failed. & exit /b 1 )

REM ── Upload to S3 ────────────────────────────────────────────────────────────
echo.
echo Uploading to s3://%RAW_BUCKET%/scripts/ ...

aws s3 cp spark_job.py      s3://%RAW_BUCKET%/scripts/spark_job.py
if %ERRORLEVEL% NEQ 0 ( echo ERROR: spark_job.py upload failed. & exit /b 1 )

aws s3 cp pipeline_deps.zip s3://%RAW_BUCKET%/scripts/pipeline_deps.zip
if %ERRORLEVEL% NEQ 0 ( echo ERROR: pipeline_deps.zip upload failed. & exit /b 1 )

echo.
echo Done. Files in S3:
aws s3 ls s3://%RAW_BUCKET%/scripts/

endlocal
