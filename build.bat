@echo off

setlocal

for /f "tokens=1-3" %%A in (%~dp0src\s3_reader\__init__.py) do (
    if %%A == __version__ (
        set ver=%%C
        goto :break
    )
)

:break
set ver=%ver:"=%

echo Cleaning s3_reader...
rm -rf dist
rm -rf src/s3_reader.egg-info
pip uninstall -y s3_reader
echo Cleaning s3_reader complete

echo Building s3_reader...
python -m build
echo Building s3_reader complete

echo Installing s3_reader...
pip install dist\s3_reader-%ver%-py3-none-any.whl
echo Installing s3_reader complete

endlocal
