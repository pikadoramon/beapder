chcp 65001
@echo off

setlocal enabledelayedexpansion
echo 以下是GPT翻译的Bat文件语言, 若运行失败请自身重试命令
echo 当前路径 %cd%
set "whl_path=%cd%\dist"
del /Q "%cd%\build\*%"
del /Q "%cd%\beapder.egg-info\*%"
del /Q "%whl_path%\*.whl%"
echo y|pip uninstall beapder
echo y|pip uninstall feapder
python setup.py bdist_wheel
if exist "%whl_path%" (
    for %%F in ("%whl_path%\*") do (
        if "%%~xF"==".whl" (
            pip cache purge
            pip install "%%~fF"
            pip install feapder==1.8.6
            pip install -r requirements.txt
        ) else (
            echo file is not whl: %%~fF
        )
    )
) else (
    echo path not exists: %whl_path%
)
