#!/bin/bash

echo 当前路径 `pwd`
whl_path=`pwd`/dist
rm -rf `pwd`/build
rm -rf `pwd`/beapder.egg-info
rm ${whl_path}/*.whl
echo y|/usr/local/bin/pip uninstall beapder
echo y|/usr/local/bin/pip uninstall feapder
/usr/local/bin/python3 setup.py bdist_wheel
if [ -d "${whl_path}" ]; then
    for file in `ls ${whl_path}`
    do
        if [ -f "${whl_path}/${file}" ]; then
            if [ "${file##*.}"x = "whl"x ]; then
                /usr/local/bin/pip3 cache purge
                /usr/local/bin/pip3 install ${whl_path}/${file}
                /usr/local/bin/pip3 install feapder==1.8.6
                /usr/local/bin/pip3 install -r requirements.txt
            else
                echo file is not whl:${whl_path}/${file}
            fi
        else
            echo file is not exists:${whl_path}/${file}
        fi
    done
else
    echo path not exists:${whl_path}
fi
