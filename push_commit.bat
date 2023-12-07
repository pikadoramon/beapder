chcp 65001
@echo off

setlocal enabledelayedexpansion

git remote -v
git remote remove origin
git remote -v
git init
git remote add origin https://github.com/pikadoramon/beapder.git
git remote -v
git config --global user.email ""
git config --global user.name "pikadoramon"
git init
git checkout --orphan develop