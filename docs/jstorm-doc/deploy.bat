::###############################################################################
::  Licensed to the Apache Software Foundation (ASF) under one
::  or more contributor license agreements.  See the NOTICE file
::  distributed with this work for additional information
::  regarding copyright ownership.  The ASF licenses this file
::  to you under the Apache License, Version 2.0 (the
::  "License"); you may not use this file except in compliance
::  with the License.  You may obtain a copy of the License at
::
::      http://www.apache.org/licenses/LICENSE-2.0
::
::  Unless required by applicable law or agreed to in writing, software
::  distributed under the License is distributed on an "AS IS" BASIS,
::  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
::  See the License for the specific language governing permissions and
:: limitations under the License.
::###############################################################################

@echo off
:start
call jekyll -version >nul 2>&1
if "%errorlevel%"=="0" goto check_redcarpet
echo ERROR: Could not find jekyll.
echo Please install with 'gem install jekyll' (see http://jekyllrb.com).
exit /b 1

:check_redcarpet
call redcarpet -version >nul 2>&1
if "%errorlevel%"=="0" goto check_pygments
echo ERROR: Could not find redcarpet. 
echo Please install with 'gem install redcarpet' (see https://github.com/vmg/redcarpet).
echo Redcarpet is needed for Markdown parsing and table of contents generation.
exit /b 1

:check_pygments
call python -c "import pygments" >nul 2>&1
if "%errorlevel%"=="0" goto check_git
echo ERROR: Could not find pygments.
echo Please install with 'sudo easy_install Pygments' (requires Python; see http://pygments.org). 
echo Pygments is needed for syntax highlighting of the code examples.
exit /b 1

:check_git
call git --version >nul 2>&1
if "%errorlevel%"=="0" goto execute
echo ERROR: Could not find Git.
echo Please install Git and add it into $PATH. 
echo Git is needed for auto-deploy.
exit /b 1

:execute
SET "DOCS_SRC=%cd%"
SET "DOCS_DST=%DOCS_SRC%\_site"
SET "DEPLOY_DATE=%date:~0,10% %time:~0,2%:%time:~3,2%"

::build doc
echo Start to build doc by Jekyll...
call jekyll build >nul 2>&1
echo Build complete...

::push pages to github.com
echo Setting up Git deployment...
call rm -rf .deploy_tmp
call mkdir .deploy_tmp
call cp -r _site/* .deploy_tmp/ >nul 2>&1
echo Deploying...
call cd .deploy_tmp
call rm -rf jekyll/
call rm deploy.bat
call git config --global http.postBuffer 8528000 >nul 2>&1
call git init >nul 2>&1
call git add -A >nul 2>&1
call git commit -m "updated: %DEPLOY_DATE%" >nul 2>&1
call git push -u https://github.com/alibaba/jstorm.git HEAD:documents --force
call cd ..
call rm -rf .deploy_tmp
echo Deploy successfully!
echo Press any key to exit
pause >nul 2>&1
exit