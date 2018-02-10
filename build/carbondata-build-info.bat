@echo off

rem
rem Licensed to the Apache Software Foundation (ASF) under one or more
rem contributor license agreements.  See the NOTICE file distributed with
rem this work for additional information regarding copyright ownership.
rem The ASF licenses this file to You under the Apache License, Version 2.0
rem (the "License"); you may not use this file except in compliance with
rem the License.  You may obtain a copy of the License at
rem
rem    http://www.apache.org/licenses/LICENSE-2.0
rem
rem Unless required by applicable law or agreed to in writing, software
rem distributed under the License is distributed on an "AS IS" BASIS,
rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
rem See the License for the specific language governing permissions and
rem limitations under the License.
rem

rem This script generates the build info for carbondata and places it into the carbondata-version-info.properties file.
rem Arguments:
rem   build_tgt_directory - The target directory where properties file would be created. [./core/target/extra-resources]
rem   carbondata_version - The current version of carbondata

set RESOURCE_DIR=%1
md "%RESOURCE_DIR%"
set CARBONDATA_BUILD_INFO=%RESOURCE_DIR%/carbondata-version-info.properties

echo version=%2> %CARBONDATA_BUILD_INFO%

for /f %%i in ('git rev-parse HEAD') do set revision=%%i
echo revision=%revision%>> %CARBONDATA_BUILD_INFO%

for /f %%i in ('git rev-parse --abbrev-ref HEAD') do set branch=%%i
echo branch=%branch%>> %CARBONDATA_BUILD_INFO%

for /f "delims=," %%i in ('echo %date:/=-%T%time%Z') do set day=%%i
echo date=%day%>> %CARBONDATA_BUILD_INFO%