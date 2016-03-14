#!/bin/sh
cd ../Molap
mvn clean install -Dmaven.test.skip=true
cd ../build
ant -f build.xml
sh SHAGenerator.sh
