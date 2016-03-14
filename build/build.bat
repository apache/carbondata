cd ../Molap
call mvn clean install -Dmaven.test.skip=true
cd ../build
call ant -f build.xml
