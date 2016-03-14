WORKSPACE=${WORKSPACE:-`pwd`}
export VERSION=${VERSION:-V100R002C10}
export RELEASE_PROJECT_VERSION=${RELEASE_PROJECT_VERSION:-V100R002C10}
export MANIFEST_VERSION=${MANIFEST_VERSION:-$VERSION}
export BUILD_TIME=`date +"%d %b %Y %T"`

SCRIPT_DIR=$(dirname $0)
SCRIPT_DIR=$(cd $SCRIPT_DIR;pwd)
createSnapshotHtml(){
  echo -e "<html><body><h1 align=\"center\"><u>Environment SNAPSHOT</u></h1>" > snapshot_Carbon.html

  #Print System Info
  echo -e "<table border=\"1\"  cellspacing=\"0\" cellpadding=\"1\">" >> snapshot_Carbon.html
  echo -e "<h2>System Info</h2>" >> snapshot_Carbon.html
  echo -e "<tr><th align=\"left\">Parameter Name</th><th align=\"left\">Parameter Value</th></tr>" >> snapshot_Carbon.html
  echo "<tr><td width=\"200px\">OS Name</td><td>">>snapshot_Carbon.html
  echo `cat /etc/issue  | sed 's/Welcome to //g' | cut -d '(' -f 1 | sed '/^$/d'` >> snapshot_Carbon.html 
  echo "</td></tr>" >>snapshot_Carbon.html
  echo "<tr><td width=\"200px\">Kernel Release</td><td>">>snapshot_Carbon.html 
  echo `uname -r` >> snapshot_Carbon.html
  echo "</td></tr>" >>snapshot_Carbon.html
  echo "<tr><td width=\"200px\">Kernel Version</td><td>">>snapshot_Carbon.html
  echo `uname -v` >> snapshot_Carbon.html
  echo "</td></tr>" >>snapshot_Carbon.html
  echo "<tr><td width=\"200px\">Processor</td><td>">>snapshot_Carbon.html
  echo `uname -p` >> snapshot_Carbon.html
  echo "</td></tr>" >>snapshot_Carbon.html
  echo "<tr><td width=\"200px\">Node Name</td><td>">>snapshot_Carbon.html
  echo `uname -n` >> snapshot_Carbon.html
  echo "</td></tr>" >>snapshot_Carbon.html
  echo -e "</table>">>snapshot_Carbon.html

  #Print Tool Info
  echo -e "<table border=\"1\"  cellspacing=\"0\" cellpadding=\"1\">" >> snapshot_Carbon.html
  echo -e "<h2>Tools Info</h2>" >> snapshot_Carbon.html
  echo -e "<tr><th align=\"left\">Tool Name</th><th align=\"left\">Version</th></tr>" >> snapshot_Carbon.html
  echo "<tr><td width=\"200px\">JAVA</td><td>">>snapshot_Carbon.html
  java -version 2>temp_java
  echo `grep "java version" temp_java | cut -d '"' -f 2` >> snapshot_Carbon.html
  echo "</td></tr>" >>snapshot_Carbon.html
  echo "<tr><td width=\"200px\">Apache Maven</td><td>">>snapshot_Carbon.html
  echo `mvn --version | head -1` >> snapshot_Carbon.html
  echo "</td></tr>" >>snapshot_Carbon.html
  echo "<tr><td width=\"200px\">Apache Ant</td><td>">>snapshot_Carbon.html
  echo `ant -version` >> snapshot_Carbon.html
  echo "</td></tr>" >>snapshot_Carbon.html

  # Print environment variables
  echo -e "<table border=\"1\"  cellspacing=\"0\" cellpadding=\"1\">" >> snapshot_Carbon.html
  echo -e "<h2>Environment Variables</h2>" >> snapshot_Carbon.html
  echo -e "<tr><th align=\"left\">Environment Variable Name</th><th align=\"left\">Value</th></tr>" >> snapshot_Carbon.html
  x=`env`
  while read line; do
    echo "<tr><td>">>snapshot_Carbon.html
    echo "$line" | sed -e 's/=/<\/td><td>/1'>>snapshot_Carbon.html 
    echo "</td></tr>">>snapshot_Carbon.html
  done <<< "$x"
  echo -e "</table>">>snapshot_Carbon.html
  echo -e "</body></html>" >> snapshot_Carbon.html
}
createSnapshotHtml
