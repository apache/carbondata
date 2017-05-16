#  CarbonData on docker
This tutorial provides an easy way to try CarbonData. To follow along with this guide, please install [docker](https://www.docker.com/products/docker) at first.

## 1 Building carbondata
To get the jar files, we need build carbondata project. Here Spark version is 1.6.1 and Hadoop version is 2.7.2.
```shell
git clone https://github.com/apache/incubator-carbondata.git carbondata
cd carbondata
mvn clean -DskipTests -Pspark-1.6.1 -Phadoop-2.7.2 package
```

## 2 Building docker image
We use /dev/dockerfile to build a docker image. This image depends on sequenceiq/spark:1.6.0.
```shell
cd carbondata
cp assembly/target/scala-2.10/carbondata_*.jar dev/carbondata.jar
cp -r processing/carbonplugins dev
cp examples/src/main/resources/data.csv dev
docker build -t carbondata dev
```

## 3 Running docker image
We run the image on interactive mode, and set hostname to 'sandbox'.
```shell
docker run -it -h sandbox carbondata /bin/bash
```

## 4 Running example
At first, we put a csv file to hdfs for data loading.
```shell
cd ${SPARK_HOME}
hadoop fs -put carbondata/data.csv /user/data.csv
```

### Local mode
```shell
./bin/spark-shell \
--master local \
--jars ./lib/carbondata.jar
```

### YARN-client mode
```shell
spark-shell \
--master yarn-client \
--driver-memory 1g \
--executor-memory 1g \
--executor-cores 1 \
--jars ./lib/carbondata.jar

```
### Example
After started spark-shell, we can run this example.

```scala
import java.io.File
import org.apache.spark.sql.CarbonContext
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.util.CarbonProperties

val cc = new CarbonContext(sc, "hdfs://sandbox:9000/carbonstore")
cc.setConf("carbon.kettle.home", new File("").getCanonicalPath + "/carbondata/carbonplugins")

cc.sql("DROP TABLE IF EXISTS t3")

cc.sql("CREATE TABLE IF NOT EXISTS t3 (ID Int, date Timestamp, country String, name String, phonetype String, serialname String, salary Int) STORED BY 'carbondata' ")
CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/mm/dd")
cc.sql("LOAD DATA LOCAL INPATH 'hdfs://sandbox:9000/user/data.csv' into table t3 ")
cc.sql("SELECT country, count(salary) AS amount FROM t3 GROUP BY country ").show()

cc.sql("DROP TABLE IF EXISTS t3")
```
