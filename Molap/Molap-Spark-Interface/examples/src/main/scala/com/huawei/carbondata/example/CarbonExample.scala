package com.huawei.carbondata.example

import org.apache.spark.sql.OlapContext
import org.apache.spark.{SparkConf, SparkContext}

object CarbonExample {

  def main(args: Array[String]) {

    val hdfsCarbonBasePath = "/home/root1/carbon/metadata"
    val sc = new SparkContext(new SparkConf()
      .setAppName("CarbonSpark")
      .setMaster("local[2]")
      .set("carbon.storelocation", hdfsCarbonBasePath)
      .set("molap.kettle.home", "../../Molap/Molap-Data-Processor/molapplugins/molapplugins")
      .set("molap.is.columnar.storage", "true")
      .set("spark.sql.bigdata.register.dialect", "org.apache.spark.sql.MolapSqlDDLParser")
      .set("spark.sql.bigdata.register.strategyRule", "org.apache.spark.sql.hive.CarbonStrategy")
      .set("spark.sql.bigdata.initFunction", "org.apache.spark.sql.CarbonEnv")
      .set("spark.sql.bigdata.acl.enable", "false")
      .set("molap.tempstore.location", System.getProperty("java.io.tmpdir"))
      .set("spark.sql.bigdata.register.strategy.useFunction", "true")
      .set("hive.security.authorization.enabled", "false")
      .set("spark.sql.bigdata.register.analyseRule", "org.apache.spark.sql.QueryStatsRule"))

    val oc = new OlapContext(sc, hdfsCarbonBasePath)
    oc.setConf("molap.kettle.home", "/home/root1/carbon/carbondata/Molap/Molap-Data-Processor/molapplugins/molapplugins")

    oc.sql("drop cube alldatatypescube")

    oc.sql("CREATE CUBE alldatatypescube DIMENSIONS (empno Integer, empname String, " +
      "designation String, doj Timestamp, workgroupcategory Integer, workgroupcategoryname String, " +
      "deptno Integer, deptname String, projectcode Integer, projectjoindate Timestamp, " +
      "projectenddate Timestamp) MEASURES (attendance Integer,utilization Integer,salary Integer) " +
      "OPTIONS (PARTITIONER [PARTITION_COUNT=1])")

    oc.sql("LOAD DATA fact from '/home/root1/carbon/carbondata/CI/CarbonTestSuite/TestData/data.csv' INTO CUBE alldatatypescube PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"')");

    oc.sql("select empno,empname,utilization,count(salary),sum(empno) from alldatatypescube where empname in ('arvind','ayushi') group by empno,empname,utilization").show()

  }

}
