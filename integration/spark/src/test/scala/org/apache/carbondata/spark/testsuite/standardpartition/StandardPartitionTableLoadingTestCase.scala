/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.carbondata.spark.testsuite.standardpartition

import java.io.{File, FileWriter, IOException}
import java.util
import java.util.concurrent.{Callable, Executors, ExecutorService}

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.execution.strategy.CarbonDataSourceScan
import org.apache.spark.sql.optimizer.CarbonFilters
import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.{AnalysisException, CarbonEnv, Row}
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.Strings
import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.{CarbonFile, CarbonFileFilter}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.{CarbonMetadata, SegmentFileStore}
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.spark.rdd.CarbonScanRDD

class StandardPartitionTableLoadingTestCase extends QueryTest with BeforeAndAfterAll {
  var executorService: ExecutorService = _
  override def beforeAll {
    printConfiguration()
    dropTable

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    sql(
      """
        | CREATE TABLE originTable (empno int, empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | STORED AS carbondata
      """.stripMargin)

    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE originTable OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    sql(
      """
        | CREATE TABLE originMultiLoads (empno int, empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | STORED AS carbondata
      """.stripMargin)

    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE originMultiLoads OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE originMultiLoads OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE originMultiLoads OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
  }

  def validateDataFiles(tableUniqueName: String, segmentId: String, partition: Int): Unit = {
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable(tableUniqueName)
    val partitions = CarbonFilters
      .getPartitions(Seq.empty,
        sqlContext.sparkSession,
        TableIdentifier(carbonTable.getTableName, Some(carbonTable.getDatabaseName)))
    assert(partitions.get.length == partition)
  }

  test("data loading for partition table for one partition column") {
    sql(
      """
        | CREATE TABLE partitionone (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (empno int)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitionone OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    validateDataFiles("default_partitionone", "0", 10)

    checkAnswer(sql("select empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary from partitionone order by empno"),
      sql("select  empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary from originTable order by empno"))

  }

  test("data loading for partition table for two partition column") {
    sql(
      """
        | CREATE TABLE partitiontwo (empno int, designation String,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (doj Timestamp, empname String)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitiontwo OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    validateDataFiles("default_partitiontwo", "0", 10)

    checkAnswer(sql("select empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary from partitiontwo order by empno"),
      sql("select empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary from originTable order by empno"))

  }

  test("data loading for partition table for three partition column") {
    sql(
      """
        | CREATE TABLE partitionthree (empno int, doj Timestamp,
        |  workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (workgroupcategory int, empname String, designation String)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitionthree OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    validateDataFiles("default_partitionthree", "0", 10)

    checkAnswer(sql("select empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary from partitionthree order by empno"),
      sql("select empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary from originTable order by empno"))
  }

  test("data loading for partition table for five partition column") {
    sql(
      """
        | CREATE TABLE partitionfive (empno int, doj Timestamp,
        |  workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int)
        | PARTITIONED BY (utilization int,salary int,workgroupcategory int, empname String,
        | designation String)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitionfive OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    validateDataFiles("default_partitionfive", "0", 10)

    checkAnswer(sql("select empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary from partitionfive order by empno"),
      sql("select empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary from originTable order by empno"))

    checkAnswer(sql("select empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary from partitionfive where empno>15 order by empno "),
      sql("select empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary from originTable where empno>15 order by empno"))
  }

  test("multiple data loading for partition table for three partition column") {
    sql(
      """
        | CREATE TABLE partitionmultiplethree (empno int, doj Timestamp,
        |  workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (workgroupcategory int, empname String, designation String)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitionmultiplethree OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitionmultiplethree OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitionmultiplethree OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    validateDataFiles("default_partitionmultiplethree", "1", 10)
    validateDataFiles("default_partitionmultiplethree", "2", 10)
    checkAnswer(sql("select empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary from partitionmultiplethree order by empno"),
      sql("select empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary from originMultiLoads order by empno"))
  }

  test("insert data for partition table for three partition column") {
    sql(
      """
        | CREATE TABLE insertpartitionthree (empno int, doj Timestamp,
        |  workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (workgroupcategory int, empname String, designation String)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"""insert into insertpartitionthree select empno,doj,workgroupcategoryname,deptno,deptname,projectcode,projectjoindate,projectenddate,attendance,utilization,salary,workgroupcategory,empname,designation from originTable""")
    sql(s"""insert into insertpartitionthree select empno,doj,workgroupcategoryname,deptno,deptname,projectcode,projectjoindate,projectenddate,attendance,utilization,salary,workgroupcategory,empname,designation from originTable""")
    sql(s"""insert into insertpartitionthree select empno,doj,workgroupcategoryname,deptno,deptname,projectcode,projectjoindate,projectenddate,attendance,utilization,salary,workgroupcategory,empname,designation from originTable""")

    validateDataFiles("default_insertpartitionthree", "0", 10)

    checkAnswer(sql("select empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary from insertpartitionthree order by empno"),
      sql("select empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary from originMultiLoads order by empno"))
  }

  test("data loading for partition table for one static partition column") {
    sql(
      """
        | CREATE TABLE staticpartitionone (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (empno int)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"""insert into staticpartitionone PARTITION(empno='1') select empname,designation,doj,workgroupcategory,workgroupcategoryname,deptno,deptname,projectcode,projectjoindate,projectenddate,attendance,utilization,salary from originTable""")

    validateDataFiles("default_staticpartitionone", "0", 1)
  }

  test("data loading for partition table for one static partition column with load syntax") {
    sql(
      """
        | CREATE TABLE loadstaticpartitionone (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (empno int)
        | STORED AS carbondata
      """.stripMargin)

    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE loadstaticpartitionone PARTITION(empno='1') OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    checkAnswer(sql("select distinct empno from loadstaticpartitionone"), Seq(Row(1)))
  }

  test("overwrite partition table for one static partition column with load syntax") {
    sql(
      """
        | CREATE TABLE loadstaticpartitiononeoverwrite (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (empno int)
        | STORED AS carbondata
      """.stripMargin)

    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE loadstaticpartitiononeoverwrite PARTITION(empno='1') OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    val rows = sql("select count(*) from loadstaticpartitiononeoverwrite").collect()

    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE loadstaticpartitiononeoverwrite PARTITION(empno='1') OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE loadstaticpartitiononeoverwrite PARTITION(empno='1') OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' OVERWRITE INTO TABLE loadstaticpartitiononeoverwrite PARTITION(empno='1') OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    checkAnswer(sql("select count(*) from loadstaticpartitiononeoverwrite"), rows)
  }

  test("test partition column with special characters") {
    sql(
      """
        | CREATE TABLE loadpartitionwithspecialchar (empno int, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (empname String)
        | STORED AS carbondata
      """.stripMargin)

    sql(s"""LOAD DATA local inpath '$resourcesPath/data_with_special_char.csv' INTO TABLE loadpartitionwithspecialchar OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    checkAnswer(sql("select count(*) from loadpartitionwithspecialchar"), Seq(Row(10)))
    checkAnswer(sql("select count(*) from loadpartitionwithspecialchar where empname='sibi=56'"), Seq(Row(1)))
    checkAnswer(sql("select count(*) from loadpartitionwithspecialchar where empname='arvind,ss'"), Seq(Row(1)))
  }

  test("Restrict streaming on partitioned table") {
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE streamingpartitionedtable (empname String, designation String, doj
          | Timestamp,
          |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
          |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
          |  utilization int,salary int)
          | PARTITIONED BY (empno int)
          | STORED AS carbondata TBLPROPERTIES('streaming'='true')
        """.stripMargin)
    }
  }

  // TODO fix 
  ignore("concurrent partition table load test") {
    executorService = Executors.newCachedThreadPool()
    sql(
      """
        | CREATE TABLE partitionmultiplethreeconcurrent (empno int, doj Timestamp,
        |  workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (workgroupcategory int, empname String, designation String)
        | STORED AS carbondata
      """.stripMargin)

    val tasks = new util.ArrayList[Callable[String]]()
    tasks.add(new QueryTask(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitionmultiplethreeconcurrent OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')"""))
    tasks.add(new QueryTask(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitionmultiplethreeconcurrent OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')"""))
    tasks.add(new QueryTask(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitionmultiplethreeconcurrent OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')"""))
    val results = executorService.invokeAll(tasks)
    for (i <- 0 until tasks.size()) {
      val res = results.get(i).get
      assert("PASS".equals(res))
    }
    executorService.shutdown()
    checkAnswer(sql("select count(*) from partitionmultiplethreeconcurrent"), Seq(Row(30)))
  }

  class QueryTask(query: String) extends Callable[String] {
    override def call(): String = {
      var result = "PASS"
      try {
        LOGGER.info("Executing :" + Thread.currentThread().getName)
        sql(query)
      } catch {
        case ex: Exception =>
          ex.printStackTrace()
          result = "FAIL"
      }
      result
    }
  }

  ignore("merge carbon index disable data loading for partition table for three partition column") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "false")
    sql(
      """
        | CREATE TABLE mergeindexpartitionthree (empno int, doj Timestamp,
        |  workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (workgroupcategory int, empname String, designation String)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE mergeindexpartitionthree OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default_mergeindexpartitionthree")
    val details = SegmentStatusManager.readTableStatusFile(CarbonTablePath.getTableStatusFilePath(carbonTable.getTablePath))
    val store = new SegmentFileStore(carbonTable.getTablePath, details(0).getSegmentFile)
    store.readIndexFiles(new Configuration(false))
    store.getIndexFiles
    assert(store.getIndexFiles.size() == 10)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT,
        CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT_DEFAULT)
  }

  test("load static partition table for one static partition column with load syntax issue") {
    sql(
      """
        | CREATE TABLE loadstaticpartitiononeissue (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (empno int)
        | STORED AS carbondata
      """.stripMargin)

    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE loadstaticpartitiononeissue PARTITION(empno='1')""")
    val df = sql("show partitions loadstaticpartitiononeissue")
    assert(df.collect().length == 1)
    checkExistence(df, true,  "empno=1")
  }

  test("bad record test with null values") {
    sql(s"""CREATE TABLE IF NOT EXISTS emp1 (emp_no int,ename string,job string,mgr_id int,date_of_joining string,salary int,bonus int) partitioned by (dept_no int) STORED AS carbondata""")
    sql(s"""LOAD DATA INPATH '$resourcesPath/emp.csv' overwrite INTO TABLE emp1 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '\')""")
    val rows = sql(s"select count(*) from emp1").collect()
    sql(s"""LOAD DATA INPATH '$resourcesPath/emp.csv' overwrite INTO TABLE emp1 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '\','BAD_RECORDS_ACTION'='FORCE')""")
    checkAnswer(sql(s"select count(*) from emp1"), rows)
  }

  test("test restore partition table") {
    sql(
      """
        | CREATE TABLE restorepartition (doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (empno int, empname String, designation String)
        | STORED AS carbondata
      """.stripMargin)

    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE restorepartition""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE restorepartition PARTITION(empno='99', empname='ravi', designation='xx')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE restorepartition PARTITION(empno='100', empname='indra', designation='yy')""")
    val rows = sql("select count(*) from restorepartition").collect()
    val partitions = sql("show partitions restorepartition").collect()
    val table = CarbonMetadata.getInstance().getCarbonTable("default_restorepartition")
    val dblocation = table.getTablePath.substring(0, table.getTablePath.lastIndexOf("/"))
    backUpData(dblocation, "restorepartition")
    sql("drop table restorepartition")
    if (!CarbonEnv.getInstance(sqlContext.sparkSession).carbonMetaStore.isReadFromHiveMetaStore) {
      restoreData(dblocation, "restorepartition")
      sql("refresh table restorepartition")
      checkAnswer(sql("select count(*) from restorepartition"), rows)
      checkAnswer(sql("show partitions restorepartition"), partitions)
    }
  }

  test("test case sensitive on partition columns") {
    sql(
      """
        | CREATE TABLE casesensitivepartition (doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (empNo int, empName String, designation String)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE casesensitivepartition""")
    checkAnswer(sql("select * from  casesensitivepartition where empNo=17"),
      sql("select * from  casesensitivepartition where empno=17"))
  }

  test("Partition LOAD with small files") {
    sql("DROP TABLE IF EXISTS smallpartitionfiles")
    sql(
      """
        | CREATE TABLE smallpartitionfiles(id INT, name STRING, age INT) PARTITIONED BY(city STRING)
        | STORED AS carbondata
      """.stripMargin)
    val inputPath = new File("target/small_files").getCanonicalPath
    val folder = new File(inputPath)
    if (folder.exists()) {
      FileUtils.deleteDirectory(folder)
    }
    folder.mkdir()
    for (i <- 0 to 100) {
      val file = s"$folder/file$i.csv"
      val writer = new FileWriter(file)
      writer.write("id,name,city,age\n")
      writer.write(s"$i,name_$i,city_${i % 5},${ i % 100 }")
      writer.close()
    }
    sql(s"LOAD DATA LOCAL INPATH '$inputPath' INTO TABLE smallpartitionfiles")
    FileUtils.deleteDirectory(folder)
    val specs = CarbonFilters.getPartitions(Seq.empty, sqlContext.sparkSession, TableIdentifier("smallpartitionfiles"))
    specs.get.foreach{s =>
      assert(new File(s.getLocation.toString).listFiles().length < 10)
    }
  }

  test("verify partition read with small files") {
    try {
      CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_TASK_DISTRIBUTION,
        CarbonCommonConstants.CARBON_TASK_DISTRIBUTION_MERGE_FILES)
      sql("DROP TABLE IF EXISTS smallpartitionfilesread")
      sql(
        """
          | CREATE TABLE smallpartitionfilesread(id INT, name STRING, age INT) PARTITIONED BY
          | (city STRING)
          | STORED AS carbondata
        """.stripMargin)
      val inputPath = new File("target/small_files").getCanonicalPath
      val folder = new File(inputPath)
      if (folder.exists()) {
        FileUtils.deleteDirectory(folder)
      }
      folder.mkdir()
      for (i <- 0 until 100) {
        val file = s"$folder/file$i.csv"
        val writer = new FileWriter(file)
        writer.write("id,name,city,age\n")
        writer.write(s"$i,name_$i,city_${ i },${ i % 100 }")
        writer.close()
      }
      sql(s"LOAD DATA LOCAL INPATH '$inputPath' INTO TABLE smallpartitionfilesread")
      FileUtils.deleteDirectory(folder)
      val dataFrame = sql("select * from smallpartitionfilesread")
      val scanRdd = dataFrame.queryExecution.sparkPlan.collect {
        case b: CarbonDataSourceScan if b.rdd.isInstanceOf[CarbonScanRDD[InternalRow]] => b.rdd
          .asInstanceOf[CarbonScanRDD[InternalRow]]
      }.head
      assert(scanRdd.getPartitions.length < 10)
      assertResult(100)(dataFrame.count)
    } finally {
      CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_TASK_DISTRIBUTION ,
        CarbonCommonConstants.CARBON_TASK_DISTRIBUTION_DEFAULT)
    }
  }

  test("test number of segment files should not be more than 1 per segment") {
    printConfiguration()
    sql("drop table if exists new_par")
    sql("create table new_par(a string) partitioned by ( b int) STORED AS carbondata")
    sql("insert into new_par select 'k',1")
    assert(new File(s"$storeLocation/new_par/Metadata/segments/").listFiles().size == 1)
  }

  test("test index and data size after merge index on partition table") {
    sql("drop table if exists new_par")
    sql("create table new_par(a int) partitioned by (b string) STORED AS carbondata")
    sql("insert into new_par select 1,'k'")
    val result = sql("show segments for table new_par").collectAsList()
    val dataAndIndexSize = getDataAndIndexSize(s"$storeLocation/new_par/b=k")
    assert(result.get(0).get(6).equals(dataAndIndexSize._1))
    assert(result.get(0).get(7).equals(dataAndIndexSize._2))
  }

  def getDataAndIndexSize(path: String): (String, String) = {
    val mergeIndexFiles = FileFactory.getCarbonFile(path).listFiles(new CarbonFileFilter {
      override def accept(file: CarbonFile): Boolean = {
        file.getName.endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT)
      }
    })
    val dataFiles = FileFactory.getCarbonFile(path).listFiles(new CarbonFileFilter {
      override def accept(file: CarbonFile): Boolean = {
        file.getName.endsWith(CarbonTablePath.CARBON_DATA_EXT)
      }
    })
    var indexSize: Long = 0
    for (file <- mergeIndexFiles) {
      indexSize += file.getSize
    }
    var dataSize: Long = 0
    for (file <- dataFiles) {
      dataSize += file.getSize
    }
    (Strings.formatSize(dataSize.toFloat), Strings.formatSize(indexSize.toFloat))
  }

  def restoreData(dblocation: String, tableName: String) = {
    val destination = dblocation + CarbonCommonConstants.FILE_SEPARATOR + tableName
    val source = dblocation+ "_back" + CarbonCommonConstants.FILE_SEPARATOR + tableName
    try {
      FileUtils.copyDirectory(new File(source), new File(destination))
      FileUtils.deleteDirectory(new File(source))
    } catch {
      case e : Exception =>
        throw new IOException("carbon table data restore failed.")
    } finally {

    }
  }
  def backUpData(dblocation: String, tableName: String) = {
    val source = dblocation + CarbonCommonConstants.FILE_SEPARATOR + tableName
    val destination = dblocation+ "_back" + CarbonCommonConstants.FILE_SEPARATOR + tableName
    try {
      FileUtils.copyDirectory(new File(source), new File(destination))
    } catch {
      case e : Exception =>
        throw new IOException("carbon table data backup failed.", e)
    }
  }


  override def afterAll = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
      .addProperty(CarbonCommonConstants.CARBON_TASK_DISTRIBUTION,
        CarbonCommonConstants.CARBON_TASK_DISTRIBUTION_DEFAULT)
    dropTable
    if (executorService != null && !executorService.isShutdown) {
      executorService.shutdownNow()
    }
  }

  def dropTable = {
    sql("drop table if exists originTable")
    sql("drop table if exists originMultiLoads")
    sql("drop table if exists partitionone")
    sql("drop table if exists partitiontwo")
    sql("drop table if exists partitionthree")
    sql("drop table if exists partitionfive")
    sql("drop table if exists partitionmultiplethree")
    sql("drop table if exists insertpartitionthree")
    sql("drop table if exists staticpartitionone")
    sql("drop table if exists singlepasspartitionone")
    sql("drop table if exists loadstaticpartitionone")
    sql("drop table if exists loadstaticpartitiononeoverwrite")
    sql("drop table if exists streamingpartitionedtable")
    sql("drop table if exists mergeindexpartitionthree")
    sql("drop table if exists loadstaticpartitiononeissue")
    sql("drop table if exists partitionmultiplethreeconcurrent")
    sql("drop table if exists loadpartitionwithspecialchar")
    sql("drop table if exists emp1")
    sql("drop table if exists restorepartition")
    sql("drop table if exists casesensitivepartition")
    sql("drop table if exists new_par")
  }

}
