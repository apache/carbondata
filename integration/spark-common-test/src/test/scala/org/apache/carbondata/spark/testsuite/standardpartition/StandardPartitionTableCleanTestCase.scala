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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.optimizer.CarbonFilters
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.{CarbonMetadata, SegmentFileStore}
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath

class StandardPartitionTableCleanTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    dropTable

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "dd-MM-yyyy")
    sql(
      """
        | CREATE TABLE originTable (empno int, empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Date,attendance int,
        |  utilization int,salary int)
        | STORED AS carbondata
      """.stripMargin)

    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE originTable OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

  }

  def validateDataFiles(tableUniqueName: String, segmentId: String, partition: Int, indexes: Int): Unit = {
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable(tableUniqueName)
    val partitions = CarbonFilters.getPartitions(
      Seq.empty,
      sqlContext.sparkSession,
      TableIdentifier(carbonTable.getTableName, Some(carbonTable.getDatabaseName)))
    assert(partitions.get.length == partition)
    val details = SegmentStatusManager.readLoadMetadata(CarbonTablePath.getMetadataPath(carbonTable.getTablePath))
    val segLoad = details.find(_.getLoadName.equals(segmentId)).get
    val seg = new SegmentFileStore(carbonTable.getTablePath, segLoad.getSegmentFile)
    assert(seg.getIndexOrMergeFiles.size == indexes)
  }

  test("clean up partition table for int partition column") {
    sql(
      """
        | CREATE TABLE partitionone (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Date,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (empno int)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitionone OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    checkAnswer(
      sql(s"""select count (*) from partitionone"""),
      sql(s"""select count (*) from originTable"""))

    checkAnswer(
      sql(s"""select count (*) from partitionone where empno=11"""),
      sql(s"""select count (*) from originTable where empno=11"""))

    sql(s"""ALTER TABLE partitionone DROP PARTITION(empno='11')""")
    validateDataFiles("default_partitionone", "0", 9, 9)
    sql(s"CLEAN FILES FOR TABLE partitionone").show()
    validateDataFiles("default_partitionone", "0", 9, 9)
    checkExistence(sql(s"""SHOW PARTITIONS partitionone"""), false, "empno=11")
    checkAnswer(
      sql(s"""select count (*) from partitionone where empno=11"""),
      Seq(Row(0)))

  }

    test("clean up partition on table for more partition columns") {
      sql(
        """
          | CREATE TABLE partitionmany (empno int, empname String, designation String,
          |  workgroupcategory int, workgroupcategoryname String, deptno int,
          |  projectjoindate Timestamp, projectenddate Date,attendance int,
          |  utilization int,salary int)
          | PARTITIONED BY (deptname String,doj Timestamp,projectcode int)
          | STORED AS carbondata
        """.stripMargin)
      sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitionmany OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
      sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitionmany OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
      sql(s"""ALTER TABLE partitionmany DROP PARTITION(deptname='Learning')""")
      validateDataFiles("default_partitionmany", "0", 8, 8)
      validateDataFiles("default_partitionmany", "1", 8, 8)
      sql(s"CLEAN FILES FOR TABLE partitionmany").show()
      validateDataFiles("default_partitionmany", "0", 8, 8)
      validateDataFiles("default_partitionmany", "1", 8, 8)
      checkExistence(sql(s"""SHOW PARTITIONS partitionmany"""), false, "deptname=Learning", "projectcode=928479")
      checkAnswer(
        sql(s"""select count (*) from partitionmany where deptname='Learning'"""),
        Seq(Row(0)))
    }

  test("clean up after dropping all partition on table") {
    sql(
      """
        | CREATE TABLE partitionall (empno int, empname String, designation String,
        |  workgroupcategory int, workgroupcategoryname String, deptno int,
        |  projectjoindate Timestamp, projectenddate Date,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (deptname String,doj Timestamp,projectcode int)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitionall OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitionall OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""ALTER TABLE partitionall DROP PARTITION(deptname='Learning')""")
    sql(s"""ALTER TABLE partitionall DROP PARTITION(deptname='configManagement')""")
    sql(s"""ALTER TABLE partitionall DROP PARTITION(deptname='network')""")
    sql(s"""ALTER TABLE partitionall DROP PARTITION(deptname='protocol')""")
    sql(s"""ALTER TABLE partitionall DROP PARTITION(deptname='security')""")
    assert(sql(s"""SHOW PARTITIONS partitionall""").collect().length == 0)
    validateDataFiles("default_partitionall", "0", 0, 0)
    sql(s"CLEAN FILES FOR TABLE partitionall").show()
    validateDataFiles("default_partitionall", "0", 0, 0)
    checkAnswer(
      sql(s"""select count (*) from partitionall"""),
      Seq(Row(0)))
  }

  test("clean up after deleting segments on table") {
    sql(
      """
        | CREATE TABLE partitionalldeleteseg (empno int, empname String, designation String,
        |  workgroupcategory int, workgroupcategoryname String, deptno int,
        |  projectjoindate Timestamp, projectenddate Date,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (deptname String,doj Timestamp,projectcode int)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitionalldeleteseg OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitionalldeleteseg OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitionalldeleteseg OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE partitionalldeleteseg OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    assert(sql(s"show segments for table partitionalldeleteseg").count == 4)
    checkAnswer(sql(s"Select count(*) from partitionalldeleteseg"), Seq(Row(40)))
    sql(s"delete from table partitionalldeleteseg where segment.id in (1)").show()
    checkExistence(sql(s"show segments for table partitionalldeleteseg"), true, "Marked for Delete")
    checkAnswer(sql(s"Select count(*) from partitionalldeleteseg"), Seq(Row(30)))
    sql(s"CLEAN FILES FOR TABLE partitionalldeleteseg").show()
    assert(sql(s"show segments for table partitionalldeleteseg").count == 3)
  }


  override def afterAll = {
    dropTable
  }

  def dropTable = {
    sql("drop table if exists originTable")
    sql("drop table if exists originMultiLoads")
    sql("drop table if exists partitionone")
    sql("drop table if exists partitionall")
    sql("drop table if exists partitionmany")
    sql("drop table if exists partitionshow")
    sql("drop table if exists staticpartition")
    sql("drop table if exists partitionalldeleteseg")
  }

}
