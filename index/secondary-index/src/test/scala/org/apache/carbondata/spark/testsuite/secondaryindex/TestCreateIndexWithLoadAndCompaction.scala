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
package org.apache.carbondata.spark.testsuite.secondaryindex

import com.google.gson.Gson
import mockit.{Mock, MockUp}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.test.SparkTestQueryExecutor
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.OperationContext
import org.apache.carbondata.indexserver.DistributedRDDUtils
import org.apache.carbondata.spark.testsuite.secondaryindex.TestSecondaryIndexUtils.isFilterPushedDownToSI

/**
 * test cases for testing creation of index table with load and compaction
 */
class TestCreateIndexWithLoadAndCompaction extends QueryTest with BeforeAndAfterAll {
  // scalastyle:off lineLength
  override def beforeAll {
    sql("drop table if exists index_test")
    sql("CREATE TABLE index_test (integer_column1 string,date1 timestamp,date2 timestamp,ID String,string_column1 string,string_column2 string) STORED AS CARBONDATA")
    val currentFormat = CarbonProperties.getInstance().getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy-MM-dd HH:mm:ss")
    sql(s"LOAD DATA INPATH '$resourcesPath/secindex/index.csv' into table index_test OPTIONS('DELIMITER'=',' ,'FILEHEADER'='ID,integer_column1,date1,date2,string_column1,string_column2')")
    if(null != currentFormat) {
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, currentFormat)
    }
  }

//  test("test create index after update and delete") {
//    sql("create table dest2 (c1 string,c2 int,c3 string,c5 string) STORED AS CARBONDATA")
//    sql("load data inpath './src/test/resources/secindex/dest.csv' INTO table dest2")
//    sql("load data inpath './src/test/resources/secindex/dest1.csv' INTO table dest2")
//    sql("load data inpath './src/test/resources/secindex/dest2.csv' INTO table dest2")
//    sql("load data inpath './src/test/resources/secindex/dest3.csv' INTO table dest2")
//    sql("create table source2 (c11 string,c22 int,c33 string,c55 string, c66 int) STORED AS CARBONDATA")
//    sql("LOAD DATA LOCAL INPATH './src/test/resources/secindex/source3.csv' INTO table source2")
//    sql("update dest2 d set (d.c3, d.c5 ) = (select s.c33,s.c55 from source2 s where d.c1 = s.c11 and s.c22 < 3 or (s.c22 > 10 and s.c22 < 13) or (s.c22 > 20 and s.c22 < 23) or (s.c22 > 30 and s.c22 < 33))")
//    sql("delete from dest2 where (c2 < 2) or (c2 > 10 and c2 < 13) or (c2 > 20 and c2 < 23) or (c2 > 30 and c2 < 33)")
//    sql("delete from dest2 where (c2 > 3 and c2 < 5) or (c2 > 13 and c2 < 15) or (c2 > 23 and c2 < 25) or (c2 > 33 and c2 < 35)")
//    sql("delete from dest2 where (c2 > 5 and c2 < 8) or (c2 > 15 and c2 < 18 ) or (c2 > 25 and c2 < 28) or (c2 > 35 and c2 < 38)")
//    sql("create index indexdest2 on table dest2 (c3) AS 'carbondata'")
//  }

  test("test create index table with load after index table creation") {
    sql("drop table if exists load_after_index")
    sql("CREATE table load_after_index (empno int, empname String, " +
        "designation String, doj Timestamp, workgroupcategory int, " +
        "workgroupcategoryname String, deptno int, deptname String, projectcode int, " +
        "projectjoindate Timestamp, projectenddate Timestamp, attendance int, " +
        "utilization int,salary int) STORED AS CARBONDATA")
    sql("drop index if exists index_no_dictionary on load_after_index")
    sql("create index index_no_dictionary on table load_after_index (workgroupcategoryname,empname) AS 'carbondata'")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO " +
        "TABLE load_after_index OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\"', 'BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='FORCE')")
    checkAnswer(sql("select count(*) from load_after_index"),
      sql("select count(*) from index_no_dictionary"))
    sql("drop table if exists load_after_index")
  }

  test("test create index table with load before index table creation") {
    sql("drop table if exists multiple_load")
    sql("CREATE table multiple_load (empno int, empname String, " +
        "designation String, doj Timestamp, workgroupcategory int, " +
        "workgroupcategoryname String, deptno String, deptname String, projectcode int, " +
        "projectjoindate Timestamp, projectenddate Timestamp, attendance int, " +
        "utilization int,salary int) STORED AS CARBONDATA")

    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO " +
        "TABLE multiple_load OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\"', 'BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='FORCE')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO " +
        "TABLE multiple_load OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\"', 'BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='FORCE')")

    sql("drop index if exists index_no_dictionary1 on multiple_load")
    sql("create index index_no_dictionary1 on table multiple_load (workgroupcategoryname,empname) AS 'carbondata'")
    sql("drop index if exists index_dictionary on multiple_load")
    sql("create index index_dictionary on table multiple_load (deptno) AS 'carbondata'")

    checkAnswer(sql("select count(*) from multiple_load"),
      sql("select count(*) from index_no_dictionary1"))
    checkAnswer(sql("select count(*) from index_dictionary"), Seq(Row(10)))

    sql("drop table if exists multiple_load")
  }

  test("test manual index table creation after compaction") {
    sql("drop table if exists compaction_load")
    sql("CREATE table compaction_load (empno int, empname String, " +
        "designation String, doj Timestamp, workgroupcategory int, " +
        "workgroupcategoryname String, deptno int, deptname String, projectcode int, " +
        "projectjoindate Timestamp, projectenddate Timestamp, attendance int, " +
        "utilization int,salary int) STORED AS CARBONDATA")

    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO " +
        "TABLE compaction_load OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\"', 'BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='FORCE')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO " +
        "TABLE compaction_load OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\"', 'BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='FORCE')")

    sql("alter table compaction_load compact 'major'")

    sql("drop index if exists index_no_dictionary2 on compaction_load")

    sql("create index index_no_dictionary2 on table compaction_load (workgroupcategoryname,empname) AS 'carbondata'")

    checkAnswer(sql("select count(*) from index_no_dictionary2"), Seq(Row(10)))

    sql("drop table if exists compaction_load")
  }

//  test("test auto index table creation after compaction") {
//    sql("drop table if exists auto_compaction_index")
//    sql("CREATE table auto_compaction_index (empno int, empname String, " +
//        "designation String, doj Timestamp, workgroupcategory int, " +
//        "workgroupcategoryname String, deptno int, deptname String, projectcode int, " +
//        "projectjoindate Timestamp, projectenddate Timestamp, attendance int, " +
//        "utilization int,salary int) STORED AS CARBONDATA " +
//        "TBLPROPERTIES('DICTIONARY_EXCLUDE'='empname')")
//
//    sql("LOAD DATA LOCAL INPATH './src/test/resources/data.csv' INTO " +
//        "TABLE auto_compaction_index OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\"', 'BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='FORCE')")
//    sql("LOAD DATA LOCAL INPATH './src/test/resources/data.csv' INTO " +
//        "TABLE auto_compaction_index OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\"', 'BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='FORCE')")
//
//    sql("drop index if exists index_no_dictionary3 on auto_compaction_index")
//    sql("create index index_no_dictionary3 on table auto_compaction_index (empname) AS 'carbondata'")
//
//    sql("alter table auto_compaction_index compact 'major'")
//
//    checkAnswer(sql("select count(*) from index_no_dictionary3"), Seq(Row(10)))
//
//    sql("drop table if exists auto_compaction_index")
//  }

  test("test create index with jumbled order of parent table cols") {
    sql("drop index if exists indextable05 ON index_test")
    sql("CREATE INDEX indextable05 ON TABLE index_test (string_column2,id,date2,date1) AS 'carbondata'")
    checkAnswer(sql("select string_column2,id,date2,date1 from index_test"),
        sql("select string_column2,id,date2,date1 from indextable05"))
  }
  test("Load from 2 csv's with unique value for index column and compare the query with or condition") {
    sql("drop table if exists seccust")
    sql("create table seccust (id string, c_custkey string, c_name string, c_address string, c_nationkey string, c_phone string,c_acctbal decimal, c_mktsegment string, c_comment string) STORED AS CARBONDATA")
    sql(s"load data  inpath '$resourcesPath/secindex/firstunique.csv' into table seccust options('DELIMITER'='|','FILEHEADER'='id,c_custkey,c_name,c_address,c_nationkey,c_phone,c_acctbal,c_mktsegment,c_comment')")
    sql(s"load data  inpath '$resourcesPath/secindex/secondunique.csv' into table seccust options('DELIMITER'='|','FILEHEADER'='id,c_custkey,c_name,c_address,c_nationkey,c_phone,c_acctbal,c_mktsegment,c_comment')")
    val count1BeforeIndex = sql("select count(*) from seccust where c_phone = '25-989-741-2988' or c_mktsegment ='BUILDING'").collect
    val count2BeforeIndex = sql("select count(*) from seccust where (c_mktsegment ='BUILDING' and c_phone ='25-989-741-2989') or c_phone = '25-989-741-2988'").collect

    sql("drop index if exists sc_indx5 on seccust")
    sql("drop index if exists sc_indx6 on seccust")
    sql("create index sc_indx5 on table seccust(c_phone) AS 'carbondata'")
    sql("create index sc_indx6 on table seccust(c_mktsegment) AS 'carbondata'")
    checkAnswer(sql("select count(*) from seccust where c_phone = '25-989-741-2988' or c_mktsegment ='BUILDING'"),
      count1BeforeIndex)
    checkAnswer(sql("select count(*) from seccust where (c_mktsegment ='BUILDING' and c_phone ='25-989-741-2989') or c_phone = '25-989-741-2988'"),
      count2BeforeIndex)
    sql("drop table if exists seccust")
  }

  /* test("Load once and create sec index and load again and do select ") {
    sql("drop table if exists seccust1")
    sql("create table seccust1 (id string, c_custkey string, c_name string, c_address string, c_nationkey string, c_phone string,c_acctbal decimal, c_mktsegment string, c_comment string) STORED AS carbondata")
    sql("load data  inpath './src/test/resources/secindex/firstunique.csv' into table seccust1 options('DELIMITER'='|','QUOTECHAR'='\"','FILEHEADER'='id,c_custkey,c_name,c_address,c_nationkey,c_phone,c_acctbal,c_mktsegment,c_comment')")
    sql("drop index if exists sc_indx5 on seccust1")
    sql("create index sc_indx5 on table seccust1(c_phone) AS 'carbondata'")
    sql("load data  inpath './src/test/resources/secindex/firstunique.csv' into table seccust1 options('DELIMITER'='|','QUOTECHAR'='\"','FILEHEADER'='id,c_custkey,c_name,c_address,c_nationkey,c_phone,c_acctbal,c_mktsegment,c_comment')")
    checkAnswer(sql("select c_phone from sc_indx5"),
      Seq(Row("25-989-741-2989"),Row("25-989-741-2989")))
    sql("drop table if exists seccust1")
  } */

  test("test SI with auto compaction and check that table status is changed to compacted") {
    try {
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD, "2")
      sql("DROP TABLE IF EXISTS si_compaction_test")
      sql("DROP INDEX IF EXISTS alter_i1 on si_compaction_test")
      // create table
      sql(
        "CREATE table si_compaction_test (empno int, empname String, designation String) STORED " +
        "AS carbondata")
      // create index
      sql(
        "create index alter_i1 on table si_compaction_test (designation) AS 'carbondata'")
      // insert data
      sql("insert into si_compaction_test select 11,'arvind','lead'")
      sql("insert into si_compaction_test select 12,'krithi','TA'")
      // perform compaction operation
      sql("alter table si_compaction_test compact 'minor'")

      // get index table from relation
      val indexCarbonTable = CarbonEnv.getInstance(SparkTestQueryExecutor.spark).carbonMetaStore
        .lookupRelation(Option("default"), "alter_i1")(SparkTestQueryExecutor.spark)
        .asInstanceOf[CarbonRelation].carbonTable
      // read load metadata details
      val loadDetails: Array[LoadMetadataDetails] = SegmentStatusManager
        .readLoadMetadata(CarbonTablePath.getMetadataPath(indexCarbonTable.getTablePath), indexCarbonTable.getTableStatusVersion)
      assert(loadDetails.length == 3)
      // compacted status segment should only be 2
      val compactedStatusSegments = loadDetails
        .filter(detail => detail.getSegmentStatus == SegmentStatus.COMPACTED)
      assert(compactedStatusSegments.size == 2)
      val successStatusSegments = loadDetails
        .filter(detail => detail.getSegmentStatus == SegmentStatus.SUCCESS)
      // success status segment should only be 1
      assert(successStatusSegments.size == 1)
      sql("DROP INDEX IF EXISTS alter_i1 on si_compaction_test")
      sql("DROP TABLE IF EXISTS si_compaction_test")
    } finally {
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD,
          CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD)
    }
  }

  test("test block SI on table flat folder structure") {
    sql("drop table if exists table_with_flat")
    sql(
      "create table table_with_flat (a string,b string) STORED AS carbondata tblproperties('flat_folder'='true')")
    sql("insert into table_with_flat values('k','r')")
    val ex = intercept[Exception] {
      sql("create index index2 on table table_with_flat(b) AS 'carbondata' ")
    }
    assert(ex.getMessage.contains("Index table creation is not permitted on table with flat folder structure"))
  }

  test("test SI for select after delete records from compacted table") {
    sql("drop table if exists table1")
    sql("create table table1(c1 int,c2 string,c3 string) stored as carbondata")
    sql("insert into table1 values(1,'a1','b1')")
    sql("insert into table1 values(1,'a1','b3')")
    sql("create index idx1 on table table1(c3) as 'carbondata'")
    sql("insert into table1 values(2,'a2','b2')")
    sql("alter table table1 compact 'major'")
    sql("insert into table1 values(3,'a3','b3')")
    sql("delete from table1 where c3='b3'")
    checkAnswer(sql("select * from table1 where c3 = 'b2' or c3 = 'b3'"),
      Seq(Row(2, "a2", "b2")))
    sql("select * from table1 where c3 = 'b2' or c3 = 'b3'")
    sql("drop table if exists table1")
  }

  test("test compaction on SI table") {
    sql("drop table if exists table1")
    sql("create table table1(c1 int,c2 string,c3 string) stored as carbondata")
    sql("create index idx1 on table table1(c3) as 'carbondata'")
    for (i <- 0 until 5) {
      sql(s"insert into table1 values(${i + 1},'a$i','b$i')")
    }
    var ex = intercept[Exception] {
      sql("ALTER TABLE idx1 COMPACT 'CUSTOM' WHERE SEGMENT.ID IN (1,2,3)")
    }
    assert(ex.getMessage.contains("Unsupported alter operation on carbon table: Compaction is not supported on SI table"))
    ex = intercept[Exception] {
      sql("ALTER TABLE idx1 COMPACT 'minor'")
    }
    assert(ex.getMessage.contains("Unsupported alter operation on carbon table: Compaction is not supported on SI table"))
    sql("drop table if exists table1")
  }

  test("test custom compaction on main table which have SI tables") {
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED, "true")
    sql("drop table if exists table1")
    sql("create table table1(c1 int,c2 string,c3 string) stored as carbondata")
    sql("create index idx1 on table table1(c3) as 'carbondata'")
    for (i <- 0 until 5) {
      sql(s"insert into table1 values(${i + 1},'a$i','b$i')")
    }
    sql("ALTER TABLE table1 COMPACT 'CUSTOM' WHERE SEGMENT.ID IN (1,2,3)")

    val segInfos = checkSegmentList(6)
    assert(segInfos.contains(("0", "Success")))
    assert(segInfos.contains(("1", "Compacted")))
    assert(segInfos.contains(("2", "Compacted")))
    assert(segInfos.contains(("3", "Compacted")))
    assert(segInfos.contains(("1.1", "Success")))
    assert(segInfos.contains(("4", "Success")))
    checkAnswer(sql("select * from table1 where c3='b2'"), Seq(Row(3, "a2", "b2")))

    // after clean files
    val mock = mockreadSegmentList()
    sql("CLEAN FILES FOR TABLE table1 options('force'='true')")
    mock.tearDown()
    val table = CarbonEnv
      .getCarbonTable(Some("default"), "idx1")(sqlContext.sparkSession)
    val details = SegmentStatusManager.readLoadMetadata(table.getMetadataPath,
      table.getTableStatusVersion)
    assert(SegmentStatusManager.countInvisibleSegments(details, 4) == 1)
    checkSegmentList(4)
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED,
          CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED_DEFAULT)
  }

  def checkSegmentList(segmentSie: Int): Array[(String, String)] = {
    val segments = sql("SHOW SEGMENTS FOR TABLE idx1")
    val segInfos = segments.collect().map { each =>
      (each.toSeq.head.toString, each.toSeq (1).toString)
    }
    assert(segInfos.length == segmentSie)
    segInfos
  }

  test("test minor compaction on table with non-empty segment list" +
    "and custom compaction with empty segment list") {
    sql("drop table if exists table1")
    sql("create table table1(c1 int,c2 string,c3 string) stored as carbondata")
    sql("create index idx1 on table table1(c3) as 'carbondata'")
    for (i <- 0 until 3) {
      sql(s"insert into table1 values(${i + 1},'a$i','b$i')")
    }
    var ex = intercept[Exception] {
      sql("ALTER TABLE table1 COMPACT 'minor' WHERE SEGMENT.ID IN (1,2)")
    }
    assert(ex.getMessage.contains("Custom segments not supported when doing MINOR compaction"))
    ex = intercept[Exception] {
      sql("ALTER TABLE table1 COMPACT 'custom'")
    }
    assert(ex.getMessage.contains("Segment ids should not be empty when doing CUSTOM compaction"))
    sql("drop table if exists table1")
  }

  test("test custom compaction with global sort SI") {
    sql("drop table if exists table1")
    sql("create table table1(c1 int,c2 string,c3 string) stored as carbondata")
    sql("create index idx1 on table table1(c3) as 'carbondata' " +
      "properties('sort_scope'='global_sort', 'Global_sort_partitions'='1')")
    for (i <- 0 until 3) {
      sql(s"insert into table1 values(${i + 1},'a$i','b$i')")
    }
    sql("ALTER TABLE table1 COMPACT 'CUSTOM' WHERE SEGMENT.ID IN (1,2)")

    val segments = sql("SHOW SEGMENTS FOR TABLE idx1")
    val segInfos = segments.collect().map { each =>
      (each.toSeq.head.toString, each.toSeq (1).toString)
    }
    assert(segInfos.length == 4)
    checkAnswer(sql("select * from table1 where c3='b2'"), Seq(Row(3, "a2", "b2")))
    sql("drop table if exists table1")
  }

  test("test SI load data when exception occurred") {
    sql("use default")
    sql("drop table if exists table1")
    sql("create table table1(c1 int,c2 string,c3 string) stored as carbondata")
    sql("create index idx1 on table table1(c3) as 'carbondata' ")
    val mock = TestSecondaryIndexUtils.mockLoadEventListner()
    val ex = intercept[RuntimeException] {
      sql(s"insert into table1 values(1,'a1','b1')")
    }
    assert(ex.getMessage.contains("An exception occurred while loading data to SI table"))
    mock.tearDown()
    sql("drop table if exists table1")
  }

  test("test compaction when pre priming will throw exception") {
    sql("drop table if exists table1")
    sql("create table table1(c1 int,c2 string,c3 string) stored as carbondata")
    sql("create index idx1 on table table1(c3) as 'carbondata' ")
    sql("create index idx2 on table table1(c2, c3) as 'carbondata' ")
    for (i <- 0 until 3) {
      sql(s"insert into table1 values(${i + 1},'a$i','b$i')")
    }
    var prePriming = 0
    val mock: MockUp[DistributedRDDUtils.type ] = new MockUp[DistributedRDDUtils.type]() {
      @Mock
      def triggerPrepriming(sparkSession: SparkSession,
        carbonTable: CarbonTable,
        invalidSegments: Seq[String],
        operationContext: OperationContext,
        conf: Configuration,
        segmentId: List[String]): Unit = {
        prePriming += 1
        if (prePriming > 1) {
          throw new RuntimeException("An exception occurred while triggering pre priming.")
        }
      }
    }
    val ex = intercept[Exception] {
      sql("ALTER TABLE table1 COMPACT 'CUSTOM' WHERE SEGMENT.ID IN (1,2)")
    }
    assert(ex.getMessage.contains("An exception occurred while triggering pre priming."))
    mock.tearDown()
    checkExistence(sql("show indexes on table table1"), true,
      "idx1", "idx2", "enabled")
  }

  def mockreadSegmentList(): MockUp[SegmentStatusManager] = {
    val mock: MockUp[SegmentStatusManager] = new MockUp[SegmentStatusManager]() {
      @Mock
      def readTableStatusFile(tableStatusPath: String): Array[LoadMetadataDetails] = {
        if (tableStatusPath.contains("integration/spark/target/warehouse/idx1/Metadata")) {
          new Gson().fromJson("[{\"timestamp\":\"1608113216908\",\"loadStatus\":\"Success\"," +
              "\"loadName\":\"0\",\"dataSize\":\"790\",\"indexSize\":\"514\",\"loadStartTime\"" +
              ":\"1608113213170\",\"segmentFile\":\"0_1608113213170.segment\"}," +
              "{\"timestamp\":\"1608113217855\",\"loadStatus\":\"Success\",\"loadName\":\"1\"," +
              "\"dataSize\":\"791\",\"indexSize\":\"514\",\"modificationOrDeletionTimestamp\"" +
              ":\"1608113228366\",\"loadStartTime\":\"1608113217188\",\"mergedLoadName\":\"1.1\"," +
              "\"segmentFile\":\"1_1608113217188.segment\"},{\"timestamp\":\"1608113218341\"," +
              "\"loadStatus\":\"Compacted\",\"loadName\":\"2\",\"dataSize\":\"791\"," +
              "\"indexSize\":" +
              "\"514\",\"modificationOrDeletionTimestamp\":\"1608113228366\",\"loadStartTime\":" +
              "\"1608113218057\",\"mergedLoadName\":\"1.1\",\"segmentFile\":\"2_1608113218057" +
              ".segment\"},{\"timestamp\":\"1608113219267\",\"loadStatus\":\"Success\"," +
              "\"loadName\":\"4\",\"dataSize\":\"791\",\"indexSize\":\"514\",\"loadStartTime\":" +
              "\"1608113218994\",\"segmentFile\":\"4_1608113218994.segment\"},{\"timestamp\":" +
              "\"1608113228366\",\"loadStatus\":\"Success\",\"loadName\":\"1.1\",\"dataSize\":" +
              "\"831\",\"indexSize\":\"526\",\"loadStartTime\":\"1608113219441\",\"segmentFile\":" +
              "\"1.1_1608113219441.segment\"}]", classOf[Array[LoadMetadataDetails]])
        } else {
          new Gson().fromJson("[{\"timestamp\":\"1608113216908\",\"loadStatus\":\"Success\"," +
              "\"loadName\":\"0\",\"dataSize\":\"790\",\"indexSize\":\"514\",\"loadStartTime\"" +
              ":\"1608113213170\",\"segmentFile\":\"0_1608113213170.segment\"}," +
              "{\"timestamp\":\"1608113217855\",\"loadStatus\":\"Compacted\",\"loadName\":\"1\"," +
              "\"dataSize\":\"791\",\"indexSize\":\"514\",\"modificationOrDeletionTimestamp\"" +
              ":\"1608113228366\",\"loadStartTime\":\"1608113217188\",\"mergedLoadName\":\"1.1\"," +
              "\"segmentFile\":\"1_1608113217188.segment\"},{\"timestamp\":\"1608113218341\"," +
              "\"loadStatus\":\"Compacted\",\"loadName\":\"2\",\"dataSize\":\"791\"," +
              "\"indexSize\":" +
              "\"514\",\"modificationOrDeletionTimestamp\":\"1608113228366\",\"loadStartTime\":" +
              "\"1608113218057\",\"mergedLoadName\":\"1.1\",\"segmentFile\":\"2_1608113218057" +
              ".segment\"},{\"timestamp\":\"1608113219267\",\"loadStatus\":\"Success\"," +
              "\"loadName\":\"4\",\"dataSize\":\"791\",\"indexSize\":\"514\",\"loadStartTime\":" +
              "\"1608113218994\",\"segmentFile\":\"4_1608113218994.segment\"},{\"timestamp\":" +
              "\"1608113228366\",\"loadStatus\":\"Success\",\"loadName\":\"1.1\",\"dataSize\":" +
              "\"831\",\"indexSize\":\"526\",\"loadStartTime\":\"1608113219441\",\"segmentFile\":" +
              "\"1.1_1608113219441.segment\"}]", classOf[Array[LoadMetadataDetails]])
        }
      }
    }
    mock
  }

  test("test SI with compaction when parent and child table seg are not in sync") {
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED, "true")
    try {
      sql("drop table if exists table1")
      sql("create table table1(c1 int,c2 string,c3 string) stored as carbondata")
      sql("create index idx1 on table table1(c3) as 'carbondata'")
      for (i <- 0 until 5) {
        sql(s"insert into table1 values(${i + 1},'a$i','b')")
      }
      sql("delete from table idx1 where segment.ID in (1,2)")
      sql("clean files for table idx1 options('force'='true')")
      assert(sql("show segments on idx1").collect().length == 3)
      sql("alter table table1 compact 'minor'")
      sql("clean files for table idx1 options('force' = 'true')")
      assert(sql("show segments on idx1").collect().length == 2)
      assert(sql("select * from table1 where c3='b'").collect().length == 5)
      checkExistence(sql("show indexes on table1"),
        true, "idx1", "enabled")
      val df = sql("select * from table1 where c3='b'").queryExecution.sparkPlan
      assert(isFilterPushedDownToSI(df))
      sql("drop table if exists table1")
    } finally {
      CarbonProperties.getInstance()
          .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED,
            CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED_DEFAULT)
    }
  }

  override def afterAll: Unit = {
    sql("drop table if exists index_test")
    sql("drop table if exists seccust1")
    sql("drop table if exists table_with_flat")
    sql("drop table if exists table1")
  }
  // scalastyle:on lineLength
}
