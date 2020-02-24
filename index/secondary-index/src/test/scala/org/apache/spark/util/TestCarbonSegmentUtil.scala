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
package org.apache.spark.util

import java.util

import org.junit.Test
import scala.collection.JavaConverters._

import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.strategy.CarbonDataSourceScan
import org.apache.spark.sql.secondaryindex.joins.BroadCastSIFilterPushJoin
import org.apache.spark.sql.secondaryindex.util.SecondaryIndexUtil
import org.apache.spark.sql.test.{SparkTestQueryExecutor, TestQueryExecutor}
import org.apache.spark.sql.test.util.QueryTest

import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatusManager}
import org.apache.carbondata.spark.rdd.CarbonScanRDD

/**
 * This test class will test the functionality of APIs
 * present in CarbonSegmentUtil class
 */
class TestCarbonSegmentUtil extends QueryTest {

  val tableName: String = "test_table"
  val databaseName: String = "default"

  @Test
  // Test get Filtered Segments using the carbonScanRDD
  def test_getFilteredSegments() {
    createTable(tableName)
    val dataFrame = sql(s"select * from $tableName")
    val scanRdd = dataFrame.queryExecution.sparkPlan.collect {
      case b: CarbonDataSourceScan if b.rdd.isInstanceOf[CarbonScanRDD[InternalRow]] => b.rdd
        .asInstanceOf[CarbonScanRDD[InternalRow]]
    }.head
    val expected = BroadCastSIFilterPushJoin.getFilteredSegments(scanRdd)
    assert(expected.length == 4)
    dropTables(tableName)
  }

  @Test
  // Test get Filtered Segments using the Data Frame
  def test_getFilteredSegmentsUsingDataFrame() {
    createTable(tableName)
    val expected = BroadCastSIFilterPushJoin
      .getFilteredSegments(s"select * from $tableName", SparkTestQueryExecutor.spark)
    assert(expected.length == 4)
    dropTables(tableName)
  }

  @Test
  // Test get Filtered Segments using the Data Frame with multiple tables
  def test_getFilteredSegmentsUsingDataFrame_multiple() {
    createTable(tableName)
    createTable(tableName + 1)
    val exception = intercept[UnsupportedOperationException] {
      BroadCastSIFilterPushJoin
        .getFilteredSegments("select * from test_table t1 join test_table1 t2 on t1.c1=t2.c1",
          SparkTestQueryExecutor.spark)
    }
    exception.getMessage.contains("Get Filter Segments API supports if and only if only " +
                                  "one carbon main table is present in query.")
  }

  @Test
  // Test get Filtered Segments using the Data Frame with non-carbon tables
  def test_getFilteredSegmentsUsingDataFrame_non_carbon_tables() {
    sql(s"drop table if exists $tableName")
    sql(s"CREATE TABLE $tableName(c1 string, c2 int, c3 string)")
    sql(s"INSERT INTO $tableName SELECT 'c1v1', 1, 'c3v1'")
    val exception = intercept[UnsupportedOperationException] {
      BroadCastSIFilterPushJoin
        .getFilteredSegments(s"select * from $tableName",
          SparkTestQueryExecutor.spark)
    }
    exception.getMessage.contains("Get Filter Segments API supports if and only if " +
                                  "only one carbon main table is present in query.")
  }

  @Test
  // Test identify segments to be merged with Major Compaction
  def test_identifySegmentsToBeMerged_Major() {
    createTable(tableName)
    val expected = SecondaryIndexUtil
      .identifySegmentsToBeMerged(SparkTestQueryExecutor.spark,
        tableName,
        databaseName)
    assert(expected.size() == 4)
    dropTables(tableName)
  }

  @Test
  // Test identify segments to be merged with Major Compaction
  def test_identifySegmentsToBeMerged_Major_With_one_segment() {
    createTable(tableName)
    sql(s"delete from table $tableName where SEGMENT.ID in (3)")
    sql(s"delete from table $tableName where SEGMENT.ID in (2)")
    sql(s"delete from table $tableName where SEGMENT.ID in (1)")
    sql(s"show segments for table $tableName").show(false)
    val expected = SecondaryIndexUtil
      .identifySegmentsToBeMerged(SparkTestQueryExecutor.spark,
        tableName,
        databaseName)
    assert(expected.size() == 0)
    dropTables(tableName)
  }

  @Test
  // Test identify segments to be merged with Custom Compaction type
  def test_identifySegmentsToBeMergedCustom() {
    createTable(tableName)
    val carbonTable = CarbonEnv
      .getCarbonTable(Option(databaseName), tableName)(SparkTestQueryExecutor.spark)
    val customSegments = new util.ArrayList[String]()
    customSegments.add("1")
    customSegments.add("2")
    val expected = SecondaryIndexUtil
      .identifySegmentsToBeMergedCustom(SparkTestQueryExecutor.spark,
        tableName,
        databaseName,
        customSegments
      )
    assert(expected.size() == 2)
    dropTables(tableName)
  }

  @Test
  // Verify merged load name
  def test_getMergedLoadName() {
    createTable(tableName)
    val carbonTable = CarbonEnv
      .getCarbonTable(Option(databaseName), tableName)(SparkTestQueryExecutor.spark)
    val loadMetadataDetails = SegmentStatusManager.readLoadMetadata(carbonTable.getMetadataPath)
    val expected = SecondaryIndexUtil
      .getMergedLoadName(loadMetadataDetails.toList.asJava)
    assert(expected.equalsIgnoreCase("Segment_0.1"))
    dropTables(tableName)
  }

  @Test
  // Verify merged load name with one segment
  def test_getMergedLoadName_with_one_segment() {
    sql(s"drop table if exists $tableName")
    sql(s"CREATE TABLE $tableName(c1 string, c2 string, c3 string) STORED AS carbondata")
    sql(s"INSERT INTO $tableName SELECT 'c1v1', '1', 'c3v1'")
    val carbonTable = CarbonEnv
      .getCarbonTable(Option(databaseName), tableName)(SparkTestQueryExecutor.spark)
    val loadMetadataDetails = SegmentStatusManager.readLoadMetadata(carbonTable.getMetadataPath)
    val exception = intercept[UnsupportedOperationException] {
      SecondaryIndexUtil
        .getMergedLoadName(loadMetadataDetails.toList.asJava)
    }
    exception.getMessage
      .contains("Compaction requires atleast 2 segments to be merged." +
                "But the input list size is 1")
    dropTables(tableName)
  }

  @Test
  // Verify merged load name with unsorted segment lsit
  def test_getMergedLoadName_unsorted_segment_list() {
    createTable(tableName)
    val carbonTable = CarbonEnv
      .getCarbonTable(Option(databaseName), tableName)(SparkTestQueryExecutor.spark)
    val loadMetadataDetails = SegmentStatusManager.readLoadMetadata(carbonTable.getMetadataPath)
    val segments: util.List[LoadMetadataDetails] = new util.ArrayList[LoadMetadataDetails]()
    val load1 = new LoadMetadataDetails()
    load1.setLoadName("1")
    load1.setLoadStartTime(System.currentTimeMillis())
    segments.add(load1)
    val load = new LoadMetadataDetails()
    load.setLoadName("0")
    load.setLoadStartTime(System.currentTimeMillis())
    segments.add(load)
    val expected = SecondaryIndexUtil
      .getMergedLoadName(segments)
    println(expected)
    assert(expected.equalsIgnoreCase("Segment_0.1"))
    dropTables(tableName)
  }

  @Test
  // Test get Filtered Segments using the query with set segments
  def test_getFilteredSegments_set_segments() {
    createTable(tableName)
    val expected = BroadCastSIFilterPushJoin
      .getFilteredSegments(s"select * from $tableName", SparkTestQueryExecutor.spark)
    assert(expected.length == 4)
    sql(s"set carbon.input.segments.$databaseName.$tableName=0")
    val dataFrame_with_set_seg = sql(s"select count(*) from $tableName where c1='c1v1'")
    assert(dataFrame_with_set_seg.collect().length == 1)
    sql(s"set carbon.input.segments.$databaseName.$tableName")
    dropTables(tableName)
  }

  @Test
  // Test get Filtered Segments using the carbonScanRDD with SI
  def test_getFilteredSegments_with_secondary_index() {
    sql(s"drop table if exists $tableName")
    sql(s"CREATE TABLE $tableName(c1 string, c2 string, c3 string) STORED AS carbondata")
    sql(s"INSERT INTO $tableName SELECT 'c1v1', '1', 'c3v1'")
    sql(s"INSERT INTO $tableName SELECT 'c1v2', '2', 'c3v2'")
    sql(s"INSERT INTO $tableName SELECT 'c1v1', '1', 'c3v1'")
    sql(s"INSERT INTO $tableName SELECT 'c1v2', '2', 'c3v2'")
    sql(s"create index si_index_table on table $tableName(c3) AS 'carbondata' ")
    sql(s"create index si_index_table1 on table $tableName(c2) AS 'carbondata' ")
    assert(BroadCastSIFilterPushJoin
             .getFilteredSegments(s"select * from $tableName where c3='c3v1'",
               SparkTestQueryExecutor.spark).length == 2)
    assert(BroadCastSIFilterPushJoin
             .getFilteredSegments(s"select * from $tableName where c3='c3v1' or c2 ='2'",
               SparkTestQueryExecutor.spark).length == 4)
    val exception = intercept[UnsupportedOperationException] {
      BroadCastSIFilterPushJoin
        .getFilteredSegments(s"select * from si_index_table",
          SparkTestQueryExecutor.spark)
    }
    exception.getMessage.contains("Get Filter Segments API supports if and only if " +
                                  "only one carbon main table is present in query.")
    sql(s"drop index if exists si_index_table on $tableName")
    sql(s"drop index if exists si_index_table1 on $tableName")
    dropTables(tableName)
  }

  @Test
  // Test get Filtered Segments with more than 100 columns
  def test_getFilteredSegments_with_more_than_100_columns(): Unit = {
    dropTables(tableName)
    val csvPath = TestQueryExecutor.resourcesPath.replaceAll("\\\\", "/")
    sql(
      s"create table $tableName (RECORD_ID string,CDR_ID string,LOCATION_CODE int,SYSTEM_ID " +
      s"string," +
      "CLUE_ID string,HIT_ELEMENT string,CARRIER_CODE string,CAP_TIME date,DEVICE_ID string," +
      "DATA_CHARACTER string,NETCELL_ID string,NETCELL_TYPE int,EQU_CODE string,CLIENT_MAC " +
      "string,SERVER_MAC string,TUNNEL_TYPE string,TUNNEL_IP_CLIENT string,TUNNEL_IP_SERVER " +
      "string,TUNNEL_ID_CLIENT string,TUNNEL_ID_SERVER string,SIDE_ONE_TUNNEL_ID string," +
      "SIDE_TWO_TUNNEL_ID string,CLIENT_IP string,SERVER_IP string,TRANS_PROTOCOL string," +
      "CLIENT_PORT int,SERVER_PORT int,APP_PROTOCOL string,CLIENT_AREA bigint,SERVER_AREA bigint," +
      "LANGUAGE string,STYPE string,SUMMARY string,FILE_TYPE string,FILENAME string,FILESIZE " +
      "string,BILL_TYPE string,ORIG_USER_NUM string,USER_NUM string,USER_IMSI string,USER_IMEI " +
      "string,USER_BELONG_AREA_CODE string,USER_BELONG_COUNTRY_CODE string,USER_LONGITUDE double," +
      "USER_LATITUDE double,USER_MSC string,USER_BASE_STATION string,USER_CURR_AREA_CODE string," +
      "USER_CURR_COUNTRY_CODE string,USER_SIGNAL_POINT string,USER_IP string,ORIG_OPPO_NUM " +
      "string,OPPO_NUM string,OPPO_IMSI string,OPPO_IMEI string,OPPO_BELONG_AREA_CODE string," +
      "OPPO_BELONG_COUNTRY_CODE string,OPPO_LONGITUDE double,OPPO_LATITUDE double,OPPO_MSC " +
      "string,OPPO_BASE_STATION string,OPPO_CURR_AREA_CODE string,OPPO_CURR_COUNTRY_CODE string," +
      "OPPO_SIGNAL_POINT string,OPPO_IP string,RING_TIME timestamp,CALL_ESTAB_TIME timestamp," +
      "END_TIME timestamp,CALL_DURATION bigint,CALL_STATUS_CODE int,DTMF string,ORIG_OTHER_NUM " +
      "string,OTHER_NUM string,ROAM_NUM string,SEND_TIME timestamp,ORIG_SMS_CONTENT string," +
      "ORIG_SMS_CODE int,SMS_CONTENT string,SMS_NUM int,SMS_COUNT int,REMARK string," +
      "CONTENT_STATUS int,VOC_LENGTH bigint,FAX_PAGE_COUNT int,COM_OVER_CAUSE int,ROAM_TYPE int," +
      "SGSN_ADDR string,GGSN_ADDR string,PDP_ADDR string,APN_NI string,APN_OI string,CARD_ID " +
      "string,TIME_OUT int,LOGIN_TIME timestamp,USER_IMPU string,OPPO_IMPU string,USER_LAST_IMPI " +
      "string,USER_CURR_IMPI string,SUPSERVICE_TYPE bigint,SUPSERVICE_TYPE_SUBCODE bigint," +
      "SMS_CENTERNUM string,USER_LAST_LONGITUDE double,USER_LAST_LATITUDE double,USER_LAST_MSC " +
      "string,USER_LAST_BASE_STATION string,LOAD_ID bigint,P_CAP_TIME string) STORED AS carbondata")
    sql(
      s"load data inpath '$csvPath/secindex/datafile_100.csv' into table $tableName options( " +
      "'delimiter'= ',','fileheader'='RECORD_ID,CDR_ID,LOCATION_CODE,SYSTEM_ID,CLUE_ID," +
      "HIT_ELEMENT,CARRIER_CODE,DEVICE_ID,CAP_TIME,DATA_CHARACTER,NETCELL_ID,NETCELL_TYPE," +
      "EQU_CODE,CLIENT_MAC,SERVER_MAC,TUNNEL_TYPE,TUNNEL_IP_CLIENT,TUNNEL_IP_SERVER," +
      "TUNNEL_ID_CLIENT,TUNNEL_ID_SERVER,SIDE_ONE_TUNNEL_ID,SIDE_TWO_TUNNEL_ID,CLIENT_IP," +
      "SERVER_IP,TRANS_PROTOCOL,CLIENT_PORT,SERVER_PORT,APP_PROTOCOL,CLIENT_AREA,SERVER_AREA," +
      "LANGUAGE,STYPE,SUMMARY,FILE_TYPE,FILENAME,FILESIZE,BILL_TYPE,ORIG_USER_NUM,USER_NUM," +
      "USER_IMSI,USER_IMEI,USER_BELONG_AREA_CODE,USER_BELONG_COUNTRY_CODE,USER_LONGITUDE," +
      "USER_LATITUDE,USER_MSC,USER_BASE_STATION,USER_CURR_AREA_CODE,USER_CURR_COUNTRY_CODE," +
      "USER_SIGNAL_POINT,USER_IP,ORIG_OPPO_NUM,OPPO_NUM,OPPO_IMSI,OPPO_IMEI," +
      "OPPO_BELONG_AREA_CODE,OPPO_BELONG_COUNTRY_CODE,OPPO_LONGITUDE,OPPO_LATITUDE,OPPO_MSC," +
      "OPPO_BASE_STATION,OPPO_CURR_AREA_CODE,OPPO_CURR_COUNTRY_CODE,OPPO_SIGNAL_POINT,OPPO_IP," +
      "RING_TIME,CALL_ESTAB_TIME,END_TIME,CALL_DURATION,CALL_STATUS_CODE,DTMF,ORIG_OTHER_NUM," +
      "OTHER_NUM,ROAM_NUM,SEND_TIME,ORIG_SMS_CONTENT,ORIG_SMS_CODE,SMS_CONTENT,SMS_NUM,SMS_COUNT," +
      "REMARK,CONTENT_STATUS,VOC_LENGTH,FAX_PAGE_COUNT,COM_OVER_CAUSE,ROAM_TYPE,SGSN_ADDR," +
      "GGSN_ADDR,PDP_ADDR,APN_NI,APN_OI,CARD_ID,TIME_OUT,LOGIN_TIME,USER_IMPU,OPPO_IMPU," +
      "USER_LAST_IMPI,USER_CURR_IMPI,SUPSERVICE_TYPE,SUPSERVICE_TYPE_SUBCODE,SMS_CENTERNUM," +
      "USER_LAST_LONGITUDE,USER_LAST_LATITUDE,USER_LAST_MSC,USER_LAST_BASE_STATION,LOAD_ID," +
      "P_CAP_TIME','bad_records_action'='force')")
    assert(BroadCastSIFilterPushJoin
             .getFilteredSegments(s"select * from $tableName",
               SparkTestQueryExecutor.spark).length == 1)
    dropTables(tableName)
  }

  def createTable(tableName: String) {
    sql(s"drop table if exists $tableName")
    sql(s"CREATE TABLE $tableName(c1 string, c2 int, c3 string) STORED AS carbondata")
    sql(s"INSERT INTO $tableName SELECT 'c1v1', 1, 'c3v1'")
    sql(s"INSERT INTO $tableName SELECT 'c1v2', 2, 'c3v2'")
    sql(s"INSERT INTO $tableName SELECT 'c1v1', 1, 'c3v1'")
    sql(s"INSERT INTO $tableName SELECT 'c1v2', 2, 'c3v2'")
  }

  def dropTables(tableName: String) {
    sql(s"drop table if exists $tableName")
  }

}
