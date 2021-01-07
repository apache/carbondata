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

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
 * test cases for testing clean and delete segment functionality for index tables
 */
class TestCreateIndexForCleanAndDeleteSegment extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table if exists carbon")
    sql("drop table if exists delete_segment_by_id")
    sql("drop table if exists clean_files_test")
  }

  test("test secondary index for delete segment by id and date") {
    sql("drop index if exists index_no_dictionary on delete_segment_by_id")
    sql("drop table if exists delete_segment_by_id")

    sql("CREATE table delete_segment_by_id (empno int, empname String, " +
        "designation String, doj Timestamp, workgroupcategory int, " +
        "workgroupcategoryname String, deptno int, deptname String, projectcode int, " +
        "projectjoindate Timestamp, projectenddate Timestamp, attendance int, " +
        "utilization int,salary int) STORED AS carbondata")

    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE delete_segment_by_id " +
        "OPTIONS('DELIMITER'=',','BAD_RECORDS_LOGGER_ENABLE'='FALSE','BAD_RECORDS_ACTION'='FORCE')")


    sql("create index index_no_dictionary on table delete_segment_by_id (" +
        "workgroupcategoryname, empname) AS 'carbondata'")

    sql("delete from table delete_segment_by_id where segment.id IN(0)")

    checkAnswer(sql("select count(*) from delete_segment_by_id"),
      sql("select count(*) from index_no_dictionary"))

    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE delete_segment_by_id " +
      "OPTIONS('DELIMITER'=',','BAD_RECORDS_LOGGER_ENABLE'='FALSE','BAD_RECORDS_ACTION'='FORCE')")
    val preDeleteSegmentsByDate = sql("SHOW SEGMENTS FOR TABLE delete_segment_by_id").count()
    // delete segment by date
    sql("delete from table delete_segment_by_id where " +
      "SEGMENT.STARTTIME BEFORE '2025-06-01 12:05:06'")
    sql("create materialized view mv1 as select empname, deptname, " +
      "avg(salary) from  delete_segment_by_id group by empname, deptname")
    var dryRun = sql("clean files for table delete_segment_by_id OPTIONS('dryrun'='true')")
      .collect()
    var cleanFiles = sql("clean files for table delete_segment_by_id").collect()
    assert(cleanFiles(0).get(0) == dryRun(0).get(0))
    checkAnswer(sql("select count(*) from delete_segment_by_id"),
      sql("select count(*) from index_no_dictionary"))
    val postDeleteSegmentsByDate = sql("SHOW SEGMENTS FOR TABLE delete_segment_by_id").count()
    assert(preDeleteSegmentsByDate == postDeleteSegmentsByDate)
    val result = sql("show materialized views on table delete_segment_by_id").collectAsList()
    assert(result.get(0).get(2).toString.equalsIgnoreCase("ENABLED"))
    assert(result.get(0).get(3).toString.equalsIgnoreCase("full"))
    assert(result.get(0).get(4).toString.equalsIgnoreCase("on_commit"))
    dryRun = sql("clean files for table delete_segment_by_id" +
      " OPTIONS('dryrun'='true', 'force'='true')").collect()
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED, "true")
    cleanFiles = sql("clean files for table delete_segment_by_id OPTIONS('force'='true')").collect()
    CarbonProperties.getInstance()
      .removeProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED)
    assert(cleanFiles(0).get(0) == dryRun(0).get(0))
    sql("drop materialized view if exists mv1 ")
    sql("drop table if exists delete_segment_by_id")
  }

//  test("test secondary index for clean files") {
//    sql("drop table if exists clean_files_test")
//
//    sql("CREATE table clean_files_test (empno int, empname String, " +
//        "designation String, doj Timestamp, workgroupcategory int, " +
//        "workgroupcategoryname String, deptno int, deptname String, projectcode int, " +
//        "projectjoindate Timestamp, projectenddate Timestamp, attendance int, " +
//        "utilization int,salary int) STORED AS carbondata " +
//        "TBLPROPERTIES('DICTIONARY_EXCLUDE'='empname')")
//
//    sql("LOAD DATA LOCAL INPATH './src/test/resources/data.csv' INTO " +
//        "TABLE clean_files_test OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\"',
//        'BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='FORCE')")
//
//    sql("drop index if exists index_no_dictionary on clean_files_test")
//
//    sql("create index index_no_dictionary on table clean_files_test (empname) AS 'carbondata'")
//
//    sql("delete from table clean_files_test where segment.id IN(0)")
//
//    sql("clean files for table clean_files_test")
//
//    val indexTable = CarbonMetadata.getInstance().getCarbonTable("default_index_no_dictionary")
//    val carbonTablePath: CarbonTablePath = CarbonStorePath.getCarbonTablePath(
//    indexTable.getStorePath, indexTable.getCarbonTableIdentifier)
//    val dataDirectoryPath: String = carbonTablePath.getCarbonDataDirectoryPath("0", "0")
//    if (CarbonUtil.isFileExists(dataDirectoryPath)) {
//      assert(false)
//    }
//    assert(true)
//
//    sql("drop table if exists clean_files_test")
//  }

  override def afterAll: Unit = {
    sql("drop table if exists carbon")
//    sql("drop table if exists delete_segment_by_id")
    sql("drop table if exists clean_files_test")
  }

}
