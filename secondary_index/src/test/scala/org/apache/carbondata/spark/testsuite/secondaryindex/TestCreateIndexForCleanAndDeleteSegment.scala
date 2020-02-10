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

/**
 * test cases for testing clean and delete segment functionality for index tables
 */
class TestCreateIndexForCleanAndDeleteSegment extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table if exists carbon")
    sql("drop table if exists delete_segment_by_id")
    sql("drop table if exists clean_files_test")
  }

  test("test secondary index for delete segment by id") {
    sql("drop index if exists index_no_dictionary on delete_segment_by_id")
    sql("drop table if exists delete_segment_by_id")

    sql("CREATE table delete_segment_by_id (empno int, empname String, " +
        "designation String, doj Timestamp, workgroupcategory int, " +
        "workgroupcategoryname String, deptno int, deptname String, projectcode int, " +
        "projectjoindate Timestamp, projectenddate Timestamp, attendance int, " +
        "utilization int,salary int) STORED AS carbondata")

    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO " +
    "TABLE delete_segment_by_id OPTIONS('DELIMITER'=',', 'BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='FORCE')")


    sql("create index index_no_dictionary on table delete_segment_by_id (workgroupcategoryname, empname) AS 'carbondata'")

    sql("delete from table delete_segment_by_id where segment.id IN(0)")

    checkAnswer(sql("select count(*) from delete_segment_by_id"),
      sql("select count(*) from index_no_dictionary"))

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
//        "TABLE clean_files_test OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\"', 'BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='FORCE')")
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
//    val carbonTablePath: CarbonTablePath = CarbonStorePath.getCarbonTablePath(indexTable.getStorePath, indexTable.getCarbonTableIdentifier)
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
