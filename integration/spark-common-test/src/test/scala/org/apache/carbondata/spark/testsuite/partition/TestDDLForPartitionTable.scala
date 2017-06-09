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

package org.apache.carbondata.spark.testsuite.partition

import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.core.metadata.datatype.DataType
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.partition.PartitionType

class TestDDLForPartitionTable  extends QueryTest with BeforeAndAfterAll {

  override def beforeAll = {
    dropTable
  }

  test("create partition table: hash partition") {
    sql(
      """
        | CREATE TABLE default.hashTable (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (empno int)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('PARTITION_TYPE'='HASH','NUM_PARTITIONS'='3')
      """.stripMargin)

    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default_hashTable")
    val partitionInfo = carbonTable.getPartitionInfo(carbonTable.getFactTableName)
    assert(partitionInfo != null)
    assert(partitionInfo.getColumnSchemaList.get(0).getColumnName.equalsIgnoreCase("empno"))
    assert(partitionInfo.getColumnSchemaList.get(0).getDataType == DataType.INT)
    assert(partitionInfo.getColumnSchemaList.get(0).getEncodingList.size == 0)
    assert(partitionInfo.getPartitionType ==  PartitionType.HASH)
    assert(partitionInfo.getNumPartitions == 3)
  }

  test("create partition table: range partition") {
    sql(
      """
        | CREATE TABLE default.rangeTable (empno int, empname String, designation String,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (doj Timestamp)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('PARTITION_TYPE'='RANGE',
        |  'RANGE_INFO'='01-01-2010, 01-01-2015, 01-04-2015, 01-07-2015')
      """.stripMargin)

    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default_rangeTable")
    val partitionInfo = carbonTable.getPartitionInfo(carbonTable.getFactTableName)
    assert(partitionInfo != null)
    assert(partitionInfo.getColumnSchemaList.get(0).getColumnName.equalsIgnoreCase("doj"))
    assert(partitionInfo.getColumnSchemaList.get(0).getDataType == DataType.TIMESTAMP)
    assert(partitionInfo.getColumnSchemaList.get(0).getEncodingList.size == 3)
    assert(partitionInfo.getColumnSchemaList.get(0).getEncodingList.get(0) == Encoding.DICTIONARY)
    assert(partitionInfo.getColumnSchemaList.get(0).getEncodingList.get(1) == Encoding.DIRECT_DICTIONARY)
    assert(partitionInfo.getColumnSchemaList.get(0).getEncodingList.get(2) == Encoding.INVERTED_INDEX)
    assert(partitionInfo.getPartitionType == PartitionType.RANGE)
    assert(partitionInfo.getRangeInfo.size == 4)
    assert(partitionInfo.getRangeInfo.get(0).equals("01-01-2010"))
    assert(partitionInfo.getRangeInfo.get(1).equals("01-01-2015"))
    assert(partitionInfo.getRangeInfo.get(2).equals("01-04-2015"))
    assert(partitionInfo.getRangeInfo.get(3).equals("01-07-2015"))
  }

  test("create partition table: list partition") {
    sql(
      """
        | CREATE TABLE default.listTable (empno int, empname String, designation String, doj Timestamp,
        |  workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (workgroupcategory string)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('PARTITION_TYPE'='LIST',
        |  'LIST_INFO'='0, 1, (2, 3)')
      """.stripMargin)
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default_listTable")
    val partitionInfo = carbonTable.getPartitionInfo(carbonTable.getFactTableName)
    assert(partitionInfo != null)
    assert(partitionInfo.getColumnSchemaList.get(0).getColumnName.equalsIgnoreCase("workgroupcategory"))
    assert(partitionInfo.getColumnSchemaList.get(0).getDataType == DataType.STRING)
    assert(partitionInfo.getColumnSchemaList.get(0).getEncodingList.size == 2)
    assert(partitionInfo.getColumnSchemaList.get(0).getEncodingList.get(0) == Encoding.DICTIONARY)
    assert(partitionInfo.getColumnSchemaList.get(0).getEncodingList.get(1) == Encoding.INVERTED_INDEX)
    assert(partitionInfo.getPartitionType == PartitionType.LIST)
    assert(partitionInfo.getListInfo.size == 3)
    assert(partitionInfo.getListInfo.get(0).size == 1)
    assert(partitionInfo.getListInfo.get(0).get(0).equals("0"))
    assert(partitionInfo.getListInfo.get(1).size == 1)
    assert(partitionInfo.getListInfo.get(1).get(0).equals("1"))
    assert(partitionInfo.getListInfo.get(2).size == 2)
    assert(partitionInfo.getListInfo.get(2).get(0).equals("2"))
    assert(partitionInfo.getListInfo.get(2).get(1).equals("3"))
  }

  test("test exception if partition column is dropped") {
    sql("drop table if exists test")
    sql(
      "create table test(a int, b string) partitioned by (c int) stored by 'carbondata' " +
      "tblproperties('PARTITION_TYPE'='LIST','list_info'='0,10,5,20')")
    intercept[Exception] { sql("alter table test drop columns(c)") }
  }

  test("test exception when values in list_info can not match partition column type") {
    sql("DROP TABLE IF EXISTS test_list_1")
    try {
      val errorMessage_list_1 = intercept[Exception] {
        sql(
          """
            |CREATE TABLE test_list_1(col1 INT, col2 STRING) PARTITIONED BY (col3 INT)
            |STORED BY 'carbondata' TBLPROPERTIES('PARTITION_TYPE'='LIST', 'LIST_INFO'='abc,def')
          """.stripMargin)
      }
      assert(errorMessage_list_1.getMessage.contains("Invalid partition definition"))
    }

    sql("DROP TABLE IF EXISTS test_list_2")
    try {
      val errorMessage_list_2 = intercept[Exception] {
        sql(
          """
            |CREATE TABLE test_list_2(col1 INT, col2 STRING) PARTITIONED BY (col3 SHORT)
            |STORED BY 'carbondata' TBLPROPERTIES('PARTITION_TYPE'='LIST', 'LIST_INFO'='abc,def')
          """.stripMargin)
      }
      assert(errorMessage_list_2.getMessage.contains("Invalid partition definition"))
    }

    sql("DROP TABLE IF EXISTS test_list_3")
    try {
      val errorMessage_list_3 = intercept[Exception] {
        sql(
          """
            |CREATE TABLE test_list_3(col1 INT, col2 STRING) PARTITIONED BY (col3 FLOAT)
            |STORED BY 'carbondata' TBLPROPERTIES('PARTITION_TYPE'='LIST', 'LIST_INFO'='abc,def')
          """.stripMargin)
      }
      assert(errorMessage_list_3.getMessage.contains("Invalid partition definition"))
    }

    sql("DROP TABLE IF EXISTS test_list_4")
    try {
      val errorMessage_list_4 = intercept[Exception] {
        sql(
          """
            |CREATE TABLE test_list_4(col1 INT, col2 STRING) PARTITIONED BY (col3 DOUBLE)
            |STORED BY 'carbondata' TBLPROPERTIES('PARTITION_TYPE'='LIST', 'LIST_INFO'='abc,def')
          """.stripMargin)
      }
      assert(errorMessage_list_4.getMessage.contains("Invalid partition definition"))
    }

    sql("DROP TABLE IF EXISTS test_list_5")
    try {
      val errorMessage_list_5 = intercept[Exception] {
        sql(
          """
            |CREATE TABLE test_list_5(col1 INT, col2 STRING) PARTITIONED BY (col3 LONG)
            |STORED BY 'carbondata' TBLPROPERTIES('PARTITION_TYPE'='LIST', 'LIST_INFO'='abc,def')
          """.stripMargin)
      }
      assert(errorMessage_list_5.getMessage.contains("Invalid partition definition"))
    }

    //    sql("DROP TABLE IF EXISTS test_list_6")
    //    try {
    //      val errorMessage_list_6 = intercept[Exception] {
    //        sql(
    //          """
    //            |CREATE TABLE test_list_6(col1 INT, col2 STRING) PARTITIONED BY (col3 DECIMAL)
    //            |STORED BY 'carbondata' TBLPROPERTIES('PARTITION_TYPE'='LIST', 'LIST_INFO'='abc,def')
    //          """.stripMargin)
    //      }
    //      assert(errorMessage_list_6.getMessage.contains("Invalid partition definition"))
    //    }

    sql("DROP TABLE IF EXISTS test_list_7")
    try {
      val errorMessage_list_7 = intercept[Exception] {
        sql(
          """
            |CREATE TABLE test_list_7(col1 INT, col2 STRING) PARTITIONED BY (col3 BYTE)
            |STORED BY 'carbondata' TBLPROPERTIES('PARTITION_TYPE'='LIST', 'LIST_INFO'='abc,def')
          """.stripMargin)
      }
      assert(errorMessage_list_7.getMessage.contains("Invalid partition definition"))
    }

    sql("DROP TABLE IF EXISTS test_list_8")
    try {
      val errorMessage_list_8 = intercept[Exception] {
        sql(
          """
            |CREATE TABLE test_list_8(col1 INT, col2 STRING) PARTITIONED BY (col3 BOOLEAN)
            |STORED BY 'carbondata' TBLPROPERTIES('PARTITION_TYPE'='LIST', 'LIST_INFO'='abc,def')
          """.stripMargin)
      }
      assert(errorMessage_list_8.getMessage.contains("Invalid partition definition"))
    }
  }

  test("test exception when values in range_info can not match partition column type") {
    sql("DROP TABLE IF EXISTS test_range_1")
    try {
      val errorMessage_range_1 = intercept[Exception] {
        sql(
          """
            |CREATE TABLE test_range_1(col1 INT, col2 STRING) PARTITIONED BY (col3 INT)
            |STORED BY 'carbondata' TBLPROPERTIES('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='abc,def')
          """.stripMargin)
      }
      assert(errorMessage_range_1.getMessage.contains("Invalid partition definition"))
    }

    sql("DROP TABLE IF EXISTS test_range_2")
    try {
      val errorMessage_range_2 = intercept[Exception] {
        sql(
          """
            |CREATE TABLE test_range_2(col1 INT, col2 STRING) PARTITIONED BY (col3 SHORT)
            |STORED BY 'carbondata' TBLPROPERTIES('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='abc,def')
          """.stripMargin)
      }
      assert(errorMessage_range_2.getMessage.contains("Invalid partition definition"))
    }

    sql("DROP TABLE IF EXISTS test_range_3")
    try {
      val errorMessage_range_3 = intercept[Exception] {
        sql(
          """
            |CREATE TABLE test_range_3(col1 INT, col2 STRING) PARTITIONED BY (col3 FLOAT)
            |STORED BY 'carbondata' TBLPROPERTIES('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='abc,def')
          """.stripMargin)
      }
      assert(errorMessage_range_3.getMessage.contains("Invalid partition definition"))
    }

    sql("DROP TABLE IF EXISTS test_range_4")
    try {
      val errorMessage_range_4 = intercept[Exception] {
        sql(
          """
            |CREATE TABLE test_range_4(col1 INT, col2 STRING) PARTITIONED BY (col3 DOUBLE)
            |STORED BY 'carbondata' TBLPROPERTIES('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='abc,def')
          """.stripMargin)
      }
      assert(errorMessage_range_4.getMessage.contains("Invalid partition definition"))
    }

    sql("DROP TABLE IF EXISTS test_range_5")
    try {
      val errorMessage_range_5 = intercept[Exception] {
        sql(
          """
            |CREATE TABLE test_range_5(col1 INT, col2 STRING) PARTITIONED BY (col3 LONG)
            |STORED BY 'carbondata' TBLPROPERTIES('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='abc,def')
          """.stripMargin)
      }
      assert(errorMessage_range_5.getMessage.contains("Invalid partition definition"))
    }

//    sql("DROP TABLE IF EXISTS test_range_6")
//    try {
//      val errorMessage_range_6 = intercept[Exception] {
//        sql(
//          """
//            |CREATE TABLE test_range_6(col1 INT, col2 STRING) PARTITIONED BY (col3 DECIMAL)
//            |STORED BY 'carbondata' TBLPROPERTIES('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='abc,def')
//          """.stripMargin)
//      }
//      assert(errorMessage_range_6.getMessage.contains("Invalid partition definition"))
//    }

    sql("DROP TABLE IF EXISTS test_range_7")
    try {
      val errorMessage_range_7 = intercept[Exception] {
        sql(
          """
            |CREATE TABLE test_range_7(col1 INT, col2 STRING) PARTITIONED BY (col3 BYTE)
            |STORED BY 'carbondata' TBLPROPERTIES('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='abc,def')
          """.stripMargin)
      }
      assert(errorMessage_range_7.getMessage.contains("Invalid partition definition"))
    }

    sql("DROP TABLE IF EXISTS test_range_8")
    try {
      val errorMessage_range_8 = intercept[Exception] {
        sql(
          """
            |CREATE TABLE test_range_8(col1 INT, col2 STRING) PARTITIONED BY (col3 BOOLEAN)
            |STORED BY 'carbondata' TBLPROPERTIES('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='abc,def')
          """.stripMargin)
      }
      assert(errorMessage_range_8.getMessage.contains("Invalid partition definition"))
    }

  }

  override def afterAll = {
    dropTable
  }

  def dropTable = {
    sql("drop table if exists hashTable")
    sql("drop table if exists rangeTable")
    sql("drop table if exists listTable")
  }

}
