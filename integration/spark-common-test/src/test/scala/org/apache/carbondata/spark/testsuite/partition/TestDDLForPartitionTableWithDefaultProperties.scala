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

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.core.metadata.datatype.{DataType, DataTypes}
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.partition.PartitionType
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class TestDDLForPartitionTableWithDefaultProperties  extends QueryTest with BeforeAndAfterAll {

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
    val partitionInfo = carbonTable.getPartitionInfo(carbonTable.getTableName)
    assert(partitionInfo != null)
    assert(partitionInfo.getColumnSchemaList.get(0).getColumnName.equalsIgnoreCase("empno"))
    assert(partitionInfo.getColumnSchemaList.get(0).getDataType == DataTypes.INT)
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
        |  'RANGE_INFO'='2017-06-11 00:00:02, 2017-06-13 23:59:59','DICTIONARY_INCLUDE'='doj')
      """.stripMargin)

    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default_rangeTable")
    val partitionInfo = carbonTable.getPartitionInfo(carbonTable.getTableName)
    assert(partitionInfo != null)
    assert(partitionInfo.getColumnSchemaList.get(0).getColumnName.equalsIgnoreCase("doj"))
    assert(partitionInfo.getColumnSchemaList.get(0).getDataType == DataTypes.TIMESTAMP)
    assert(partitionInfo.getColumnSchemaList.get(0).getEncodingList.size == 3)
    assert(partitionInfo.getColumnSchemaList.get(0).getEncodingList.get(0) == Encoding.DICTIONARY)
    assert(partitionInfo.getColumnSchemaList.get(0).getEncodingList.get(1) == Encoding.DIRECT_DICTIONARY)
    assert(partitionInfo.getColumnSchemaList.get(0).getEncodingList.get(2) == Encoding.INVERTED_INDEX)
    assert(partitionInfo.getPartitionType == PartitionType.RANGE)
    assert(partitionInfo.getRangeInfo.size == 2)
    assert(partitionInfo.getRangeInfo.get(0).equals("2017-06-11 00:00:02"))
    assert(partitionInfo.getRangeInfo.get(1).equals("2017-06-13 23:59:59"))
  }

  test("create partition table: list partition with timestamp datatype") {
    sql(
      """
        | CREATE TABLE default.listTable (empno int, empname String, designation String, doj Timestamp,
        |  workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (projectenddate Timestamp)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('PARTITION_TYPE'='LIST',
        |  'LIST_INFO'='2017-06-11 00:00:02, 2017-06-13 23:59:59',
        |  'DICTIONARY_INCLUDE'='projectenddate')
      """.stripMargin)
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default_listTable")
    val partitionInfo = carbonTable.getPartitionInfo(carbonTable.getTableName)
    assert(partitionInfo != null)
    assert(partitionInfo.getColumnSchemaList.get(0).getColumnName.equalsIgnoreCase("projectenddate"))
    assert(partitionInfo.getColumnSchemaList.get(0).getDataType == DataTypes.TIMESTAMP)
    assert(partitionInfo.getColumnSchemaList.get(0).getEncodingList.size == 3)
    assert(partitionInfo.getColumnSchemaList.get(0).getEncodingList.get(0) == Encoding.DICTIONARY)
    assert(partitionInfo.getColumnSchemaList.get(0).getEncodingList.get(1) == Encoding.DIRECT_DICTIONARY)
    assert(partitionInfo.getColumnSchemaList.get(0).getEncodingList.get(2) == Encoding.INVERTED_INDEX)
    assert(partitionInfo.getPartitionType == PartitionType.LIST)
    assert(partitionInfo.getListInfo.size == 2)
    assert(partitionInfo.getListInfo.get(0).size == 1)
    assert(partitionInfo.getListInfo.get(0).get(0).equals("2017-06-11 00:00:02"))
    assert(partitionInfo.getListInfo.get(1).size == 1)
    assert(partitionInfo.getListInfo.get(1).get(0).equals("2017-06-13 23:59:59"))
  }

  test("create partition table: list partition with date datatype") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy-MM-dd")

    sql(
      """
        | CREATE TABLE default.listTableDate (empno int, empname String, designation String, doj Timestamp,
        |  workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (projectenddate date)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('PARTITION_TYPE'='LIST',
        |  'LIST_INFO'='2017-06-11 , 2017-06-13')
      """.stripMargin)
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default_listTableDate")
    val partitionInfo = carbonTable.getPartitionInfo(carbonTable.getTableName)
    assert(partitionInfo != null)
    assert(partitionInfo.getColumnSchemaList.get(0).getColumnName.equalsIgnoreCase("projectenddate"))
    assert(partitionInfo.getColumnSchemaList.get(0).getDataType == DataTypes.DATE)
    assert(partitionInfo.getColumnSchemaList.get(0).getEncodingList.size == 3)
    assert(partitionInfo.getColumnSchemaList.get(0).getEncodingList.get(0) == Encoding.DICTIONARY)
    assert(partitionInfo.getColumnSchemaList.get(0).getEncodingList.get(1) == Encoding.DIRECT_DICTIONARY)
    assert(partitionInfo.getColumnSchemaList.get(0).getEncodingList.get(2) == Encoding.INVERTED_INDEX)
    assert(partitionInfo.getPartitionType == PartitionType.LIST)
    assert(partitionInfo.getListInfo.size == 2)
    assert(partitionInfo.getListInfo.get(0).size == 1)
    assert(partitionInfo.getListInfo.get(0).get(0).equals("2017-06-11"))
    assert(partitionInfo.getListInfo.get(1).size == 1)
    assert(partitionInfo.getListInfo.get(1).get(0).equals("2017-06-13"))
  }

  test("test exception when values in list_info can not match partition column type") {
    sql("DROP TABLE IF EXISTS test_list_int")
    val exception_test_list_int: Exception = intercept[Exception] {
      sql(
        """
          | CREATE TABLE test_list_int(col1 INT, col2 STRING)
          | PARTITIONED BY (col3 INT) STORED BY 'carbondata'
          | TBLPROPERTIES('PARTITION_TYPE'='LIST', 'LIST_INFO'='1,2,(abc,efg)')
        """.stripMargin)
    }
    assert(exception_test_list_int.getMessage.contains("Invalid Partition Values"))
  }

  test("test exception when partition values in rangeTable are in group ") {
    sql("DROP TABLE IF EXISTS rangeTable")
    val exception_test_list_int: Exception = intercept[Exception] {
      sql(
        """
          |CREATE TABLE default.rangeTable (empno int, empname String, designation String,
          |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
          |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
          |  utilization int,salary int)
          | PARTITIONED BY (doj Timestamp)
          | STORED BY 'org.apache.carbondata.format'
          | TBLPROPERTIES('PARTITION_TYPE'='RANGE',
          |  'RANGE_INFO'='2017-06-11 00:00:02, (2017-06-13 23:59:59, 2017-09-13 23:45:59)')
        """.stripMargin)
    }
    assert(exception_test_list_int.getMessage.contains("Invalid Partition Values"))
  }

  test("test exception when values in rangeTable does not match partition column type") {
    sql("DROP TABLE IF EXISTS rangeTable")
    val exception_test_list_int: Exception = intercept[Exception] {
      sql(
        """
          |CREATE TABLE default.rangeTable (empno int, empname String, designation String,
          |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
          |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
          |  utilization int,salary int)
          | PARTITIONED BY (doj Timestamp)
          | STORED BY 'org.apache.carbondata.format'
          | TBLPROPERTIES('PARTITION_TYPE'='RANGE',
          |  'RANGE_INFO'='2017-06-11 00:00:02, abc, 2017-09-13 23:45:59')
        """.stripMargin)
    }
    assert(exception_test_list_int.getMessage.contains("Invalid Partition Values"))
  }


  override def afterAll = {
    dropTable
  }

  def dropTable = {
    sql("drop table if exists hashTable")
    sql("drop table if exists rangeTable")
    sql("drop table if exists listTable")
    sql("drop table if exists listTableDate")
  }

}
