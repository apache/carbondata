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
import org.apache.spark.sql.test.TestQueryExecutor
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.core.metadata.datatype.DataType
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.partition.PartitionType
import org.apache.carbondata.core.util.CarbonProperties

class TestDDLForPartitionTable  extends QueryTest with BeforeAndAfterAll {

  override def beforeAll = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy-MM-dd HH:mm:ss")
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy-MM-dd")
    dropTable
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
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
        |  'RANGE_INFO'='2017-06-11 00:00:02, 2017-06-13 23:59:59')
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
    assert(partitionInfo.getRangeInfo.size == 2)
    assert(partitionInfo.getRangeInfo.get(0).equals("2017-06-11 00:00:02"))
    assert(partitionInfo.getRangeInfo.get(1).equals("2017-06-13 23:59:59"))
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

  test("test describe formatted for partition column") {
    sql(
      """create table des(a int, b string) partitioned by (c string) stored by 'carbondata'
        |tblproperties ('partition_type'='list','list_info'='1,2')""".stripMargin)
    checkExistence(sql("describe formatted des"), true, "Partition Columns")
    sql("drop table if exists des")
  }

  test("test exception if hash number is invalid") {
    sql("DROP TABLE IF EXISTS test_hash_1")
    val exception_test_hash_1: Exception = intercept[Exception] {
      sql(
        """
          | CREATE TABLE test_hash_1(col1 INT, col2 STRING)
          | PARTITIONED BY (col3 INT) STORED BY 'carbondata'
          | TBLPROPERTIES('PARTITION_TYPE'='HASH', 'NUM_PARTITIONS'='2.1')
        """.stripMargin
      )
    }
    assert(exception_test_hash_1.getMessage.contains("Invalid partition definition"))

    sql("DROP TABLE IF EXISTS test_hash_2")
    val exception_test_hash_2: Exception = intercept[Exception] {
      sql(
        """
          | CREATE TABLE test_hash_2(col1 INT, col2 STRING)
          | PARTITIONED BY (col3 INT) STORED BY 'carbondata'
          | TBLPROPERTIES('PARTITION_TYPE'='HASH', 'NUM_PARTITIONS'='abc')
        """.stripMargin
      )
    }
    assert(exception_test_hash_2.getMessage.contains("Invalid partition definition"))

    sql("DROP TABLE IF EXISTS test_hash_3")
    val exception_test_hash_3: Exception = intercept[Exception] {
      sql(
        """
          | CREATE TABLE test_hash_3(col1 INT, col2 STRING)
          | PARTITIONED BY (col3 INT) STORED BY 'carbondata'
          | TBLPROPERTIES('PARTITION_TYPE'='HASH', 'NUM_PARTITIONS'='-2.1')
        """.stripMargin
      )
    }
    assert(exception_test_hash_3.getMessage.contains("Invalid partition definition"))
  }


  test("test exception when values in list_info can not match partition column type") {
    sql("DROP TABLE IF EXISTS test_list_int")
    val exception_test_list_int: Exception = intercept[Exception] {
      sql(
        """
          | CREATE TABLE test_list_int(col1 INT, col2 STRING)
          | PARTITIONED BY (col3 INT) STORED BY 'carbondata'
          | TBLPROPERTIES('PARTITION_TYPE'='LIST', 'LIST_INFO'='abc,def')
        """.stripMargin)
    }
    assert(exception_test_list_int.getMessage.contains("Invalid partition definition"))

    sql("DROP TABLE IF EXISTS test_list_small")
    val exception_test_list_small: Exception = intercept[Exception] {
      sql(
        """
          | CREATE TABLE test_list_small(col1 INT, col2 STRING)
          | PARTITIONED BY (col3 SMALLINT) STORED BY 'carbondata'
          | TBLPROPERTIES('PARTITION_TYPE'='LIST', 'LIST_INFO'='abc,def')
        """.stripMargin)
    }
    assert(exception_test_list_small.getMessage.contains("Invalid partition definition"))

    sql("DROP TABLE IF EXISTS test_list_float")
    val exception_test_list_float: Exception = intercept[Exception] {
      sql(
        """
          | CREATE TABLE test_list_float(col1 INT, col2 STRING)
          | PARTITIONED BY (col3 FLOAT) STORED BY 'carbondata'
          | TBLPROPERTIES('PARTITION_TYPE'='LIST', 'LIST_INFO'='abc,def')
        """.stripMargin)
    }
    assert(exception_test_list_float.getMessage.contains("Invalid partition definition"))

    sql("DROP TABLE IF EXISTS test_list_double")
    val exception_test_list_double: Exception = intercept[Exception] {
      sql(
        """
          | CREATE TABLE test_list_double(col1 INT, col2 STRING)
          | PARTITIONED BY (col3 DOUBLE) STORED BY 'carbondata'
          | TBLPROPERTIES('PARTITION_TYPE'='LIST', 'LIST_INFO'='abc,def')
        """.stripMargin)
    }
    assert(exception_test_list_double.getMessage.contains("Invalid partition definition"))

    sql("DROP TABLE IF EXISTS test_list_bigint")
    val exception_test_list_bigint: Exception = intercept[Exception] {
      sql(
        """
          | CREATE TABLE test_list_bigint(col1 INT, col2 STRING)
          | PARTITIONED BY (col3 BIGINT) STORED BY 'carbondata'
          | TBLPROPERTIES('PARTITION_TYPE'='LIST', 'LIST_INFO'='abc,def')
        """.stripMargin)
    }
    assert(exception_test_list_bigint.getMessage.contains("Invalid partition definition"))

    sql("DROP TABLE IF EXISTS test_list_date")
    val exception_test_list_date: Exception = intercept[Exception] {
      sql(
        """
          | CREATE TABLE test_list_date(col1 INT, col2 STRING)
          | PARTITIONED BY (col3 DATE) STORED BY 'carbondata'
          | TBLPROPERTIES('PARTITION_TYPE'='LIST', 'LIST_INFO'='abc,def')
        """.stripMargin)
    }
    assert(exception_test_list_date.getMessage.contains("Invalid partition definition"))

    sql("DROP TABLE IF EXISTS test_list_timestamp")
    val exception_test_list_timestamp: Exception = intercept[Exception] {
      sql(
        """
          | CREATE TABLE test_list_timestamp(col1 INT, col2 STRING)
          | PARTITIONED BY (col3 TIMESTAMP) STORED BY 'carbondata'
          | TBLPROPERTIES('PARTITION_TYPE'='LIST', 'LIST_INFO'='abc,def')
        """.stripMargin)
    }
    assert(exception_test_list_timestamp.getMessage.contains("Invalid partition definition"))

    sql("DROP TABLE IF EXISTS test_list_decimal")
    val exception_test_list_decimal: Exception = intercept[Exception] {
      sql(
        """
          | CREATE TABLE test_list_decimal(col1 INT, col2 STRING)
          | PARTITIONED BY (col3 DECIMAL(25, 4)) STORED BY 'carbondata'
          | TBLPROPERTIES('PARTITION_TYPE'='LIST', 'LIST_INFO'='23.23111,2.32')
        """.stripMargin)
    }
    assert(exception_test_list_decimal.getMessage.contains("Invalid partition definition"))
  }

  test("test exception when values in range_info can not match partition column type") {
    sql("DROP TABLE IF EXISTS test_range_int")
    val exception_test_range_int: Exception = intercept[Exception] {
      sql(
        """
          | CREATE TABLE test_range_int(col1 INT, col2 STRING)
          | PARTITIONED BY (col3 INT) STORED BY 'carbondata'
          | TBLPROPERTIES('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='abc,def')
        """.stripMargin)
    }
    assert(exception_test_range_int.getMessage.contains("Invalid partition definition"))

    sql("DROP TABLE IF EXISTS test_range_smallint")
    val exception_test_range_smallint: Exception = intercept[Exception] {
      sql(
        """
          | CREATE TABLE test_range_smallint(col1 INT, col2 STRING)
          | PARTITIONED BY (col3 SMALLINT) STORED BY 'carbondata'
          | TBLPROPERTIES('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='abc,def')
        """.stripMargin)
    }
    assert(exception_test_range_smallint.getMessage.contains("Invalid partition definition"))

    sql("DROP TABLE IF EXISTS test_range_float")
    val exception_test_range_float: Exception = intercept[Exception] {
      sql(
        """
          | CREATE TABLE test_range_float(col1 INT, col2 STRING)
          | PARTITIONED BY (col3 FLOAT) STORED BY 'carbondata'
          | TBLPROPERTIES('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='abc,def')
        """.stripMargin)
    }
    assert(exception_test_range_float.getMessage.contains("Invalid partition definition"))

    sql("DROP TABLE IF EXISTS test_range_double")
    val exception_test_range_double: Exception = intercept[Exception] {
      sql(
        """
          | CREATE TABLE test_range_double(col1 INT, col2 STRING)
          | PARTITIONED BY (col3 DOUBLE) STORED BY 'carbondata'
          | TBLPROPERTIES('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='abc,def')
        """.stripMargin)
    }
    assert(exception_test_range_double.getMessage.contains("Invalid partition definition"))

    sql("DROP TABLE IF EXISTS test_range_bigint")
    val exception_test_range_bigint: Exception = intercept[Exception] {
      sql(
        """
          | CREATE TABLE test_range_bigint(col1 INT, col2 STRING)
          | PARTITIONED BY (col3 BIGINT) STORED BY 'carbondata'
          | TBLPROPERTIES('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='abc,def')
        """.stripMargin)
    }
    assert(exception_test_range_bigint.getMessage.contains("Invalid partition definition"))

    sql("DROP TABLE IF EXISTS test_range_date")
    val exception_test_range_date: Exception = intercept[Exception] {
      sql(
        """
          | CREATE TABLE test_range_date(col1 INT, col2 STRING)
          | PARTITIONED BY (col3 DATE) STORED BY 'carbondata'
          | TBLPROPERTIES('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='abc,def')
        """.stripMargin)
    }
    assert(exception_test_range_date.getMessage.contains("Invalid partition definition"))

    sql("DROP TABLE IF EXISTS test_range_timestamp")
    val exception_test_range_timestamp: Exception = intercept[Exception] {
      sql(
        """
          | CREATE TABLE test_range_timestamp(col1 INT, col2 STRING)
          | PARTITIONED BY (col3 TIMESTAMP) STORED BY 'carbondata'
          | TBLPROPERTIES('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='abc,def')
        """.stripMargin)
    }
    assert(exception_test_range_timestamp.getMessage.contains("Invalid partition definition"))

    sql("DROP TABLE IF EXISTS test_range_decimal")
    val exception_test_range_decimal: Exception = intercept[Exception] {
      sql(
        """
          | CREATE TABLE test_range_decimal(col1 INT, col2 STRING)
          | PARTITIONED BY (col3 DECIMAL(25, 4)) STORED BY 'carbondata'
          | TBLPROPERTIES('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='abc,def')
        """.stripMargin)
    }
    assert(exception_test_range_decimal.getMessage.contains("Invalid partition definition"))
  }

  test("test range info for overlapping values") {
    // test for int type
    intercept[Exception] {
      sql(
        "create table range_int(a string, b int) PARTITIONED BY (c int) stored by 'carbondata' " +
        "tblproperties ('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='1,3,2,7')")
    }
    // test for String type
    intercept[Exception] {
      sql(
        "create table range_string(a string, b int) PARTITIONED BY (c string) stored by " +
        "'carbondata' tblproperties ('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='abc,xyz,def,pqr')")
    }
    // test for long type
    intercept[Exception] {
      sql(
        "create table range_long(a string, b int) PARTITIONED BY (c long) stored by " +
        "'carbondata' tblproperties ('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='21474836471," +
        "2147483649,214748764712,11474836471')")
    }
    // test for float type
    intercept[Exception] {
      sql(
        "create table range_float(a string, b int) PARTITIONED BY (c float) stored by " +
        "'carbondata' tblproperties ('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='214.23f,219.12f,211" +
        ".2f,236.23f')")
    }
    // test for double type
    intercept[Exception] {
      sql(
        "create table range_double(a string, b int) PARTITIONED BY (c double) stored by " +
        "'carbondata' tblproperties ('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='1200.24,1800.24,1600" +
        ".24,2200.24')")
    }
    // test for byte type
    intercept[Exception] {
      sql(
        "create table range_byte(a string, b int) PARTITIONED BY (c byte) stored by " +
        "'carbondata' tblproperties ('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='84,90,87,112')")
    }
    // test for short type
    intercept[Exception] {
      sql(
        "create table range_short(a string, b int) PARTITIONED BY (c short) stored by " +
        "'carbondata' tblproperties ('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='1014,1019,1017,2110')")
    }
    // test for boolean type
    intercept[Exception] {
      sql(
        "create table range_boolean(a string, b int) PARTITIONED BY (c boolean) stored by " +
        "'carbondata' tblproperties ('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='true,false')")
    }
    // test for decimal type
    intercept[Exception] {
      sql(
        "create table range_decimal(a string, b int) PARTITIONED BY (c decimal(7,2)) stored by " +
        "'carbondata' tblproperties ('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='1014.27,1019.27,1017" +
        ".27,2110.27')")
    }
    // test for date type
    intercept[Exception] {
      sql(
        "create table range_date(a string, b int) PARTITIONED BY (c date) stored by " +
        "'carbondata' tblproperties ('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='2000-01-01," +
        "2004-01-01,2002-01-01,2008-01-01')")
    }
    // test for timestamp type
    intercept[Exception] {
      sql(
        "create table range_timestamp(a string, b int) PARTITIONED BY (c timestamp) stored by " +
        "'carbondata' tblproperties ('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='01-01-2000 00:00:00," +
        "01-01-2004 00:00:00,01-01-2002 00:00:00,01-01-2008 00:00:00')")
    }
  }

  override def afterAll = {
    dropTable
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, TestQueryExecutor.timestampFormat)
  }

  def dropTable = {
    sql("drop table if exists hashTable")
    sql("drop table if exists rangeTable")
    sql("drop table if exists listTable")
    sql("drop table if exists test")
    sql("DROP TABLE IF EXISTS test_hash_1")
    sql("DROP TABLE IF EXISTS test_hash_2")
    sql("DROP TABLE IF EXISTS test_hash_3")
    sql("DROP TABLE IF EXISTS test_list_int")
    sql("DROP TABLE IF EXISTS test_list_smallint")
    sql("DROP TABLE IF EXISTS test_list_bigint")
    sql("DROP TABLE IF EXISTS test_list_float")
    sql("DROP TABLE IF EXISTS test_list_double")
    sql("DROP TABLE IF EXISTS test_list_date")
    sql("DROP TABLE IF EXISTS test_list_timestamp")
    sql("DROP TABLE IF EXISTS test_list_decimal")
    sql("DROP TABLE IF EXISTS test_range_int")
    sql("DROP TABLE IF EXISTS test_range_smallint")
    sql("DROP TABLE IF EXISTS test_range_bigint")
    sql("DROP TABLE IF EXISTS test_range_float")
    sql("DROP TABLE IF EXISTS test_range_double")
    sql("DROP TABLE IF EXISTS test_range_date")
    sql("DROP TABLE IF EXISTS test_range_timestamp")
    sql("DROP TABLE IF EXISTS test_range_decimal")
    sql("drop table if exists range_int")
    sql("drop table if exists range_string")
    sql("drop table if exists range_long")
    sql("drop table if exists range_float")
    sql("drop table if exists range_double")
    sql("drop table if exists range_byte")
    sql("drop table if exists range_short")
    sql("drop table if exists range_boolean")
    sql("drop table if exists range_decimal")
    sql("drop table if exists range_date")
    sql("drop table if exists range_timestamp")
  }

}
