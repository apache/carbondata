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

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.{CarbonFile, CarbonFileFilter}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath

class TestAlterPartitionTable extends QueryTest with BeforeAndAfterAll {


  override def beforeAll {
    dropTable
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy-MM-dd")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    /**
     * list_table_area_origin
     * list_table_area
     */
    sql("""
          | CREATE TABLE IF NOT EXISTS list_table_area_origin
          | (
          | id Int,
          | vin string,
          | logdate Timestamp,
          | phonenumber Long,
          | country string,
          | salary Int
          | )
          | PARTITIONED BY (area string)
          | STORED BY 'carbondata'
          | TBLPROPERTIES('PARTITION_TYPE'='LIST',
          | 'LIST_INFO'='Asia, America, Europe')
        """.stripMargin)
    sql("""
          | CREATE TABLE IF NOT EXISTS list_table_area
          | (
          | id Int,
          | vin string,
          | logdate Timestamp,
          | phonenumber Long,
          | country string,
          | salary Int
          | )
          | PARTITIONED BY (area string)
          | STORED BY 'carbondata'
          | TBLPROPERTIES('PARTITION_TYPE'='LIST',
          | 'LIST_INFO'='Asia, America, Europe')
        """.stripMargin)

    /**
     * range_table_logdate_origin
     * range_table_logdate
     */
    sql(
      """
        | CREATE TABLE IF NOT EXISTS range_table_logdate_origin
        | (
        | id Int,
        | vin string,
        | phonenumber Long,
        | country string,
        | area string,
        | salary Int
        | )
        | PARTITIONED BY (logdate Timestamp)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('PARTITION_TYPE'='RANGE',
        | 'RANGE_INFO'='2014/01/01, 2015/01/01, 2016/01/01')
      """.stripMargin)
    sql(
      """
        | CREATE TABLE IF NOT EXISTS range_table_logdate
        | (
        | id Int,
        | vin string,
        | phonenumber Long,
        | country string,
        | area string,
        | salary Int
        | )
        | PARTITIONED BY (logdate Timestamp)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('PARTITION_TYPE'='RANGE',
        | 'RANGE_INFO'='2014/01/01, 2015/01/01, 2016/01/01', 'DICTIONARY_INCLUDE'='logdate')
      """.stripMargin)

    /**
     * list_table_country_origin
     * list_table_country
     */
    sql(
      """
        | CREATE TABLE IF NOT EXISTS list_table_country_origin
        | (
        | id Int,
        | vin string,
        | logdate Timestamp,
        | phonenumber Long,
        | area string,
        | salary Int
        | )
        | PARTITIONED BY (country string)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('PARTITION_TYPE'='LIST',
        | 'LIST_INFO'='(China, US),UK ,Japan,(Canada,Russia, Good, NotGood), Korea ')
      """.stripMargin)
    sql(
      """
        | CREATE TABLE IF NOT EXISTS list_table_country
        | (
        | id Int,
        | vin string,
        | logdate Timestamp,
        | phonenumber Long,
        | area string,
        | salary Int
        | )
        | PARTITIONED BY (country string)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('PARTITION_TYPE'='LIST',
        | 'LIST_INFO'='(China, US),UK ,Japan,(Canada,Russia, Good, NotGood), Korea ')
      """.stripMargin)

    /**
     * range_table_logdate_split_origin
     * range_table_logdate_split
     */
    sql(
      """
        | CREATE TABLE IF NOT EXISTS range_table_logdate_split_origin
        | (
        | id Int,
        | vin string,
        | phonenumber Long,
        | country string,
        | area string,
        | salary Int
        | )
        | PARTITIONED BY (logdate Timestamp)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('PARTITION_TYPE'='RANGE',
        | 'RANGE_INFO'='2014/01/01, 2015/01/01, 2016/01/01, 2018/01/01')
      """.stripMargin)
    sql(
      """
        | CREATE TABLE IF NOT EXISTS range_table_logdate_split
        | (
        | id Int,
        | vin string,
        | phonenumber Long,
        | country string,
        | area string,
        | salary Int
        | )
        | PARTITIONED BY (logdate Timestamp)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('PARTITION_TYPE'='RANGE',
        | 'RANGE_INFO'='2014/01/01, 2015/01/01, 2016/01/01, 2018/01/01',
        | 'DICTIONARY_INCLUDE'='logdate')
      """.stripMargin)

    /**
     * range_table_bucket_origin
     * range_table_bucket
     */
    sql(
      """
        | CREATE TABLE IF NOT EXISTS range_table_bucket_origin
        | (
        | id Int,
        | vin string,
        | phonenumber Long,
        | country string,
        | area string,
        | salary Int
        | )
        | PARTITIONED BY (logdate Timestamp)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('PARTITION_TYPE'='RANGE',
        | 'RANGE_INFO'='2014/01/01, 2015/01/01, 2016/01/01, 2018/01/01',
        | 'BUCKETNUMBER'='3',
        | 'BUCKETCOLUMNS'='country')
      """.stripMargin)
    sql(
      """
        | CREATE TABLE IF NOT EXISTS range_table_bucket
        | (
        | id Int,
        | vin string,
        | phonenumber Long,
        | country string,
        | area string,
        | salary Int
        | )
        | PARTITIONED BY (logdate Timestamp)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('PARTITION_TYPE'='RANGE',
        | 'RANGE_INFO'='2014/01/01, 2015/01/01, 2016/01/01, 2018/01/01',
        | 'DICTIONARY_INCLUDE'='logdate',
        | 'BUCKETNUMBER'='3',
        | 'BUCKETCOLUMNS'='country')
      """.stripMargin)

    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/partition_data.csv' INTO TABLE list_table_area_origin OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/partition_data.csv' INTO TABLE range_table_logdate_origin OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/partition_data.csv' INTO TABLE list_table_country_origin OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/partition_data.csv' INTO TABLE range_table_logdate_split_origin OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/partition_data.csv' INTO TABLE range_table_bucket_origin OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/partition_data.csv' INTO TABLE list_table_area OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/partition_data.csv' INTO TABLE range_table_logdate OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/partition_data.csv' INTO TABLE list_table_country OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/partition_data.csv' INTO TABLE range_table_logdate_split OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/partition_data.csv' INTO TABLE range_table_bucket OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

  }

  test("Alter table add partition: List Partition") {
    sql("""ALTER TABLE list_table_area ADD PARTITION ('OutSpace', 'Hi')""".stripMargin)
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default", "list_table_area")
    val partitionInfo = carbonTable.getPartitionInfo(carbonTable.getTableName)
    val partitionIds = partitionInfo.getPartitionIds
    val list_info = partitionInfo.getListInfo
    assert(partitionIds == List(0, 1, 2, 3, 4, 5).map(Integer.valueOf(_)).asJava)
    assert(partitionInfo.getMaxPartitionId == 5)
    assert(partitionInfo.getNumPartitions == 6)
    assert(list_info.get(0).get(0) == "Asia")
    assert(list_info.get(1).get(0) == "America")
    assert(list_info.get(2).get(0) == "Europe")
    assert(list_info.get(3).get(0) == "OutSpace")
    assert(list_info.get(4).get(0) == "Hi")
    validateDataFiles("default_list_table_area", "0", Seq(1, 2, 4))
    val result_after = sql("select id, vin, logdate, phonenumber, country, area, salary from list_table_area")
    val result_origin = sql("select id, vin, logdate, phonenumber, country, area, salary from list_table_area_origin")
    checkAnswer(result_after, result_origin)

    val result_after1 = sql(s"select id, vin, logdate, phonenumber, country, area, salary from list_table_area where area < 'OutSpace' ")
    val result_origin1 = sql(s"select id, vin, logdate, phonenumber, country, area, salary from list_table_area_origin where area < 'OutSpace' ")
    checkAnswer(result_after1, result_origin1)

    val result_after2 = sql("select id, vin, logdate, phonenumber, country, area, salary from list_table_area where area <= 'OutSpace' ")
    val result_origin2 = sql("select id, vin, logdate, phonenumber, country, area, salary from list_table_area_origin where area <= 'OutSpace' ")
    checkAnswer(result_after2, result_origin2)

    val result_after3 = sql("select id, vin, logdate, phonenumber, country, area, salary from list_table_area where area = 'OutSpace' ")
    val result_origin3 = sql("select id, vin, logdate, phonenumber, country, area, salary from list_table_area_origin where area = 'OutSpace' ")
    checkAnswer(result_after3, result_origin3)

    val result_after4 = sql("select id, vin, logdate, phonenumber, country, area, salary from list_table_area where area > 'OutSpace' ")
    val result_origin4 = sql("select id, vin, logdate, phonenumber, country, area, salary from list_table_area_origin where area > 'OutSpace' ")
    checkAnswer(result_after4, result_origin4)

    val result_after5 = sql("select id, vin, logdate, phonenumber, country, area, salary from list_table_area where area >= 'OutSpace' ")
    val result_origin5 = sql("select id, vin, logdate, phonenumber, country, area, salary from list_table_area_origin where area >= 'OutSpace' ")
    checkAnswer(result_after5, result_origin5)

    intercept[Exception]  { sql("""ALTER TABLE DROP PARTITION(0)""")}
    intercept[Exception]  { sql("""ALTER TABLE DROP PARTITION(0) WITH DATA""")}
    
    sql("""ALTER TABLE list_table_area DROP PARTITION(2) WITH DATA""")
    val carbonTable2 = CarbonMetadata.getInstance().getCarbonTable("default", "list_table_area")
    val partitionInfo2 = carbonTable2.getPartitionInfo(carbonTable.getTableName)
    val partitionIds2 = partitionInfo2.getPartitionIds
    val list_info2 = partitionInfo2.getListInfo
    assert(partitionIds2 == List(0, 1, 3, 4, 5).map(Integer.valueOf(_)).asJava)
    assert(partitionInfo2.getMaxPartitionId == 5)
    assert(partitionInfo2.getNumPartitions == 5)
    assert(list_info2.get(0).get(0) == "Asia")
    assert(list_info2.get(1).get(0) == "Europe")
    assert(list_info2.get(2).get(0) == "OutSpace")
    assert(list_info2.get(3).get(0) == "Hi")
    validateDataFiles("default_list_table_area", "0", Seq(1, 4))
    checkAnswer(sql("select id, vin, logdate, phonenumber, country, area, salary from list_table_area"),
      sql("select id, vin, logdate, phonenumber, country, area, salary from list_table_area_origin where area <> 'America' "))
  }

  test("Alter table add partition: Range Partition") {
    sql("""ALTER TABLE range_table_logdate ADD PARTITION ('2017/01/01', '2018/01/01')""")
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default", "range_table_logdate")
    val partitionInfo = carbonTable.getPartitionInfo(carbonTable.getTableName)
    val partitionIds = partitionInfo.getPartitionIds
    val range_info = partitionInfo.getRangeInfo
    assert(partitionIds == List(0, 1, 2, 3, 4, 5).map(Integer.valueOf(_)).asJava)
    assert(partitionInfo.getMaxPartitionId == 5)
    assert(partitionInfo.getNumPartitions == 6)
    assert(range_info.get(0) == "2014/01/01")
    assert(range_info.get(1) == "2015/01/01")
    assert(range_info.get(2) == "2016/01/01")
    assert(range_info.get(3) == "2017/01/01")
    assert(range_info.get(4) == "2018/01/01")
    validateDataFiles("default_range_table_logdate", "0", Seq(1, 2, 3, 4, 5))
    val result_after = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_logdate""")
    val result_origin = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_logdate_origin""")
    checkAnswer(result_after, result_origin)

    val result_after1 = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_logdate where logdate < cast('2017/01/12 00:00:00' as timestamp) """)
    val result_origin1 = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_logdate_origin where logdate < cast('2017/01/12 00:00:00' as timestamp) """)
    checkAnswer(result_after1, result_origin1)

    val result_after2 = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_logdate where logdate <= cast('2017/01/12 00:00:00' as timestamp) """)
    val result_origin2 = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_logdate_origin where logdate <= cast('2017/01/12 00:00:00' as timestamp) """)
    checkAnswer(result_after2, result_origin2)

    val result_after3 = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_logdate where logdate = cast('2017/01/12 00:00:00' as timestamp) """)
    val result_origin3 = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_logdate_origin where logdate = cast('2017/01/12 00:00:00' as timestamp) """)
    checkAnswer(result_after3, result_origin3)

    val result_after4 = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_logdate where logdate >= cast('2017/01/12 00:00:00' as timestamp) """)
    val result_origin4 = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_logdate_origin where logdate >= cast('2017/01/12 00:00:00' as timestamp) """)
    checkAnswer(result_after4, result_origin4)

    val result_after5 = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_logdate where logdate > cast('2017/01/12 00:00:00' as timestamp) """)
    val result_origin5 = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_logdate_origin where logdate > cast('2017/01/12 00:00:00' as timestamp) """)
    checkAnswer(result_after5, result_origin5)

    sql("""ALTER TABLE range_table_logdate DROP PARTITION(3) WITH DATA;""")
    val carbonTable1 = CarbonMetadata.getInstance().getCarbonTable(
      "default",
      "range_table_logdate"
    )
    val partitionInfo1 = carbonTable1.getPartitionInfo(carbonTable.getTableName)
    val partitionIds1 = partitionInfo1.getPartitionIds
    val range_info1 = partitionInfo1.getRangeInfo
    assert(partitionIds1 == List(0, 1, 2, 4, 5).map(Integer.valueOf(_)).asJava)
    assert(partitionInfo1.getMaxPartitionId == 5)
    assert(partitionInfo1.getNumPartitions == 5)
    assert(range_info1.get(0) == "2014/01/01")
    assert(range_info1.get(1) == "2015/01/01")
    assert(range_info1.get(2) == "2017/01/01")
    assert(range_info1.get(3) == "2018/01/01")
    assert(range_info1.size() == 4)
    validateDataFiles("default_range_table_logdate", "0", Seq(1, 2, 4, 5))
    val result_after6 = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_logdate""")
    val result_origin6 = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_logdate_origin where logdate < '2015/01/01 00:00:00' or logdate >= '2016/01/01 00:00:00' """)
    checkAnswer(result_after6, result_origin6)
  }

  test("test exception if invalid partition id is provided in alter command") {
    sql("drop table if exists test_invalid_partition_id")

    sql("CREATE TABLE test_invalid_partition_id (CUST_NAME String,ACTIVE_EMUI_VERSION string,DOB Timestamp,DOJ timestamp, " +
      "BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10)," +
      "Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) PARTITIONED BY (CUST_ID int)" +
      " STORED BY 'org.apache.carbondata.format' " +
      "TBLPROPERTIES ('PARTITION_TYPE'='RANGE','RANGE_INFO'='9090,9500,9800',\"TABLE_BLOCKSIZE\"= \"256 MB\")")
    intercept[IllegalArgumentException] { sql("ALTER TABLE test_invalid_partition_id SPLIT PARTITION(6) INTO ('9800','9900')") }
  }

  test("Alter table split partition: List Partition") {
    sql("""ALTER TABLE list_table_country SPLIT PARTITION(4) INTO ('Canada', 'Russia', '(Good, NotGood)')""".stripMargin)
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default", "list_table_country")
    val partitionInfo = carbonTable.getPartitionInfo(carbonTable.getTableName)
    val partitionIds = partitionInfo.getPartitionIds
    val list_info = partitionInfo.getListInfo
    assert(partitionIds == List(0, 1, 2, 3, 6, 7, 8, 5).map(Integer.valueOf(_)).asJava)
    assert(partitionInfo.getMaxPartitionId == 8)
    assert(partitionInfo.getNumPartitions == 8)
    assert(list_info.get(0).get(0) == "China")
    assert(list_info.get(0).get(1) == "US")
    assert(list_info.get(1).get(0) == "UK")
    assert(list_info.get(2).get(0) == "Japan")
    assert(list_info.get(3).get(0) == "Canada")
    assert(list_info.get(4).get(0) == "Russia")
    assert(list_info.get(5).get(0) == "Good")
    assert(list_info.get(5).get(1) == "NotGood")
    assert(list_info.get(6).get(0) == "Korea")
    validateDataFiles("default_list_table_country", "0", Seq(1, 2, 3, 8))
    val result_after = sql("""select id, vin, logdate, phonenumber, country, area, salary from list_table_country""")
    val result_origin = sql("""select id, vin, logdate, phonenumber, country, area, salary from list_table_country_origin""")
    checkAnswer(result_after, result_origin)

    val result_after1 = sql("""select id, vin, logdate, phonenumber, country, area, salary from list_table_country where country < 'NotGood' """)
    val result_origin1 = sql("""select id, vin, logdate, phonenumber, country, area, salary from list_table_country_origin where country < 'NotGood' """)
    checkAnswer(result_after1, result_origin1)

    val result_after2 = sql("""select id, vin, logdate, phonenumber, country, area, salary from list_table_country where country <= 'NotGood' """)
    val result_origin2 = sql("""select id, vin, logdate, phonenumber, country, area, salary from list_table_country_origin where country <= 'NotGood' """)
    checkAnswer(result_after2, result_origin2)

    val result_after3 = sql("""select id, vin, logdate, phonenumber, country, area, salary from list_table_country where country = 'NotGood' """)
    val result_origin3 = sql("""select id, vin, logdate, phonenumber, country, area, salary from list_table_country_origin where country = 'NotGood' """)
    checkAnswer(result_after3, result_origin3)

    val result_after4 = sql("""select id, vin, logdate, phonenumber, country, area, salary from list_table_country where country >= 'NotGood' """)
    val result_origin4 = sql("""select id, vin, logdate, phonenumber, country, area, salary from list_table_country_origin where country >= 'NotGood' """)
    checkAnswer(result_after4, result_origin4)

    val result_after5 = sql("""select id, vin, logdate, phonenumber, country, area, salary from list_table_country where country > 'NotGood' """)
    val result_origin5 = sql("""select id, vin, logdate, phonenumber, country, area, salary from list_table_country_origin where country > 'NotGood' """)
    checkAnswer(result_after5, result_origin5)

    sql("""ALTER TABLE list_table_country DROP PARTITION(8)""")
    val carbonTable1 = CarbonMetadata.getInstance().getCarbonTable("default", "list_table_country")
    val partitionInfo1 = carbonTable1.getPartitionInfo(carbonTable.getTableName)
    val partitionIds1 = partitionInfo1.getPartitionIds
    val list_info1 = partitionInfo1.getListInfo
    assert(partitionIds1 == List(0, 1, 2, 3, 6, 7, 5).map(Integer.valueOf(_)).asJava)
    assert(partitionInfo1.getMaxPartitionId == 8)
    assert(partitionInfo1.getNumPartitions == 7)
    assert(list_info1.get(0).get(0) == "China")
    assert(list_info1.get(0).get(1) == "US")
    assert(list_info1.get(1).get(0) == "UK")
    assert(list_info1.get(2).get(0) == "Japan")
    assert(list_info1.get(3).get(0) == "Canada")
    assert(list_info1.get(4).get(0) == "Russia")
    assert(list_info1.get(5).get(0) == "Korea")
    validateDataFiles("default_list_table_country", "0", Seq(0, 1, 2, 3))
    val result_origin6 = sql("""select id, vin, logdate, phonenumber, country, area, salary from list_table_country""")
    val result_after6 = sql("""select id, vin, logdate, phonenumber, country, area, salary from list_table_country_origin""")
    checkAnswer(result_origin6, result_after6)
  }

  test("Alter table split partition with different List Sequence: List Partition") {
    sql("""ALTER TABLE list_table_country ADD PARTITION ('(Part1, Part2, Part3, Part4)')""".stripMargin)
    sql("""ALTER TABLE list_table_country SPLIT PARTITION(9) INTO ('Part4', 'Part2', '(Part1, Part3)')""".stripMargin)
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default", "list_table_country")
    val partitionInfo = carbonTable.getPartitionInfo(carbonTable.getTableName)
    val partitionIds = partitionInfo.getPartitionIds
    val list_info = partitionInfo.getListInfo
    assert(partitionIds == List(0, 1, 2, 3, 6, 7, 5, 10, 11, 12).map(Integer.valueOf(_)).asJava)
    assert(partitionInfo.getMaxPartitionId == 12)
    assert(partitionInfo.getNumPartitions == 10)
    assert(list_info.get(0).get(0) == "China")
    assert(list_info.get(0).get(1) == "US")
    assert(list_info.get(1).get(0) == "UK")
    assert(list_info.get(2).get(0) == "Japan")
    assert(list_info.get(3).get(0) == "Canada")
    assert(list_info.get(4).get(0) == "Russia")
    assert(list_info.get(5).get(0) == "Korea")
    assert(list_info.get(6).get(0) == "Part4")
    assert(list_info.get(7).get(0) == "Part2")
    assert(list_info.get(8).get(0) == "Part1")
    assert(list_info.get(8).get(1) == "Part3")
    validateDataFiles("default_list_table_country", "0", Seq(0, 1, 2, 3))
    val result_after = sql("""select id, vin, logdate, phonenumber, country, area, salary from list_table_country""")
    val result_origin = sql("""select id, vin, logdate, phonenumber, country, area, salary from list_table_country_origin""")
    checkAnswer(result_after, result_origin)

    val result_after1 = sql("""select id, vin, logdate, phonenumber, country, area, salary from list_table_country where country < 'NotGood' """)
    val result_origin1 = sql("""select id, vin, logdate, phonenumber, country, area, salary from list_table_country_origin where country < 'NotGood' """)
    checkAnswer(result_after1, result_origin1)

    val result_after2 = sql("""select id, vin, logdate, phonenumber, country, area, salary from list_table_country where country <= 'NotGood' """)
    val result_origin2 = sql("""select id, vin, logdate, phonenumber, country, area, salary from list_table_country_origin where country <= 'NotGood' """)
    checkAnswer(result_after2, result_origin2)

    val result_after3 = sql("""select id, vin, logdate, phonenumber, country, area, salary from list_table_country where country = 'NotGood' """)
    val result_origin3 = sql("""select id, vin, logdate, phonenumber, country, area, salary from list_table_country_origin where country = 'NotGood' """)
    checkAnswer(result_after3, result_origin3)

    val result_after4 = sql("""select id, vin, logdate, phonenumber, country, area, salary from list_table_country where country >= 'NotGood' """)
    val result_origin4 = sql("""select id, vin, logdate, phonenumber, country, area, salary from list_table_country_origin where country >= 'NotGood' """)
    checkAnswer(result_after4, result_origin4)

    val result_after5 = sql("""select id, vin, logdate, phonenumber, country, area, salary from list_table_country where country > 'NotGood' """)
    val result_origin5 = sql("""select id, vin, logdate, phonenumber, country, area, salary from list_table_country_origin where country > 'NotGood' """)
    checkAnswer(result_after5, result_origin5)
  }

  test("Alter table split partition with extra space in New SubList: List Partition") {
    sql("""ALTER TABLE list_table_area ADD PARTITION ('(One,Two, Three, Four)')""".stripMargin)
    sql("""ALTER TABLE list_table_area SPLIT PARTITION(6) INTO ('One', '(Two, Three )', 'Four')""".stripMargin)
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default", "list_table_area")
    val partitionInfo = carbonTable.getPartitionInfo(carbonTable.getTableName)
    val partitionIds = partitionInfo.getPartitionIds
    val list_info = partitionInfo.getListInfo
    assert(partitionIds == List(0, 1, 3, 4, 5, 7, 8, 9).map(Integer.valueOf(_)).asJava)
    assert(partitionInfo.getMaxPartitionId == 9)
    assert(partitionInfo.getNumPartitions == 8)
    assert(list_info.get(0).get(0) == "Asia")
    assert(list_info.get(1).get(0) == "Europe")
    assert(list_info.get(2).get(0) == "OutSpace")
    assert(list_info.get(3).get(0) == "Hi")
    assert(list_info.get(4).get(0) == "One")
    assert(list_info.get(5).get(0) == "Two")
    assert(list_info.get(5).get(1) == "Three")
    assert(list_info.get(6).get(0) == "Four")
    validateDataFiles("default_list_table_area", "0", Seq(1, 4))
    val result_after = sql("""select id, vin, logdate, phonenumber, country, area, salary from list_table_area""")
    val result_origin = sql("""select id, vin, logdate, phonenumber, country, area, salary from list_table_area_origin where area <> 'America' """)
    checkAnswer(result_after, result_origin)

    val result_after1 = sql("""select id, vin, logdate, phonenumber, country, area, salary from list_table_area where area < 'Four' """)
    val result_origin1 = sql("""select id, vin, logdate, phonenumber, country, area, salary from list_table_area_origin where area < 'Four' and area <> 'America' """)
    checkAnswer(result_after1, result_origin1)

    val result_after2 = sql("""select id, vin, logdate, phonenumber, country, area, salary from list_table_area where area <= 'Four' """)
    val result_origin2 = sql("""select id, vin, logdate, phonenumber, country, area, salary from list_table_area_origin where area <= 'Four' and area <> 'America'  """)
    checkAnswer(result_after2, result_origin2)

    val result_after3 = sql("""select id, vin, logdate, phonenumber, country, area, salary from list_table_area where area = 'Four' """)
    val result_origin3 = sql("""select id, vin, logdate, phonenumber, country, area, salary from list_table_area_origin where area = 'Four' and area <> 'America'  """)
    checkAnswer(result_after3, result_origin3)

    val result_after4 = sql("""select id, vin, logdate, phonenumber, country, area, salary from list_table_area where area >= 'Four' """)
    val result_origin4 = sql("""select id, vin, logdate, phonenumber, country, area, salary from list_table_area_origin where area >= 'Four' and area <> 'America'  """)
    checkAnswer(result_after4, result_origin4)

    val result_after5 = sql("""select id, vin, logdate, phonenumber, country, area, salary from list_table_area where area > 'Four' """)
    val result_origin5 = sql("""select id, vin, logdate, phonenumber, country, area, salary from list_table_area_origin where area > 'Four' and area <> 'America'  """)
    checkAnswer(result_after5, result_origin5)
  }

  test("Alter table split partition: Range Partition") {
    sql("""ALTER TABLE range_table_logdate_split SPLIT PARTITION(4) INTO ('2017/01/01', '2018/01/01')""")
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable(
      "default",
      "range_table_logdate_split"
    )
    val partitionInfo = carbonTable.getPartitionInfo(carbonTable.getTableName)
    val partitionIds = partitionInfo.getPartitionIds
    val rangeInfo = partitionInfo.getRangeInfo
    assert(partitionIds == List(0, 1, 2, 3, 5, 6).map(Integer.valueOf(_)).asJava)
    assert(partitionInfo.getMaxPartitionId == 6)
    assert(partitionInfo.getNumPartitions == 6)
    assert(rangeInfo.get(0) == "2014/01/01")
    assert(rangeInfo.get(1) == "2015/01/01")
    assert(rangeInfo.get(2) == "2016/01/01")
    assert(rangeInfo.get(3) == "2017/01/01")
    assert(rangeInfo.get(4) == "2018/01/01")
    validateDataFiles("default_range_table_logdate_split", "0", Seq(1, 2, 3, 5, 6))
    val result_after = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_logdate_split""")
    val result_origin = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_logdate_split_origin""")
    checkAnswer(result_after, result_origin)

    val result_after1 = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_logdate_split where logdate < cast('2017/01/12 00:00:00' as timestamp) """)
    val result_origin1 = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_logdate_split_origin where logdate < cast('2017/01/12 00:00:00' as timestamp) """)
    checkAnswer(result_after1, result_origin1)

    val result_after2 = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_logdate_split where logdate <= cast('2017/01/12 00:00:00' as timestamp) """)
    val result_origin2 = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_logdate_split_origin where logdate <= cast('2017/01/12 00:00:00' as timestamp) """)
    checkAnswer(result_after2, result_origin2)

    val result_after3 = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_logdate_split where logdate = cast('2017/01/12 00:00:00' as timestamp) """)
    val result_origin3 = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_logdate_split_origin where logdate = cast('2017/01/12 00:00:00' as timestamp) """)
    checkAnswer(result_after3, result_origin3)

    val result_after4 = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_logdate_split where logdate >= cast('2017/01/12 00:00:00' as timestamp) """)
    val result_origin4 = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_logdate_split_origin where logdate >= cast('2017/01/12 00:00:00' as timestamp) """)
    checkAnswer(result_after4, result_origin4)

    val result_after5 = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_logdate_split where logdate > cast('2017/01/12 00:00:00' as timestamp) """)
    val result_origin5 = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_logdate_split_origin where logdate > cast('2017/01/12 00:00:00' as timestamp) """)
    checkAnswer(result_after5, result_origin5)

    sql("""ALTER TABLE range_table_logdate_split DROP PARTITION(6)""")
    val carbonTable1 = CarbonMetadata.getInstance().getCarbonTable(
      "default",
      "range_table_logdate_split"
    )
    val partitionInfo1 = carbonTable1.getPartitionInfo(carbonTable.getTableName)
    val partitionIds1 = partitionInfo1.getPartitionIds
    val rangeInfo1 = partitionInfo1.getRangeInfo
    assert(partitionIds1 == List(0, 1, 2, 3, 5).map(Integer.valueOf(_)).asJava)
    assert(partitionInfo1.getMaxPartitionId == 6)
    assert(partitionInfo1.getNumPartitions == 5)
    assert(rangeInfo1.get(0) == "2014/01/01")
    assert(rangeInfo1.get(1) == "2015/01/01")
    assert(rangeInfo1.get(2) == "2016/01/01")
    assert(rangeInfo1.get(3) == "2017/01/01")
    assert(rangeInfo1.size() == 4)
    validateDataFiles("default_range_table_logdate_split", "0", Seq(0, 1, 2, 3, 5))
    val result_after6 = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_logdate_split""")
    val result_origin6 = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_logdate_split_origin""")
    checkAnswer(result_after6, result_origin6)
  }

  test("Alter table split partition: Range Partition + Bucket") {
    sql("""ALTER TABLE range_table_bucket SPLIT PARTITION(4) INTO ('2017/01/01', '2018/01/01')""")
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default", "range_table_bucket")
    val partitionInfo = carbonTable.getPartitionInfo(carbonTable.getTableName)
    val partitionIds = partitionInfo.getPartitionIds
    val rangeInfo = partitionInfo.getRangeInfo
    assert(partitionIds == List(0, 1, 2, 3, 5, 6).map(Integer.valueOf(_)).asJava)
    assert(partitionInfo.getMaxPartitionId == 6)
    assert(partitionInfo.getNumPartitions == 6)
    assert(rangeInfo.get(0) == "2014/01/01")
    assert(rangeInfo.get(1) == "2015/01/01")
    assert(rangeInfo.get(2) == "2016/01/01")
    assert(rangeInfo.get(3) == "2017/01/01")
    assert(rangeInfo.get(4) == "2018/01/01")
    validateDataFiles("default_range_table_bucket", "0", Seq(1, 2, 3, 5, 6))
    val result_after = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_bucket""")
    val result_origin = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_bucket_origin""")
    checkAnswer(result_after, result_origin)

    val result_after1 = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_bucket where logdate < cast('2017/01/12 00:00:00' as timestamp) """)
    val result_origin1 = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_bucket_origin where logdate < cast('2017/01/12 00:00:00' as timestamp) """)
    checkAnswer(result_after1, result_origin1)

    val result_after2 = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_bucket where logdate <= cast('2017/01/12 00:00:00' as timestamp) """)
    val result_origin2 = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_bucket_origin where logdate <= cast('2017/01/12 00:00:00' as timestamp) """)
    checkAnswer(result_after2, result_origin2)

    val result_origin3 = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_bucket where logdate = cast('2017/01/12 00:00:00' as timestamp) """)
    val result_after3 = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_bucket_origin where logdate = cast('2017/01/12 00:00:00' as timestamp) """)
    checkAnswer(result_origin3, result_after3)

    val result_after4 = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_bucket where logdate >= cast('2017/01/12 00:00:00' as timestamp) """)
    val result_origin4 = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_bucket_origin where logdate >= cast('2017/01/12 00:00:00' as timestamp) """)
    checkAnswer(result_after4, result_origin4)

    val result_after5 = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_bucket where logdate > cast('2017/01/12 00:00:00' as timestamp) """)
    val result_origin5 = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_bucket_origin where logdate > cast('2017/01/12 00:00:00' as timestamp) """)
    checkAnswer(result_after5, result_origin5)

    sql("""ALTER TABLE range_table_bucket DROP PARTITION(6) WITH DATA""")
    val carbonTable1 = CarbonMetadata.getInstance().getCarbonTable("default", "range_table_bucket")
    val partitionInfo1 = carbonTable1.getPartitionInfo(carbonTable.getTableName)
    val partitionIds1 = partitionInfo1.getPartitionIds
    val rangeInfo1 = partitionInfo1.getRangeInfo
    assert(partitionIds1 == List(0, 1, 2, 3, 5).map(Integer.valueOf(_)).asJava)
    assert(partitionInfo1.getMaxPartitionId == 6)
    assert(partitionInfo1.getNumPartitions == 5)
    assert(rangeInfo1.get(0) == "2014/01/01")
    assert(rangeInfo1.get(1) == "2015/01/01")
    assert(rangeInfo1.get(2) == "2016/01/01")
    assert(rangeInfo1.get(3) == "2017/01/01")
    assert(rangeInfo1.size() == 4)
    validateDataFiles("default_range_table_bucket", "0", Seq(1, 2, 3, 5))
    val result_after6 = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_bucket""")
    val result_origin6= sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_bucket_origin where logdate < '2017/01/01 00:00:00' or logdate >= '2018/01/01 00:00:00'""")
    checkAnswer(result_after6, result_origin6)

    sql("""ALTER TABLE range_table_bucket DROP PARTITION(3)""")
    val carbonTable2 = CarbonMetadata.getInstance().getCarbonTable("default", "range_table_bucket")
    val partitionInfo2 = carbonTable2.getPartitionInfo(carbonTable.getTableName)
    val partitionIds2 = partitionInfo2.getPartitionIds
    val rangeInfo2 = partitionInfo2.getRangeInfo
    assert(partitionIds2 == List(0, 1, 2, 5).map(Integer.valueOf(_)).asJava)
    assert(partitionInfo2.getMaxPartitionId == 6)
    assert(partitionInfo2.getNumPartitions == 4)
    assert(rangeInfo2.get(0) == "2014/01/01")
    assert(rangeInfo2.get(1) == "2015/01/01")
    assert(rangeInfo2.get(2) == "2017/01/01")
    assert(rangeInfo2.size() == 3)
    validateDataFiles("default_range_table_bucket", "0", Seq(1, 2, 5))
    val result_origin7 = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_bucket""")
    val result_after7 = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_bucket_origin where logdate < '2017/01/01 00:00:00' or logdate >= '2018/01/01 00:00:00'""")
    checkAnswer(result_origin7, result_after7)

    sql("""ALTER TABLE range_table_bucket DROP PARTITION(5)""")
    val carbonTable3 = CarbonMetadata.getInstance().getCarbonTable("default", "range_table_bucket")
    val partitionInfo3 = carbonTable3.getPartitionInfo(carbonTable.getTableName)
    val partitionIds3 = partitionInfo3.getPartitionIds
    val rangeInfo3 = partitionInfo3.getRangeInfo
    assert(partitionIds3 == List(0, 1, 2).map(Integer.valueOf(_)).asJava)
    assert(partitionInfo3.getMaxPartitionId == 6)
    assert(partitionInfo3.getNumPartitions == 3)
    assert(rangeInfo3.get(0) == "2014/01/01")
    assert(rangeInfo3.get(1) == "2015/01/01")
    assert(rangeInfo3.size() == 2)
    validateDataFiles("default_range_table_bucket", "0", Seq(0, 1, 2))
    val result_after8 = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_bucket""")
    val result_origin8 = sql("""select id, vin, logdate, phonenumber, country, area, salary from range_table_bucket_origin where logdate < '2017/01/01 00:00:00' or logdate >= '2018/01/01 00:00:00'""")
    checkAnswer(result_after8, result_origin8)
  }

   test("test exception when alter partition and the values"
       + "in range_info can not match partition column type") {
     val exception_test_range_int: Exception = intercept[Exception] {
      sql(
        """
          | CREATE TABLE test_range_int(col1 INT, col2 STRING)
          | PARTITIONED BY (col3 INT) STORED BY 'carbondata'
          | TBLPROPERTIES('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='11,12')
        """.stripMargin)
       sql("ALTER TABLE test_range_int ADD PARTITION ('abc')")
    }
     assert(exception_test_range_int.getMessage
         .contains("Data in range info must be the same type with the partition field's type"))

    sql("DROP TABLE IF EXISTS test_range_smallint")
    val exception_test_range_smallint: Exception = intercept[Exception] {
      sql(
        """
          | CREATE TABLE test_range_smallint(col1 INT, col2 STRING)
          | PARTITIONED BY (col3 SMALLINT) STORED BY 'carbondata'
          | TBLPROPERTIES('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='11,12')
        """.stripMargin)
      sql("ALTER TABLE test_range_smallint ADD PARTITION ('abc')")
    }
     assert(exception_test_range_smallint.getMessage
         .contains("Data in range info must be the same type with the partition field's type"))

    sql("DROP TABLE IF EXISTS test_range_float")
    val exception_test_range_float: Exception = intercept[Exception] {
      sql(
        """
          | CREATE TABLE test_range_float(col1 INT, col2 STRING)
          | PARTITIONED BY (col3 FLOAT) STORED BY 'carbondata'
          | TBLPROPERTIES('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='1.1,2.1')
        """.stripMargin)
      sql("ALTER TABLE test_range_float ADD PARTITION ('abc')")
    }
     assert(exception_test_range_float.getMessage
         .contains("Data in range info must be the same type with the partition field's type"))

    sql("DROP TABLE IF EXISTS test_range_double")
    val exception_test_range_double: Exception = intercept[Exception] {
      sql(
        """
          | CREATE TABLE test_range_double(col1 INT, col2 STRING)
          | PARTITIONED BY (col3 DOUBLE) STORED BY 'carbondata'
          | TBLPROPERTIES('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='1000.005,2000.005')
        """.stripMargin)
      sql("ALTER TABLE test_range_double ADD PARTITION ('abc')")
    }
     assert(exception_test_range_double.getMessage
         .contains("Data in range info must be the same type with the partition field's type"))

    sql("DROP TABLE IF EXISTS test_range_bigint")
    val exception_test_range_bigint: Exception = intercept[Exception] {
      sql(
        """
          | CREATE TABLE test_range_bigint(col1 INT, col2 STRING)
          | PARTITIONED BY (col3 BIGINT) STORED BY 'carbondata'
          | TBLPROPERTIES('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='123456789,223456789')
        """.stripMargin)
       sql("ALTER TABLE test_range_bigint ADD PARTITION ('abc')")
    }
     assert(exception_test_range_bigint.getMessage
         .contains("Data in range info must be the same type with the partition field's type"))

    sql("DROP TABLE IF EXISTS test_range_date")
    val exception_test_range_date: Exception = intercept[Exception] {
      sql(
        """
          | CREATE TABLE test_range_date(col1 INT, col2 STRING)
          | PARTITIONED BY (col3 DATE) STORED BY 'carbondata'
          | TBLPROPERTIES('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='2017-06-11, 2017-06-13')
        """.stripMargin)
      sql("ALTER TABLE test_range_date ADD PARTITION ('abc')")
    }
    assert(exception_test_range_date.getMessage
      .contains("Data in range info must be the same type with the partition field's type"))

    sql("DROP TABLE IF EXISTS test_range_timestamp")
    val exception_test_range_timestamp: Exception = intercept[Exception] {
      sql(
        """
          | CREATE TABLE test_range_timestamp(col1 INT, col2 STRING)
          | PARTITIONED BY (col3 TIMESTAMP) STORED BY 'carbondata'
          | TBLPROPERTIES('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='2017/06/11, 2017/06/13')
        """.stripMargin)
      sql("ALTER TABLE test_range_timestamp ADD PARTITION ('abc')")
    }
    assert(exception_test_range_timestamp.getMessage
      .contains("Data in range info must be the same type with the partition field's type"))
    sql("DROP TABLE IF EXISTS test_range_decimal")
    val exception_test_range_decimal: Exception = intercept[Exception] {
      sql(
        """
          | CREATE TABLE test_range_decimal(col1 INT, col2 STRING)
          | PARTITIONED BY (col3 DECIMAL(25, 4)) STORED BY 'carbondata'
          | TBLPROPERTIES('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='22.22,33.33')
        """.stripMargin)
      sql("ALTER TABLE test_range_decimal ADD PARTITION ('abc')")
    }
    assert(exception_test_range_decimal.getMessage
         .contains("Data in range info must be the same type with the partition field's type"))
  }

  test("Add partition to table in or not in default database") {
    sql("DROP TABLE IF EXISTS carbon_table_default_db")
    sql(
      """
        | CREATE TABLE carbon_table_default_db(id INT, name STRING) PARTITIONED BY (dt STRING)
        | STORED BY 'carbondata' TBLPROPERTIES('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='2015,2016')
      """.stripMargin)
    sql("ALTER TABLE carbon_table_default_db ADD PARTITION ('2017')")

    val carbonTable = CarbonMetadata.getInstance().getCarbonTable(
      "default",
      "carbon_table_default_db"
    )
    val partitionInfo = carbonTable.getPartitionInfo(carbonTable.getTableName)
    val partitionIds = partitionInfo.getPartitionIds
    val range_info = partitionInfo.getRangeInfo
    assert(partitionIds == List(0, 1, 2, 3).map(Integer.valueOf(_)).asJava)
    assert(partitionInfo.getMaxPartitionId == 3)
    assert(partitionInfo.getNumPartitions == 4)
    assert(range_info.get(0) == "2015")
    assert(range_info.get(1) == "2016")
    assert(range_info.get(2) == "2017")

    sql("CREATE DATABASE IF NOT EXISTS carbondb")
    sql("DROP TABLE IF EXISTS carbondb.carbontable")
    sql(
      """
        | CREATE TABLE carbondb.carbontable(id INT, name STRING) PARTITIONED BY (dt STRING)
        | STORED BY 'carbondata' TBLPROPERTIES('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='2015,2016')
      """.stripMargin)
    sql("ALTER TABLE carbondb.carbontable ADD PARTITION ('2017')")

    val carbonTable1 = CarbonMetadata.getInstance().getCarbonTable("carbondb", "carbontable")
    val partitionInfo1 = carbonTable1.getPartitionInfo(carbonTable1.getTableName)
    val partitionIds1 = partitionInfo1.getPartitionIds
    val range_info1 = partitionInfo1.getRangeInfo
    assert(partitionIds1 == List(0, 1, 2, 3).map(Integer.valueOf(_)).asJava)
    assert(partitionInfo1.getMaxPartitionId == 3)
    assert(partitionInfo1.getNumPartitions == 4)
    assert(range_info1.get(0) == "2015")
    assert(range_info1.get(1) == "2016")
    assert(range_info1.get(2) == "2017")
  }

  test("test exception when alter partition's table doesn't exist in a perticular database") {
    val exception_test_add_partition: Exception = intercept[Exception] {
      sql("CREATE DATABASE IF NOT EXISTS carbondb")
      sql("USE default")
      sql("drop table if exists carbon_table_in_default_db")
      sql(
        """
          | CREATE TABLE carbon_table_in_default_db(id INT, name STRING)
          | PARTITIONED BY (dt STRING) STORED BY 'carbondata'
          | TBLPROPERTIES('PARTITION_TYPE'='RANGE', 'RANGE_INFO'='2015,2016')
        """.stripMargin)
      sql("ALTER TABLE carbondb.carbon_table_in_default_db ADD PARTITION ('2017')")
    }
    assert(exception_test_add_partition.getMessage
      .contains("Table or view 'carbon_table_in_default_db' not found in database 'carbondb'"))
  }

  def validateDataFiles(tableUniqueName: String, segmentId: String, partitions: Seq[Int]): Unit = {
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable(tableUniqueName)
    val dataFiles = getDataFiles(carbonTable, segmentId)
    validatePartitionTableFiles(partitions, dataFiles)
  }

  def getDataFiles(carbonTable: CarbonTable, segmentId: String): Array[CarbonFile] = {
    val tablePath = new CarbonTablePath(carbonTable.getCarbonTableIdentifier,
      carbonTable.getTablePath)
    val segmentDir = tablePath.getCarbonDataDirectoryPath(segmentId)
    val carbonFile = FileFactory.getCarbonFile(segmentDir, FileFactory.getFileType(segmentDir))
    val dataFiles = carbonFile.listFiles(new CarbonFileFilter() {
      override def accept(file: CarbonFile): Boolean = {
        return file.getName.endsWith(".carbondata")
      }
    })
    dataFiles
  }

  /**
   * should ensure answer equals to expected list, not only contains
   * @param partitions
   * @param dataFiles
   */
  def validatePartitionTableFiles(partitions: Seq[Int], dataFiles: Array[CarbonFile]): Unit = {
    val partitionIds: ListBuffer[Int] = new ListBuffer[Int]()
    dataFiles.foreach { dataFile =>
      val partitionId = CarbonTablePath.DataFileUtil.getTaskNo(dataFile.getName).split("_")(0).toInt
      partitionIds += partitionId
      assert(partitions.contains(partitionId))
    }
    partitions.foreach(id => assert(partitionIds.contains(id)))
  }

  override def afterAll = {
    dropTable
    CarbonProperties.getInstance()
    .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy-MM-dd")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
  }

  def dropTable {
    sql("DROP TABLE IF EXISTS list_table_area_origin")
    sql("DROP TABLE IF EXISTS range_table_logdate_origin")
    sql("DROP TABLE IF EXISTS list_table_country_origin")
    sql("DROP TABLE IF EXISTS range_table_logdate_split_origin")
    sql("DROP TABLE IF EXISTS range_table_bucket_origin")
    sql("DROP TABLE IF EXISTS list_table_area")
    sql("DROP TABLE IF EXISTS range_table_logdate")
    sql("DROP TABLE IF EXISTS list_table_country")
    sql("DROP TABLE IF EXISTS range_table_logdate_split")
    sql("DROP TABLE IF EXISTS range_table_bucket")
    sql("DROP TABLE IF EXISTS test_range_int")
    sql("DROP TABLE IF EXISTS test_range_smallint")
    sql("DROP TABLE IF EXISTS test_range_bigint")
    sql("DROP TABLE IF EXISTS test_range_float")
    sql("DROP TABLE IF EXISTS test_range_double")
    sql("DROP TABLE IF EXISTS test_range_date")
    sql("DROP TABLE IF EXISTS test_range_timestamp")
    sql("DROP TABLE IF EXISTS test_range_decimal")
    sql("drop table if exists test_invalid_partition_id")
  }


}
