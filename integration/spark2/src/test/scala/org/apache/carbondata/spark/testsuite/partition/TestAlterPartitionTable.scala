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
        | 'RANGE_INFO'='2014/01/01, 2015/01/01, 2016/01/01')
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
        | 'RANGE_INFO'='2014/01/01, 2015/01/01, 2016/01/01, 2018/01/01')
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
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default_list_table_area")
    val partitionInfo = carbonTable.getPartitionInfo(carbonTable.getFactTableName)
    val partitionIds = partitionInfo.getPartitionIds
    val list_info = partitionInfo.getListInfo
    assert(partitionIds == List(0, 1, 2, 3, 4, 5).map(Integer.valueOf(_)).asJava)
    assert(partitionInfo.getMAX_PARTITION == 5)
    assert(partitionInfo.getNumPartitions == 6)
    assert(list_info.get(0).get(0) == "Asia")
    assert(list_info.get(1).get(0) == "America")
    assert(list_info.get(2).get(0) == "Europe")
    assert(list_info.get(3).get(0) == "OutSpace")
    assert(list_info.get(4).get(0) == "Hi")
    validateDataFiles("default_list_table_area", "0", Seq(0, 1, 2, 4))
    val result_after = sql("select id, vin, logdate, phonenumber, country, area, salary from list_table_area")
    val result_origin = sql("select id, vin, logdate, phonenumber, country, area, salary from list_table_area_origin")
    checkAnswer(result_after, result_origin)

    val result_after1 = sql(s"select id, vin, logdate, phonenumber, country, area, salary from list_table_area where area < 'OutSpace' ")
    val rssult_origin1 = sql(s"select id, vin, logdate, phonenumber, country, area, salary from list_table_area_origin where area < 'OutSpace' ")
    checkAnswer(result_after1, rssult_origin1)

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

    sql("""ALTER TABLE list_table_area ADD PARTITION ('One', '(Two, Three)', 'Four')""".stripMargin)
    val carbonTable1 = CarbonMetadata.getInstance().getCarbonTable("default_list_table_area")
    val partitionInfo1 = carbonTable1.getPartitionInfo(carbonTable.getFactTableName)
    val partitionIds1 = partitionInfo1.getPartitionIds
    val new_list_info = partitionInfo1.getListInfo
    assert(partitionIds1 == List(0, 1, 2, 3, 4, 5, 6, 7, 8).map(Integer.valueOf(_)).asJava)
    assert(partitionInfo1.getMAX_PARTITION == 8)
    assert(partitionInfo1.getNumPartitions == 9)
    assert(new_list_info.get(0).get(0) == "Asia")
    assert(new_list_info.get(1).get(0) == "America")
    assert(new_list_info.get(2).get(0) == "Europe")
    assert(new_list_info.get(3).get(0) == "OutSpace")
    assert(new_list_info.get(4).get(0) == "Hi")
    assert(new_list_info.get(5).get(0) == "One")
    assert(new_list_info.get(6).get(0) == "Two")
    assert(new_list_info.get(6).get(1) == "Three")
    assert(new_list_info.get(7).get(0) == "Four")
    validateDataFiles("default_list_table_area", "0", Seq(0, 1, 2, 4))

    val result_after6 = sql("select id, vin, logdate, phonenumber, country, area, salary from list_table_area")
    val result_origin6 = sql("""select id, vin, logdate, phonenumber, country, area, salary from list_table_area_origin""")
    checkAnswer(result_after6, result_origin6)
  }

  test("Alter table add partition: Range Partition") {
    sql("""ALTER TABLE range_table_logdate ADD PARTITION ('2017/01/01', '2018/01/01')""")
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default_range_table_logdate")
    val partitionInfo = carbonTable.getPartitionInfo(carbonTable.getFactTableName)
    val partitionIds = partitionInfo.getPartitionIds
    val range_info = partitionInfo.getRangeInfo
    assert(partitionIds.size() == 6)
    assert(partitionIds == List(0, 1, 2, 3, 4, 5).map(Integer.valueOf(_)).asJava)
    assert(partitionInfo.getMAX_PARTITION == 5)
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
  }

  test("Alter table split partition: List Partition") {
    sql("""ALTER TABLE list_table_country SPLIT PARTITION(4) INTO ('Canada', 'Russia', '(Good, NotGood)')""".stripMargin)
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default_list_table_country")
    val partitionInfo = carbonTable.getPartitionInfo(carbonTable.getFactTableName)
    val partitionIds = partitionInfo.getPartitionIds
    val list_info = partitionInfo.getListInfo
    assert(partitionIds == List(0, 1, 2, 3, 6, 7, 8, 5).map(Integer.valueOf(_)).asJava)
    assert(partitionInfo.getMAX_PARTITION == 8)
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
    validateDataFiles("default_list_table_country", "0", Seq(0, 1, 2, 3, 8))
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

  test("Alter table split partition: Range Partition") {
    sql("""ALTER TABLE range_table_logdate_split SPLIT PARTITION(4) INTO ('2017/01/01', '2018/01/01')""")
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default_range_table_logdate_split")
    val partitionInfo = carbonTable.getPartitionInfo(carbonTable.getFactTableName)
    val partitionIds = partitionInfo.getPartitionIds
    val rangeInfo = partitionInfo.getRangeInfo
    assert(partitionIds == List(0, 1, 2, 3, 5, 6).map(Integer.valueOf(_)).asJava)
    assert(partitionInfo.getMAX_PARTITION == 6)
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
  }

  test("Alter table split partition: Range Partition + Bucket") {
    sql("""ALTER TABLE range_table_bucket SPLIT PARTITION(4) INTO ('2017/01/01', '2018/01/01')""")
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default_range_table_bucket")
    val partitionInfo = carbonTable.getPartitionInfo(carbonTable.getFactTableName)
    val partitionIds = partitionInfo.getPartitionIds
    val rangeInfo = partitionInfo.getRangeInfo
    assert(partitionIds == List(0, 1, 2, 3, 5, 6).map(Integer.valueOf(_)).asJava)
    assert(partitionInfo.getMAX_PARTITION == 6)
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
  }

  def validateDataFiles(tableUniqueName: String, segmentId: String, partitions: Seq[Int]): Unit = {
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable(tableUniqueName)
    val dataFiles = getDataFiles(carbonTable, segmentId)
    validatePartitionTableFiles(partitions, dataFiles)
  }

  def getDataFiles(carbonTable: CarbonTable, segmentId: String): Array[CarbonFile] = {
    val tablePath = new CarbonTablePath(carbonTable.getStorePath, carbonTable.getDatabaseName,
      carbonTable.getFactTableName)
    val segmentDir = tablePath.getCarbonDataDirectoryPath("0", segmentId)
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
  }


}
