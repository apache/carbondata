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

package org.apache.carbondata.cluster.sdv.generated

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class BloomFilterIndexTestCase extends QueryTest with BeforeAndAfterEach with BeforeAndAfterAll {
  // scalastyle:off lineLength
  override protected def beforeAll(): Unit = {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
      "yyyy-MM-dd")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
      "yyyy-MM-dd HH:mm:ss")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS, "true")
  }

  override protected def afterAll(): Unit = {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
      CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
      CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS,
        CarbonCommonConstants.ENABLE_QUERY_STATISTICS_DEFAULT)
  }

  private def createAllDataTypeTable(tableName: String): Unit = {
    sql(s"DROP TABLE IF EXISTS $tableName")
    sql(
      s"""
         | CREATE TABLE $tableName(
         |    booleanField boolean,
         |    shortField smallint,
         |    intField int,
         |    bigintField bigint,
         |    doubleField double,
         |    stringField string,
         |    timestampField timestamp,
         |    decimalField decimal(18,2),
         |    dateField date,
         |    charField string,
         |    floatField float,
         |    stringDictField string,
         |    stringSortField string,
         |    stringLocalDictField string,
         |    longStringField string
         | )
         | STORED AS carbondata
         | TBLPROPERTIES(
         |  'LONG_STRING_COLUMNS'='longStringField',
         |  'SORT_COLUMNS'='stringSortField',
         |  'local_dictionary_enable'='true',
         |  'local_dictionary_threshold'='10000',
         |  'local_dictionary_include'='stringLocalDictField',
         |  'CACHE_LEVEL'='BLOCKLET')
       """.stripMargin)
  }

  private def loadAllDataTypeTable(tableName: String): Unit = {
    sql(
      s"""
         | INSERT INTO TABLE $tableName VALUES
         |  (true,1,11,101,41.4,'string1','2015-04-23 12:01:01',12.34,'2015-04-23','aaa',1.5,'dict1','sort1','local_dict1','longstring1'),
         | (false,2,12,102,42.4,'string2','2015-05-23 12:01:03',23.45,'2015-05-23','bbb',2.5,'dict2','sort2','local_dict2','longstring2'),
         |  (true,3,13,163,43.4,'string3','2015-07-26 12:01:06',34.56,'2015-07-26','ccc',3.5,'dict3','sort3','local_dict3','longstring3'),
         | (NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)
       """.stripMargin)
    sql(
      s"""
         | INSERT INTO TABLE $tableName VALUES
         |  (true,${Short.MaxValue - 2},${Int.MinValue + 2},${Long.MaxValue - 2},${Double.MinValue + 2},'string1','2015-04-23 12:01:01',${Double.MinValue + 2},'2015-04-23','aaa',${Float.MaxValue - 2},'dict1','sort1','local_dict1','longstring1'),
         | (false,2,12,102,42.4,'string2','2015-05-23 12:01:03',23.45,'2015-05-23','bbb',2.5,'dict2','sort2','local_dict2','longstring2'),
         |  (true,${Short.MinValue + 2},${Int.MaxValue - 2},${Long.MinValue + 2},${Double.MaxValue - 2},'string3','2015-07-26 12:01:06',${Double.MinValue + 2},'2015-07-26','ccc',${Float.MinValue + 2},'dict3','sort3','local_dict3','longstring3'),
         | (NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)
       """.stripMargin)
  }

  test("create bloomfilter index on all datatypes") {
    val tableName = "all_data_types"
    val indexName = "dm_with_all_data_types"
    createAllDataTypeTable(tableName)
    loadAllDataTypeTable(tableName)
    // create index on all supported datatype
    sql(
      s"""
         | CREATE INDEX $indexName ON TABLE $tableName (booleanField, shortField, intField, bigintField, doubleField, stringField, timestampField, decimalField, dateField, charField, floatField, stringDictField, stringSortField, stringLocalDictField, longStringField)
         | AS 'bloomfilter'
         | PROPERTIES(
         | 'BLOOM_SIZE'='6400',
         | 'BLOOM_FPP'='0.001',
         | 'BLOOM_COMPRESS'='TRUE')
       """.stripMargin)
    loadAllDataTypeTable(tableName)
    checkExistence(sql(s"SHOW INDEXES ON TABLE $tableName"), true, "bloomfilter", indexName)
    checkAnswer(sql(s"SELECT COUNT(*) FROM $tableName"), Seq(Row(16)))
    checkExistence(
      sql(s"EXPLAIN SELECT * FROM $tableName WHERE booleanField=false AND shortField=2 AND intField=12 AND bigintField=102 AND doubleField=42.4 AND stringField='string2' AND timestampField='2015-05-23 12:01:03' AND decimalField=23.45 AND dateField='2015-05-23' AND charField='bbb' AND floatField=2.5 AND stringDictField='dict2' AND stringSortField='sort2' AND stringLocalDictField= 'local_dict2' AND longStringField='longstring2'"),
      true, "bloomfilter", indexName)
    checkAnswer(
      sql(s"SELECT COUNT(*) FROM (SELECT * FROM $tableName WHERE booleanField=false AND shortField=2 AND intField=12 AND bigintField=102 AND stringField='string2' AND timestampField='2015-05-23 12:01:03' AND decimalField=23.45 AND dateField='2015-05-23' AND charField='bbb' AND floatField=2.5 AND stringDictField='dict2' AND stringSortField='sort2' AND stringLocalDictField= 'local_dict2' AND longStringField='longstring2') b"),
      Seq(Row(4)))
    sql(s"DROP TABLE IF EXISTS $tableName")
  }

  test("create bloomfilter index on all datatypes with sort columns") {
    val tableName = "all_data_types_with_sort_column"
    val indexName = "dm_with_all_data_types_with_sort_column"
    sql(s"DROP TABLE IF EXISTS $tableName")
    // double/float/decimal/longstring cannot be sort_columns so we ignore them in SORT_COLUMNS
    sql(
      s"""
         | CREATE TABLE $tableName(
         |    booleanField boolean,
         |    shortField smallint,
         |    intField int,
         |    bigintField bigint,
         |    doubleField double,
         |    stringField string,
         |    timestampField timestamp,
         |    decimalField decimal(18,2),
         |    dateField date,
         |    charField string,
         |    floatField float,
         |    stringDictField string,
         |    stringSortField string,
         |    stringLocalDictField string,
         |    longStringField string
         | )
         | STORED AS carbondata
         | TBLPROPERTIES(
         |  'LONG_STRING_COLUMNS'='longStringField',
         |  'local_dictionary_enable'='true',
         |  'local_dictionary_threshold'='10000',
         |  'local_dictionary_include'='stringLocalDictField',
         |  'SORT_COLUMNS'='booleanField, shortField, intField, bigintField, stringField, timestampField, dateField, charField, stringDictField, stringSortField, stringLocalDictField')
       """.stripMargin)
    loadAllDataTypeTable(tableName)
    // create index on all supported datatype
    sql(
      s"""
         | CREATE INDEX $indexName ON TABLE $tableName (booleanField, shortField, intField, bigintField, doubleField, stringField, timestampField, decimalField, dateField, charField, floatField, stringDictField, stringSortField, stringLocalDictField, longStringField)
         | AS 'bloomfilter'
         | PROPERTIES(
         | 'BLOOM_SIZE'='6400',
         | 'BLOOM_FPP'='0.001',
         | 'BLOOM_COMPRESS'='TRUE')
       """.stripMargin)
    loadAllDataTypeTable(tableName)
    checkExistence(sql(s"SHOW INDEXES ON TABLE $tableName"), true, "bloomfilter", indexName)
    checkAnswer(sql(s"SELECT COUNT(*) FROM $tableName"), Seq(Row(16)))
    checkExistence(
      sql(s"EXPLAIN SELECT * FROM $tableName WHERE booleanField=false AND shortField=2 AND intField=12 AND bigintField=102 AND doubleField=42.4 AND stringField='string2' AND timestampField='2015-05-23 12:01:03' AND decimalField=23.45 AND dateField='2015-05-23' AND charField='bbb' AND floatField=2.5 AND stringDictField='dict2' AND stringSortField='sort2' AND stringLocalDictField= 'local_dict2' AND longStringField='longstring2'"),
      true, "bloomfilter", indexName)
    checkAnswer(
      sql(s"SELECT COUNT(*) FROM (SELECT * FROM $tableName WHERE booleanField=false AND shortField=2 AND intField=12 AND bigintField=102 AND stringField='string2' AND timestampField='2015-05-23 12:01:03' AND decimalField=23.45 AND dateField='2015-05-23' AND charField='bbb' AND floatField=2.5 AND stringDictField='dict2' AND stringSortField='sort2' AND stringLocalDictField= 'local_dict2' AND longStringField='longstring2') b"),
      Seq(Row(4)))
    sql(s"DROP TABLE IF EXISTS $tableName")
  }

  test("test bloom index with empty values on index column") {
    val normalTable = "normal_table"
    val bloomDMSampleTable = "bloom_table"
    val indexName = "bloom_index"
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomDMSampleTable")
    sql(s"CREATE TABLE $normalTable(c1 string, c2 int, c3 string) STORED AS carbondata")
    sql(s"CREATE TABLE $bloomDMSampleTable(c1 string, c2 int, c3 string) STORED AS carbondata")
    // load data with empty value
    sql(s"INSERT INTO $normalTable SELECT '', 1, 'xxx'")
    sql(s"INSERT INTO $bloomDMSampleTable SELECT '', 1, 'xxx'")
    sql(s"INSERT INTO $normalTable SELECT '', null, 'xxx'")
    sql(s"INSERT INTO $bloomDMSampleTable SELECT '', null, 'xxx'")

    sql(
      s"""
         | CREATE INDEX $indexName on table $bloomDMSampleTable (c1, c2)
         | AS 'bloomfilter'
       """.stripMargin)

    // load data with empty value
    sql(s"INSERT INTO $normalTable SELECT '', 1, 'xxx'")
    sql(s"INSERT INTO $bloomDMSampleTable SELECT '', 1, 'xxx'")
    sql(s"INSERT INTO $normalTable SELECT '', null, 'xxx'")
    sql(s"INSERT INTO $bloomDMSampleTable SELECT '', null, 'xxx'")

    // query on null fields
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable"),
      sql(s"SELECT * FROM $normalTable"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE c1 = null"),
      sql(s"SELECT * FROM $normalTable WHERE c1 = null"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE c1 = ''"),
      sql(s"SELECT * FROM $normalTable WHERE c1 = ''"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE isNull(c1)"),
      sql(s"SELECT * FROM $normalTable WHERE isNull(c1)"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE isNull(c2)"),
      sql(s"SELECT * FROM $normalTable WHERE isNull(c2)"))
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomDMSampleTable")
  }

  test("create multiple indexes vs create on index on multiple columns") {
    val tableName1 = "all_data_types10"
    val tableName2 = "all_data_types20"
    val indexName1 = "dm_with_all_data_types10"
    val indexName2Prefix = "dm_with_all_data_types2"
    sql(s"DROP TABLE IF EXISTS $tableName1")
    sql(s"DROP TABLE IF EXISTS $tableName2")
    createAllDataTypeTable(tableName1)
    createAllDataTypeTable(tableName2)
    loadAllDataTypeTable(tableName1)
    loadAllDataTypeTable(tableName2)
    // create one index on multiple index columns
    sql(
      s"""
         | CREATE INDEX $indexName1 ON TABLE $tableName1 (booleanField, shortField, intField, bigintField, doubleField, stringField, timestampField, decimalField, dateField, charField, floatField, stringDictField, stringSortField, stringLocalDictField, longStringField)
         | AS 'bloomfilter'
       """.stripMargin)
    // create multiple indexes each on one index column
    sql(
      s"""
         | CREATE INDEX ${indexName2Prefix}0 ON TABLE $tableName2 (booleanField)
         | AS 'bloomfilter'
       """.stripMargin)
    sql(
      s"""
         | CREATE INDEX ${indexName2Prefix}1 ON TABLE $tableName2 (shortField)
         | AS 'bloomfilter'
       """.stripMargin)
    sql(
      s"""
         | CREATE INDEX ${indexName2Prefix}2 ON TABLE $tableName2 (intField)
         | AS 'bloomfilter'
       """.stripMargin)
    sql(
      s"""
         | CREATE INDEX ${indexName2Prefix}3 ON TABLE $tableName2 (bigintField)
         | AS 'bloomfilter'
       """.stripMargin)
    sql(
      s"""
         | CREATE INDEX ${indexName2Prefix}4 ON TABLE $tableName2 (doubleField)
         | AS 'bloomfilter'
       """.stripMargin)
    sql(
      s"""
         | CREATE INDEX ${indexName2Prefix}5 ON TABLE $tableName2 (stringField)
         | AS 'bloomfilter'
       """.stripMargin)
    sql(
      s"""
         | CREATE INDEX ${indexName2Prefix}6 ON TABLE $tableName2 (timestampField)
         | AS 'bloomfilter'
         | DMPROPERTIES(
         | 'INDEX_COLUMNS'='timestampField'
         | )
       """.stripMargin)
    sql(
      s"""
         | CREATE INDEX ${indexName2Prefix}7 ON TABLE $tableName2 (decimalField)
         | AS 'bloomfilter'
       """.stripMargin)
    sql(
      s"""
         | CREATE INDEX ${indexName2Prefix}8 ON TABLE $tableName2 (dateField)
         | AS 'bloomfilter'
       """.stripMargin)
    sql(
      s"""
         | CREATE INDEX ${indexName2Prefix}9 ON TABLE $tableName2 (charField)
         | AS 'bloomfilter'
       """.stripMargin)
    sql(
      s"""
         | CREATE INDEX ${indexName2Prefix}10 ON TABLE $tableName2 (floatField)
         | AS 'bloomfilter'
       """.stripMargin)
    sql(
      s"""
         | CREATE INDEX ${indexName2Prefix}11 ON TABLE $tableName2 (stringDictField)
         | AS 'bloomfilter'
       """.stripMargin)
    sql(
      s"""
         | CREATE INDEX ${indexName2Prefix}12 ON TABLE $tableName2 (stringSortField)
         | AS 'bloomfilter'
       """.stripMargin)
    sql(
      s"""
         | CREATE INDEX ${indexName2Prefix}13 ON TABLE $tableName2 (stringLocalDictField)
         | AS 'bloomfilter'
       """.stripMargin)
    sql(
      s"""
         | CREATE INDEX ${indexName2Prefix}14 ON TABLE $tableName2 (longStringField)
         | AS 'bloomfilter'
       """.stripMargin)

    loadAllDataTypeTable(tableName1)
    loadAllDataTypeTable(tableName2)
    assert(sql(s"SHOW INDEXES ON TABLE $tableName1").collect().length == 1)
    assert(sql(s"SHOW INDEXES ON TABLE $tableName2").collect().length == 15)
    checkAnswer(sql(s"SELECT COUNT(*) FROM $tableName1"), sql(s"SELECT COUNT(*) FROM $tableName2"))
    checkExistence(
      sql(s"EXPLAIN SELECT * FROM $tableName1 WHERE booleanField=false AND shortField=2 AND intField=12 AND bigintField=102 AND doubleField=42.4 AND stringField='string2' AND timestampField='2015-05-23 12:01:03' AND decimalField=23.45 AND dateField='2015-05-23' AND charField='bbb' AND floatField=2.5 AND stringDictField='dict2' AND stringSortField='sort2' AND stringLocalDictField= 'local_dict2' AND longStringField='longstring2'"),
      true, "bloomfilter", indexName1)
    val allIndexesOnTable2 = (0 to 14).map(p => s"$indexName2Prefix$p")
    val existedString = (allIndexesOnTable2 :+ "bloomfilter").toArray
    checkExistence(
      sql(s"EXPLAIN SELECT * FROM $tableName2 WHERE booleanField=false AND shortField=2 AND intField=12 AND bigintField=102 AND doubleField=42.4 AND stringField='string2' AND timestampField='2015-05-23 12:01:03' AND decimalField=23.45 AND dateField='2015-05-23' AND charField='bbb' AND floatField=2.5 AND stringDictField='dict2' AND stringSortField='sort2' AND stringLocalDictField= 'local_dict2' AND longStringField='longstring2'"),
      true, existedString: _*)

    checkAnswer(
      sql(s"SELECT COUNT(*) FROM (SELECT * FROM $tableName1 WHERE booleanField=false AND shortField=2 AND intField=12 AND bigintField=102 AND stringField='string2' AND timestampField='2015-05-23 12:01:03' AND decimalField=23.45 AND dateField='2015-05-23' AND charField='bbb' AND floatField=2.5 AND stringDictField='dict2' AND stringSortField='sort2' AND stringLocalDictField= 'local_dict2' AND longStringField='longstring2') b"),
      sql(s"SELECT COUNT(*) FROM (SELECT * FROM $tableName2 WHERE booleanField=false AND shortField=2 AND intField=12 AND bigintField=102 AND stringField='string2' AND timestampField='2015-05-23 12:01:03' AND decimalField=23.45 AND dateField='2015-05-23' AND charField='bbb' AND floatField=2.5 AND stringDictField='dict2' AND stringSortField='sort2' AND stringLocalDictField= 'local_dict2' AND longStringField='longstring2') b"))
    sql(s"DROP TABLE IF EXISTS $tableName1")
    sql(s"DROP TABLE IF EXISTS $tableName2")
  }
  // scalastyle:on lineLength
}
