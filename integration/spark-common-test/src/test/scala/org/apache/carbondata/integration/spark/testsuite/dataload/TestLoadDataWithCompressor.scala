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

package org.apache.carbondata.integration.spark.testsuite.dataload

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class TestLoadDataWithCompressor extends QueryTest with BeforeAndAfterEach with BeforeAndAfterAll {
  private val tableName = "load_test_with_compressor"

  override protected def afterEach(): Unit = {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT,
      CarbonCommonConstants.ENABLE_OFFHEAP_SORT_DEFAULT)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.COMPRESSOR,
      CarbonCommonConstants.DEFAULT_COMPRESSOR)
    sql(s"DROP TABLE IF EXISTS $tableName")
  }

  private def createTable(): Unit = {
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
         | STORED BY 'carbondata'
         | TBLPROPERTIES(
         |  'LONG_STRING_COLUMNS'='longStringField',
         |  'SORT_COLUMNS'='stringSortField',
         |  'DICTIONARY_INCLUDE'='stringDictField',
         |  'local_dictionary_enable'='true',
         |  'local_dictionary_threshold'='10000',
         |  'local_dictionary_include'='stringLocalDictField')
       """.stripMargin)
  }

  private def loadData(): Unit = {
    sql(
      s"""
         | INSERT INTO TABLE $tableName VALUES
         |  (true,1,11,101,41.4,'string1','2015/4/23 12:01:01',12.34,'2015/4/23','aaa',1.5,'dict1','sort1','local_dict1','longstring1'),
         | (false,2,12,102,42.4,'string2','2015/5/23 12:01:03',23.45,'2015/5/23','bbb',2.5,'dict2','sort2','local_dict2','longstring2'),
         |  (true,3,13,163,43.4,'string3','2015/7/26 12:01:06',34.56,'2015/7/26','ccc',3.5,'dict3','sort3','local_dict3','longstring3'),
         | (NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)
       """.stripMargin)
    sql(
      s"""
         | INSERT INTO TABLE $tableName VALUES
         |  (true,${Short.MaxValue - 2},${Int.MinValue + 2},${Long.MaxValue - 2},${Double.MinValue + 2},'string1','2015/4/23 12:01:01',${Double.MinValue + 2},'2015/4/23','aaa',${Float.MaxValue - 2},'dict1','sort1','local_dict1','longstring1'),
         | (false,2,12,102,42.4,'string2','2015/5/23 12:01:03',23.45,'2015/5/23','bbb',2.5,'dict2','sort2','local_dict2','longstring2'),
         |  (true,${Short.MinValue + 2},${Int.MaxValue - 2},${Long.MinValue + 2},${Double.MaxValue - 2},'string3','2015/7/26 12:01:06',${Double.MinValue + 2},'2015/7/26','ccc',${Float.MinValue + 2},'dict3','sort3','local_dict3','longstring3'),
         | (NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)
       """.stripMargin)
  }

  private def testQuery(): Unit = {
    sql(s"SELECT * FROM $tableName").show(false)
    checkAnswer(sql(s"SELECT count(*) FROM $tableName"), Seq(Row(8)))
  }

  test("test data loading with snappy compressor and offheap") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, "true")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.COMPRESSOR, "snappy")
    createTable()
    loadData()
    testQuery()
  }

  test("test data loading with zstd compressor and offheap") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, "true")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.COMPRESSOR, "zstd")
    createTable()
    loadData()
    testQuery()
  }

  test("test data loading with zstd compressor and onheap") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, "false")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.COMPRESSOR, "zstd")
    createTable()
    loadData()
    testQuery()
  }

  test("test data loading with unsupported compressor and onheap") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, "false")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.COMPRESSOR, "fake")
    createTable()
    val exception = intercept[Throwable] {
      loadData()
    }
  }
}
