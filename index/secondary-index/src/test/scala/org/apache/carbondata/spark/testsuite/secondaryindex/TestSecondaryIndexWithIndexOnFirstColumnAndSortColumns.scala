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

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class TestSecondaryIndexWithIndexOnFirstColumnAndSortColumns extends QueryTest with BeforeAndAfterAll {

  var count1BeforeIndex : Array[Row] = null

  override def beforeAll {

    sql("drop table if exists seccust")
    sql("create table seccust (id string, c_custkey string, c_name string, c_address string, c_nationkey string, c_phone string,c_acctbal decimal, c_mktsegment string, c_comment string) " +
        "STORED AS carbondata TBLPROPERTIES ('table_blocksize'='128','SORT_COLUMNS'='c_custkey,c_name','NO_INVERTED_INDEX'='c_nationkey')")
    sql(s"""load data  inpath '${resourcesPath}/secindex/firstunique.csv' into table seccust options('DELIMITER'='|','QUOTECHAR'='"','FILEHEADER'='id,c_custkey,c_name,c_address,c_nationkey,c_phone,c_acctbal,c_mktsegment,c_comment')""")
    sql(s"""load data  inpath '${resourcesPath}/secindex/secondunique.csv' into table seccust options('DELIMITER'='|','QUOTECHAR'='\"','FILEHEADER'='id,c_custkey,c_name,c_address,c_nationkey,c_phone,c_acctbal,c_mktsegment,c_comment')""")
    count1BeforeIndex = sql("select * from seccust where id = '1' limit 1").collect()
    sql("create index sc_indx1 on table seccust(id) AS 'carbondata'")
  }

  test("Test secondry index on 1st column and with sort columns") {
    checkAnswer(sql("select count(*) from seccust where id = '1'"),Row(2))
  }

  override def afterAll {
    sql("drop table if exists orders")
  }
}