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
package org.apache.carbondata.spark.testsuite.allqueries

import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.test.TestQueryExecutor
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class InsertIntoCarbonTable2TestCase extends QueryTest with BeforeAndAfterAll {
  override def beforeAll {
    dropTable
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd HH:mm:ss")
  }

  test("insert->insert with functions") {
    // Create table
      sql(
        s"""
           | CREATE TABLE carbon_table(
           |    shortField smallint,
           |    intField int,
           |    bigintField bigint,
           |    doubleField double,
           |    stringField string,
           |    timestampField timestamp,
           |    decimalField decimal(18,2),
           |    dateField date,
           |    charField string,
           |    floatField float
           | )
           | STORED BY 'carbondata'
           | TBLPROPERTIES('DICTIONARY_INCLUDE'='dateField, charField')
       """.stripMargin)

      sql(
        s"""
           | CREATE TABLE carbon_table1(
           |    shortField smallint,
           |    intField int,
           |    bigintField bigint,
           |    doubleField double,
           |    stringField string,
           |    timestampField timestamp,
           |    decimalField decimal(18,2),
           |    dateField date,
           |    charField string,
           |    floatField float
           | )
           | STORED BY 'carbondata'
           | TBLPROPERTIES('DICTIONARY_INCLUDE'='dateField, charField')
         """.stripMargin)

      sql(
        s"""
           | LOAD DATA LOCAL INPATH '${resourcesPath + "/data_with_all_types.csv"}'
           | INTO TABLE carbon_table
           | options('FILEHEADER'='shortField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField')
         """.stripMargin)

      sql("""insert into table carbon_table1 select shortField,intField,bigintField,doubleField,ASCII(stringField),
                  timestampField,decimalField,dateField,charField,floatField from carbon_table
                """)
  }

  test("insert->insert with different names and aliases") {
    sql(
      s"""
       CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES ("TABLE_BLOCKSIZE"= "256 MB")
     """.stripMargin)
    sql(s"""CREATE TABLE student (CUST_ID2 int,CUST_ADDR String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES ("TABLE_BLOCKSIZE"= "256 MB")""")

    sql(s"""LOAD DATA inpath '${resourcesPath + "/data_with_all_types.csv"}' INTO table uniqdata options('DELIMITER'=',', 'FILEHEADER'='CUST_ID, CUST_NAME, ACTIVE_EMUI_VERSION, DOB, DOJ, BIGINT_COLUMN1, BIGINT_COLUMN2, DECIMAL_COLUMN1, DECIMAL_COLUMN2, Double_COLUMN1, Double_COLUMN2, INTEGER_COLUMN1')""")
    sql("""insert into student select * from uniqdata""")
  }

  override def afterAll = {
    dropTable
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, TestQueryExecutor.timestampFormat)
  }

  def dropTable = {
    sql("DROP TABLE IF EXISTS carbon_table")
    sql("DROP TABLE IF EXISTS carbon_table1")
    sql("DROP TABLE IF EXISTS uniqdata")
    sql("DROP TABLE IF EXISTS student")
  }
}