
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

package org.apache.carbondata.spark.testsuite.sdv.generated

import org.apache.spark.sql.common.util._
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for targetuniqdata to verify all scenerios
 */

class TARGETUNIQDATATestCase extends QueryTest with BeforeAndAfterAll {
         

//Insert_04_Drop
test("Insert_04_Drop", Include) {
  sql(s"""drop table if exists  target_uniqdata""").collect

  sql(s"""drop table if exists  target_uniqdata_hive""").collect

}
       

//Insert_04
test("Insert_04", Include) {
  sql(s"""CREATE TABLE target_uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES ("TABLE_BLOCKSIZE"= "256 MB")""").collect

  sql(s"""CREATE TABLE target_uniqdata_hive (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Insert_07
test("Insert_07", Include) {
  sql(s"""LOAD DATA inpath '$resourcesPath/Data/uniqdata/2000_UniqData.csv' INTO table target_uniqdata options('DELIMITER'=',', 'FILEHEADER'='CUST_ID, CUST_NAME, ACTIVE_EMUI_VERSION, DOB, DOJ, BIGINT_COLUMN1, BIGINT_COLUMN2, DECIMAL_COLUMN1, DECIMAL_COLUMN2, Double_COLUMN1, Double_COLUMN2, INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA inpath '$resourcesPath/Data/uniqdata/2000_UniqData.csv' INTO table target_uniqdata_hive """).collect

}
       

//Insert_38a
test("Insert_38a", Include) {
  sql(s"""Select * from  target_uniqdata""").collect
}
       

override def afterAll {
sql("drop table if exists target_uniqdata")
sql("drop table if exists target_uniqdata_hive")
}
}