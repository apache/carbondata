
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

import org.apache.spark.sql.common.util._
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for uniqdatadatedictionary to verify all scenerios
 */

class UNIQDATADATEDICTIONARYTestCase extends QueryTest with BeforeAndAfterAll {
         

//date_01_Drop
test("date_01_Drop", Include) {
  sql(s"""drop table if exists  uniqdata_date_dictionary""").collect

  sql(s"""drop table if exists  uniqdata_date_dictionary_hive""").collect

}
       

//drop_date_001
test("drop_date_001", Include) {
  sql(s"""Drop table  if exists  uniqdata_date_dictionary""").collect

  sql(s"""Drop table  if exists  uniqdata_date_dictionary_hive""").collect

}
       

//date_01
test("date_01", Include) {
  sql(s"""CREATE TABLE uniqdata_date_dictionary (CUST_ID int,CUST_NAME string,ACTIVE_EMUI_VERSION string, DOB date,DOJ date, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'TBLPROPERTIES ("TABLE_BLOCKSIZE"= "256 MB")""").collect

  sql(s"""CREATE TABLE uniqdata_date_dictionary_hive (CUST_ID int,CUST_NAME string,ACTIVE_EMUI_VERSION string, DOB date,DOJ date, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//date_05
test("date_05", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/4000_UniqData.csv' into table uniqdata_date_dictionary OPTIONS('FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/4000_UniqData.csv' into table uniqdata_date_dictionary_hive """).collect

}
       
override def afterAll {
sql("drop table if exists uniqdata_date_dictionary")
sql("drop table if exists uniqdata_date_dictionary_hive")
}
}