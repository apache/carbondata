
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
 * Test Class for uniqdataINCLUDEDICTIONARYsp to verify all scenerios
 */

class UNIQDATAINCLUDEDICTIONARYSPTestCase extends QueryTest with BeforeAndAfterAll {
         

//Drop_sp_08
test("Drop_sp_08", Include) {
  sql(s"""Drop table if exists uniqdata_INCLUDEDICTIONARY_sp""").collect

  sql(s"""Drop table if exists uniqdata_INCLUDEDICTIONARY_sp_hive""").collect

}
       

//Single_Pass_Loading_01
test("Single_Pass_Loading_01", Include) {
  sql(s"""CREATE TABLE uniqdata_INCLUDEDICTIONARY_Sp (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""CREATE TABLE uniqdata_INCLUDEDICTIONARY_Sp_hive (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Single_Pass_Loading_03
test("Single_Pass_Loading_03", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY_sp OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='true')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY_sp_hive """).collect

}
       

//Single_Pass_Loading_04
test("Single_Pass_Loading_04", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY_sp OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='default')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY_sp_hive """).collect

}
       

//Single_Pass_Loading_05
test("Single_Pass_Loading_05", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY_sp OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='false')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY_sp_hive """).collect

}
       

//Single_Pass_Loading_06
test("Single_Pass_Loading_06", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/1lac_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY_sp OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='false')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/1lac_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY_sp_hive """).collect

}
       

//Single_Pass_Loading_07
test("Single_Pass_Loading_07", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/1lac_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY_sp OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='true')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/1lac_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY_sp_hive """).collect

}
       

//Single_Pass_Loading_08
test("Single_Pass_Loading_08", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY_sp OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='true')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY_sp_hive """).collect

}
       

//Single_Pass_Loading_09
test("Single_Pass_Loading_09", Include) {
  sql(s"""select distinct(CUST_NAME) from uniqdata_INCLUDEDICTIONARY_sp""").collect
}
       

//Single_Pass_Loading_10
test("Single_Pass_Loading_10", Include) {
  sql(s"""select count (distinct CUST_NAME) from uniqdata_INCLUDEDICTIONARY_sp""").collect
}
       

//Single_Pass_Loading_11
test("Single_Pass_Loading_11", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY_sp OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='false')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY_sp_hive """).collect

}
       

//Single_Pass_Loading_12
test("Single_Pass_Loading_12", Include) {
  sql(s"""select distinct(CUST_NAME) from uniqdata_INCLUDEDICTIONARY_sp""").collect
}
       

//Single_Pass_Loading_13
test("Single_Pass_Loading_13", Include) {
  sql(s"""select count (distinct CUST_NAME) from uniqdata_INCLUDEDICTIONARY_sp""").collect
}
       

//Single_Pass_Loading_31
test("Single_Pass_Loading_31", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY_sp OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='true')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY_sp_hive """).collect

}
       

//Single_Pass_Loading_32
test("Single_Pass_Loading_32", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/3000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY_sp OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='true')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/3000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY_sp_hive """).collect

}
       

//Single_Pass_Loading_33
test("Single_Pass_Loading_33", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/5000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY_sp OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='true')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/5000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY_sp_hive """).collect

}
       

//Single_Pass_Loading_34
test("Single_Pass_Loading_34", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY_sp OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='true')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY_sp_hive """).collect

}
       

//Single_Pass_Loading_35
test("Single_Pass_Loading_35", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY_sp OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='false')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY_sp_hive """).collect

}
       

//Single_Pass_Loading_36
test("Single_Pass_Loading_36", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/3000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY_sp OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='false')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/3000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY_sp_hive """).collect

}
       

//Single_Pass_Loading_37
test("Single_Pass_Loading_37", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/5000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY_sp OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='false')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/5000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY_sp_hive """).collect

}
       

//Single_Pass_Loading_38
test("Single_Pass_Loading_38", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY_sp OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='false')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY_sp_hive """).collect

}
       
override def afterAll {
sql("drop table if exists uniqdata_INCLUDEDICTIONARY_sp")
sql("drop table if exists uniqdata_INCLUDEDICTIONARY_sp_hive")
sql("drop table if exists uniqdata_INCLUDEDICTIONARY_Sp")
sql("drop table if exists uniqdata_INCLUDEDICTIONARY_Sp_hive")
}
}