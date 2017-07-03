
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
 * Test Class for uniqincludedictionarycompminorsp to verify all scenerios
 */

class UNIQINCLUDEDICTIONARYCOMPMINORSPTestCase extends QueryTest with BeforeAndAfterAll {
         

//Drop_sp_02
test("Drop_sp_02", Include) {
  sql(s"""Drop table if exists uniq_include_dictionary_comp_minor_sp""").collect

  sql(s"""Drop table if exists uniq_include_dictionary_comp_minor_sp_hive""").collect

}
       

//Single_Pass_Loading_112
test("Single_Pass_Loading_112", Include) {
  sql(s"""CREATE TABLE uniq_include_dictionary_comp_minor_sp(CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID,Double_COLUMN2,DECIMAL_COLUMN2')""").collect

  sql(s"""CREATE TABLE uniq_include_dictionary_comp_minor_sp_hive(CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Single_Pass_Loading_113
test("Single_Pass_Loading_113", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniq_include_dictionary_comp_minor_sp OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='false')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniq_include_dictionary_comp_minor_sp_hive """).collect

}
       

//Single_Pass_Loading_114
test("Single_Pass_Loading_114", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/3000_UniqData.csv' into table uniq_include_dictionary_comp_minor_sp OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='false')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/3000_UniqData.csv' into table uniq_include_dictionary_comp_minor_sp_hive """).collect

}
       

//Single_Pass_Loading_116
test("Single_Pass_Loading_116", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/6000_UniqData.csv' into table uniq_include_dictionary_comp_minor_sp OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='false')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/6000_UniqData.csv' into table uniq_include_dictionary_comp_minor_sp_hive """).collect

}
       

//Single_Pass_Loading_117
test("Single_Pass_Loading_117", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniq_include_dictionary_comp_minor_sp OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='false')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniq_include_dictionary_comp_minor_sp_hive """).collect

}
       

//Single_Pass_Loading_121
test("Single_Pass_Loading_121", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniq_include_dictionary_comp_minor_sp OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='true')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniq_include_dictionary_comp_minor_sp_hive """).collect

}
       

//Single_Pass_Loading_122
test("Single_Pass_Loading_122", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/3000_UniqData.csv' into table uniq_include_dictionary_comp_minor_sp OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='true')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/3000_UniqData.csv' into table uniq_include_dictionary_comp_minor_sp_hive """).collect

}
       

//Single_Pass_Loading_123
test("Single_Pass_Loading_123", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/5000_UniqData.csv' into table uniq_include_dictionary_comp_minor_sp OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='true')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/5000_UniqData.csv' into table uniq_include_dictionary_comp_minor_sp_hive """).collect

}
       

//Single_Pass_Loading_124
test("Single_Pass_Loading_124", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/6000_UniqData.csv' into table uniq_include_dictionary_comp_minor_sp OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='true')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/6000_UniqData.csv' into table uniq_include_dictionary_comp_minor_sp_hive """).collect

}
       

//Single_Pass_Loading_125
test("Single_Pass_Loading_125", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniq_include_dictionary_comp_minor_sp OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='true')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniq_include_dictionary_comp_minor_sp_hive """).collect

}
       

//Single_Pass_Loading_125a
test("Single_Pass_Loading_125a", Include) {
  sql(s"""select * from uniq_include_dictionary_comp_minor_sp""").collect
}
       
override def afterAll {
sql("drop table if exists uniq_include_dictionary_comp_minor_sp")
sql("drop table if exists uniq_include_dictionary_comp_minor_sp_hive")
}
}