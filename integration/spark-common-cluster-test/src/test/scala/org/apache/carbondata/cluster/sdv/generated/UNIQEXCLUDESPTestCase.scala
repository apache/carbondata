
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
 * Test Class for uniqexcludesp to verify all scenerios
 */

class UNIQEXCLUDESPTestCase extends QueryTest with BeforeAndAfterAll {
         

//Drop_sp_09
test("Drop_sp_09", Include) {
  sql(s"""Drop table if exists uniq_exclude_sp""").collect

  sql(s"""Drop table if exists uniq_exclude_sp_hive""").collect

}
       

//Single_Pass_Loading_14
test("Single_Pass_Loading_14", Include) {
  sql(s"""CREATE TABLE uniq_exclude_sp (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='CUST_NAME,ACTIVE_EMUI_VERSION')""").collect

  sql(s"""CREATE TABLE uniq_exclude_sp_hive (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Single_Pass_Loading_15
test("Single_Pass_Loading_15", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniq_exclude_sp OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='true')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniq_exclude_sp_hive """).collect

}
       

//Single_Pass_Loading_16
test("Single_Pass_Loading_16", Include) {
  sql(s"""select distinct(CUST_NAME) from uniq_exclude_sp""").collect
}
       

//Single_Pass_Loading_17
test("Single_Pass_Loading_17", Include) {
  sql(s"""select ACTIVE_EMUI_VERSION from uniq_exclude_sp""").collect
}
       

//Single_Pass_Loading_18
test("Single_Pass_Loading_18", Include) {
  sql(s"""select CUST_ID from uniq_exclude_sp""").collect
}
       

//Single_Pass_Loading_43
test("Single_Pass_Loading_43", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniq_exclude_sp OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='false')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniq_exclude_sp_hive """).collect

}
       

//Single_Pass_Loading_44
test("Single_Pass_Loading_44", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniq_exclude_sp OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='true')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniq_exclude_sp_hive """).collect

}
       
override def afterAll {
sql("drop table if exists uniq_exclude_sp")
sql("drop table if exists uniq_exclude_sp_hive")
}
}