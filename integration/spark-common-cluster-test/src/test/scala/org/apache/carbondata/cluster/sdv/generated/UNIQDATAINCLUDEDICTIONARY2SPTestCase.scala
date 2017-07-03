
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
 * Test Class for uniqdataINCLUDEDICTIONARY2sp to verify all scenerios
 */

class UNIQDATAINCLUDEDICTIONARY2SPTestCase extends QueryTest with BeforeAndAfterAll {
         

//Drop_sp_04
test("Drop_sp_04", Include) {
  sql(s"""Drop table if exists uniqdata_INCLUDEDICTIONARY2_sp""").collect

  sql(s"""Drop table if exists uniqdata_INCLUDEDICTIONARY2_sp_hive""").collect

}
       

//Single_Pass_Loading_55_1
test("Single_Pass_Loading_55_1", Include) {
  sql(s"""CREATE TABLE uniqdata_INCLUDEDICTIONARY2_sp (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""CREATE TABLE uniqdata_INCLUDEDICTIONARY2_sp_hive (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Single_Pass_Loading_52
test("Single_Pass_Loading_52", Include) {
  sql(s"""select * from uniqdata_INCLUDEDICTIONARY2_sp""").collect
}
       

//Single_Pass_Loading_54
test("Single_Pass_Loading_54", Include) {
  sql(s"""select * from uniqdata_INCLUDEDICTIONARY2_sp""").collect
}
       

//Single_Pass_Loading_57
test("Single_Pass_Loading_57", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY2_sp OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='false')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY2_sp_hive """).collect

}
       

//Single_Pass_Loading_58
test("Single_Pass_Loading_58", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY2_sp OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='true')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY2_sp_hive """).collect

}
       

//Single_Pass_Loading_59
test("Single_Pass_Loading_59", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY2_sp OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='true')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY2_sp_hive """).collect

}
       

//Single_Pass_Loading_60
test("Single_Pass_Loading_60", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/3000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY2_sp OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='true')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/3000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY2_sp_hive """).collect

}
       

//Single_Pass_Loading_61
test("Single_Pass_Loading_61", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/5000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY2_sp OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='true')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/5000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY2_sp_hive """).collect

}
       

//Single_Pass_Loading_65
test("Single_Pass_Loading_65", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY2_sp OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='false')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY2_sp_hive """).collect

}
       

//Single_Pass_Loading_67
test("Single_Pass_Loading_67", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY2_sp OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='true')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY2_sp_hive """).collect

}
       

//Single_Pass_Loading_68
test("Single_Pass_Loading_68", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY2_sp OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='true')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY2_sp_hive """).collect

}
       

//Single_Pass_Loading_69
test("Single_Pass_Loading_69", Include) {
  sql(s"""select count (distinct CUST_NAME) from uniqdata_INCLUDEDICTIONARY2_sp""").collect
}
       

//Single_Pass_Loading_70
test("Single_Pass_Loading_70", Include) {
  sql(s"""select count(CUST_NAME) from uniqdata_INCLUDEDICTIONARY2_sp""").collect
}
       

//Single_Pass_Loading_71
test("Single_Pass_Loading_71", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY2_sp OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='true')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY2_sp_hive """).collect

}
       

//Single_Pass_Loading_72
test("Single_Pass_Loading_72", Include) {
  sql(s"""select count (distinct CUST_NAME) from uniqdata_INCLUDEDICTIONARY2_sp""").collect
}
       

//Single_Pass_Loading_73
test("Single_Pass_Loading_73", Include) {
  sql(s"""select count(CUST_NAME) from uniqdata_INCLUDEDICTIONARY2_sp""").collect
}
       

//Single_Pass_Loading_74
test("Single_Pass_Loading_74", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY2_sp OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='false')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY2_sp_hive """).collect

}
       

//Single_Pass_Loading_75
test("Single_Pass_Loading_75", Include) {
  sql(s"""select count (distinct CUST_NAME) from uniqdata_INCLUDEDICTIONARY2_sp""").collect
}
       

//Single_Pass_Loading_76
test("Single_Pass_Loading_76", Include) {
  sql(s"""select count(CUST_NAME) from uniqdata_INCLUDEDICTIONARY2_sp""").collect
}
       

//Single_Pass_Loading_77
test("Single_Pass_Loading_77", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY2_sp OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1','SINGLE_Pass'='false')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata_INCLUDEDICTIONARY2_sp_hive """).collect

}
       
override def afterAll {
sql("drop table if exists uniqdata_INCLUDEDICTIONARY2_sp")
sql("drop table if exists uniqdata_INCLUDEDICTIONARY2_sp_hive")
}
}