
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
 * Test Class for uniqdatachar to verify all scenerios
 */

class UNIQDATACHARTestCase extends QueryTest with BeforeAndAfterAll {
         

//char_01_Drop
test("char_01_Drop", Include) {
  sql(s"""drop table if exists  uniqdata_char""").collect

  sql(s"""drop table if exists  uniqdata_char_hive""").collect

}
       

//drop_char_001
test("drop_char_001", Include) {
  sql(s"""Drop table if exists  uniqdata_char""").collect

  sql(s"""Drop table if exists  uniqdata_char_hive""").collect

}
       

//char_01
test("char_01", Include) {
  sql(s"""CREATE TABLE uniqdata_char (CUST_ID int,CUST_NAME char(30),ACTIVE_EMUI_VERSION char(30), DOB timestamp,
DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'
TBLPROPERTIES ("TABLE_BLOCKSIZE"= "256 MB")""").collect

  sql(s"""CREATE TABLE uniqdata_char_hive (CUST_ID int,CUST_NAME char(30),ACTIVE_EMUI_VERSION char(30), DOB timestamp,
DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),
DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,
INTEGER_COLUMN1 int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//char_04
test("char_04", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata_char OPTIONS ('DELIMITER'=',' 
,'QUOTECHAR'='"','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata_char_hive """).collect

}
       

//char_09
test("char_09", Include) {
  checkAnswer(s"""select count(CUST_NAME) from uniqdata_char""",
    s"""select count(CUST_NAME) from uniqdata_char_hive""")
}
       

//char_10
test("char_10", Include) {
  checkAnswer(s"""select count (concat('Hi ' ,cust_name)) from uniqdata_char""",
    s"""select count (concat('Hi ' ,cust_name)) from uniqdata_char_hive""")
}
       

//char_11
test("char_11", Include) {
  checkAnswer(s"""select count (upper(cust_name)) from uniqdata_char""",
    s"""select count (upper(cust_name)) from uniqdata_char_hive""")
}
       

//char_12
test("char_12", Include) {
  checkAnswer(s"""select count (lower(cust_name)) from uniqdata_char""",
    s"""select count (lower(cust_name)) from uniqdata_char_hive""")
}
       

//char_13
test("char_13", Include) {
  checkAnswer(s"""select count (substr(cust_name,5,7)) from uniqdata_char""",
    s"""select count (substr(cust_name,5,7)) from uniqdata_char_hive""")
}
       

//char_14
test("char_14", Include) {
  checkAnswer(s"""select count (substr(cust_name,5,2)) from uniqdata_char""",
    s"""select count (substr(cust_name,5,2)) from uniqdata_char_hive""")
}
       

//char_15
test("char_15", Include) {
  checkAnswer(s"""select trim(cust_name) from uniqdata_char""",
    s"""select trim(cust_name) from uniqdata_char_hive""")
}
       

//char_16
test("char_16", Include) {
  checkAnswer(s"""select count (substring_index(cust_name, '_', 2)) from uniqdata_char""",
    s"""select count (substring_index(cust_name, '_', 2)) from uniqdata_char_hive""")
}
       

//char_17
test("char_17", Include) {
  checkAnswer(s"""select count (split(cust_name, 'NAME')) from uniqdata_char""",
    s"""select count (split(cust_name, 'NAME')) from uniqdata_char_hive""")
}
       

//char_18
test("char_18", Include) {
  checkAnswer(s"""select count (reverse(cust_name)) from uniqdata_char""",
    s"""select count (reverse(cust_name)) from uniqdata_char_hive""")
}
       

//char_20
test("char_20", Include) {
  checkAnswer(s"""select count (cust_name, length(cust_name)) from uniqdata_char""",
    s"""select count (cust_name, length(cust_name)) from uniqdata_char_hive""")
}
       

//char_21
test("char_21", Include) {
  checkAnswer(s"""select count (rpad(cust_name, 22, " tester")) from uniqdata_char""",
    s"""select count (rpad(cust_name, 22, " tester")) from uniqdata_char_hive""")
}
       

//char_22
test("char_22", Include) {
  checkAnswer(s"""select rtrim(" testing ") from uniqdata_char""",
    s"""select rtrim(" testing ") from uniqdata_char_hive""")
}
       

//char_23
test("char_23", Include) {
  checkAnswer(s"""select count (sentences(cust_name)) from uniqdata_char""",
    s"""select count (sentences(cust_name)) from uniqdata_char_hive""")
}
       

//char_24
test("char_24", Include) {
  checkAnswer(s"""select space(10)from uniqdata_char""",
    s"""select space(10)from uniqdata_char_hive""")
}
       

//char_25
test("char_25", Include) {
  checkAnswer(s"""SELECT count (SUBSTRING(cust_name, -3)) from uniqdata_char""",
    s"""SELECT count (SUBSTRING(cust_name, -3)) from uniqdata_char_hive""")
}
       

//char_26
test("char_26", Include) {
  checkAnswer(s"""select format_string('data', cust_name) from uniqdata_char""",
    s"""select format_string('data', cust_name) from uniqdata_char_hive""")
}
       

//char_27
test("char_27", Include) {
  checkAnswer(s"""select regexp_replace('Tester', 'T', 't') from uniqdata_char""",
    s"""select regexp_replace('Tester', 'T', 't') from uniqdata_char_hive""")
}
       

//char_28
test("char_28", Include) {
  checkAnswer(s"""select ascii('A') from uniqdata_char""",
    s"""select ascii('A') from uniqdata_char_hive""")
}
       

//char_36
test("char_36", Include) {
  sql(s"""select count(cust_name) from uniqdata_char group by dob""").collect
}
       

//char_37
test("char_37", Include) {
  sql(s"""select cust_name from uniqdata_char ORDER BY cust_name DESC""").collect
}
       

//char_38
test("char_38", Include) {
  sql(s"""select * from uniqdata_char ORDER BY cust_name ASC""").collect
}
       

//char_39
test("char_39", Include) {
  sql(s"""select * from uniqdata_char where CUST_NAME like '%_NAME_%'""").collect
}
       
override def afterAll {
sql("drop table if exists uniqdata_char")
sql("drop table if exists uniqdata_char_hive")
}
}