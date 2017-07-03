
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
 * Test Class for uniqdatadate to verify all scenerios
 */

class UNIQDATADATETestCase extends QueryTest with BeforeAndAfterAll {
         

//date_02_Drop
test("date_02_Drop", Include) {
  sql(s"""drop table if exists  uniqdata_date""").collect

  sql(s"""drop table if exists  uniqdata_date_hive""").collect

}
       

//drop_date_002
test("drop_date_002", Include) {
  sql(s"""Drop table  if exists  uniqdata_date""").collect

  sql(s"""Drop table  if exists  uniqdata_date_hive""").collect

}
       

//date_02
test("date_02", Include) {
  sql(s"""CREATE TABLE  uniqdata_date (CUST_ID int,CUST_NAME string,ACTIVE_EMUI_VERSION string, DOB timestamp,DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'TBLPROPERTIES ("TABLE_BLOCKSIZE"= "256 MB")""").collect

  sql(s"""CREATE TABLE  uniqdata_date_hive (CUST_ID int,CUST_NAME string,ACTIVE_EMUI_VERSION string, DOB timestamp,DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//date_06
test("date_06", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/4000_UniqData.csv' into table uniqdata_date OPTIONS('FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/4000_UniqData.csv' into table uniqdata_date_hive """).collect

}
       

//date_09
test("date_09", Include) {
  checkAnswer(s"""select doj from uniqdata_date""",
    s"""select doj from uniqdata_date_hive""")
}
       

//date_10
test("date_10", Include) {
  checkAnswer(s"""select cust_id, cust_name from uniqdata_date where doj is null""",
    s"""select cust_id, cust_name from uniqdata_date_hive where doj is null""")
}
       

//date_11
test("date_11", Include) {
  checkAnswer(s"""select cust_id, cust_name,dob from uniqdata_date where date_format(dob, "yyyy-MM-dd") = '1975-06-22'""",
    s"""select cust_id, cust_name,dob from uniqdata_date_hive where date_format(dob, "yyyy-MM-dd") = '1975-06-22'""")
}
       

//date_12
test("date_12", Include) {
  checkAnswer(s"""select cust_id, cust_name,dob from uniqdata_date where date_format(dob, "yyyy-MM-dd") != '1975-06-22'""",
    s"""select cust_id, cust_name,dob from uniqdata_date_hive where date_format(dob, "yyyy-MM-dd") != '1975-06-22'""")
}
       

//date_13
test("date_13", Include) {
  checkAnswer(s"""select doj from uniqdata_date where regexp_replace(doj, '-', '/') NOT IN ('1975/05/22')""",
    s"""select doj from uniqdata_date_hive where regexp_replace(doj, '-', '/') NOT IN ('1975/05/22')""")
}
       

//date_14
test("date_14", Include) {
  checkAnswer(s"""select doj from uniqdata_date where regexp_replace(doj, '-', '/') IN ('1975/05/22')""",
    s"""select doj from uniqdata_date_hive where regexp_replace(doj, '-', '/') IN ('1975/05/22')""")
}
       

//date_15
test("date_15", Include) {
  checkAnswer(s"""select count(doj) from uniqdata_date""",
    s"""select count(doj) from uniqdata_date_hive""")
}
       

//date_16
test("date_16", Include) {
  checkAnswer(s"""select count(doj) from uniqdata_date group by cust_name""",
    s"""select count(doj) from uniqdata_date_hive group by cust_name""")
}
       

//date_17
test("date_17", Include) {
  checkAnswer(s"""select cust_id, cust_name,dob from uniqdata_date where date_format(dob, "yyyy-MM-dd") > '1975-01-22'""",
    s"""select cust_id, cust_name,dob from uniqdata_date_hive where date_format(dob, "yyyy-MM-dd") > '1975-01-22'""")
}
       

//date_18
test("date_18", Include) {
  checkAnswer(s"""select cust_id, cust_name,dob from uniqdata_date where date_format(dob, "yyyy-MM-dd") < '1975-05-22'""",
    s"""select cust_id, cust_name,dob from uniqdata_date_hive where date_format(dob, "yyyy-MM-dd") < '1975-05-22'""")
}
       

//date_19
test("date_19", Include) {
  checkAnswer(s"""select cust_id, cust_name,dob from uniqdata_date where date_format(dob, "yyyy-MM-dd") >= '1975-01-22'""",
    s"""select cust_id, cust_name,dob from uniqdata_date_hive where date_format(dob, "yyyy-MM-dd") >= '1975-01-22'""")
}
       

//date_20
test("date_20", Include) {
  checkAnswer(s"""select max(doj) from uniqdata_date where doj is not null""",
    s"""select max(doj) from uniqdata_date_hive where doj is not null""")
}
       

//date_21
test("date_21", Include) {
  checkAnswer(s"""select cust_id, cust_name,dob from uniqdata_date where date_format(dob, "yyyy-MM-dd") between '1975-06-20' and '1975-06-22'""",
    s"""select cust_id, cust_name,dob from uniqdata_date_hive where date_format(dob, "yyyy-MM-dd") between '1975-06-20' and '1975-06-22'""")
}
       

//date_22
test("date_22", Include) {
  checkAnswer(s"""select cust_id, cust_name,dob from uniqdata_date where date_format(dob, "yyyy-MM-dd") between '1975-06-20' and '1975-06-22'""",
    s"""select cust_id, cust_name,dob from uniqdata_date_hive where date_format(dob, "yyyy-MM-dd") between '1975-06-20' and '1975-06-22'""")
}
       

//date_23
test("date_23", Include) {
  checkAnswer(s"""select cust_id, cust_name,dob from uniqdata_date where date_format(dob, "yyyy-MM-dd") not between '1975-06-20' and '1975-06-22'""",
    s"""select cust_id, cust_name,dob from uniqdata_date_hive where date_format(dob, "yyyy-MM-dd") not between '1975-06-20' and '1975-06-22'""")
}
       

//date_24
test("date_24", Include) {
  checkAnswer(s"""select unix_timestamp('2016-12-20', 'yyyy-MM-dd') from uniqdata_date""",
    s"""select unix_timestamp('2016-12-20', 'yyyy-MM-dd') from uniqdata_date_hive""")
}
       

//date_25
test("date_25", Include) {
  checkAnswer(s"""select to_date("2016-10-20 05:06:07") from uniqdata_date""",
    s"""select to_date("2016-10-20 05:06:07") from uniqdata_date_hive""")
}
       

//date_26
test("date_26", Include) {
  checkAnswer(s"""select year(dob) from uniqdata_date""",
    s"""select year(dob) from uniqdata_date_hive""")
}
       

//date_27
test("date_27", Include) {
  checkAnswer(s"""select quarter(dob) from uniqdata_date""",
    s"""select quarter(dob) from uniqdata_date_hive""")
}
       

//date_28
test("date_28", Include) {
  checkAnswer(s"""select month(dob) from uniqdata_date""",
    s"""select month(dob) from uniqdata_date_hive""")
}
       

//date_29
test("date_29", Include) {
  checkAnswer(s"""select day(dob) from uniqdata_date""",
    s"""select day(dob) from uniqdata_date_hive""")
}
       

//date_30
test("date_30", Include) {
  checkAnswer(s"""select hour("2016-10-20 05:06:07") from uniqdata_date""",
    s"""select hour("2016-10-20 05:06:07") from uniqdata_date_hive""")
}
       

//date_31
test("date_31", Include) {
  checkAnswer(s"""select minute("2016-10-20 05:06:07") from uniqdata_date""",
    s"""select minute("2016-10-20 05:06:07") from uniqdata_date_hive""")
}
       

//date_32
test("date_32", Include) {
  checkAnswer(s"""select second("2016-10-20 05:06:07") from uniqdata_date""",
    s"""select second("2016-10-20 05:06:07") from uniqdata_date_hive""")
}
       

//date_33
test("date_33", Include) {
  checkAnswer(s"""select datediff('2009-03-01', '2009-02-27') from uniqdata_date""",
    s"""select datediff('2009-03-01', '2009-02-27') from uniqdata_date_hive""")
}
       

//date_34
test("date_34", Include) {
  checkAnswer(s"""select date_add('2008-12-31', 1) from uniqdata_date""",
    s"""select date_add('2008-12-31', 1) from uniqdata_date_hive""")
}
       

//date_35
test("date_35", Include) {
  checkAnswer(s"""select date_add(dob, 1) from uniqdata_date""",
    s"""select date_add(dob, 1) from uniqdata_date_hive""")
}
       

//date_36
test("date_36", Include) {
  checkAnswer(s"""select date_sub('2008-12-31', 1) from uniqdata_date""",
    s"""select date_sub('2008-12-31', 1) from uniqdata_date_hive""")
}
       

//date_37
test("date_37", Include) {
  checkAnswer(s"""select date_sub(dob, 1) from uniqdata_date""",
    s"""select date_sub(dob, 1) from uniqdata_date_hive""")
}
       

//date_38
test("date_38", Include) {
  checkAnswer(s"""select from_utc_timestamp('2016-12-12 08:00:00','PST') from uniqdata_date""",
    s"""select from_utc_timestamp('2016-12-12 08:00:00','PST') from uniqdata_date_hive""")
}
       

//date_39
test("date_39", Include) {
  checkAnswer(s"""select from_utc_timestamp(dob,'IST') from uniqdata_date""",
    s"""select from_utc_timestamp(dob,'IST') from uniqdata_date_hive""")
}
       

//date_40
test("date_40", Include) {
  checkAnswer(s"""select to_utc_timestamp('2017-01-01 00:00:00','PST') from uniqdata_date""",
    s"""select to_utc_timestamp('2017-01-01 00:00:00','PST') from uniqdata_date_hive""")
}
       

//date_41
test("date_41", Include) {
  checkAnswer(s"""select to_utc_timestamp(dob,'IST') from uniqdata_date""",
    s"""select to_utc_timestamp(dob,'IST') from uniqdata_date_hive""")
}
       

//date_43
test("date_43", Include) {
  sql(s"""select current_timestamp() from uniqdata_date""").collect
}
       

//date_44
test("date_44", Include) {
  checkAnswer(s"""select add_months(dob, 5) from uniqdata_date""",
    s"""select add_months(dob, 5) from uniqdata_date_hive""")
}
       

//date_45
test("date_45", Include) {
  checkAnswer(s"""select last_day(dob) from uniqdata_date""",
    s"""select last_day(dob) from uniqdata_date_hive""")
}
       

//date_46
test("date_46", Include) {
  checkAnswer(s"""select next_day('2015-01-14', 'TU') from uniqdata_date""",
    s"""select next_day('2015-01-14', 'TU') from uniqdata_date_hive""")
}
       

//date_47
test("date_47", Include) {
  checkAnswer(s"""select months_between('2016-12-28', '2017-01-30') from uniqdata_date""",
    s"""select months_between('2016-12-28', '2017-01-30') from uniqdata_date_hive""")
}
       

//date_48
test("date_48", Include) {
  checkAnswer(s"""select date_format(dob, 'y') from uniqdata_date""",
    s"""select date_format(dob, 'y') from uniqdata_date_hive""")
}
       

//date_49
test("date_49", Include) {
  checkAnswer(s"""select add_months(dob, 10) from uniqdata_date""",
    s"""select add_months(dob, 10) from uniqdata_date_hive""")
}
       

//date_50
test("date_50", Include) {
  checkAnswer(s"""select date_add(dob, 32) from uniqdata_date""",
    s"""select date_add(dob, 32) from uniqdata_date_hive""")
}
       

//date_51
test("date_51", Include) {
  checkAnswer(s"""select date_sub('2008-12-31', 32) from uniqdata_date""",
    s"""select date_sub('2008-12-31', 32) from uniqdata_date_hive""")
}
       

//date_52
test("date_52", Include) {
  checkAnswer(s"""select date_sub(dob, 32) from uniqdata_date""",
    s"""select date_sub(dob, 32) from uniqdata_date_hive""")
}
       

//date_53
test("date_53", Include) {
  checkAnswer(s"""select to_date("2016-15-50 05:06:07") from uniqdata_date""",
    s"""select to_date("2016-15-50 05:06:07") from uniqdata_date_hive""")
}
       

//date_54
test("date_54", Include) {
  checkAnswer(s"""select add_months('2015-04-08', 10) from uniqdata_date""",
    s"""select add_months('2015-04-08', 10) from uniqdata_date_hive""")
}
       

//date_63
test("date_63", Include) {
  checkAnswer(s"""select cust_name, dob from uniqdata_date ORDER BY cust_name,dob DESC""",
    s"""select cust_name, dob from uniqdata_date_hive ORDER BY cust_name,dob DESC""")
}
       

//date_64
test("date_64", Include) {
  checkAnswer(s"""select dob from uniqdata_date ORDER BY dob ASC""",
    s"""select dob from uniqdata_date_hive ORDER BY dob ASC""")
}
       
override def afterAll {
sql("drop table if exists uniqdata_date")
sql("drop table if exists uniqdata_date_hive")
}
}