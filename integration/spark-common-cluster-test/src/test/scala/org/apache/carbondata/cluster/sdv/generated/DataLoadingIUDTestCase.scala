
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

import java.sql.Timestamp

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util._
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, BeforeAndAfterEach}

/**
 * Test Class for DataLoadingIUDTestCase to verify all scenerios
 */

class DataLoadingIUDTestCase extends QueryTest with BeforeAndAfterAll with BeforeAndAfter with BeforeAndAfterEach {

  override def beforeAll {
    sql("use default").collect
    sql("drop table if exists t_carbn02").collect
    sql("drop table if exists t_carbn01").collect
    sql("drop table if exists T_Parq1").collect
    sql("drop table if exists table_C21").collect
    sql("drop table if exists t_hive01").collect
    sql("drop table if exists t_carbn2").collect
    sql("drop table if exists t_carbn1").collect
    sql("drop table if exists t1").collect
    sql("drop table if exists t2").collect
    sql("drop table if exists t_carbn21").collect
    sql("drop table if exists t_carbn22").collect
    sql("drop table if exists t_carbn23").collect
    sql("drop table if exists t_carbn24").collect
    sql("drop table if exists t_carbn25").collect
    sql("drop table if exists t_carbn26").collect
    sql("drop table if exists t_carbn27").collect
    sql("drop table if exists t_carbn28").collect
    sql("drop table if exists t_carbn20").collect
    sql("drop table if exists t_carbn30").collect
    sql("drop table if exists t_carbn31").collect
    sql("drop table if exists uniqdata0001_Test").collect
    sql("drop table if exists uniqdata").collect
    sql("drop table if exists uniqdata1").collect
    sql("drop table if exists uniqdata2").collect
    sql("drop table if exists uniqdata023456").collect
    sql("drop table if exists t_carbn01b").collect
    sql("drop table if exists T_Hive1").collect
    sql("drop table if exists T_Hive6").collect
    sql(s"""create table default.t_carbn01b(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table default.t_carbn01B options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect

  }

  override def before(fun: => Any) {
    sql(s"""drop table if exists t_carbn01""").collect
    sql(s"""drop table if exists default.t_carbn01""").collect
  }

  override def beforeEach(): Unit = {
    sql(s"""drop table if exists t_carbn01""").collect
    sql(s"""drop table if exists default.t_carbn01""").collect
  }


//NA
test("IUD-01-01-01_001-001", Include) {
   sql("create table T_Hive1(Active_status BOOLEAN, Item_type_cd TINYINT, Qty_day_avg SMALLINT, Qty_total INT, Sell_price BIGINT, Sell_pricep FLOAT, Discount_price DOUBLE , Profit DECIMAL(3,2), Item_code STRING, Item_name VARCHAR(50), Outlet_name CHAR(100), Update_time TIMESTAMP, Create_date DATE) row format delimited fields terminated by ',' collection items terminated by '$'")
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1_hive10.csv' overwrite into table T_Hive1""").collect
 sql("create table T_Hive6(Item_code STRING, Sub_item_cd ARRAY<string>)row format delimited fields terminated by ',' collection items terminated by '$'")
 sql(s"""load data inpath '$resourcesPath/Data/InsertData/T_Hive1_hive11.csv' overwrite into table T_Hive6""").collect
 sql(s"""create table t_carbn02(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into t_carbn02 select * from default.t_carbn01b limit 4""").collect
  checkAnswer(s"""select count(*) from t_carbn01b""",
    Seq(Row(10)), "DataLoadingIUDTestCase_IUD-01-01-01_001-001")

}
       

//Check for update Carbon table using a data value
test("IUD-01-01-01_001-01", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (active_status, profit) = ('YES',1) where active_status = 'TRUE'""").collect
  checkAnswer(s"""select active_status,profit from default.t_carbn01  where active_status='YES' group by active_status,profit""",
    Seq(Row("YES",1.00)), "DataLoadingIUDTestCase_IUD-01-01-01_001-01")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table using a data value on a string column where it was udpated before
test("IUD-01-01-01_001-02", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (active_status) = ('YES') where active_status = 'TRUE'""").collect
 sql(s"""update default.t_carbn01  set (active_status) = ('NO') where active_status = 'YES'""").collect
  checkAnswer(s"""select active_status,profit from default.t_carbn01  where active_status='NO' group by active_status,profit""",
    Seq(Row("NO",2.44)), "DataLoadingIUDTestCase_IUD-01-01-01_001-02")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table using a data value on a string column without giving values in semi quote
test("IUD-01-01-01_001-03", Include) {
  intercept[Exception] {
   sql(s"""drop table IF EXISTS default.t_carbn01""").collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (active_status) = (NO) """).collect
    sql(s"""NA""").collect
    
  }
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table using a data value on a string column using numeric value
test("IUD-01-01-01_001-04", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (active_status) = (234530508098098098080)""").collect
  checkAnswer(s"""select active_status from default.t_carbn01  group by active_status""",
    Seq(Row("234530508098098098080")), "DataLoadingIUDTestCase_IUD -01-01-01_001-04")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table using a data value on a string column using numeric value in single quote
test("IUD-01-01-01_001-05", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (active_status) = ('234530508098098098080')""").collect
  checkAnswer(s"""select active_status from default.t_carbn01  group by active_status""",
    Seq(Row("234530508098098098080")), "DataLoadingIUDTestCase_IUD -01-01-01_001-05")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table using a data value on a string column using decimal value
test("IUD-01-01-01_001-06", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (active_status) = (2.55860986095689088)""").collect
  checkAnswer(s"""select active_status from default.t_carbn01  group by active_status""",
    Seq(Row("2.55860986095689088")), "DataLoadingIUDTestCase_IUD-01 -01-01_001-06")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table using a data value on a string column using decimal value
test("IUD-01-01-01_001-07", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (active_status) = ('2.55860986095689088')""").collect
  checkAnswer(s"""select active_status from default.t_carbn01  group by active_status""",
    Seq(Row("2.55860986095689088")), "DataLoadingIUDTestCase_IUD-01 -01-01_001-07")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table using a data value on a string column using string value which is having special characters
test("IUD-01-01-01_001-11", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (active_status) = ('fdfdskflksdf#?…..fdffs')""").collect
  checkAnswer(s"""select active_status from default.t_carbn01  group by active_status""",
    Seq(Row("fdfdskflksdf#?…..fdffs")), "DataLoadingIUDTestCase_IUD-01-01-01_001-11")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table using a data value on a string column using array value having ')'
//test("IUD-01-01-01_001-12", Include) {
//   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
// sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
// sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
// sql(s"""update default.t_carbn01  set (active_status) = ('abd$asjdh$adasj$l;sdf$*)$*)(&^)')""").collect
//  checkAnswer(s"""select count(*) from t_carbn01b""",
//    Seq(Row(10)), "DataLoadingIUDTestCase_IUD-01-01-01_001-12")
//   sql(s"""drop table default.t_carbn01  """).collect
//}
       

//Check for update Carbon table for a column where column  name is mentioned incorrectly
test("IUD-01-01-01_001-14", Include) {
  intercept[Exception] {
    sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
    sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
    sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
    sql(s"""update default.t_carbn01  set (item_status_cd)  = ('10')""").collect
    sql(s"""NA""").collect
  }
  sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table for a numeric value column
test("IUD-01-01-01_001-15", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (item_type_cd)  = (10)""").collect
  checkAnswer(s"""select item_type_cd from default.t_carbn01  group by item_type_cd""",
    Seq(Row(10)), "DataLoadingIUDTestCase_IUD-01-01-01_001-15")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table for a numeric value column in single quote
test("IUD-01-01-01_001-16", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (item_type_cd)  = ('10')""").collect
  checkAnswer(s"""select item_type_cd from default.t_carbn01  group by item_type_cd""",
    Seq(Row(10)), "DataLoadingIUDTestCase_IUD-01-01-01_001-16")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table for a numeric value column using string value
test("IUD-01-01-01_001-17", Include) {
  intercept[Exception] {
    sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
    sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
    sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
    sql(s"""update default.t_carbn01  set (item_type_cd)  = ('Orange')""").collect
    sql(s"""NA""").collect
  }
  sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table for a numeric value column using decimal value
test("IUD-01-01-01_001-18", Include) {
  intercept[Exception] {
    sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
    sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
    sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
    sql(s"""update default.t_carbn01  set (item_type_cd)  = ('10.11')""").collect
    sql(s"""NA""").collect
  }
  sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table for a numeric Int value column using large numeric value
test("IUD-01-01-01_001-19", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (item_type_cd)  = (2147483647)""").collect
  checkAnswer(s"""select item_type_cd from default.t_carbn01  group by item_type_cd""",
    Seq(Row(2147483647)), "DataLoadingIUDTestCase_IUD-01-01-01_001-19")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table for a numeric Int value column using large numeric negative value
test("IUD-01-01-01_001-20", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (item_type_cd)  = (-2147483648)""").collect
  checkAnswer(s"""select item_type_cd from default.t_carbn01  group by item_type_cd""",
    Seq(Row(-2147483648)), "DataLoadingIUDTestCase_IUD-01-01-01_001-20")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table for a numeric Int value column using large numeric value which is beyond 32 bit
test("IUD-01-01-01_001-21", Include) {
  intercept[Exception] {
    sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
    sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
    sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
    sql(s"""update default.t_carbn01  set (item_type_cd)  = (-2147483649)""").collect
    sql(s"""NA""").collect
  }
  sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table for a numeric BigInt value column using large numeric value which is at the boundary of 64 bit
test("IUD-01-01-01_001-22", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (sell_price)  = (9223372036854775807)""").collect
  checkAnswer(s"""select sell_price from default.t_carbn01  group by sell_price""",
    Seq(Row(9223372036854775807L)), "DataLoadingIUDTestCase_IUD-01-01-01_001-22")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table for a decimal value column using decimal value
test("IUD-01-01-01_001-23", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (profit) = (1.11)""").collect
  checkAnswer(s"""select profit from default.t_carbn01  group by profit""",
    Seq(Row(1.11)), "DataLoadingIUDTestCase_IUD-01-01-01_001-23")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table for a decimal value column using decimal value in quote
test("IUD-01-01-01_001-24", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (profit)  = ('1.11')""").collect
  checkAnswer(s"""select profit from default.t_carbn01  group by profit""",
    Seq(Row(1.11)), "DataLoadingIUDTestCase_IUD-01-01-01_001-24")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table for a decimal value column using numeric value
test("IUD-01-01-01_001-25", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (profit)  = (1)""").collect
  checkAnswer(s"""select profit from default.t_carbn01  group by profit""",
    Seq(Row(1.00)), "DataLoadingIUDTestCase_IUD-01-01-01_001-25")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table for a decimal value column (3,2) using numeric value which is greater than the allowed
test("IUD-01-01-01_001-26", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (profit)  = (10)""").collect
  checkAnswer(s"""select count(Active_status) from default.t_carbn01 where profit = 10 """,
    Seq(Row(0)), "DataLoadingIUDTestCase_IUD-01-01-01_001-26")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table for a decimal value column using String value
test("IUD-01-01-01_001-27", Include) {
  intercept[Exception] {
    sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
    sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
    sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
    sql(s"""update default.t_carbn01  set (profit)  = ('hakshk')""").collect
    sql(s"""NA""").collect
  }
  sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table for a decimal value(3,2) column using a decimal value which is having 1 decimal
test("IUD-01-01-01_001-28", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (profit)  = ('1.1')""").collect
  checkAnswer(s"""select profit from default.t_carbn01  group by profit""",
    Seq(Row(1.10)), "DataLoadingIUDTestCase_IUD-01-01-01_001-28")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table for a decimal value(3,2) column using a decimal value which is having 3 decimal
test("IUD-01-01-01_001-29", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (profit)  = ('1.118')""").collect
  checkAnswer(s"""select profit from default.t_carbn01  group by profit""",
    Seq(Row(1.12)), "DataLoadingIUDTestCase_IUD-01-01-01_001-29")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table for a double column using a decimal value which is having 3 decimal
test("IUD-01-01-01_001-30", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (sell_pricep)  = ('10.1116756')""").collect
  checkAnswer(s"""select sell_pricep from default.t_carbn01  group by sell_pricep""",
    Seq(Row(10.1116756)), "DataLoadingIUDTestCase_IUD-01-01-01_001-30")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table for a time stamp  value column using date timestamp
test("IUD-01-01-01_001-31", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set(update_time) = ('2016-11-04 18:13:59.113')""").collect
  checkAnswer(s"""select update_time from default.t_carbn01  group by update_time""",
    Seq(Row(Timestamp.valueOf("2016-11-04 18:13:59.0"))), "DataLoadingIUDTestCase_IUD-01-01-01_001-31")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table for a time stamp  value column using date timestamp all formats.
test("IUD-01-01-01_001-35", Include) {
  intercept[Exception] {
    sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
    sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
    sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
    sql(s"""update default.t_carbn01  set(update_time) = ('04-11-20004 18:13:59.113')""").collect
    sql(s"""NA""").collect
  }
  sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table for a time stamp  value column using string value
test("IUD-01-01-01_001-32", Include) {
  intercept[Exception] {
    sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
    sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
    sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
    sql(s"""update default.t_carbn01  set(update_time) = ('fhjfhjfdshf')""").collect
    sql(s"""NA""").collect
  }
  sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table for a time stamp  value column using numeric
test("IUD-01-01-01_001-33", Include) {
  intercept[Exception] {
    sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
    sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
    sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
    sql(s"""update default.t_carbn01  set(update_time) = (56546)""").collect
    sql(s"""NA""").collect
  }
  sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table for a time stamp  value column using date 
test("IUD-01-01-01_001-34", Include) {
  intercept[Exception] {
    sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
    sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
    sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
    sql(s"""update default.t_carbn01  set(update_time) = ('2016-11-04')""").collect
    sql(s"""NA""").collect
  }
  sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table for a time stamp  value column using date timestamp
test("IUD-01-01-01_001-36", Include) {
  intercept[Exception] {
    sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
    sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
    sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
    sql(s"""update default.t_carbn01  set(update_time) = ('2016-11-04 18:63:59.113')""").collect
    sql(s"""NA""").collect
  }
  sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table for a time stamp  value column using date timestamp
test("IUD-01-01-01_001-37", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set(update_time) = ('2016-11-04 18:13:59.113435345345433 ')""").collect
  checkAnswer(s"""select update_time from default.t_carbn01  group by update_time""",
    Seq(Row(Timestamp.valueOf("2016-11-04 18:13:59.0"))), "DataLoadingIUDTestCase_IUD-01-01-01_001-37")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check update Carbon table using a * operation on a column value
test("IUD-01-01-01_001-40", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set(profit, item_type_cd)= (profit*1.2, item_type_cd*3)""").collect
  checkAnswer(s"""select profit, item_type_cd from default.t_carbn01  group by profit, item_type_cd""",
    Seq(Row(2.93,342),Row(2.93,369),Row(2.93,3),Row(2.93,6),Row(2.93,9),Row(2.93,12),Row(2.93,33),Row(2.93,39),Row(2.93,42),Row(2.93,123)), "DataLoadingIUDTestCase_IUD-01-01-01_001-40")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check update Carbon table using a / operation on a column value
test("IUD-01-01-01_001-41", Include) {
  intercept[Exception] {
    sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
    sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
    sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
    sql(s"""update default.t_carbn01  set(item_type_cd)= (item_type_cd/1)""").collect
    sql(s"""NA""").collect
  }
  sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check update Carbon table using a / operation on a column value
test("IUD-01-01-01_001-42", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set(profit)= (profit/1)""").collect
  checkAnswer(s"""select profit from default.t_carbn01  group by profit""",
    Seq(Row(2.44)), "DataLoadingIUDTestCase_IUD-01-01-01_001-42")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check update Carbon table using a - operation on a column value
test("IUD-01-01-01_001-43", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set(profit, item_type_cd)= (profit-1.2, item_type_cd-3)""").collect
  checkAnswer(s"""select profit, item_type_cd from default.t_carbn01  group by profit, item_type_cd""",
    Seq(Row(1.24,111),Row(1.24,120),Row(1.24,0),Row(1.24,-1),Row(1.24,-2),Row(1.24,1),Row(1.24,8),Row(1.24,10),Row(1.24,11),Row(1.24,38)), "DataLoadingIUDTestCase_IUD-01-01-01_001-43")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check update Carbon table using a + operation on a column value
test("IUD-01-01-01_001-44", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set(profit, item_type_cd)= (profit+1.2, item_type_cd+qty_day_avg)""").collect
  checkAnswer(s"""select profit, item_type_cd from default.t_carbn01  where profit = 3.64 and item_type_cd = 4291""",
    Seq(Row(3.64,4291)), "DataLoadingIUDTestCase_IUD-01-01-01_001-44")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check update Carbon table using a + operation on a column value which is string
test("IUD-01-01-01_001-45", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set(item_code) = (item_code+1)""").collect
  checkAnswer(s"""select count(*) from t_carbn01""",
    Seq(Row(10)), "DataLoadingIUDTestCase_IUD-01-01-01_001-45")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table without where clause
test("IUD-01-01-01_002-01", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""Update default.t_carbn01  set (active_status) = ('NO')""").collect
  checkAnswer(s"""select active_status from default.t_carbn01  group by active_status""",
    Seq(Row("NO")), "DataLoadingIUDTestCase_IUD-01-01-01_002-01")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table with where clause
test("IUD-01-01-01_002-02", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""Update default.t_carbn01  set (active_status) = ('NO') where active_status = 'TRUE' """).collect
  checkAnswer(s"""select active_status from default.t_carbn01  where active_status='NO' group by active_status""",
    Seq(Row("NO")), "DataLoadingIUDTestCase_IUD-01-01-01_002-02")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table with where exists clause
test("IUD-01-01-01_002-03", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""Update default.t_carbn01  X set (active_status) = ('NO') where exists (select 1 from default.t_carbn01b Y where Y.item_code = X.item_code)""").collect
  checkAnswer(s"""select active_status from default.t_carbn01   group by active_status""",
    Seq(Row("NO")), "DataLoadingIUDTestCase_IUD-01-01-01_002-03")
   sql(s"""drop table default.t_carbn01""").collect
}
       

//Check for delete Carbon table without where clause
test("IUD-01-01-01_002-04", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""Delete from default.t_carbn01 """).collect
  checkAnswer(s"""select count(*) from default.t_carbn01 """,
    Seq(Row(0)), "DataLoadingIUDTestCase_IUD-01-01-01_002-04")
   sql(s"""drop table default.t_carbn01 """).collect
}
       

//Check for delete Carbon table with where clause
test("IUD-01-01-01_002-05", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""Delete from default.t_carbn01  where active_status = 'TRUE'""").collect
  checkAnswer(s"""select count(*) from default.t_carbn01  where active_status='TRUE'""",
    Seq(Row(0)), "DataLoadingIUDTestCase_IUD-01-01-01_002-05")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for delete Carbon table with where exists clause
test("IUD-01-01-01_002-06", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""Delete from default.t_carbn01  X where exists (select 1 from default.t_carbn01b Y where Y.item_code = X.item_code)""").collect
  checkAnswer(s"""select count(*) from default.t_carbn01 """,
    Seq(Row(0)), "DataLoadingIUDTestCase_IUD-01-01-01_002-06")
   sql(s"""drop table default.t_carbn01  """).collect
}
       
//Check for update Carbon table using query involving filters
test("IUD-01-01-01_003-03", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  a set ( a.item_type_cd, a.profit) = ( select b.item_type_cd, b.profit from default.t_carbn01b b where b.item_type_cd = 2)""").collect
  checkAnswer(s"""select item_type_cd, profit from default.t_carbn01  limit 1""",
    Seq(Row(2,2.44)), "DataLoadingIUDTestCase_IUD-01-01-01_003-03")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table using query involving sub query
test("IUD-01-01-01_003-04", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  a set ( a.item_type_cd, a.Profit) = ( select b.item_type_cd, b.profit from default.t_carbn01b b where a.item_type_cd = b.item_type_cd and b.item_type_cd in (select c.item_type_cd from t_carbn02 c where c.item_type_cd=2))""").collect
  checkAnswer(s"""select item_type_cd, profit from default.t_carbn01 order by item_type_cd limit 1""",
    Seq(Row(1,2.44)), "DataLoadingIUDTestCase_IUD-01-01-01_003-04")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table using query involving sub query
test("IUD-01-01-01_003-04_01", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  a set (a.item_type_cd, a.Profit) = (select b.item_type_cd, b.profit from default.t_carbn01b b where b.item_type_cd not in (select c.item_type_cd from t_carbn02 c where c.item_type_cd != 2) and a.item_type_cd = b.item_type_cd)""").collect
  checkAnswer(s"""select item_type_cd, profit from default.t_carbn01 order by item_type_cd limit 1""",
    Seq(Row(1,2.44)), "DataLoadingIUDTestCase_IUD-01-01-01_003-04_01")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table using query involving Logical operation
test("IUD-01-01-01_003-05", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  A set (a.item_type_cd, a.profit) = ( select b.item_type_cd, b.profit from default.t_carbn01b b where b.profit > 1 AND b.item_type_cd <3 and a.item_type_cd = b.item_type_cd)""").collect
  checkAnswer(s"""select item_type_cd, profit from default.t_carbn01 order by item_type_cd limit 1""",
    Seq(Row(1,2.44)), "DataLoadingIUDTestCase_IUD-01-01-01_003-05")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table using query involving group by
test("IUD-01-01-01_003-06", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  a set (a.item_type_cd, a.profit) = ( select b.item_type_cd, b.profit from default.t_carbn01b b where b.item_type_cd =2)""").collect
  checkAnswer(s"""select item_type_cd, profit from default.t_carbn01 limit 1""",
    Seq(Row(2,2.44)), "DataLoadingIUDTestCase_IUD-01-01-01_003-06")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table using inner join and filter condition on a table to pick only non duplicate records
test("IUD-01-01-01_003-07", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update t_carbn01 a set (a.active_status) = (select b.active_status from t_carbn01b b where a.item_type_cd = b.item_type_cd and b.item_code in (select item_code from t_carbn01b group by item_code, profit having count(*)>1))""").collect
  checkAnswer(s"""select count(active_status) from t_carbn01 where active_status = 'true' limit 1""",
    Seq(Row(0)), "DataLoadingIUDTestCase_IUD-01-01-01_003-07")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table using query involving max
test("IUD-01-01-01_004-01", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update t_carbn01  a set (a.item_type_cd) = ((select c.code from (select max(b.item_type_cd) as code  from t_carbn01b b) c))""").collect
  checkAnswer(s"""select item_type_cd from default.t_carbn01 limit 1""",
    Seq(Row(123)), "DataLoadingIUDTestCase_IUD-01-01-01_004-01")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table using query involving spark functions
test("IUD-01-01-01_004-02", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  a set (a.create_date) = (select to_date(b.create_date) from default.t_carbn01b b where b.update_time = '2012-01-06 06:08:05.0')""").collect
  checkAnswer(s"""select create_date from default.t_carbn01 limit 1""",
    Seq(Row("2012-01-20")), "DataLoadingIUDTestCase_IUD-01-01-01_004-02")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table for all data types using data values
test("IUD-01-01-01_004-03", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (active_status,item_type_cd,qty_day_avg,qty_total,sell_price,sell_pricep,discount_price,profit,item_code,item_name,outlet_name,update_time,create_date) = ('true',34,344,456,1,5.5,1.1,1.1,'hheh','gfhfhfdh','fghfdhdfh',current_timestamp,'01-10-1900') where item_code='ASD423ee'""").collect
  checkAnswer(s"""select create_date from default.t_carbn01  where create_date = '01-10-1900' limit 1""",
    Seq(Row("01-10-1900")), "DataLoadingIUDTestCase_IUD-01-01-01_004-03")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table where source table is havign numeric and target is having string value column for update
test("IUD-01-01-01_004-04", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  a set (a.item_code) = (select b.sell_price from default.t_carbn01b b where b.sell_price=200000000003454300)""").collect
  checkAnswer(s"""select count(*) from default.t_carbn01 """,
    Seq(Row(10)), "DataLoadingIUDTestCase_IUD-01-01-01_004-04")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table where source table is havign numeric and target is having decimal value column for update
test("IUD-01-01-01_004-05", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  a set (a.profit) = (select b.item_type_cd from default.t_carbn01b b where b.item_type_cd = 2 and b.active_status='TRUE' )""").collect
  checkAnswer(s"""select profit from default.t_carbn01  limit 1""",
    Seq(Row(2.00)), "DataLoadingIUDTestCase_IUD-01-01-01_004-05")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table where source table is having big int and target is having int value column for update
test("IUD-01-01-01_004-06", Include) {
  intercept[Exception] {
    sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
    sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
    sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
    sql(s"""update default.t_carbn01  a set (a.item_type_cd) = (select b.sell_price from default.t_carbn01b b where b.sell_price=200000343430000000)""").collect
    sql(s"""NA""").collect
  }
  sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table where source table is having string and target is having numeric value column for update
test("IUD-01-01-01_004-07", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  a set (a.item_code) = (select b.item_type_cd from default.t_carbn01b b where b.item_code='DE3423ee')""").collect
  checkAnswer(s"""select item_type_cd from default.t_carbn01  order by item_type_cd limit 1""",
    Seq(Row(1)), "DataLoadingIUDTestCase_IUD-01-01-01_004-07")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table where source table is having string and target is having decimal value column for update
test("IUD-01-01-01_004-08", Include) {
  intercept[Exception] {
    sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
    sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
    sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
    sql(s"""update default.t_carbn01  a set (a.profit) = (select b.item_code from default.t_carbn01b b where b.item_code='DE3423ee')""").collect
    sql(s"""NA""").collect
  }
  sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table where source table is having string and target is having timestamp column for update
test("IUD-01-01-01_004-09", Include) {
  intercept[Exception] {
    sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
    sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
    sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
    sql(s"""update default.t_carbn01  a set (a.update_time) = (select b.item_code from default.t_carbn01b b where b.item_code='DE3423ee')""").collect
    sql(s"""NA""").collect
  }
  sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table where source table is having decimal and target is having numeric column for update
test("IUD-01-01-01_004-10", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  a set (a.item_type_cd) = (select b.profit from default.t_carbn01b b where b.profit=2.445)""").collect
  checkAnswer(s"""select count(*) from default.t_carbn01 """,
    Seq(Row(10)), "DataLoadingIUDTestCase_IUD-01-01-01_004-10")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table where source table is having float and target is having numeric column for update
test("IUD-01-01-01_004-11", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  a set (a.item_type_cd) = (select b.sell_pricep from default.t_carbn01b b where b.sell_pricep=11.5)""").collect
  checkAnswer(s"""select count(*) from default.t_carbn01 """,
    Seq(Row(10)), "DataLoadingIUDTestCase_IUD-01-01-01_004-11")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table where source table is having float and target is having double column for update
test("IUD-01-01-01_004-12", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  a set (a.discount_price) = (select b.sell_pricep from default.t_carbn01b b where b.sell_pricep=11.5)""").collect
  checkAnswer(s"""select count(*) from default.t_carbn01 """,
    Seq(Row(10)), "DataLoadingIUDTestCase_IUD-01-01-01_004-12")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table where source table is having Decimal(4,3)   and target is having Decimal(3,2) column for update
test("IUD-01-01-01_004-13", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  a set (a.profit) = (select b.profit*.2 from default.t_carbn01b b where b.profit=2.444)""").collect
  checkAnswer(s"""select count(*) from default.t_carbn01 """,
    Seq(Row(10)), "DataLoadingIUDTestCase_IUD-01-01-01_004-13")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table for all data types using query on a different table
test("IUD-01-01-01_004-14", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  a set (a.Active_status,a.Item_type_cd,a.Qty_day_avg,a.Qty_total,a.Sell_price,a.Sell_pricep,a.Discount_price,a.Profit,a.Item_code,a.Item_name,a.Outlet_name,a.Update_time,a.Create_date) = (select b.Active_status,b.Item_type_cd,b.Qty_day_avg,b.Qty_total,b.Sell_price,b.Sell_pricep,b.Discount_price,b.Profit,b.Item_code,b.Item_name,b.Outlet_name,b.Update_time,b.Create_date from default.t_carbn01b b where b.Item_type_cd=2)""").collect
  checkAnswer(s"""select count(*) from default.t_carbn01""",
    Seq(Row(10)), "DataLoadingIUDTestCase_IUD-01-01-01_004-14")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table where a update column is having a shared dictionary. Check dictionary file being updated.
test("IUD-01-01-01_005-11", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format' TBLPROPERTIES("COLUMNPROPERTIES.Item_code.shared_column"="sharedFolder.Item_code")""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""Update default.t_carbn01  set (item_code) = ('Ram')""").collect
  checkAnswer(s"""select item_code from default.t_carbn01  group by item_code""",
    Seq(Row("Ram")), "DataLoadingIUDTestCase_IUD-01-01-01_005-11")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table where a update column is measue and is defined with include ddictionary. Check dictionary file being updated.
test("IUD-01-01-01_005-12", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Item_type_cd INT, Profit DECIMAL(3,2))STORED BY 'org.apache.carbondata.format' """).collect
 sql(s"""insert into default.t_carbn01  select item_type_cd, profit from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (item_type_cd) = (100100)""").collect
  checkAnswer(s"""select item_type_cd from default.t_carbn01  group by item_type_cd""",
    Seq(Row(100100)), "DataLoadingIUDTestCase_IUD-01-01-01_005-12")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table where a update column is dimension and is defined with exclude dictionary. 
test("IUD-01-01-01_005-13", Include) {
  sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
  sql(s"""create table default.t_carbn01 (Item_type_cd INT, Profit DECIMAL(3,2))STORED BY 'org.apache.carbondata.format' """).collect
  sql(s"""insert into default.t_carbn01  select item_type_cd, profit from default.t_carbn01b""").collect
  val currProperty = CarbonProperties.getInstance().getProperty(CarbonCommonConstants
    .CARBON_BAD_RECORDS_ACTION);
  CarbonProperties.getInstance()
    .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, "FAIL")
  intercept[Exception] {
    sql(s"""update default.t_carbn01  set (item_type_cd) = ('ASASDDD')""").collect
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, currProperty)
  }
  CarbonProperties.getInstance()
    .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, currProperty)
  sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table where a update column is dimension and is defined with exclude dictionary. 
test("IUD-01-01-01_005-14", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format' """).collect
 sql(s""" insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (Item_code) = ('Ram')""").collect
  checkAnswer(s"""select item_code from default.t_carbn01  group by item_code""",
    Seq(Row("Ram")), "DataLoadingIUDTestCase_IUD-01-01-01_005-14")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update Carbon table where a update column is dimension and is defined with exclude dictionary. 
test("IUD-01-01-01_005-15", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format' """).collect
 sql(s""" insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (Item_code) = ('123')""").collect
  checkAnswer(s"""select item_code from default.t_carbn01  group by item_code""",
    Seq(Row("123")), "DataLoadingIUDTestCase_IUD-01-01-01_005-15")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check update on data in multiple blocks
test("IUD-01-01-01_006-01", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (item_code) = ('Ram' ) where Item_code = 'RE3423ee'""").collect
  sql(s"""select Item_code from default.t_carbn01  where Item_code = 'RE3423ee' group by item_code""").collect
  
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check update on data in multiple blocks
test("IUD-01-01-01_007-01", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (item_type_cd) = ('120') where Item_type_cd = '114'""").collect
  checkAnswer(s"""select item_type_cd from default.t_carbn01   where item_type_cd = 120 group by item_type_cd""",
    Seq(Row(120)), "DataLoadingIUDTestCase_IUD-01-01-01_007-01")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//check update using parquet table
test("IUD-01-01-02_022-01", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""drop table if exists T_Parq1""").collect
 sql(s"""create table T_Parq1(Active_status BOOLEAN, Item_type_cd TINYINT, Qty_day_avg SMALLINT, Qty_total INT, Sell_price BIGINT, Sell_pricep FLOAT, Discount_price DOUBLE , Profit DECIMAL(3,2), Item_code STRING, Item_name VARCHAR(500), Outlet_name CHAR(100), Update_time TIMESTAMP, Create_date DATE)stored as parquet""").collect
 sql(s"""insert into T_Parq1 select * from t_hive1""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  a set (a.Active_status,a.Item_type_cd,a.Qty_day_avg,a.Qty_total,a.Sell_price,a.Sell_pricep,a.Discount_price,a.Profit,a.Item_code,a.Item_name,a.Outlet_name,a.Update_time,a.Create_date) = (select b.Active_status,b.Item_type_cd,b.Qty_day_avg,b.Qty_total,b.Sell_price,b.Sell_pricep,b.Discount_price,b.Profit,b.Item_code,b.Item_name,b.Outlet_name,b.Update_time,b.Create_date from T_Parq1 b where a.item_type_cd = b.item_type_cd)""").collect
  checkAnswer(s"""select profit from default.t_carbn01   group by profit""",
    Seq(Row(2.44)), "DataLoadingIUDTestCase_IUD-01-01-02_022-01")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check update on carbon table using query on Parquet table
test("IUD-01-01-01_009-01", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""drop table if exists T_Parq1""").collect
 sql(s"""create table T_Parq1(Active_status BOOLEAN, Item_type_cd TINYINT, Qty_day_avg SMALLINT, Qty_total INT, Sell_price BIGINT, Sell_pricep FLOAT, Discount_price DOUBLE , Profit DECIMAL(3,2), Item_code STRING, Item_name VARCHAR(500), Outlet_name CHAR(100), Update_time TIMESTAMP, Create_date DATE)stored as parquet""").collect
 sql(s"""insert into T_Parq1 select * from t_hive1""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  a set (a.Active_status,a.Item_type_cd,a.Qty_day_avg,a.Qty_total,a.Sell_price,a.Sell_pricep,a.Discount_price,a.Profit,a.Item_code,a.Item_name,a.Outlet_name,a.Update_time,a.Create_date) = (select b.Active_status,b.Item_type_cd,b.Qty_day_avg,b.Qty_total,b.Sell_price,b.Sell_pricep,b.Discount_price,b.Profit,b.Item_code,b.Item_name,b.Outlet_name,b.Update_time,b.Create_date from T_Parq1 b where a.item_type_cd = b.item_type_cd)""").collect
  checkAnswer(s"""select profit from default.t_carbn01   group by profit""",
    Seq(Row(2.44)), "DataLoadingIUDTestCase_IUD-01-01-01_009-01")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check update on carbon table using incorrect data value
test("IUD-01-01-01_010-01", Include) {
  intercept[Exception] {
    sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
    sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
    sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
    sql(s"""update default.t_carbn01  set Update_time = '11-11-2012 77:77:77') where item_code='ASD423ee')""").collect
    sql(s"""NA""").collect
  }
  sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check multiple updates on the same column - for correctness of data and horizontal compaction of delta file
test("IUD-01-01-02_001-02", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""Update default.t_carbn01  set (Item_code) = ('Ram')""").collect
 sql(s"""Update default.t_carbn01  set (Item_code) = ('Orange') where Item_code = 'Ram'""").collect
 sql(s"""Update default.t_carbn01  set (Item_code) = ('Ram') where Item_code = 'Orange'""").collect
 sql(s"""Update default.t_carbn01  set (Item_code) = ('Orange') where Item_code = 'Ram'""").collect
 sql(s"""Update default.t_carbn01  set (Item_code) = ('Ram') where Item_code = 'Orange'""").collect
 sql(s"""Update default.t_carbn01  set (Item_code) = ('Orange') where Item_code = 'Ram'""").collect
 sql(s"""Update default.t_carbn01  set (Item_code) = ('Ram') where Item_code = 'Orange'""").collect
 sql(s"""Update default.t_carbn01  set (item_code) = ('Orange') where item_code = 'Ram'""").collect
  checkAnswer(s"""select item_code from default.t_carbn01  group by item_code""",
    Seq(Row("Orange")), "DataLoadingIUDTestCase_IUD-01-01-02_001-02")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for compaction of delta files within a segment working fine as per the configuration
test("IUD-01-01-02_003-01", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""Update default.t_carbn01  set (Item_code) = ('Ram')""").collect
 sql(s"""Update default.t_carbn01  set (Item_code) = ('Orange') where Item_code = 'Ram'""").collect
 sql(s"""Update default.t_carbn01  set (Item_code) = ('Ram') where Item_code = 'Orange'""").collect
 sql(s"""Update default.t_carbn01  set (Item_code) = ('Orange') where Item_code = 'Ram'""").collect
 sql(s"""Update default.t_carbn01  set (Item_code) = ('Ram') where Item_code = 'Orange'""").collect
 sql(s"""Update default.t_carbn01  set (Item_code) = ('Orange') where Item_code = 'Ram'""").collect
 sql(s"""Update default.t_carbn01  set (Item_code) = ('Ram') where Item_code = 'Orange'""").collect
 sql(s"""Update default.t_carbn01  set (item_code) = ('Orange') where item_code = 'Ram'""").collect
  checkAnswer(s"""select item_code from default.t_carbn01  group by item_code""",
    Seq(Row("Orange")), "DataLoadingIUDTestCase_IUD-01-01-02_003-01")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check multiple updates on the same column - for correctness of data along with horizontal compaction of delta file
test("IUD-01-01-02_002-01", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""Update default.t_carbn01  set (Item_code) = ('Ram')""").collect
 sql(s"""Update default.t_carbn01  set (Item_code) = ('Orange') where Item_code = 'Ram'""").collect
 sql(s"""Update default.t_carbn01  set (Item_code) = ('Ram') where Item_code = 'Orange'""").collect
 sql(s"""Update default.t_carbn01  set (Item_code) = ('Orange') where Item_code = 'Ram'""").collect
 sql(s"""Update default.t_carbn01  set (Item_code) = ('Ram') where Item_code = 'Orange'""").collect
 sql(s"""Update default.t_carbn01  set (Item_code) = ('Orange') where Item_code = 'Ram'""").collect
 sql(s"""Update default.t_carbn01  set (Item_code) = ('Ram') where Item_code = 'Orange'""").collect
 sql(s"""Update default.t_carbn01  set (item_code) = ('Orange') where item_code = 'Ram'""").collect
  checkAnswer(s"""select item_code from default.t_carbn01  group by item_code""",
    Seq(Row("Orange")), "DataLoadingIUDTestCase_IUD-01-01-02_002-01")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check multiple updates on the different column - for correctness of data and horizontal compaction of delta file
test("IUD-01-01-01_012-01", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""Update default.t_carbn01  set (Item_code) = ('Orange')""").collect
 sql(s"""update default.t_carbn01  set (Item_type_cd) = (24523)""").collect
 sql(s"""Update default.t_carbn01  set (Item_code) = ('Banana')""").collect
 sql(s"""update default.t_carbn01  set (Item_type_cd) = (1111)""").collect
 sql(s"""Update default.t_carbn01  set (Item_code) = ('Orange')""").collect
 sql(s"""update default.t_carbn01  set (Item_type_cd) = (24523)""").collect
 sql(s"""Update default.t_carbn01 set (Item_code) = ('Banana')""").collect
 sql(s"""update default.t_carbn01  set (Item_type_cd) = (1111)""").collect
 sql(s"""Update default.t_carbn01  set (Item_code) = ('Orange')""").collect
 sql(s"""update default.t_carbn01  set (Item_type_cd) = (24523)""").collect
 sql(s"""Update default.t_carbn01  set (Item_code) = ('Banana')""").collect
 sql(s"""update default.t_carbn01  set (Item_type_cd) = (1111)""").collect
  checkAnswer(s"""select item_code from default.t_carbn01  group by item_code""",
    Seq(Row("Banana")), "DataLoadingIUDTestCase_IUD-01-01-01_012-01")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for delta files handling during table compaction and not breaking the data integrity
test("IUD-01-01-02_004-01", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""Update default.t_carbn01  set (Item_code) = ('Ram') """).collect
 sql(s"""Update default.t_carbn01  set (Item_code) = ('Orange') where Item_code = 'Ram'""").collect
 sql(s"""Update default.t_carbn01  set (Item_code) = ('Ram') where Item_code = 'Orange'""").collect
 sql(s"""ALTER TABLE T_Carbn01 COMPACT 'MINOR'""").collect
 sql(s"""select item_code from default.t_carbn01  group by item_code""").collect
  checkAnswer(s"""select item_code from t_carbn01  group by item_code""",
    Seq(Row("Ram")), "DataLoadingIUDTestCase_IUD-01-01-02_004-01")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check update by doing data insert before and after update also check data consistency, no residual file left in HDFS
test("IUD-01-01-02_006-01", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b """).collect
 sql(s"""update default.t_carbn01  set (profit) = (1.2) where item_type_cd = 2 """).collect
 sql(s"""insert into t_carbn01 select * from t_carbn01b""").collect
  checkAnswer(s"""select count(profit) from default.t_carbn01""",
    Seq(Row(20)), "DataLoadingIUDTestCase_IUD-01-01-02_006-01")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check update by doing data load before and after update also check data consistency, no residual file left in HDFS
test("IUD-01-01-02_006-02", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b """).collect
 sql(s"""update default.t_carbn01  set (profit) = (1.2) where item_type_cd = 2 """).collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table default.t_carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""select count(*) from default.t_carbn01""").collect
  checkAnswer(s"""select count(profit) from default.t_carbn01""",
    Seq(Row(20)), "DataLoadingIUDTestCase_IUD-01-01-02_006-02")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//do a delete rows after update and see that the updated columns are deleted
test("IUD-01-01-02_006-12", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b """).collect
 sql(s"""update default.t_carbn01  set (profit) = (1.2) where item_type_cd = 2 """).collect
 sql(s"""delete from default.t_carbn01  where profit = 1.2 """).collect
  sql(s"""select count(profit) from default.t_carbn01  where (profit=1.2) or (item_type_cd=2)  group by profit""").collect
  
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//do an update after delete rows and see that update is not done on the deleted rows(should not fethch those rows)
test("IUD-01-01-02_006-13", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b """).collect
 sql(s"""delete from default.t_carbn01  where item_type_cd = 2""").collect
 sql(s"""update default.t_carbn01  set (profit) = (1.22) where item_type_cd = 2""").collect
  sql(s"""select count(profit) from default.t_carbn01  where profit = 1.22 group by profit""").collect
  
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Run select query with count(column) after update and esnure the correct count is fetched
test("IUD-01-01-01_014-01", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b """).collect
 sql(s"""update default.t_carbn01  set (profit) = (1.2) where item_type_cd = 2""").collect
  checkAnswer(s"""select count(profit) from  default.t_carbn01  where profit=1.2 """,
    Seq(Row(1)), "DataLoadingIUDTestCase_IUD-01-01-01_014-01")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Run select query with count(*) after delete and esnure the correct count is fetched
test("IUD-01-01-01_014-02", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""delete from default.t_carbn01  where item_type_cd = 2""").collect
  checkAnswer(s"""select count(*) from  default.t_carbn01  where item_type_cd = 2""",
    Seq(Row(0)), "DataLoadingIUDTestCase_IUD-01-01-01_014-02")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Run select query with a filter condition after update and esnure the correct count is fetched
test("IUD-01-01-01_014-03", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (profit) = (1.2) where item_type_cd = 2""").collect
  checkAnswer(s"""select count(profit) from  default.t_carbn01  where profit=1.2""",
    Seq(Row(1)), "DataLoadingIUDTestCase_IUD-01-01-01_014-03")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Run select query with a filter condition after delete and esnure the correct count is fetched
test("IUD-01-01-01_014-04", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""delete from default.t_carbn01  where item_type_cd = 2""").collect
  checkAnswer(s"""select count(*) from  default.t_carbn01  where item_type_cd = 2""",
    Seq(Row(0)), "DataLoadingIUDTestCase_IUD-01-01-01_014-04")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Run select * on table after update operation and ensure the correct data is fetched
test("IUD-01-01-01_014-05", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (profit) = (1.2) where item_type_cd = 2""").collect
  checkAnswer(s"""select profit from default.t_carbn01  where profit = 1.2 group by profit""",
    Seq(Row(1.20)), "DataLoadingIUDTestCase_IUD-01-01-01_014-05")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Run select (coumn) on table after update operation and ensure the correct data is fetched
test("IUD-01-01-01_014-06", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (profit) = (1.2) where item_type_cd = 2""").collect
  checkAnswer(s"""select profit from default.t_carbn01  where profit = 1.2 group by profit""",
    Seq(Row(1.20)), "DataLoadingIUDTestCase_IUD-01-01-01_014-06")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Run select * on table after delete operation and ensure the correct data is fetched
test("IUD-01-01-01_014-07", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b """).collect
 sql(s"""delete from default.t_carbn01 """).collect
  checkAnswer(s"""select count(*) from  default.t_carbn01  """,
    Seq(Row(0)), "DataLoadingIUDTestCase_IUD-01-01-01_014-07")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Run select (coumn) on table after delete operation and ensure the correct data is fetched
test("IUD-01-01-01_014-08", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b """).collect
 sql(s"""delete from default.t_carbn01 """).collect
  sql(s"""select profit from  default.t_carbn01 """).collect
  
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Run select query joining another carbon table after update is done and check that correct data is fetched
test("IUD-01-01-01_014-09", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b """).collect
 sql(s"""update default.t_carbn01  set (item_type_cd) = (20) where item_type_cd in (2)""").collect
  checkAnswer(s""" select c.item_type_cd from default.t_carbn01  c  where exists(select a.item_type_cd from default.t_carbn01  a, default.t_carbn01b b  where a.item_type_cd = b.item_type_cd)and c.item_type_cd = 20""",
    Seq(Row(20)), "DataLoadingIUDTestCase_IUD-01-01-01_014-09")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Run select query joining another carbon table after delete  is done and check that correct data is fetched
test("IUD-01-01-01_014-10", Include) {
   sql(s"""drop table IF EXISTS default.t_carbn01 """).collect
 sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""delete from default.t_carbn01  where qty_day_avg < 4550""").collect
  checkAnswer(s"""select a.qty_day_avg, a.item_code from default.t_carbn01  a, default.t_carbn01b b  where a.qty_day_avg = b.qty_day_avg """,
    Seq(Row(4590,"ASD423ee")), "DataLoadingIUDTestCase_IUD-01-01-01_014-10")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Run select query with limit condition after delete is done and check that correct data is fetched
test("IUD-01-01-01_014-15", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""delete from default.t_carbn01  where item_type_cd = 2""").collect
  checkAnswer(s"""select count(*) from default.t_carbn01  where qty_day_avg >= 4500 limit 3 """,
    Seq(Row(4)), "DataLoadingIUDTestCase_IUD-01-01-01_014-15")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Run select query when the data is distrbuted in multiple blocks(do multiple insert on the table) after an update and check the correct data is fetched
test("IUD-01-01-01_014-16", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (item_type_cd) = (20) where item_type_cd < 10""").collect
  checkAnswer(s"""select count(*) from default.t_carbn01  where item_type_cd < 10""",
    Seq(Row(0)), "DataLoadingIUDTestCase_IUD-01-01-01_014-16")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Run select query when the data is distrbuted in single blocks(do single insert on the table and keep data small) after an update and check the correct data is fetched
test("IUD-01-01-01_014-17", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s""" update default.t_carbn01  set (item_type_cd) = (20) where item_type_cd < 10""").collect
  checkAnswer(s"""select count(*) from default.t_carbn01  where item_type_cd =20""",
    Seq(Row(4)), "DataLoadingIUDTestCase_IUD-01-01-01_014-17")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Run select query when the data is distrbuted in multiple blocks(do multiple insert on the table) after an delete and check the correct data is fetched
test("IUD-01-01-01_014-18", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""delete from default.t_carbn01   where item_type_cd < 10""").collect
  checkAnswer(s"""select count(*) from default.t_carbn01  where item_type_cd < 10""",
    Seq(Row(0)), "DataLoadingIUDTestCase_IUD-01-01-01_014-18")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check data consistency when select is executed after multiple updates on different columns
test("IUD-01-01-01_015-01", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""Update default.t_carbn01  set (item_code) = ('Orange')""").collect
 sql(s"""update default.t_carbn01  set (item_type_cd) = (24523)""").collect
 sql(s"""Update default.t_carbn01  set (item_code) = ('Banana')""").collect
 sql(s"""update default.t_carbn01  set (item_type_cd) = (1111)""").collect
 sql(s"""Update default.t_carbn01  set (item_code) = ('Orange')""").collect
 sql(s"""update default.t_carbn01  set (item_type_cd) = (24523)""").collect
 sql(s"""Update default.t_carbn01  set (item_code) = ('Banana')""").collect
 sql(s"""update default.t_carbn01  set (item_type_cd) = (1111)""").collect
 sql(s"""Update default.t_carbn01  set (item_code) = ('Orange')""").collect
 sql(s"""update default.t_carbn01  set (item_type_cd) = (24523)""").collect
 sql(s"""Update default.t_carbn01  set (item_code) = ('Banana')""").collect
 sql(s"""update default.t_carbn01  set (item_type_cd) = (1111)""").collect
 sql(s"""select item_code, item_type_cd from default.t_carbn01  group by item_code, item_type_cd""").collect
  checkAnswer(s"""select count(*) from default.t_carbn01  where item_code = 'Banana'""",
    Seq(Row(10)), "DataLoadingIUDTestCase_IUD-01-01-01_015-01")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check data consistency when select is executed after multiple updates on same row and same columns
test("IUD-01-01-01_016-01", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""Update default.t_carbn01  set (item_code) = ('Ram')""").collect
 sql(s"""update default.t_carbn01  set (item_code) = ('Orange')""").collect
 sql(s"""Update default.t_carbn01  set (item_code) = ('Ram')""").collect
 sql(s"""update default.t_carbn01  set (item_code) = ('Orange')""").collect
 sql(s"""Update default.t_carbn01  set (item_code) = ('Ram')""").collect
 sql(s"""update default.t_carbn01  set (item_code) = ('Orange')""").collect
  checkAnswer(s"""select count(*) from default.t_carbn01  where item_code = 'Orange'""",
    Seq(Row(10)), "DataLoadingIUDTestCase_IUD-01-01-01_016-01")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Run select query on the updated column after multiple updates on the same column at different rows(control this using where condition) and enforce horizontal compaction and see that there is no data loss
test("IUD-01-01-01_016-02", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""Update default.t_carbn01  set (Item_code) = ('Orange')""").collect
 sql(s"""update default.t_carbn01  set (Item_type_cd) = (24523)""").collect
 sql(s"""Update default.t_carbn01  set (Item_code) = ('Banana')""").collect
 sql(s"""update default.t_carbn01  set (Item_type_cd) = (1111)""").collect
 sql(s"""Update default.t_carbn01  set (Item_code) = ('Orange')""").collect
 sql(s"""update default.t_carbn01  set (Item_type_cd) = (24523)""").collect
 sql(s"""Update default.t_carbn01  set (Item_code) = ('Banana')""").collect
 sql(s"""update default.t_carbn01  set (Item_type_cd) = (1111)""").collect
 sql(s"""Update default.t_carbn01  set (Item_code) = ('Orange')""").collect
 sql(s"""update default.t_carbn01  set (Item_type_cd) = (24523)""").collect
 sql(s"""Update default.t_carbn01  set (Item_code) = ('Banana')""").collect
 sql(s"""update default.t_carbn01  set (Item_type_cd) = (1111)""").collect
 sql(s"""select count(*) from default.t_carbn01 """).collect
  checkAnswer(s"""select count(*) from default.t_carbn01 """,
    Seq(Row(10)), "DataLoadingIUDTestCase_IUD-01-01-01_016-02")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Run select query after multiple deletes(control this using where condition) and enforce horizontal compaction and see that there is no data loss
test("IUD-01-01-01_016-03", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""delete from default.t_carbn01  where item_type_cd = 123 """).collect
 sql(s"""delete from default.t_carbn01  where item_type_cd = 41 """).collect
 sql(s"""delete from default.t_carbn01  where item_type_cd = 14 """).collect
 sql(s"""delete from default.t_carbn01  where item_type_cd = 13 """).collect
 sql(s"""delete from default.t_carbn01  where item_type_cd = 114 """).collect
 sql(s"""delete from default.t_carbn01  where item_type_cd = 11 """).collect
 sql(s"""delete from default.t_carbn01  where item_type_cd = 3 """).collect
 sql(s"""delete from default.t_carbn01  where item_type_cd = 4 """).collect
  checkAnswer(s"""select count(*) from default.t_carbn01 """,
    Seq(Row(2)), "DataLoadingIUDTestCase_IUD-01-01-01_016-03")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Run select query on the updated column after multiple updates on different column at different rows(control this using where condition) and enforce horizontal compaction and see that there is no data loss
test("IUD-01-01-01_016-04", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (item_type_cd) = (21) where item_type_cd = 123""").collect
 sql(s"""update default.t_carbn01  set (item_type_cd) = (21) where item_type_cd = 41""").collect
 sql(s"""update default.t_carbn01  set (item_type_cd) = (21) where item_type_cd = 14""").collect
 sql(s"""update default.t_carbn01  set (item_type_cd) = (21) where item_type_cd = 13""").collect
 sql(s"""update default.t_carbn01  set (item_type_cd) = (21) where item_type_cd = 114""").collect
 sql(s"""update default.t_carbn01  set (item_type_cd) = (21) where item_type_cd = 11""").collect
 sql(s"""update default.t_carbn01  set (item_type_cd) = (21) where item_type_cd = 3""").collect
 sql(s"""update default.t_carbn01  set (item_type_cd) = (21) where item_type_cd = 4""").collect
 sql(s"""update default.t_carbn01  set (item_type_cd) = (21) where item_type_cd = 2""").collect
 sql(s"""select item_type_cd from default.t_carbn01  group by item_type_cd """).collect
  checkAnswer(s"""select item_type_cd from default.t_carbn01 order by item_type_cd limit 1""",
    Seq(Row(1)), "DataLoadingIUDTestCase_IUD-01-01-01_016-04")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Run alternate update and insert and do a vertical compaction and see that there is no data loss
test("IUD-01-01-01_016-06", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""Update default.t_carbn01  set (item_code) = ('Banana')""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""Update default.t_carbn01  set (item_code) = ('Orange')""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""Update default.t_carbn01  set (item_code) = ('Banana')""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""Update default.t_carbn01  set (item_code) = ('Orange')""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""Update default.t_carbn01  set (item_code) = ('Banana')""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""Update default.t_carbn01  set (item_code) = ('Orange')""").collect
 sql(s"""select item_code, count(*)  from default.t_carbn01  group by item_code""").collect
  checkAnswer(s"""select item_code, count(*)  from default.t_carbn01  group by item_code""",
    Seq(Row("Orange",60)), "DataLoadingIUDTestCase_IUD-01-01-01_016-06")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//do a delete after segment deletion and see that the delta files are not created in the deleted segmnet
test("IUD-01-01-02_006-15", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""delete from table default.t_carbn01  where segment.id in (1) """).collect
 sql(s"""delete from t_carbn01 where item_type_cd =14""").collect
  checkAnswer(s"""select count(*)  from default.t_carbn01""",
    Seq(Row(9)), "DataLoadingIUDTestCase_IUD-01-01-02_006-15")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//do an update after segment delete and see that delta files are not created in the deleted segments
test("IUD-01-01-02_006-14", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""delete from table default.t_carbn01  where segment.id in (1) """).collect
 sql(s"""update t_carbn01 set (item_code) = ('Apple')""").collect
  checkAnswer(s"""select count(*)  from default.t_carbn01 where item_code = 'Apple'""",
    Seq(Row(10)), "DataLoadingIUDTestCase_IUD-01-01-02_006-14")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check data consistency when select is executed after update and delete segment
test("IUD-01-01-02_007-01", Include) {
   sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""Update default.t_carbn01  set (item_code) = ('Banana')""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""Update default.t_carbn01  set (item_code) = ('Orange')""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""Update default.t_carbn01  set (item_code) = ('Banana')""").collect
 sql(s"""delete from table default.t_carbn01  where segment.id in (2) """).collect
 sql(s"""select item_code, count(*)  from default.t_carbn01  group by item_code""").show
  checkAnswer(s"""select item_code, count(*)  from default.t_carbn01  group by item_code""",
    Seq(Row("Banana",20)), "DataLoadingIUDTestCase_IUD-01-01-02_007-01")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check select after deleting segment and reloading and reupdating same data.
test("IUD-01-01-02_008-01", Include) {
   sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""Update default.t_carbn01  set (item_code) = ('Banana')""").collect
 sql(s"""delete from table default.t_carbn01  where segment.id in (0)""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""Update default.t_carbn01  set (item_code) = ('Banana')""").collect
  checkAnswer(s"""select item_code, count(*)  from default.t_carbn01  group by item_code""",
    Seq(Row("Banana",10)), "DataLoadingIUDTestCase_IUD-01-01-02_008-01")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Run 2 deletes on a table where update is done after data load - 1 block from load, 1 block from update in a segment(set detla threshold = 1).
test("IUD-01-01-02_009-01", Include) {
   sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
  sql(s"""select item_type_cd from default.t_carbn01""").show(100, false)
 sql(s"""Update default.t_carbn01  set (item_code) = ('Banana')""").collect
 sql(s"""delete from t_carbn01 where item_type_cd =2""").collect
 sql(s"""delete from t_carbn01 where item_type_cd =14""").collect
  checkAnswer(s"""select count(item_type_cd)  from default.t_carbn01""",
    Seq(Row(8)), "DataLoadingIUDTestCase_IUD-01-01-02_009-01")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check update on carbon table where a column being updated with incorrect data type.
test("IUD-01-01-02_011-01", Include) {
  intercept[Exception] {
    sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
    sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
    sql(s"""Update T_Carbn04 set (Item_type_cd) = ('Banana')""").collect
    sql(s"""NA""").collect
  }
  sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check update on empty carbon table where a column being updated with incorrect data type.
test("IUD-01-01-01_022-01", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""Update default.t_carbn01  set (item_type_cd) = (11) """).collect
  checkAnswer(s"""select item_type_cd from default.t_carbn01  where item_type_cd=11 limit 1""",
    Seq(Row(11)), "DataLoadingIUDTestCase_IUD-01-01-01_022-01")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check update on carbon table where multiple values are returned in expression.
test("IUD-01-01-01_023-00", Include) {
  intercept[Exception] {
    sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
    sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
    sql(s"""Update default.t_carbn01  set Item_type_cd = (select Item_type_cd from default.t_carbn01b )""").collect
    sql(s"""NA""").collect
  }
  sql(s"""drop table default.t_carbn01  """).collect
}
       

//check update using case statement joiining 2 tables
test("IUD-01-01-02_023-01", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""drop table if exists T_Parq1""").collect
 sql(s"""create table T_Parq1(Active_status BOOLEAN, Item_type_cd TINYINT, Qty_day_avg SMALLINT, Qty_total INT, Sell_price BIGINT, Sell_pricep FLOAT, Discount_price DOUBLE , Profit DECIMAL(3,2), Item_code STRING, Item_name VARCHAR(500), Outlet_name CHAR(100), Update_time TIMESTAMP, Create_date DATE)stored as parquet""").collect
 sql(s"""insert into T_Parq1 select * from t_hive1""").collect
 sql(s"""update t_carbn01 a set(a.item_code) = (select (case when b.item_code = 'RE3423ee' then c.item_code else b.item_code end) from t_parq1 b, t_hive1 c where b.item_type_cd = 14 and b.item_type_cd=c.item_type_cd)""").collect
  checkAnswer(s"""select count(item_code) from default.t_carbn01  where item_code = 'SE3423ee'""",
    Seq(Row(0)), "DataLoadingIUDTestCase_IUD-01-01-02_023-01")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check update on carbon table where non matching values are returned from expression.
test("IUD-01-01-01_024-01", Include) {
  intercept[Exception] {
    sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
    sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
    sql(s"""Update default.t_carbn01  set Item_type_cd = (select Item_code from default.t_carbn01b)""").collect
    sql(s"""NA""").collect
  }
  sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a base64 function value
test("IUD-01-01-01_040-01", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (active_status)= (base64('A')) """).collect
  checkAnswer(s""" select count(active_status) from default.t_carbn01  group by active_status """,
    Seq(Row(10)), "DataLoadingIUDTestCase_IUD-01-01-01_040-01")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a ascii function value
test("IUD-01-01-01_040-02", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (active_status)= (ascii(FALSE)) """).collect
  checkAnswer(s""" select active_status from default.t_carbn01  group by active_status """,
    Seq(Row("102")), "DataLoadingIUDTestCase_IUD-01-01-01_040-02")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a concat function value
test("IUD-01-01-01_040-03", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (active_status)= (concat('FAL','SE')) """).collect
  checkAnswer(s""" select active_status from default.t_carbn01  group by active_status """,
    Seq(Row("FALSE")), "DataLoadingIUDTestCase_IUD-01-01-01_040-03")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column to a value returned by concat_ws function
test("IUD-01-01-01_040-04", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (active_status)= (concat_ws('FAL','SE')) """).collect
  checkAnswer(s""" select active_status from default.t_carbn01  group by active_status """,
    Seq(Row("SE")), "DataLoadingIUDTestCase_IUD-01-01-01_040-04")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column to a value returned by find_in_set function
test("IUD-01-01-01_040-05", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (active_status)= (find_in_set('t','test1')) """).collect
  checkAnswer(s""" select active_status from default.t_carbn01  group by active_status """,
    Seq(Row("0")), "DataLoadingIUDTestCase_IUD-01-01-01_040-05")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column to a value returned by format_number function
test("IUD-01-01-01_040-06", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (active_status)= (format_number(10,12)) """).collect
  checkAnswer(s""" select active_status from default.t_carbn01  group by active_status """,
    Seq(Row("10.000000000000")), "DataLoadingIUDTestCase_IUD-01-01-01_040-06")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column to a value returned by get_json_object function
test("IUD-01-01-01_040-07", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (active_status)= (get_json_object('test','test1')) """).collect
  checkAnswer(s""" select active_status from default.t_carbn01  group by active_status """,
    Seq(Row(null)), "DataLoadingIUDTestCase_IUD-01-01-01_040-07")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by instr function
test("IUD-01-01-01_040-08", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (active_status)= (instr('test','test1')) """).collect
  checkAnswer(s""" select active_status from default.t_carbn01  group by active_status """,
    Seq(Row("0")), "DataLoadingIUDTestCase_IUD-01-01-01_040-08")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by length function
test("IUD-01-01-01_040-09", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (active_status)= (length('test')) """).collect
  checkAnswer(s""" select active_status from default.t_carbn01  group by active_status """,
    Seq(Row("4")), "DataLoadingIUDTestCase_IUD-01-01-01_040-09")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by locate function
test("IUD-01-01-01_040-10", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (active_status)= (locate('test','test1')) """).collect
  checkAnswer(s""" select active_status from default.t_carbn01  group by active_status """,
    Seq(Row("1")), "DataLoadingIUDTestCase_IUD-01-01-01_040-10")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by lower function
test("IUD-01-01-01_040-11", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (active_status)= (lower('test')) """).collect
  checkAnswer(s""" select active_status from default.t_carbn01  group by active_status """,
    Seq(Row("test")), "DataLoadingIUDTestCase_IUD-01-01-01_040-11")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by lcase function
test("IUD-01-01-01_040-12", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (active_status)= (lcase('test')) """).collect
  checkAnswer(s""" select active_status from default.t_carbn01  group by active_status """,
    Seq(Row("test")), "DataLoadingIUDTestCase_IUD-01-01-01_040-12")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by lpad function
test("IUD-01-01-01_040-13", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (active_status)= (lpad('te',1,'test1')) """).collect
  checkAnswer(s""" select active_status from default.t_carbn01  group by active_status """,
    Seq(Row("t")), "DataLoadingIUDTestCase_IUD-01-01-01_040-13")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by ltrim function
test("IUD-01-01-01_040-14", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (active_status)= (ltrim('te')) """).collect
  checkAnswer(s""" select active_status from default.t_carbn01  group by active_status """,
    Seq(Row("te")), "DataLoadingIUDTestCase_IUD-01-01-01_040-14")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by parse_url function
test("IUD-01-01-01_040-15", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (active_status)= (parse_url('test','test1')) """).collect
  checkAnswer(s""" select active_status from default.t_carbn01  group by active_status """,
    Seq(Row(null)), "DataLoadingIUDTestCase_IUD-01-01-01_040-15")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by printf function
test("IUD-01-01-01_040-16", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (active_status)= (printf('test','test1')) """).collect
  checkAnswer(s""" select active_status from default.t_carbn01  group by active_status """,
    Seq(Row("test")), "DataLoadingIUDTestCase_IUD-01-01-01_040-16")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by regexp_extract function
test("IUD-01-01-01_040-17", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (active_status)= (regexp_extract('test','test1',1)) """).collect
  checkAnswer(s""" select count(active_status) from default.t_carbn01  group by active_status """,
    Seq(Row(10)), "DataLoadingIUDTestCase_IUD-01-01-01_040-17")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by regexp_replace function
test("IUD-01-01-01_040-18", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (active_status)= (regexp_replace('test','test1','test2')) """).collect
  checkAnswer(s""" select active_status from default.t_carbn01  group by active_status """,
    Seq(Row("test")), "DataLoadingIUDTestCase_IUD-01-01-01_040-18")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by repeat function
test("IUD-01-01-01_040-19", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (active_status)= (repeat('test',1)) """).collect
  checkAnswer(s""" select active_status from default.t_carbn01  group by active_status """,
    Seq(Row("test")), "DataLoadingIUDTestCase_IUD-01-01-01_040-19")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by reverse function
test("IUD-01-01-01_040-20", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (active_status)= (reverse('test')) """).collect
  checkAnswer(s""" select active_status from default.t_carbn01  group by active_status """,
    Seq(Row("tset")), "DataLoadingIUDTestCase_IUD-01-01-01_040-20")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by rpad function
test("IUD-01-01-01_040-21", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (active_status)= (rpad('test',1,'test1')) """).collect
  checkAnswer(s""" select active_status from default.t_carbn01  group by active_status """,
    Seq(Row("t")), "DataLoadingIUDTestCase_IUD-01-01-01_040-21")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by rtrim function
test("IUD-01-01-01_040-22", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (active_status)= (rtrim('test')) """).collect
  checkAnswer(s""" select active_status from default.t_carbn01  group by active_status """,
    Seq(Row("test")), "DataLoadingIUDTestCase_IUD-01-01-01_040-22")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by sentences function
ignore("IUD-01-01-01_040-23", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (active_status)= (sentences('Hello there! How are you?')) """).collect
  checkAnswer(s""" select active_status from default.t_carbn01  group by active_status """,
    Seq(Row("Hello\\:there\\\\$How\\:are:\\you\\\\")), "DataLoadingIUDTestCase_IUD-01-01-01_040-23")
   sql(s"""drop table default.t_carbn01  """).collect
}


  //Check for updating carbon table set column value to a value returned by space function
  test("IUD-01-01-01_040-24", Include) {
    sql(s"""create table default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
    sql(s"""select active_status from default.t_carbn01b""").show
  sql(s"""select active_status from default.t_carbn01""").show

  sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
  sql(s"""select active_status from default.t_carbn01""").show(100, false)
 sql(s"""update default.t_carbn01  set (active_status)= (space(1)) """).collect
  checkAnswer(s"""select count(active_status) from default.t_carbn01  group by active_status """,
    Seq(Row(10)), "DataLoadingIUDTestCase_IUD-01-01-01_040-24")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by split function
//Split will give us array value
test("IUD-01-01-01_040-25", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 intercept[Exception] {
   sql(s"""update default.t_carbn01  set (active_status)= (split('t','a')) """).collect
 }
}
       

//Check for updating carbon table set column value to a value returned by substr function with 2 parameters
test("IUD-01-01-01_040-26", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (active_status)= (substr('test',1)) """).collect
  checkAnswer(s""" select active_status from default.t_carbn01  group by active_status """,
    Seq(Row("test")), "DataLoadingIUDTestCase_IUD-01-01-01_040-26")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by substring function with 2 parameters
test("IUD-01-01-01_040-27", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (active_status)= (substring('test',1,2)) """).collect
  checkAnswer(s""" select active_status from default.t_carbn01  group by active_status """,
    Seq(Row("te")), "DataLoadingIUDTestCase_IUD-01-01-01_040-27")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by substring function  with 3 parameters
test("IUD-01-01-01_040-28", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (active_status)= (substr('test1',2,3)) """).collect
  checkAnswer(s""" select active_status from default.t_carbn01  group by active_status """,
    Seq(Row("est")), "DataLoadingIUDTestCase_IUD-01-01-01_040-28")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by translate function
test("IUD-01-01-01_040-29", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (active_status)= (translate('test','test1','test2')) """).collect
  checkAnswer(s""" select active_status from default.t_carbn01  group by active_status """,
    Seq(Row("test")), "DataLoadingIUDTestCase_IUD-01-01-01_040-29")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by trim function
test("IUD-01-01-01_040-30", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (active_status)= (trim('test')) """).collect
  checkAnswer(s""" select active_status from default.t_carbn01  group by active_status """,
    Seq(Row("test")), "DataLoadingIUDTestCase_IUD-01-01-01_040-30")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by unbase64 function
test("IUD-01-01-01_040-31", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (active_status)= (unbase64('test')) """).collect
  checkAnswer(s""" select count(*) from default.t_carbn01  group by active_status""",
    Seq(Row(10)), "DataLoadingIUDTestCase_IUD-01-01-01_040-31")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by upper function
test("IUD-01-01-01_040-32", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (active_status)= (upper('test')) """).collect
  checkAnswer(s""" select active_status from default.t_carbn01  group by active_status """,
    Seq(Row("TEST")), "DataLoadingIUDTestCase_IUD-01-01-01_040-32")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by lower function
test("IUD-01-01-01_040-33", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (active_status)= (lower('TEST')) """).collect
  checkAnswer(s""" select active_status from default.t_carbn01  group by active_status """,
    Seq(Row("test")), "DataLoadingIUDTestCase_IUD-01-01-01_040-33")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by levenshtein function
test("IUD-01-01-01_040-35", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (active_status)= ( levenshtein('kitten','sitting')) """).collect
  checkAnswer(s""" select active_status from default.t_carbn01  group by active_status """,
    Seq(Row("3")), "DataLoadingIUDTestCase_IUD-01-01-01_040-35")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by round function with single parameter
test("IUD-01-01-01_040-36", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s""" update default.t_carbn01  set (sell_pricep)= (round(10.66)) """).collect
  checkAnswer(s"""select  sell_pricep from default.t_carbn01  group by  sell_pricep """,
    Seq(Row(11.0)), "DataLoadingIUDTestCase_IUD-01-01-01_040-36")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by round function with 2  parameters
test("IUD-01-01-01_040-37", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (sell_pricep)= (round(10.66,1)) """).collect
  checkAnswer(s"""select  sell_pricep from default.t_carbn01  group by  sell_pricep """,
    Seq(Row(10.7)), "DataLoadingIUDTestCase_IUD-01-01-01_040-37")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by bround function having single parameter
test("IUD-01-01-01_040-38", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (sell_pricep)= (bround(10.66)) """).collect
  checkAnswer(s"""select  sell_pricep from default.t_carbn01  group by  sell_pricep """,
    Seq(Row(11.0)), "DataLoadingIUDTestCase_IUD-01-01-01_040-38")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by bround function having 2 parameters
test("IUD-01-01-01_040-39", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (sell_pricep)= (bround(10.66,1)) """).collect
  checkAnswer(s"""select  sell_pricep from default.t_carbn01  group by  sell_pricep """,
    Seq(Row(10.7)), "DataLoadingIUDTestCase_IUD-01-01-01_040-39")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by floor function
test("IUD-01-01-01_040-40", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (sell_pricep)= (floor(10.1)) """).collect
  checkAnswer(s"""select  sell_pricep from default.t_carbn01  group by  sell_pricep """,
    Seq(Row(10.0)), "DataLoadingIUDTestCase_IUD-01-01-01_040-40")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by ceil function
test("IUD-01-01-01_040-41", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (sell_pricep)= (ceil(10.1)) """).collect
  checkAnswer(s"""select  sell_pricep from default.t_carbn01  group by  sell_pricep """,
    Seq(Row(11.0)), "DataLoadingIUDTestCase_IUD-01-01-01_040-41")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by ceiling function
test("IUD-01-01-01_040-42", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (sell_pricep)= (ceiling(11.1)) """).collect
  checkAnswer(s"""select  sell_pricep from default.t_carbn01  group by  sell_pricep """,
    Seq(Row(12.0)), "DataLoadingIUDTestCase_IUD-01-01-01_040-42")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by exp function with parameters
test("IUD-01-01-01_040-45", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (sell_pricep)= (exp(10.1234242323)) """).collect
  checkAnswer(s"""select  sell_pricep from default.t_carbn01  group by  sell_pricep """,
    Seq(Row(24919.956624251117)), "DataLoadingIUDTestCase_IUD-01-01-01_040-45")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by ln function with parameters
test("IUD-01-01-01_040-46", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (sell_pricep)= (ln(10.1)) """).collect
  checkAnswer(s"""select  sell_pricep from default.t_carbn01  group by  sell_pricep """,
    Seq(Row(2.312535423847214)), "DataLoadingIUDTestCase_IUD-01-01-01_040-46")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by log10 function
test("IUD-01-01-01_040-47", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (sell_pricep)= (log10(10.1)) """).collect
  checkAnswer(s"""select  sell_pricep from default.t_carbn01  group by  sell_pricep """,
    Seq(Row(1.0043213737826426)), "DataLoadingIUDTestCase_IUD-01-01-01_040-47")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by log2 function
test("IUD-01-01-01_040-48", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (sell_pricep)= (log2(10.1)) """).collect
  checkAnswer(s"""select  sell_pricep from default.t_carbn01  group by  sell_pricep """,
    Seq(Row(3.3362833878644325)), "DataLoadingIUDTestCase_IUD-01-01-01_040-48")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by log function
test("IUD-01-01-01_040-49", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (sell_pricep)= (log(10.1,10.2)) """).collect
  checkAnswer(s"""select  sell_pricep from default.t_carbn01  group by  sell_pricep """,
    Seq(Row(1.0042603872534936)), "DataLoadingIUDTestCase_IUD-01-01-01_040-49")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by pow function
test("IUD-01-01-01_040-50", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (sell_pricep)= (pow(10.1,10.2)) """).collect
  checkAnswer(s"""select  sell_pricep from default.t_carbn01  group by  sell_pricep """,
    Seq(Row(1.754195580765244E10)), "DataLoadingIUDTestCase_IUD-01-01-01_040-50")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by power function
test("IUD-01-01-01_040-51", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (sell_pricep)= (power(11.1,11.2)) """).collect
  checkAnswer(s"""select  sell_pricep from default.t_carbn01  group by  sell_pricep """,
    Seq(Row(5.100554147653899E11)), "DataLoadingIUDTestCase_IUD-01-01-01_040-51")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by sqrt function
test("IUD-01-01-01_040-52", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (sell_pricep)= (sqrt(11.1)) """).collect
  checkAnswer(s"""select  sell_pricep from default.t_carbn01  group by  sell_pricep """,
    Seq(Row(3.331666249791536)), "DataLoadingIUDTestCase_IUD-01-01-01_040-52")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by bin function
test("IUD-01-01-01_040-53", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s""" update default.t_carbn01  set (sell_pricep)= (bin(1)) """).collect
  checkAnswer(s"""select  sell_pricep from default.t_carbn01  group by  sell_pricep """,
    Seq(Row(1.0)), "DataLoadingIUDTestCase_IUD-01-01-01_040-53")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by hex function
test("IUD-01-01-01_040-54", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (sell_pricep)= (hex(1)) """).collect
  checkAnswer(s"""select  sell_pricep from default.t_carbn01  group by  sell_pricep """,
    Seq(Row(1.0)), "DataLoadingIUDTestCase_IUD-01-01-01_040-54")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by unhex function
test("IUD-01-01-01_040-55", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (active_status)= (unhex(1)) """).collect
  checkAnswer(s"""select count(active_status) from default.t_carbn01  group by active_status """,
    Seq(Row(10)), "DataLoadingIUDTestCase_IUD-01-01-01_040-55")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by conv function
test("IUD-01-01-01_040-56", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (active_status)= (conv(1,1,2)) """).collect
  checkAnswer(s""" select active_status from default.t_carbn01  group by active_status """,
    Seq(Row(null)), "DataLoadingIUDTestCase_IUD-01-01-01_040-56")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by abs function
test("IUD-01-01-01_040-57", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (sell_pricep)= (abs(1.9)) """).collect
  checkAnswer(s"""select  sell_pricep from default.t_carbn01  group by  sell_pricep """,
    Seq(Row(1.9)), "DataLoadingIUDTestCase_IUD-01-01-01_040-57")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by pmod function
test("IUD-01-01-01_040-58", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (sell_pricep)= (pmod(1,2)) """).collect
  checkAnswer(s"""select  sell_pricep from default.t_carbn01  group by  sell_pricep """,
    Seq(Row(1.0)), "DataLoadingIUDTestCase_IUD-01-01-01_040-58")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by sin function
test("IUD-01-01-01_040-59", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (sell_pricep)= (sin(1.2)) """).collect
  checkAnswer(s"""select  sell_pricep from default.t_carbn01  group by  sell_pricep """,
    Seq(Row(0.9320390859672263)), "DataLoadingIUDTestCase_IUD-01-01-01_040-59")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by cos function
test("IUD-01-01-01_040-60", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (sell_pricep)= (cos(1.2)) """).collect
  checkAnswer(s"""select  sell_pricep from default.t_carbn01  group by  sell_pricep """,
    Seq(Row(0.3623577544766736)), "DataLoadingIUDTestCase_IUD-01-01-01_040-60")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by tan function
test("IUD-01-01-01_040-61", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (sell_pricep)= (tan(1.2)) """).collect
  checkAnswer(s"""select  sell_pricep from default.t_carbn01  group by  sell_pricep """,
    Seq(Row(2.5721516221263188)), "DataLoadingIUDTestCase_IUD-01-01-01_040-61")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by atan function
test("IUD-01-01-01_040-62", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (sell_pricep)= (atan(1.2)) """).collect
  checkAnswer(s"""select  sell_pricep from default.t_carbn01  group by  sell_pricep """,
    Seq(Row(0.8760580505981934)), "DataLoadingIUDTestCase_IUD-01-01-01_040-62")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by degrees function
test("IUD-01-01-01_040-63", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (sell_pricep)= (degrees(1.2)) """).collect
  checkAnswer(s"""select  sell_pricep from default.t_carbn01  group by  sell_pricep """,
    Seq(Row(68.75493541569878)), "DataLoadingIUDTestCase_IUD-01-01-01_040-63")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by radians function
test("IUD-01-01-01_040-64", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (sell_pricep)= (radians(1.2)) """).collect
  checkAnswer(s"""select  sell_pricep from default.t_carbn01  group by  sell_pricep """,
    Seq(Row(0.020943951023931952)), "DataLoadingIUDTestCase_IUD-01-01-01_040-64")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by positive function
test("IUD-01-01-01_040-65", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (sell_pricep)= (positive(1.2)) """).collect
  checkAnswer(s"""select  sell_pricep from default.t_carbn01  group by  sell_pricep """,
    Seq(Row(1.2)), "DataLoadingIUDTestCase_IUD-01-01-01_040-65")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by negative function
test("IUD-01-01-01_040-66", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (sell_pricep)= (negative(1.2)) """).collect
  checkAnswer(s"""select  sell_pricep from default.t_carbn01  group by  sell_pricep """,
    Seq(Row(-1.2)), "DataLoadingIUDTestCase_IUD-01-01-01_040-66")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by sign function
test("IUD-01-01-01_040-67", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (sell_pricep)= (sign(2.1)) """).collect
  checkAnswer(s"""select  sell_pricep from default.t_carbn01  group by  sell_pricep """,
    Seq(Row(1.0)), "DataLoadingIUDTestCase_IUD-01-01-01_040-67")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by e() function
test("IUD-01-01-01_040-68", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (sell_pricep)= (e()) """).collect
  checkAnswer(s"""select  sell_pricep from default.t_carbn01  group by  sell_pricep """,
    Seq(Row(2.718281828459045)), "DataLoadingIUDTestCase_IUD-01-01-01_040-68")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by pi() function
test("IUD-01-01-01_040-69", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (sell_pricep)= (pi()) """).collect
  checkAnswer(s"""select  sell_pricep from default.t_carbn01  group by  sell_pricep """,
    Seq(Row(3.141592653589793)), "DataLoadingIUDTestCase_IUD-01-01-01_040-69")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by factorial function
test("IUD-01-01-01_040-70", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s""" update default.t_carbn01  set (sell_pricep)= (factorial(5)) """).collect
  checkAnswer(s"""select  sell_pricep from default.t_carbn01  group by  sell_pricep """,
    Seq(Row(120.0)), "DataLoadingIUDTestCase_IUD-01-01-01_040-70")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by cbrt function
test("IUD-01-01-01_040-71", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s""" update default.t_carbn01  set (sell_pricep)= (cbrt(5.1)) """).collect
  checkAnswer(s"""select  sell_pricep from default.t_carbn01  group by  sell_pricep """,
    Seq(Row(1.721300620726316)), "DataLoadingIUDTestCase_IUD-01-01-01_040-71")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by greatest function
test("IUD-01-01-01_040-72", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (item_type_cd)= (greatest(2,3)) """).collect
  checkAnswer(s"""select  item_type_cd from default.t_carbn01  group by  item_type_cd """,
    Seq(Row(3)), "DataLoadingIUDTestCase_IUD-01-01-01_040-72")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for updating carbon table set column value to a value returned by least function
test("IUD-01-01-01_040-73", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""update default.t_carbn01  set (item_type_cd)= (least(2,3)) """).collect
  checkAnswer(s"""select  item_type_cd from default.t_carbn01  group by  item_type_cd """,
    Seq(Row(2)), "DataLoadingIUDTestCase_IUD-01-01-01_040-73")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for delete where in (select from tabl2)
test("IUD-01-01-02_023-03", Include) {
   sql(s"""create table if not exists default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""create database if not exists test1""").collect
 sql(s"""create table if not exists test1.t_carbn02(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into test1.t_carbn02 select * from default.t_carbn01b""").collect
 sql(s"""delete from default.t_carbn01  a where (a.item_code) in (select b.item_code from test1.t_carbn02 b)""").collect
  sql(s"""select  item_type_cd from default.t_carbn01  order by  item_type_cd limit 1""").collect
  
   sql(s"""drop table default.t_carbn01 """).collect
 sql(s"""drop table test1.t_carbn02""").collect
 sql(s"""drop database test1""").collect
}
       

//delete using a temp table
test("IUD-01-01-02_023-04", Include) {
   sql(s"""create table IF NOT EXISTS  default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""delete from default.t_carbn01  a where a.item_code in (select b.item_code from (select c.item_code, c.item_type_cd from t_carbn01b c)b)""").collect
  checkAnswer(s"""select  count(*) from t_carbn01  """,
    Seq(Row(0)), "DataLoadingIUDTestCase_IUD-01-01-02_023-04")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for delete using a temp table using group by using subquery
test("IUD-01-01-02_023-05", Include) {
   sql(s"""create table IF NOT EXISTS  default.t_carbn01 (Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into default.t_carbn01  select * from default.t_carbn01b""").collect
 sql(s"""delete from default.t_carbn01  a where a.item_type_cd in ( select b.profit from (select sum(item_type_cd) profit from default.t_carbn01b group by item_code) b)""").collect
  checkAnswer(s"""select  item_type_cd from default.t_carbn01  order by  item_type_cd  limit 1""",
    Seq(Row(1)), "DataLoadingIUDTestCase_IUD-01-01-02_023-05")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//Check for update with null value for multiple update operations
test("IUD-01-01-01_016-05", Include) {
   sql(s"""CREATE TABLE table_C21 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv'  into table table_C21 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
 sql(s"""update table_C21 set (cust_id)= (10000) where cust_name='CUST_NAME_00000'""").collect
 sql(s"""update table_C21 set (cust_id)= (NULL) where cust_name='CUST_NAME_00000'""").collect
 sql(s"""update table_C21 set (cust_name)= (NULL) where cust_id='9001'""").collect
  checkAnswer(s"""select cust_name from table_C21 where cust_id='9001'""",
    Seq(Row(null)), "DataLoadingIUDTestCase_IUD-01-01-01_016-05")
   sql(s"""drop table table_C21 """).collect
}
       

//Test horizontal compaction when In same segment, single block, full update in one update
test("IUD-01-01-02_023-09", Include) {
   sql(s"""drop table if exists t_carbn01""").collect
 sql(s"""create table t_carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into t_carbn01 select * from t_carbn01b""").collect
 sql(s""" update t_carbn01 set(item_name) = ('Ram')""").collect
 sql(s"""clean files for table t_carbn01""").collect
  checkAnswer(s"""select  count(item_type_cd) from t_carbn01""",
    Seq(Row(10)), "DataLoadingIUDTestCase_IUD-01-01-02_023-09")
   sql(s"""drop table t_carbn01  """).collect
}
       

//Test horizontal compaction when ,In same segment, single block, full delete in one delete
test("IUD-01-01-02_023-10", Include) {
   sql(s"""drop table  if exists  t_carbn01""").collect
 sql(s"""create table t_carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into t_carbn01 select * from t_carbn01b""").collect
 sql(s"""delete from t_carbn01""").collect
 sql(s"""clean files for table t_carbn01""").collect
  checkAnswer(s"""select  count(item_type_cd) from t_carbn01""",
    Seq(Row(0)), "DataLoadingIUDTestCase_IUD-01-01-02_023-10")
   sql(s"""drop table t_carbn01  """).collect
}
       

//Test horizontal compaction when  In same segment, single block, sequential updates leading to full delete
test("IUD-01-01-02_023-11", Include) {
   sql(s"""drop table  if exists  t_carbn01""").collect
 sql(s"""create table t_carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""update t_carbn01 set (item_name) =('Apple') where item_type_cd =123""").collect
 sql(s"""update t_carbn01 set (item_name) =('Apple') where item_type_cd =41""").collect
 sql(s"""update t_carbn01 set (item_name) =('Apple') where item_type_cd =14""").collect
 sql(s"""update t_carbn01 set (item_name) =('Apple') where item_type_cd =13""").collect
 sql(s"""update t_carbn01 set (item_name) =('Apple') where item_type_cd =114""").collect
 sql(s"""update t_carbn01 set (item_name) =('Apple') where item_type_cd =11""").collect
 sql(s"""update t_carbn01 set (item_name) =('Apple') where item_type_cd =3""").collect
 sql(s"""update t_carbn01 set (item_name) =('Apple') where item_type_cd =4""").collect
 sql(s"""update t_carbn01 set (item_name) =('Apple') where item_type_cd =2""").collect
 sql(s"""clean files for table t_carbn01""").collect
  checkAnswer(s"""select  count(item_name) from t_carbn01""",
    Seq(Row(10)), "DataLoadingIUDTestCase_IUD-01-01-02_023-11")
   sql(s"""drop table t_carbn01""").collect
}
       

//Test horizontal compaction when  In same segment, single block, sequential deletes leading to full delete
test("IUD-01-01-02_023-12", Include) {
   sql(s"""drop table if exists t_carbn01""").collect
 sql(s""" create table t_carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""delete from t_carbn01 where item_type_cd =123""").collect
 sql(s"""delete from t_carbn01 where item_type_cd =41""").collect
 sql(s"""delete from t_carbn01 where item_type_cd =14""").collect
 sql(s"""delete from t_carbn01 where item_type_cd =13""").collect
 sql(s"""delete from t_carbn01 where item_type_cd =114""").collect
 sql(s"""delete from t_carbn01 where item_type_cd =11""").collect
 sql(s"""delete from t_carbn01 where item_type_cd =3""").collect
 sql(s"""delete from t_carbn01 where item_type_cd =4""").collect
 sql(s"""delete from t_carbn01 where item_type_cd =2""").collect
 sql(s"""clean files for table t_carbn01""").collect
  checkAnswer(s"""select  count(item_type_cd) from t_carbn01""",
    Seq(Row(1)), "DataLoadingIUDTestCase_IUD-01-01-02_023-12")
   sql(s"""drop table t_carbn01""").collect
}
       

//Run updates 2 times after multiple data load - 1 block and multiple segment and check that compaction is applied in all segmnets(set detla threshold = 1).
test("IUD-01-01-02_023-07", Include) {
   sql(s"""drop table if exists  t_carbn01""").collect
 sql(s""" create table t_carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""update t_carbn01 set (item_name) =('Apple') """).collect
 sql(s"""update t_carbn01 set (item_name) =('Apple1')""").collect
  checkAnswer(s"""select  count(item_name) from t_carbn01""",
    Seq(Row(30)), "DataLoadingIUDTestCase_IUD-01-01-02_023-07")
   sql(s"""drop table t_carbn01""").collect
}
       

//Test horizontal compaction when In same segment, single block, multiple full updates
test("IUD-01-01-02_023-13", Include) {
   sql(s"""drop table if exists  t_carbn01""").collect
 sql(s""" create table t_carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""update t_carbn01 set (item_name) =('Apple1') """).collect
 sql(s"""update t_carbn01 set (item_name) =('Apple2') """).collect
 sql(s"""update t_carbn01 set (item_name) =('Apple3') """).collect
 sql(s"""update t_carbn01 set (item_name) =('Apple4') """).collect
 sql(s"""update t_carbn01 set (item_name) =('Apple5') """).collect
 sql(s"""update t_carbn01 set (item_name) =('Apple6') """).collect
 sql(s"""update t_carbn01 set (item_name) =('Apple7') """).collect
 sql(s"""update t_carbn01 set (item_name) =('Apple8') """).collect
 sql(s"""update t_carbn01 set (item_name) =('Apple9') """).collect
 sql(s"""clean files for table t_carbn01""").collect
  checkAnswer(s"""select  count(item_name) from t_carbn01""",
    Seq(Row(10)), "DataLoadingIUDTestCase_IUD-01-01-02_023-13")
   sql(s"""drop table t_carbn01""").collect
}
       

//Test horizontal compaction when In same segment, single block, Delete on udpates
test("IUD-01-01-02_023-14", Include) {
   sql(s"""drop table if exists  t_carbn01""").collect
 sql(s""" create table t_carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""update t_carbn01 set (item_name) =('Apple1')""").collect
 sql(s"""delete from t_carbn01""").collect
 sql(s"""clean files for table t_carbn01""").collect
  checkAnswer(s"""select  count(item_name) from t_carbn01""",
    Seq(Row(0)), "DataLoadingIUDTestCase_IUD-01-01-02_023-14")
   sql(s"""drop table t_carbn01""").collect
}
       

//Test horizontal compaction when In same segment, single block, Load after complete deletion
test("IUD-01-01-02_023-15", Include) {
   sql(s"""drop table if exists  t_carbn01""").collect
 sql(s"""create table t_carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s""" insert into t_carbn01 select * from t_carbn01b""").collect
 sql(s"""delete from t_carbn01""").collect
 sql(s"""clean files for table t_carbn01""").collect
 sql(s"""insert into t_carbn01 select * from t_carbn01b""").collect
  checkAnswer(s"""select  count(item_type_cd) from t_carbn01""",
    Seq(Row(10)), "DataLoadingIUDTestCase_IUD-01-01-02_023-15")
   sql(s"""drop table t_carbn01""").collect
}
       

//Test horizontal compaction when In same segment, single block, Insert after complete deletion
test("IUD-01-01-02_023-16", Include) {
   sql(s"""drop table if exists  t_carbn01""").collect
 sql(s""" create table t_carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into t_carbn01 select * from t_carbn01b""").collect
 sql(s"""delete from t_carbn01""").collect
 sql(s"""clean files for table t_carbn01""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""show tables""").collect
  checkAnswer(s"""select  count(item_type_cd) from t_carbn01""",
    Seq(Row(10)), "DataLoadingIUDTestCase_IUD-01-01-02_023-16")
   sql(s"""drop table t_carbn01""").collect
}
       

//Test horizontal compaction when In same segment, single block, Veritcal compaction does not consider the deleted segment
test("IUD-01-01-02_023-17", Include) {
   sql(s"""drop table if exists  t_carbn01""").collect
 sql(s"""create table t_carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s""" insert into t_carbn01 select * from t_carbn01b""").collect
 sql(s"""delete from t_carbn01""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""show tables""").collect
  checkAnswer(s"""select  count(item_type_cd) from t_carbn01""",
    Seq(Row(30)), "DataLoadingIUDTestCase_IUD-01-01-02_023-17")
   sql(s"""drop table t_carbn01""").collect
}
       

//Test horizontal compaction when In same segment, single block, where the block deleted is in 2nd segment
test("IUD-01-01-02_023-18", Include) {
   sql(s"""drop table if exists  t_carbn1""").collect
 sql(s"""drop table if exists  t_carbn2""").collect
 sql(s"""drop table if exists  t_hive01""").collect
 sql(s"""create table t_carbn1(item_type_cd int, sell_price bigint, profit decimal(10,4), item_name string, update_time timestamp) stored by 'carbondata'  """).collect
 sql(s"""insert into t_carbn1 select 2, 200000,23.3,'Apple','2012-11-11 11:11:11'""").collect
 sql(s"""insert into t_carbn1 select 2,300000,33.3,'Orange','2012-11-11 11:11:11'""").collect
 sql(s"""insert into t_carbn1 select 2, 200000,23.3,'Banana','2012-11-11 11:11:11'""").collect
 sql(s"""create table t_hive01 as select * from t_carbn1""").collect
 sql(s"""create table t_carbn2(item_type_cd int, sell_price bigint, profit decimal(10,4), item_name string, update_time timestamp) stored by 'carbondata'  """).collect
 sql(s"""insert into t_carbn2 select * from t_hive01""").collect
 sql(s"""insert into t_carbn2 select 2, 200000,23.3,'Banana','2012-11-11 11:11:11'""").collect
 sql(s"""delete from t_carbn2 where item_name = 'Banana'""").collect
 sql(s"""clean files for table t_carbn2""").collect
  checkAnswer(s"""select  count(item_type_cd) from t_carbn2""",
    Seq(Row(2)), "DataLoadingIUDTestCase_IUD-01-01-02_023-18")
   sql(s"""drop table t_carbn2""").collect
}
       

//Test horizontal compaction when In same segment, single block(single block data created out of full update), Perform delete on updated block
test("IUD-01-01-02_023-19", Include) {
   sql(s"""drop table if exists  t_carbn01""").collect
 sql(s""" create table t_carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""update t_carbn01 set (item_name) =('Apple1')""").collect
 sql(s"""delete from t_carbn01""").collect
 sql(s"""clean files for table t_carbn01""").collect
  checkAnswer(s"""select  count(item_type_cd) from t_carbn01""",
    Seq(Row(0)), "DataLoadingIUDTestCase_IUD-01-01-02_023-19")
   sql(s"""drop table t_carbn01""").collect
}
       

//Test horizontal compaction when In same segment, single block,partial update creates a block and rest of the current block is deleted
test("IUD-01-01-02_023-20", Include) {
   sql(s"""drop table if exists  t_carbn01""").collect
 sql(s""" create table t_carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""update t_carbn01 set (item_name) =('Apple1') where item_type_cd in (123,41,14,13,114)""").collect
 sql(s"""delete from t_carbn01 where item_type_cd in (11,3,4,2)""").collect
 sql(s"""clean files for table t_carbn01""").collect
  checkAnswer(s"""select  count(item_type_cd) from t_carbn01""",
    Seq(Row(6)), "DataLoadingIUDTestCase_IUD-01-01-02_023-20")
   sql(s"""drop table t_carbn01""").collect
}
       

//Test horizontal compaction when In same segment, multiple blocks data,delete from one block
test("IUD-01-01-02_023-21", Include) {
   sql(s"""drop table if exists  t_carbn1""").collect
 sql(s"""drop table if exists  t_carbn2""").collect
 sql(s"""create table t_carbn1(item_type_cd int, sell_price bigint, profit decimal(10,4), item_name string, update_time timestamp) stored by 'carbondata'  """).collect
 sql(s"""insert into t_carbn1 select 2, 200000,23.3,'Apple','2012-11-11 11:11:11'""").collect
 sql(s"""insert into t_carbn1 select 2,300000,33.3,'Orange','2012-11-11 11:11:11'""").collect
 sql(s"""create table t_carbn2(item_type_cd int, sell_price bigint, profit decimal(10,4), item_name string, update_time timestamp) stored by 'carbondata'  """).collect
 sql(s"""insert into t_carbn2 select * from t_carbn1""").collect
 sql(s"""delete from t_carbn2 where item_name = 'Apple'""").collect
 sql(s"""clean files for table t_carbn2""").collect
  checkAnswer(s"""select  count(item_type_cd) from t_carbn2""",
    Seq(Row(1)), "DataLoadingIUDTestCase_IUD-01-01-02_023-21")
   sql(s"""drop table t_carbn2""").collect
}
       

//Test horizontal compaction when In same segment, multiple blocks data,delete from all blocks
test("IUD-01-01-02_023-22", Include) {
   sql(s"""drop table if exists  t_carbn1""").collect
 sql(s"""drop table if exists  t_carbn2""").collect
 sql(s"""create table t_carbn1(item_type_cd int, sell_price bigint, profit decimal(10,4), item_name string, update_time timestamp) stored by 'carbondata'  """).collect
 sql(s"""insert into t_carbn1 select 2, 200000,23.3,'Apple','2012-11-11 11:11:11'""").collect
 sql(s"""insert into t_carbn1 select 2,300000,33.3,'Orange','2012-11-11 11:11:11'""").collect
 sql(s"""create table t_carbn2(item_type_cd int, sell_price bigint, profit decimal(10,4), item_name string, update_time timestamp) stored by 'carbondata'  """).collect
 sql(s"""insert into t_carbn2 select * from t_carbn1""").collect
 sql(s"""delete from t_carbn2 where item_name in ('Orange','Apple')""").collect
 sql(s"""clean files for table t_carbn2""").collect
  checkAnswer(s"""select  count(item_type_cd) from t_carbn2""",
    Seq(Row(0)), "DataLoadingIUDTestCase_IUD-01-01-02_023-22")
   sql(s"""drop table t_carbn2""").collect
}
       

//Test horizontal compaction when different segments same data ,full delete across all segments
test("IUD-01-01-02_023-23", Include) {
   sql(s"""drop table if exists  t_carbn1""").collect
 sql(s"""create table t_carbn1(item_type_cd int, sell_price bigint, profit decimal(10,4), item_name string, update_time timestamp) stored by 'carbondata'  """).collect
 sql(s"""insert into t_carbn1 select 2, 200000,23.3,'Apple','2012-11-11 11:11:11'""").collect
 sql(s"""insert into t_carbn1 select 2,300000,33.3,'Apple','2012-11-11 11:11:11'""").collect
 sql(s"""delete from t_carbn1 where item_name = 'Apple'""").collect
 sql(s"""clean files for table t_carbn1""").collect
  checkAnswer(s"""select  count(item_type_cd) from t_carbn1""",
    Seq(Row(0)), "DataLoadingIUDTestCase_IUD-01-01-02_023-23")
   sql(s"""drop table t_carbn1""").collect
}
       

//Test horizontal compaction when multiple segments, compacted to single segment,set autocompaction true with level (3,4). Full update
test("IUD-01-01-02_023-24", Include) {
   sql(s"""drop table if exists  t_carbn01""").collect
 sql(s"""create table t_carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""update t_carbn01 set(item_name) = ('Apple')""").collect
 sql(s"""clean files for table t_carbn01""").collect
  checkAnswer(s"""select  count(item_type_cd) from t_carbn01""",
    Seq(Row(30)), "DataLoadingIUDTestCase_IUD-01-01-02_023-24")
   sql(s"""drop table t_carbn01""").collect
}
       

//Test horizontal compaction when multiple segments, compacted to single segment,set autocompaction true with level (3,4). Full delete
test("IUD-01-01-02_023-25", Include) {
   sql(s"""drop table if exists  t_carbn01""").collect
 sql(s"""create table t_carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""delete from t_carbn01""").collect
 sql(s"""clean files for table t_carbn01""").collect
  checkAnswer(s"""select  count(item_type_cd) from t_carbn01""",
    Seq(Row(0)), "DataLoadingIUDTestCase_IUD-01-01-02_023-25")
   sql(s"""drop table t_carbn01""").collect
}
       

//Test horizontal compaction when multiple segments, set autocompaction (carbon.enable.auto.load.merge)true with level (3,4). Partial update and delete
test("IUD-01-01-02_023-26", Include) {
   sql(s"""drop table if exists  t_carbn01""").collect
 sql(s"""create table t_carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""update t_carbn01 set (item_name) =('Apple1') where item_type_cd in (123,41,14,13,114)""").collect
 sql(s"""delete from t_carbn01 where item_type_cd in (11,3,4,2)""").collect
 sql(s"""clean files for table t_carbn01""").collect
  checkAnswer(s"""select  count(item_type_cd) from t_carbn01""",
    Seq(Row(18)), "DataLoadingIUDTestCase_IUD-01-01-02_023-26")
   sql(s"""drop table t_carbn01""").collect
}
       

//Do insert multiple time and update and do a manual minor compaction on table and check that delta files are removed and no residual file left in HDFS
test("IUD-01-01-02_006-08", Include) {
   sql(s"""drop table if exists  t_carbn01""").collect
 sql(s"""create table t_carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into t_carbn01 select * from t_carbn01b""").collect
 sql(s"""insert into t_carbn01 select * from t_carbn01b""").collect
 sql(s"""insert into t_carbn01 select * from t_carbn01b""").collect
 sql(s"""insert into t_carbn01 select * from t_carbn01b""").collect
 sql(s"""update t_carbn01 set (item_code) = ('Apple')""").collect
 sql(s"""alter table t_carbn01 compact 'minor'""").collect
  checkAnswer(s"""select  count(item_type_cd) from default.t_carbn01""",
    Seq(Row(40)), "DataLoadingIUDTestCase_IUD-01-01-02_006-08")
   sql(s"""drop table t_carbn01""").collect
}
       

//Do load multiple time and update and a delete segment do a manual minor compaction on table and check that delta files are removed and no residual file left in HDFS
test("IUD-01-01-02_006-09", Include) {
   sql(s"""drop table if exists  t_carbn01""").collect
 sql(s"""create table t_carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into t_carbn01 select * from t_carbn01b""").collect
 sql(s"""delete from t_carbn01""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""update t_carbn01 set (item_code) = ('Apple')""").collect
 sql(s"""delete from table T_Carbn01 where segment.id in (1)""").collect
 sql(s"""alter table t_carbn01 compact 'minor'""").collect
  checkAnswer(s"""select  count(item_type_cd) from t_carbn01""",
    Seq(Row(30)), "DataLoadingIUDTestCase_IUD-01-01-02_006-09")
   sql(s"""drop table default.t_carbn01""").collect
}
       

//Do load multiple time and update and do a manual minor compaction on table and check that delta files are removed and no residual file left in HDFS
test("IUD-01-01-02_006-03", Include) {
   sql(s"""drop table if exists  t_carbn01""").collect
 sql(s"""create table t_carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into t_carbn01 select * from t_carbn01b""").collect
 sql(s"""delete from t_carbn01""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""update t_carbn01 set (item_code) = ('Apple')""").collect
 sql(s"""alter table t_carbn01 compact 'minor'""").collect
  checkAnswer(s"""select  count(item_type_cd) from t_carbn01""",
    Seq(Row(30)), "DataLoadingIUDTestCase_IUD-01-01-02_006-03")
   sql(s"""drop table t_carbn01""").collect
}
       

//Do update after table insert  and run select query and check data
test("IUD-01-01-02_006-04", Include) {
   sql(s"""drop table if exists  t_carbn01""").collect
 sql(s"""create table t_carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into t_carbn01 select * from t_carbn01b""").collect
 sql(s"""update t_carbn01 set (item_code) = ('Apple')""").collect
  checkAnswer(s"""select  count(item_code) from t_carbn01""",
    Seq(Row(10)), "DataLoadingIUDTestCase_IUD-01-01-02_006-04")
   sql(s"""drop table t_carbn01""").collect
}
       

//Test horizontal compaction when minor compaction after complete deletion in a segment (manual )
test("IUD-01-01-02_023-27", Include) {
   sql(s"""drop table if exists  t_carbn01""").collect
 sql(s"""create table t_carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into t_carbn01 select * from t_carbn01b""").collect
 sql(s"""delete from t_carbn01""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""alter table t_carbn01 compact 'minor'""").collect
  checkAnswer(s"""select  count(item_type_cd) from t_carbn01""",
    Seq(Row(40)), "DataLoadingIUDTestCase_IUD-01-01-02_023-27")
   sql(s"""drop table t_carbn01""").collect
}
       

//Test horizontal compaction when major compaction after complete deletion in a segment(manual)
test("IUD-01-01-02_023-28", Include) {
   sql(s"""drop table if exists  t_carbn01""").collect
 sql(s"""create table t_carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""insert into t_carbn01 select * from t_carbn01b""").collect
 sql(s"""delete from t_carbn01""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""alter table t_carbn01 compact 'major'""").collect
  checkAnswer(s"""select  count(item_type_cd) from t_carbn01""",
    Seq(Row(30)), "DataLoadingIUDTestCase_IUD-01-01-02_023-28")
   sql(s"""drop table t_carbn01""").collect
}
       

//Test horizontal compaction when h-compcation threshold set to higher value(10),full updates multiple time
test("IUD-01-01-02_023-29", Include) {
   sql(s"""drop table if exists  t_carbn1""").collect
 sql(s"""create table t_carbn1(item_type_cd int, sell_price bigint, profit decimal(10,4), item_name string, update_time timestamp) stored by 'carbondata'  """).collect
 sql(s"""insert into t_carbn1 select 2, 200000,23.3,'Apple','2012-11-11 11:11:11'""").collect
 sql(s"""insert into t_carbn1 select 3, 300000,33.3,'Orange','2012-11-11 11:11:11'""").collect
 sql(s"""update t_carbn1 set (sell_price) = (2)""").collect
 sql(s"""update t_carbn1 set (sell_price) = (3)""").collect
 sql(s"""update t_carbn1 set (sell_price) = (4)""").collect
 sql(s"""update t_carbn1 set (sell_price) = (5)""").collect
 sql(s"""update t_carbn1 set (sell_price) = (6)""").collect
 sql(s"""update t_carbn1 set (sell_price) = (7)""").collect
 sql(s"""update t_carbn1 set (sell_price) = (8)""").collect
 sql(s"""update t_carbn1 set (sell_price) = (9)""").collect
 sql(s"""update t_carbn1 set (sell_price) = (10)""").collect
 sql(s"""update t_carbn1 set (sell_price) = (11)""").collect
 sql(s"""update t_carbn1 set (sell_price) = (12)""").collect
 sql(s"""clean files for table t_carbn1""").collect
  checkAnswer(s"""select  count(item_type_cd) from t_carbn1 where sell_price=12""",
    Seq(Row(2)), "DataLoadingIUDTestCase_IUD-01-01-02_023-29")
   sql(s"""drop table t_carbn1""").collect
}
       

//Test horizontal compaction when  h-compcation threshold set to higher value(10),full deletes multiple times
test("IUD-01-01-02_023-30", Include) {
   sql(s"""drop table if exists  t_carbn01""").collect
 sql(s""" create table t_carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""delete from t_carbn01 where item_type_cd=123""").collect
 sql(s"""delete from t_carbn01 where item_type_cd=41""").collect
 sql(s"""delete from t_carbn01 where item_type_cd=14""").collect
 sql(s"""delete from t_carbn01 where item_type_cd=13""").collect
 sql(s"""delete from t_carbn01 where item_type_cd=114""").collect
 sql(s"""delete from t_carbn01 where item_type_cd=11""").collect
 sql(s"""delete from t_carbn01 where item_type_cd=3""").collect
 sql(s"""delete from t_carbn01 where item_type_cd=4""").collect
 sql(s"""delete from t_carbn01 where item_type_cd=2""").collect
 sql(s"""clean files for table t_carbn01""").collect
  checkAnswer(s"""select  count(item_type_cd) from default.t_carbn01""",
    Seq(Row(1)), "DataLoadingIUDTestCase_IUD-01-01-02_023-30")
   sql(s"""drop table default.t_carbn01""").collect
}
       

//Test horizontal compaction when  large number of segments (50),full delete across all segments
test("IUD-01-01-02_023-31", Include) {
   sql(s"""drop table if exists  t_carbn01""").collect
 sql(s"""create table t_carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""delete from t_carbn01 where item_type_cd between 1 and 100""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""delete from t_carbn01 where item_type_cd between 100 and 200""").collect

 sql(s"""clean files for table t_carbn01""").collect
  checkAnswer(s"""select  count(item_type_cd) from default.t_carbn01""",
    Seq(Row(8)), "DataLoadingIUDTestCase_IUD-01-01-02_023-31")
   sql(s"""drop table default.t_carbn01""").collect
}
       

//Test horizontal compaction when large number of segments (50), full updates across all segments
test("IUD-01-01-02_023-32", Include) {
   sql(s"""drop table if exists  t_carbn01""").collect
 sql(s"""create table t_carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""update t_carbn01 set(item_name) = ('Apple') where item_type_cd between 1 and 100""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""update t_carbn01 set (item_name) = ('Banana') where item_type_cd between 100 and 200""").collect
 sql(s"""update t_carbn01 set(item_name) = ('Apple') where item_type_cd between 1 and 100""").collect
 sql(s"""clean files for table t_carbn01""").collect
  checkAnswer(s"""select item_name, count(*) from t_carbn01 group by item_name""",
    Seq(Row("Apple",112),Row("Banana",28)), "DataLoadingIUDTestCase_IUD-01-01-02_023-32")
   sql(s"""drop table default.t_carbn01""").collect
}
       

//Test update with inner join without using alias
test("IUD-01-01-02_023-34", Include) {
   sql(s"""drop table if exists  t1""").collect
 sql(s"""drop table if exists  t2""").collect
 sql(s"""create table t1(name string, dept string) stored by 'carbondata'""").collect
 sql(s"""insert into t1 select 'Kris','HR'""").collect
 sql(s"""insert into t1 select 'Ram','DEV'""").collect
 sql(s"""create table t2(name string, dept string) stored by 'carbondata'""").collect
 sql(s"""insert into t2 select 'Kris','HR'""").collect
 sql(s"""insert into t2 select 'Ram','FIN'""").collect
 sql(s"""update t2 a set(a.dept) = (select b.dept from t1 b where a.name = b.name and a.dept != b.dept)""").collect
  checkAnswer(s"""select dept from t2 where name = 'Ram'""",
    Seq(Row("DEV")), "DataLoadingIUDTestCase_IUD-01-01-02_023-34")
   sql(s"""drop table t2""").collect
}
       

//Test update with inner join on multiple tables and update value
test("IUD-01-01-02_023-35", Include) {
   sql(s"""drop table if exists  t_carbn31""").collect
 sql(s"""drop table if exists  t_carbn21""").collect
 sql(s"""drop table if exists  t_carbn22""").collect
 sql(s"""drop table if exists  t_carbn23""").collect
 sql(s"""drop table if exists  t_carbn24""").collect
 sql(s"""drop table if exists  t_carbn25""").collect
 sql(s"""drop table if exists  t_carbn26""").collect
 sql(s"""drop table if exists  t_carbn27""").collect
 sql(s"""drop table if exists  t_carbn28""").collect
 sql(s"""drop table if exists  t_carbn29""").collect
 sql(s"""drop table if exists  t_carbn30""").collect
 sql(s"""create table t_carbn31 (item_code string, item_name1 string,item_name2 string,item_name3 string,item_name4 string,item_name5 string,item_name6 string,item_name7 string,item_name8 string,item_name9 string,item_name10 string) stored by 'carbondata'""").collect
 sql(s"""insert into t_carbn31 select 'a1','aa','ba','ca','da','ea','fa','ga','ia','ja','ka'""").collect
 sql(s"""create table t_carbn21(item_code string, item_name string) stored by 'carbondata'""").collect
 sql(s"""insert into t_carbn21 select 'a1','x1'""").collect
 sql(s"""create table t_carbn22(item_code string, item_name string) stored by 'carbondata'""").collect
 sql(s"""insert into t_carbn22 select 'a1','x2'""").collect
 sql(s"""create table t_carbn23(item_code string, item_name string) stored by 'carbondata'""").collect
 sql(s"""insert into t_carbn23 select 'a1','x3'""").collect
 sql(s"""create table t_carbn24(item_code string, item_name string) stored by 'carbondata'""").collect
 sql(s"""insert into t_carbn24 select 'a1','x4'""").collect
 sql(s"""create table t_carbn25(item_code string, item_name string) stored by 'carbondata'""").collect
 sql(s"""insert into t_carbn25 select 'a1','x5'""").collect
 sql(s"""create table t_carbn26(item_code string, item_name string) stored by 'carbondata'""").collect
 sql(s"""insert into t_carbn26 select 'a1','x6'""").collect
 sql(s"""create table t_carbn27(item_code string, item_name string) stored by 'carbondata'""").collect
 sql(s"""insert into t_carbn27 select 'a1','x7'""").collect
 sql(s"""create table t_carbn28(item_code string, item_name string) stored by 'carbondata'""").collect
 sql(s"""insert into t_carbn28 select 'a1','x8'""").collect
 sql(s"""create table t_carbn29(item_code string, item_name string) stored by 'carbondata'""").collect
 sql(s"""insert into t_carbn29 select 'a1','x9'""").collect
 sql(s"""create table t_carbn30(item_code string, item_name string) stored by 'carbondata'""").collect
 sql(s"""insert into t_carbn30 select 'a1','x10'""").collect
 sql(s"""select * from t_carbn31""").collect
 sql(s"""update t_carbn31 a set (a.item_name1, a.item_name2, a.item_name3, a.item_name4, a.item_name5, a.item_name6, a.item_name7, a.item_name8, a.item_name9, a.item_name10) = (select b.item_name,c.item_name,d.item_name,e.item_name,f.item_name,g.item_name,h.item_name,i.item_name,j.item_name,k.item_name from t_carbn21 b,t_carbn22 c,t_carbn23 d,t_carbn24 e,t_carbn25 f,t_carbn26 g,t_carbn27 h,t_carbn28 i,t_carbn29 j,t_carbn30 k where a.item_code=b.item_code and b.item_code=c.item_code and c.item_code=d.item_code and d.item_code=e.item_code and e.item_code=f.item_code and f.item_code=g.item_code and g.item_code=h.item_code and h.item_code=i.item_code and i.item_code=j.item_code and j.item_code=k.item_code)""").collect
  checkAnswer(s"""select item_name10 from t_carbn31""",
    Seq(Row("x10")), "DataLoadingIUDTestCase_IUD-01-01-02_023-35")
   sql(s"""drop table t_carbn31""").collect
}
       

//Creating table 
test("IUD-01-01-02_023-36", Include) {
   sql(s"""use default""").collect
 sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'""").collect
  checkAnswer(s"""select count(cust_name) as count from uniqdata where cust_id='9001'""",
    Seq(Row(0)), "DataLoadingIUDTestCase_IUD-01-01-02_023-36")
  
}
       

//Load the data in Uniqdata table
test("IUD-01-01-02_023-37", Include) {
   sql(s"""use default""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
  checkAnswer(s"""select cust_name from uniqdata where cust_id='9001'""",
    Seq(Row("CUST_NAME_00001")), "DataLoadingIUDTestCase_IUD-01-01-02_023-37")
  
}
       

//Update the Uniqdata table 
test("IUD-01-01-02_023-38", Include) {
   sql(s"""use default""").collect
 sql(s"""update uniqdata set (CUST_ID)= ('9001') where CUST_ID=9000""").collect
  checkAnswer(s"""select count(cust_name) as count  from uniqdata where cust_id='9001' limit 1 """,
    Seq(Row(2)), "DataLoadingIUDTestCase_IUD-01-01-02_023-38")
  
}
       

//Select the Uniqdata table
test("IUD-01-01-02_023-39", Include) {
   sql(s"""use default""").collect
 sql(s"""select CUST_ID from uniqdata where CUST_ID=9001""").collect
  checkAnswer(s"""select count(CUST_ID) as count from uniqdata where CUST_ID=9001""",
    Seq(Row(2)), "DataLoadingIUDTestCase_IUD-01-01-02_023-39")
  
}
       

//Update the Uniqdata table 
test("IUD-01-01-02_023-40", Include) {
   sql(s"""use default""").collect
 sql(s"""update uniqdata set (CUST_NAME)= ('') where CUST_NAME='CUST_NAME_00000'""").collect
  checkAnswer(s"""Select count(CUST_NAME) as count from uniqdata  where CUST_NAME='CUST_NAME_00000'""",
    Seq(Row(0)), "DataLoadingIUDTestCase_IUD-01-01-02_023-40")
  
}
       

//Select the Uniqdata table
test("IUD-01-01-02_023-41", Include) {
   sql(s"""use default""").collect
 sql(s"""select CUST_NAME from uniqdata""").collect
  checkAnswer(s"""select count(cust_name) as count  from uniqdata where cust_id='9001' limit 1 """,
    Seq(Row(2)), "DataLoadingIUDTestCase_IUD-01-01-02_023-41")
  
}
       

//Update the Uniqdata table 
test("IUD-01-01-02_023-42", Include) {
   sql(s"""use default""").collect
 sql(s"""update uniqdata set (ACTIVE_EMUI_VERSION)= ('') where ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_00000'""").collect
  checkAnswer(s"""select count(ACTIVE_EMUI_VERSION) as count from  uniqdata where ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_00000'""",
    Seq(Row(0)), "DataLoadingIUDTestCase_IUD-01-01-02_023-42")
  
}
       

//Select the Uniqdata table
test("IUD-01-01-02_023-43", Include) {
   sql(s"""use default""").collect
 sql(s"""select ACTIVE_EMUI_VERSION from uniqdata where ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_00000'""").collect
  checkAnswer(s"""select count(ACTIVE_EMUI_VERSION) as count from uniqdata where ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_00000'""",
    Seq(Row(0)), "DataLoadingIUDTestCase_IUD-01-01-02_023-43")
  
}
       

//Update the Uniqdata table 
test("IUD-01-01-02_023-44", Include) {
   sql(s"""use default""").collect
 sql(s"""update uniqdata set (CUST_NAME)= ('CUST_NAME_00001') where CUST_NAME='CUST_NAME_00000'""").collect
  checkAnswer(s"""select CUST_NAME from uniqdata where CUST_NAME='CUST_NAME_00001'""",
    Seq(Row("CUST_NAME_00001")), "DataLoadingIUDTestCase_IUD-01-01-02_023-44")
  
}
       

//Delete the uniqdata table 
test("IUD-01-01-02_023-45", Include) {
   sql(s"""use default""").collect
 sql(s"""delete from uniqdata where CUST_NAME=''""").collect
  checkAnswer(s"""select  count(CUST_NAME)as count  from uniqdata where CUST_NAME=''""",
    Seq(Row(0)), "DataLoadingIUDTestCase_IUD-01-01-02_023-45")
  
}
       

//Load the data in Uniqdata table
test("IUD-01-01-02_023-46", Include) {
   sql(s"""use default""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
  checkAnswer(s"""select count(cust_name) as count from uniqdata where cust_id='9001' limit 2""",
    Seq(Row(2)), "DataLoadingIUDTestCase_IUD-01-01-02_023-46")
  
}
       

//Update the Uniqdata table 
test("IUD-01-01-02_023-47", Include) {
   sql(s"""use default""").collect
 sql(s"""update uniqdata set (CUST_ID)= ('9001') where CUST_ID=9000""").collect
  checkAnswer(s"""Select count(CUST_ID) as count from uniqdata where CUST_ID=9001""",
    Seq(Row(3)), "DataLoadingIUDTestCase_IUD-01-01-02_023-47")
  
}
       

//Delete the uniqdata table 
test("IUD-01-01-02_023-48", Include) {
   sql(s"""use default""").collect
 sql(s"""delete from uniqdata where CUST_ID=''""").collect
  checkAnswer(s"""select count(CUST_ID) as count  FROM uniqdata where CUST_ID=''""",
    Seq(Row(0)), "DataLoadingIUDTestCase_IUD-01-01-02_023-48")
  
}
       

//Select the Uniqdata table
test("IUD-01-01-02_023-49", Include) {
   sql(s"""use default""").collect
 sql(s"""select CUST_ID from uniqdata""").collect
  checkAnswer(s"""select count(CUST_ID) as count from uniqdata limit 1""",
    Seq(Row(4000)), "DataLoadingIUDTestCase_IUD-01-01-02_023-49")
  
}
       

//Update the Uniqdata table 
test("IUD-01-01-02_023-50", Include) {
   sql(s"""use default""").collect
 sql(s"""update uniqdata set (ACTIVE_EMUI_VERSION)= ('') where ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_00000'""").collect
  checkAnswer(s"""select count(ACTIVE_EMUI_VERSION) as count from uniqdata where ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_00000'""",
    Seq(Row(0)), "DataLoadingIUDTestCase_IUD-01-01-02_023-50")
  
}
       

//Delete the uniqdata table 
test("IUD-01-01-02_023-51", Include) {
   sql(s"""use default""").collect
 sql(s"""delete from uniqdata where ACTIVE_EMUI_VERSION=''""").collect
  checkAnswer(s"""select count(ACTIVE_EMUI_VERSION) as count  from uniqdata where ACTIVE_EMUI_VERSION=''""",
    Seq(Row(0)), "DataLoadingIUDTestCase_IUD-01-01-02_023-51")
  
}
       

//Select the Uniqdata table
test("IUD-01-01-02_023-52", Include) {
   sql(s"""use default""").collect
 sql(s"""select ACTIVE_EMUI_VERSION from uniqdata""").collect
  checkAnswer(s"""Select count(ACTIVE_EMUI_VERSION ) as count from uniqdata limit 3""",
    Seq(Row(3998)), "DataLoadingIUDTestCase_IUD-01-01-02_023-52")
  
}
       

//Load the data in Uniqdata table
test("IUD-01-01-02_023-53", Include) {
   sql(s"""use default""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
  checkAnswer(s"""select count(cust_name) as count  from uniqdata where cust_id='9001' """,
    Seq(Row(3)), "DataLoadingIUDTestCase_IUD-01-01-02_023-53")
  
}
       

//Delete the uniqdata table 
test("IUD-01-01-02_023-54", Include) {
   sql(s"""use default""").collect
 sql(s"""delete from uniqdata where CUST_ID='9000'""").collect
  checkAnswer(s"""select count(CUST_ID) as count from uniqdata where CUST_ID='9000'""",
    Seq(Row(0)), "DataLoadingIUDTestCase_IUD-01-01-02_023-54")
  
}
       

//Select the Uniqdata table
test("IUD-01-01-02_023-55", Include) {
   sql(s"""use default""").collect
 sql(s"""select * from uniqdata where CUST_ID='9000'""").collect
  checkAnswer(s"""select count(CUST_ID) as count from uniqdata where CUST_ID='9000'""",
    Seq(Row(0)), "DataLoadingIUDTestCase_IUD-01-01-02_023-55")
  
}
       

//Delete the uniqdata table 
test("IUD-01-01-02_023-56", Include) {
   sql(s"""use default""").collect
 sql(s"""delete from uniqdata where BIGINT_COLUMN1=123372036854""").collect
  checkAnswer(s"""select count(BIGINT_COLUMN1) as count from uniqdata where BIGINT_COLUMN1=123372036854""",
    Seq(Row(0)), "DataLoadingIUDTestCase_IUD-01-01-02_023-56")
  
}
       

//Load the data in Uniqdata table
test("IUD-01-01-02_023-57", Include) {
   sql(s"""use default""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
  checkAnswer(s"""select cust_name from uniqdata where cust_id='9001' """,
    Seq(Row("CUST_NAME_00001"),Row("CUST_NAME_00001"),Row("CUST_NAME_00001"),Row("CUST_NAME_00001")), "DataLoadingIUDTestCase_IUD-01-01-02_023-57")
  
}
       

//Update the Uniqdata table 
test("IUD-01-01-02_023-58", Include) {
   sql(s"""use default""").collect
 sql(s"""update uniqdata set (ACTIVE_EMUI_VERSION)= ('') where ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_00000'""").collect
  checkAnswer(s""" select count(ACTIVE_EMUI_VERSION) as count from uniqdata where ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_00000'""",
    Seq(Row(0)), "DataLoadingIUDTestCase_IUD-01-01-02_023-58")
  
}
       

//Update the Uniqdata table 
test("IUD-01-01-02_023-59", Include) {
   sql(s"""use default""").collect
 sql(s"""update uniqdata set (CUST_NAME)= ('  ') where CUST_ID=9000""").collect
  checkAnswer(s"""select count(CUST_NAME) as count from uniqdata where CUST_ID=9000""",
    Seq(Row(1)), "DataLoadingIUDTestCase_IUD-01-01-02_023-59")
  
}
       

//Update the Uniqdata table 
test("IUD-01-01-02_023-60", Include) {
   sql(s"""use default""").collect
 sql(s"""update uniqdata set (DOB)= ('2012-01-12 03:14:05') where CUST_ID=9000""").collect
  checkAnswer(s"""select count(DOB) as count  from uniqdata where  CUST_ID=9000""",
    Seq(Row(1)), "DataLoadingIUDTestCase_IUD-01-01-02_023-60")
  
}
       

//Delete the uniqdata table 
test("IUD-01-01-02_023-61", Include) {
   sql(s"""use default""").collect
 sql(s"""delete from uniqdata where CUST_ID=9000""").collect
  sql(s"""select DOB from uniqdata where CUST_ID=9000""").collect
  
  
}
       

//Update the Uniqdata table 
test("IUD-01-01-02_023-62", Include) {
   sql(s"""use default""").collect
 sql(s"""update uniqdata set (DOB)= ('2012-02-12 03:14:05') where CUST_ID=9001""").collect
  checkAnswer(s"""select DOB from uniqdata where CUST_ID=9001""",
    Seq(Row(Timestamp.valueOf("2012-02-12 03:14:05.0")),Row(Timestamp.valueOf("2012-02-12 03:14:05.0")),Row(Timestamp.valueOf("2012-02-12 03:14:05.0")),Row(Timestamp.valueOf("2012-02-12 03:14:05.0"))), "DataLoadingIUDTestCase_IUD-01-01-02_023-62")
  
}
       

//Delete the uniqdata table 
test("IUD-01-01-02_023-63", Include) {
   sql(s"""use default""").collect
 sql(s"""delete from uniqdata where CUST_ID=9001""").collect
  sql(s"""select DOB from uniqdata where CUST_ID=9001""").collect
  
  
}
       

//Delete the uniqdata table 
test("IUD-01-01-02_023-64", Include) {
   sql(s"""use default""").collect
 sql(s"""delete from uniqdata""").collect
  checkAnswer(s"""select count(CUST_ID) as count from uniqdata""",
    Seq(Row(0)), "DataLoadingIUDTestCase_IUD-01-01-02_023-64")
  
}
       

//Delete the uniqdata table 
test("IUD-01-01-02_023-65", Include) {
   sql(s"""use default""").collect
 sql(s"""delete from uniqdata where CUST_ID=9001 and (CUST_NAME)='CUST_NAME_00000'""").collect
  checkAnswer(s"""select count(CUST_NAME) as count from uniqdata  where CUST_ID=9001 and (CUST_NAME)='CUST_NAME_00000'""",
    Seq(Row(0)), "DataLoadingIUDTestCase_IUD-01-01-02_023-65")
  
}
       

//Load the data in Uniqdata table
test("IUD-01-01-02_023-66", Include) {
   sql(s"""use default""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
  checkAnswer(s"""select cust_name from uniqdata where cust_id='9001' """,
    Seq(Row("CUST_NAME_00001")), "DataLoadingIUDTestCase_IUD-01-01-02_023-66")
  
}
       

//Update the Uniqdata table 
test("IUD-01-01-02_023-67", Include) {
   sql(s"""use default""").collect
 sql(s"""update uniqdata set (DOJ)= ('2012-01-12 03:14:05') where CUST_ID=9001""").collect
  checkAnswer(s"""select DOJ from uniqdata where  CUST_ID=9001""",
    Seq(Row(Timestamp.valueOf("2012-01-12 03:14:05.0"))), "DataLoadingIUDTestCase_IUD-01-01-02_023-67")
  
}
       

//Delete the uniqdata table 
test("IUD-01-01-02_023-68", Include) {
   sql(s"""use default""").collect
 try {
   sql(s"""delete from table uniqdata where segment.id IN(0)""").collect
 } catch {
   case e: Exception =>
     // ignore as data is already deleted in segment 0
 }
  checkAnswer(s"""select DOJ from uniqdata where CUST_ID=9001""",
    Seq(Row(Timestamp.valueOf("2012-01-12 03:14:05.0"))), "DataLoadingIUDTestCase_IUD-01-01-02_023-68")
  
}
       

//Load the data in Uniqdata table
test("IUD-01-01-02_023-69", Include) {
   sql(s"""use default""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
  checkAnswer(s"""select cust_name from uniqdata where cust_id='9001' """,
    Seq(Row("CUST_NAME_00001"),Row("CUST_NAME_00001")), "DataLoadingIUDTestCase_IUD-01-01-02_023-69")
  
}
       

//Update the Uniqdata table 
test("IUD-01-01-02_023-70", Include) {
   sql(s"""use default""").collect
 sql(s"""update uniqdata set (DOJ)= ('2012-01-21 12:07:28.0') where DOB='1970-01-20 01:00:03.0'""").collect
  checkAnswer(s"""select DOJ from uniqdata where CUST_ID=9001""",
    Seq(Row(Timestamp.valueOf("2012-01-12 03:14:05.0")),Row(Timestamp.valueOf("1970-01-02 02:00:03.0"))), "DataLoadingIUDTestCase_IUD-01-01-02_023-70")
  
}
       

//Update the Uniqdata table 
test("IUD-01-01-02_023-71", Include) {
   sql(s"""use default""").collect
 sql(s"""update uniqdata set (DECIMAL_COLUMN1)= (12345658901.1234000000) where CUST_ID=9000""").collect
  checkAnswer(s"""select count(DECIMAL_COLUMN1) as count from uniqdata where CUST_ID=9000""",
    Seq(Row(2)), "DataLoadingIUDTestCase_IUD-01-01-02_023-71")
  
}
       

//Delete the uniqdata table 
test("IUD-01-01-02_023-72", Include) {
   sql(s"""use default""").collect
 sql(s"""delete from uniqdata where CUST_ID=9000""").collect
  checkAnswer(s"""select count(DECIMAL_COLUMN1) as count from uniqdata where CUST_ID=9000""",
    Seq(Row(0)), "DataLoadingIUDTestCase_IUD-01-01-02_023-72")
  
}
       

//Load the data in Uniqdata table
test("IUD-01-01-02_023-73", Include) {
   sql(s"""use default""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
  checkAnswer(s"""select cust_name from uniqdata where cust_id='9001' """,
    Seq(Row("CUST_NAME_00001"),Row("CUST_NAME_00001"),Row("CUST_NAME_00001")), "DataLoadingIUDTestCase_IUD-01-01-02_023-73")
  
}
       

//Update the Uniqdata table 
test("IUD-01-01-02_023-74", Include) {
   sql(s"""use default""").collect
 sql(s"""update uniqdata set (DECIMAL_COLUMN2)= (22345676901.1234000000) where CUST_ID=9001""").collect
  checkAnswer(s"""select DECIMAL_COLUMN2 from uniqdata where  CUST_ID=9001""",
    Seq(Row(22345676901.1234000000),Row(22345676901.1234000000),Row(22345676901.1234000000)), "DataLoadingIUDTestCase_IUD-01-01-02_023-74")
  
}
       

//Delete the uniqdata table 
test("IUD-01-01-02_023-75", Include) {
   sql(s"""use default""").collect
 sql(s"""delete from uniqdata where CUST_NAME='CUST_NAME_00000' and  CUST_ID=9001""").collect
  checkAnswer(s"""select count(CUST_NAME) as count from uniqdata where CUST_NAME='CUST_NAME_00000' and CUST_ID=9001""",
    Seq(Row(0)), "DataLoadingIUDTestCase_IUD-01-01-02_023-75")
  
}
       

//Load the data in Uniqdata table
test("IUD-01-01-02_023-76", Include) {
   sql(s"""use default""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
  checkAnswer(s"""select cust_name from uniqdata where cust_id='9001' """,
    Seq(Row("CUST_NAME_00001"),Row("CUST_NAME_00001"),Row("CUST_NAME_00001"),Row("CUST_NAME_00001")), "DataLoadingIUDTestCase_IUD-01-01-02_023-76")
  
}
       

//Delete the uniqdata table 
test("IUD-01-01-02_023-77", Include) {
   sql(s"""use default""").collect
 sql(s"""delete from uniqdata where DOB='1970-01-01 01:00:03' and DECIMAL_COLUMN1=12345678901.1234000000""").collect
  checkAnswer(s"""select count(DECIMAL_COLUMN1) as count from uniqdata where DOB='1970-01-01 01:00:03' and DECIMAL_COLUMN1=12345678901.1234000000""",
    Seq(Row(0)), "DataLoadingIUDTestCase_IUD-01-01-02_023-77")
  
}
       

//Load the data in Uniqdata table
test("IUD-01-01-02_023-78", Include) {
   sql(s"""use default""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
  checkAnswer(s"""select cust_name from uniqdata where cust_id='9001' """,
    Seq(Row("CUST_NAME_00001"),Row("CUST_NAME_00001"),Row("CUST_NAME_00001"),Row("CUST_NAME_00001"),Row("CUST_NAME_00001")), "DataLoadingIUDTestCase_IUD-01-01-02_023-78")
  
}
       

//Delete the uniqdata table 
test("IUD-01-01-02_023-79", Include) {
   sql(s"""use default""").collect
 sql(s"""delete from uniqdata where CUST_NAME='CUST_NAME_00000' and  CUST_ID=9001""").collect
  checkAnswer(s"""select count(CUST_NAME) as count from uniqdata where CUST_NAME='CUST_NAME_00000' and  CUST_ID=9001""",
    Seq(Row(0)), "DataLoadingIUDTestCase_IUD-01-01-02_023-79")
  
}
       

//Update the Uniqdata table 
test("IUD-01-01-02_023-80", Include) {
   sql(s"""use default""").collect
 sql(s"""update uniqdata set (DECIMAL_COLUMN1)= (12345678901.1234000000) where DOB='1970-01-01 01:00:03'""").collect
  checkAnswer(s"""select count(DECIMAL_COLUMN1) as count from uniqdata where DOB='1970-01-01 01:00:03' limit 2""",
    Seq(Row(0)), "DataLoadingIUDTestCase_IUD-01-01-02_023-80")
  
}
       

//Update the Uniqdata table 
test("IUD-01-01-02_023-81", Include) {
   sql(s"""use default""").collect
 sql(s"""update uniqdata set (DECIMAL_COLUMN1)= (12345678703.1234000000) where DOB='1970-01-02 01:00:03'""").collect
  checkAnswer(s"""select count(DECIMAL_COLUMN1) as count from uniqdata where DOB='1970-01-02 01:00:03' limit 2""",
    Seq(Row(5)), "DataLoadingIUDTestCase_IUD-01-01-02_023-81")
  
}
       

//Update the Uniqdata table 
test("IUD-01-01-02_023-82", Include) {
   sql(s"""use default""").collect
 sql(s"""update uniqdata set (DECIMAL_COLUMN1)= (12345678901.1234000000) where DOB='1970-01-01 01:00:03'""").collect
  checkAnswer(s"""select count(DECIMAL_COLUMN1) as count from uniqdata where DOB='1970-01-01 01:00:03' limit 2""",
    Seq(Row(0)), "DataLoadingIUDTestCase_IUD-01-01-02_023-82")
  
}
       

//Load the data in Uniqdata table
test("IUD-01-01-02_023-83", Include) {
   sql(s"""use default""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
  checkAnswer(s"""select cust_name from uniqdata where cust_id='9001' """,
    Seq(Row("CUST_NAME_00001"),Row("CUST_NAME_00001"),Row("CUST_NAME_00001"),Row("CUST_NAME_00001"),Row("CUST_NAME_00001"),Row("CUST_NAME_00001")), "DataLoadingIUDTestCase_IUD-01-01-02_023-83")
  
}
       

//Delete the uniqdata table 
test("IUD-01-01-02_023-84", Include) {
   sql(s"""use default""").collect
 sql(s"""delete from uniqdata where CUST_ID=9028""").collect
  checkAnswer(s"""select count(CUST_ID) as count from uniqdata where CUST_ID=9028""",
    Seq(Row(0)), "DataLoadingIUDTestCase_IUD-01-01-02_023-84")
  
}
       

//Delete the uniqdata table 
test("IUD-01-01-02_023-85", Include) {
   sql(s"""use default""").collect
 sql(s"""delete from uniqdata where CUST_ID=9029""").collect
  checkAnswer(s"""select count(CUST_ID) as count from uniqdata where CUST_ID=9029""",
    Seq(Row(0)), "DataLoadingIUDTestCase_IUD-01-01-02_023-85")
  
}
       

//Update the Uniqdata table 
test("IUD-01-01-02_023-86", Include) {
   sql(s"""use default""").collect
 sql(s"""update uniqdata set (Double_COLUMN2)= (-11434567489.7976000000) where INTEGER_COLUMN1=1""").collect
  checkAnswer(s"""select Double_COLUMN2 from uniqdata where  INTEGER_COLUMN1=1""",
    Seq(Row(-1.14345674897976E10),Row(-1.14345674897976E10),Row(-1.14345674897976E10),Row(-1.14345674897976E10)), "DataLoadingIUDTestCase_IUD-01-01-02_023-86")
  
}
       

//Update the Uniqdata table 
test("IUD-01-01-02_023-87", Include) {
   sql(s"""use default""").collect
 sql(s"""update uniqdata set (INTEGER_COLUMN1)= (122) where INTEGER_COLUMN1=1""").collect
  checkAnswer(s"""select count(INTEGER_COLUMN1) as count from uniqdata where INTEGER_COLUMN1=1""",
    Seq(Row(0)), "DataLoadingIUDTestCase_IUD-01-01-02_023-87")
  
}
       

//Update the Uniqdata table 
test("IUD-01-01-02_023-88", Include) {
   sql(s"""use default""").collect
 sql(s"""update uniqdata set (Double_COLUMN2)= (-11434567489.7976000000) where INTEGER_COLUMN1=122""").collect
  checkAnswer(s"""select count(Double_COLUMN2) as count from uniqdata where  INTEGER_COLUMN1=122 limit 1""",
    Seq(Row(10)), "DataLoadingIUDTestCase_IUD-01-01-02_023-88")
  
}
       

//Delete the uniqdata table 
test("IUD-01-01-02_023-89", Include) {
   sql(s"""use default""").collect
 sql(s"""delete from uniqdata where INTEGER_COLUMN1=1""").collect
  checkAnswer(s"""select count(INTEGER_COLUMN1) as count  from uniqdata limit 1""",
    Seq(Row(11992)), "DataLoadingIUDTestCase_IUD-01-01-02_023-89")
  
}
       

//Delete the uniqdata table
test("IUD-01-01-02_023-90", Include) {
   sql(s"""use default""").collect
 sql(s"""Delete from uniqdata""").collect
  checkAnswer(s"""Select count(CUST_ID)as count  from uniqdata""",
    Seq(Row(0)), "DataLoadingIUDTestCase_IUD-01-01-02_023-90")
  
}
       

//Creating table 
test("IUD-01-01-02_023-91", Include) {
   sql(s"""use default""").collect
 sql(s"""CREATE TABLE uniqdata1 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'""").collect
  checkAnswer(s"""select count(cust_name) as count from uniqdata1 where cust_id='9001' """,
    Seq(Row(0)), "DataLoadingIUDTestCase_IUD-01-01-02_023-91")
  
}
       

//Creating table 
test("IUD-01-01-02_023-92", Include) {
   sql(s"""use default""").collect
 sql(s"""CREATE TABLE uniqdata2 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'""").collect
  sql(s"""select cust_name from uniqdata2 where cust_id='9001' """).collect
  
  
}
       

//Load the data in Uniqdata table
test("IUD-01-01-02_023-93", Include) {
   sql(s"""use default""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata1 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
  sql(s"""select cust_name from uniqdata2 where cust_id='9001' """).collect
  
  
}
       

//Load the data in Uniqdata table
test("IUD-01-01-02_023-94", Include) {
   sql(s"""use default""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata2 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
  checkAnswer(s"""select cust_name from uniqdata2 where cust_id='9001' """,
    Seq(Row("CUST_NAME_00001")), "DataLoadingIUDTestCase_IUD-01-01-02_023-94")
  
}
       

//Delete the uniqdata table 
test("IUD-01-01-02_023-96", Include) {
   sql(s"""use default""").collect
 sql(s"""delete from uniqdata1 a where a.CUST_ID in (Select b.CUST_ID from uniqdata b where a.CUST_ID=b.CUST_ID)""").collect
  checkAnswer(s"""select count(cust_id) as count from uniqdata1""",
    Seq(Row(2001)), "DataLoadingIUDTestCase_IUD-01-01-02_023-96")
  
}
       

//Load the data in Uniqdata table
test("IUD-01-01-02_023-97", Include) {
   sql(s"""use default""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata1 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
  checkAnswer(s"""select count(cust_name) as count from uniqdata2 where cust_id='9001' """,
    Seq(Row(1)), "DataLoadingIUDTestCase_IUD-01-01-02_023-97")
  
}
       

//Delete the uniqdata table 
test("IUD-01-01-02_023-98", Include) {
   sql(s"""use default""").collect
 sql(s"""delete from uniqdata1 a where a.CUST_ID in (Select b.CUST_ID from (Select c.CUST_ID from  uniqdata c ) b)""").collect
  checkAnswer(s"""select count(cust_id) as count from uniqdata1""",
    Seq(Row(4002)), "DataLoadingIUDTestCase_IUD-01-01-02_023-98")
  
}
       
//update the table,then query table was not working
test("HQ_Defect_TC_2016121910112", Include) {
   sql(s"""use default""").collect
 sql(s"""drop table if exists t_carbn01""").collect
 sql(s"""create table t_carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""update t_carbn01 set (item_name) = ('x') where item_type_cd < 10""").collect
 sql(s"""select * from t_carbn01""").collect
 sql(s"""update t_carbn01 set (item_name) = ('xx') where item_type_cd in (14,41)""").collect
  checkAnswer(s"""select count(*) from t_carbn01""",
    Seq(Row(10)), "DataLoadingIUDTestCase_HQ_Defect_TC_2016121910112")
   sql(s"""drop table t_carbn01  """).collect
}
       

//multple time if we execute the commands, data is not updating properly

test("HQ_Defect_TC_2016122803692", Include) {
   sql(s"""drop table if exists t_carbn01""").collect
 sql(s""" create table t_carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""update t_carbn01 set (item_type_cd) = (1) where item_code = 'RE3423ee'""").collect
 sql(s"""update t_carbn01 set (item_type_cd) = (11) where item_code = 'RE3423ee'""").collect
 sql(s"""update t_carbn01 set (item_type_cd) = (11) where item_code = 'SAD423ee'""").collect
 sql(s"""update t_carbn01 set (item_type_cd) = (22) where item_code = 'SAD423ee'""").collect
 sql(s"""update t_carbn01 set (item_type_cd) = (22) where item_code = 'ASD423ee'""").collect
 sql(s"""update t_carbn01 set (item_type_cd) = (33) where item_code = 'ASD423ee'""").collect
  checkAnswer(s"""select count(item_type_cd) from default.t_carbn01""",
    Seq(Row(20)), "DataLoadingIUDTestCase_HQ_Defect_TC_2016122803692")
   sql(s"""drop table default.t_carbn01  """).collect
}
       

//delete the column which be updated,the result has problem
test("HQ_Defect_TC_2016120804163", Include) {
   sql(s"""drop table if exists t_carbn01""").collect
 sql(s"""create table t_carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""update t_carbn01 set (item_name) = ('x') where item_type_cd < 10""").collect
 sql(s"""update t_carbn01 set (item_name) = ('xx') where item_type_cd in (14,41)""").collect
 sql(s"""delete from t_carbn01 where item_name in  ('x','xx')""").collect
  sql(s"""select item_type_cd  from t_carbn01 where item_name in  ('x','xx')""").collect
  
   sql(s"""drop table default.t_carbn01""").collect
}
       

//Carries on the algorithm update to the string type's data,the display result is inconsistent
test("HQ_Defect_TC_2016110808686", Include) {
   sql(s"""drop table if exists t_carbn01""").collect
 sql(s"""create table t_carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""update t_carbn01 set (active_status, item_type_cd,qty_day_avg,sell_price,sell_pricep,update_time) = (active_status+1, item_type_cd-10, qty_day_avg*2,sell_price%2,sell_price/2,concat(update_time,2))""").collect
  checkAnswer(s"""select active_status from t_carbn01""",
    Seq(Row(null),Row(null),Row(null),Row(null),Row(null),Row(null),Row(null),Row(null),Row(null),Row(null)), "DataLoadingIUDTestCase_HQ_Defect_TC_2016110808686")
   sql(s"""drop table t_carbn01  """).collect
}
       

//Carries on the algorithm update to the string type's data,the display result is inconsistent
test("HQ_Defect_TC_2016110901163", Include) {
   sql(s"""drop table if exists t_carbn01""").collect
 sql(s""" create table t_carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
 sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table T_Carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
 sql(s"""update t_carbn01 set(item_code) = ('xx1') where item_type_cd between 0 and 5""").collect
 sql(s"""update t_carbn01 set(item_code) = ('xx2') where item_type_cd =41 and exists(select 1 from t_carbn01 a where a.item_type_cd < 100)""").collect

 sql(s"""update t_carbn01 a set(a.item_type_cd) = (select b.qty_day_avg from t_carbn01 b where b.item_code = 'DE3423ee') where qty_day_avg = 4510""").collect
  checkAnswer(s"""select item_type_cd from t_carbn01 where qty_day_avg = 4510""",
    Seq(Row(4510)), "DataLoadingIUDTestCase_HQ_Defect_TC_2016110901163")
   sql(s"""drop table default.t_carbn01  """).collect
}

  test("[CARBONDATA-2604] ", Include){
    sql("drop table if exists brinjal").collect
    sql("create table brinjal (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('table_blocksize'='2000','sort_columns'='imei')").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/vardhandaterestruct.csv' INTO TABLE brinjal OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/vardhandaterestruct.csv' INTO TABLE brinjal OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/vardhandaterestruct.csv' INTO TABLE brinjal OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect
    sql("insert into brinjal select * from brinjal").collect
    sql("update brinjal set (AMSize)= ('8RAM size') where AMSize='4RAM size'").collect
    sql("delete from brinjal where AMSize='8RAM size'").collect
    sql("delete from table brinjal where segment.id IN(0)").collect
    sql("clean files for table brinjal").collect
    sql("alter table brinjal compact 'minor'").collect
    sql("alter table brinjal compact 'major'").collect
    checkAnswer(s"""select count(*) from brinjal""",
      Seq(Row(335)), "CARBONDATA-2604")
    sql("drop table if exists brinjal")
  }
override def afterAll {
  sql("use default").collect
  sql("drop table if exists t_carbn02").collect
  sql("drop table if exists t_carbn01").collect
  sql("drop table if exists T_Parq1").collect
  sql("drop table if exists table_C21").collect
  sql("drop table if exists t_hive01").collect
  sql("drop table if exists t_carbn2").collect
  sql("drop table if exists t_carbn1").collect
  sql("drop table if exists t1").collect
  sql("drop table if exists t2").collect
  sql("drop table if exists t_carbn21").collect
  sql("drop table if exists t_carbn22").collect
  sql("drop table if exists t_carbn23").collect
  sql("drop table if exists t_carbn24").collect
  sql("drop table if exists t_carbn25").collect
  sql("drop table if exists t_carbn26").collect
  sql("drop table if exists t_carbn27").collect
  sql("drop table if exists t_carbn28").collect
  sql("drop table if exists t_carbn20").collect
  sql("drop table if exists t_carbn30").collect
  sql("drop table if exists t_carbn31").collect
  sql("drop table if exists uniqdata0001_Test").collect
  sql("drop table if exists uniqdata").collect
  sql("drop table if exists uniqdata1").collect
  sql("drop table if exists uniqdata2").collect
  sql("drop table if exists uniqdata023456").collect
  sql("drop table if exists t_carbn01b").collect
  sql("drop table if exists T_Hive1").collect
  sql("drop table if exists T_Hive6").collect
  sql("drop table if exists brinjal")

}
}