
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

import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util._
import org.apache.spark.util.SparkUtil
import org.scalatest.BeforeAndAfterAll
import org.apache.carbondata.common.constants.LoggerAction
import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException

import org.apache.carbondata.spark.exception.ProcessMetaDataException

/**
 * Test Class for AlterTableTestCase to verify all scenerios
 */

class AlterTableTestCase extends QueryTest with BeforeAndAfterAll {
         

  //Check alter table using with alter command in lower case
  test("RenameTable_001_01", Include) {
     sql(s"""create table test1 (name string, id int) stored by 'carbondata'""").collect
   sql(s"""insert into test1 select 'xx',1""").collect
   sql(s"""alter table test1 rename to test2""").collect
    checkAnswer(s"""select count(*) from test2""",
      Seq(Row(1)), "AlterTableTestCase_RenameTable_001_01")
     sql(s"""drop table if exists test2""").collect
  }


  //Check alter table using with alter command in upper & lower case
  test("RenameTable_001_02", Include) {
     sql(s"""create table test1 (name string, id int) stored by 'carbondata'""").collect
   sql(s"""insert into test1 select 'xx',1""").collect
   sql(s"""alter table Test1 RENAME to teSt2""").collect
   sql(s"""insert into test2 select 'yy',2""").collect
    checkAnswer(s"""select count(*) from test2""",
      Seq(Row(2)), "AlterTableTestCase_RenameTable_001_02")
     sql(s"""drop table if exists test2""").collect
  }


  //Check alter table using with alter command in upper case
  test("RenameTable_001_03", Include) {
     sql(s"""create table test1 (name string, id int) stored by 'carbondata'""").collect
   sql(s"""insert into test1 select 'xx',1""").collect
   sql(s"""alter table test1 RENAME TO test2""").collect
    checkAnswer(s"""select count(*) from test2""",
      Seq(Row(1)), "AlterTableTestCase_RenameTable_001_03")
     sql(s"""drop table if exists test2""").collect
  }


  //Check alter table where target table speficifed with database name
  test("RenameTable_001_04", Include) {
     sql(s"""create table test1 (name string, id int) stored by 'carbondata'""").collect
   sql(s"""insert into test1 select 'xx',1""").collect
   sql(s"""alter table test1 RENAME TO defAult.test2""").collect
    checkAnswer(s"""select count(*) from test2""",
      Seq(Row(1)), "AlterTableTestCase_RenameTable_001_04")

  }


  //Check alter table run multiple times, revert back the name to original
  test("RenameTable_001_06", Include) {
    sql(s"""drop table if exists test2""").collect
    sql(s"""drop table if exists test1""").collect
    sql(s"""drop table if exists test3""").collect
     sql(s"""create table test1 (name string, id int) stored by 'carbondata'""").collect
   sql(s"""insert into test1 select 'xx',1""").collect
   sql(s"""alter table test1 rename to test2""").collect
   sql(s"""alter table test2 rename to test3""").collect
   sql(s"""alter table test3 rename to test1""").collect
    checkAnswer(s"""select count(*) from test1""",
      Seq(Row(1)), "AlterTableTestCase_RenameTable_001_06")
     sql(s"""drop table if exists test1""").collect
  }


  //Check data load after table rename
  test("RenameTable_001_07_1", Include) {
    sql(s"""drop table if exists test2""").collect
    sql(s"""drop table if exists test1""").collect
     sql(s"""create table test1 (name string, id int) stored by 'carbondata'""").collect
   sql(s"""insert into test1 select 'xx',1""").collect
   sql(s"""alter table test1 RENAME TO test2""").collect
   sql(s"""Insert into test2 select 'yy',2""").collect
    checkAnswer(s"""select count(*) from test2""",
      Seq(Row(2)), "AlterTableTestCase_RenameTable_001_07_1")

  }


  //Check data load after table rename
  test("RenameTable_001_07_2", Include) {

    checkAnswer(s"""select name from test2 where name = 'yy'""",
      Seq(Row("yy")), "AlterTableTestCase_RenameTable_001_07_2")
     sql(s"""drop table if exists test2""").collect
  }


  //Check alter table when the altered name is already present in the database
  test("RenameTable_001_08", Include) {
    intercept[Exception] {
      sql(s"""create table test1 (name string, id int) stored by 'carbondata'""").collect
      sql(s"""insert into test1 select 'xx',1""").collect
      sql(s"""create table test2 (name string, id int) stored by 'carbondata'""").collect
      sql(s"""alter table test1 RENAME TO test2""").collect
    }

    sql(s"""drop table if exists test1""").collect
    sql(s"""drop table if exists test2""").collect
  }


  //Check alter table when the altered name is given multiple times
  test("RenameTable_001_09", Include) {
    intercept[Exception] {
      sql(s"""create table test1 (name string, id int) stored by 'carbondata'""").collect
      sql(s"""insert into test1 select 'xx',1""").collect
      sql(s"""alter table test1 RENAME TO test2 test3""").collect
    }
    sql(s"""drop table if exists test1""").collect
  }


  //Check delete column for dimension column
  test("DeleteCol_001_01", Include) {
    intercept[Exception] {
      sql(s"""create table test1 (name string, id int) stored by 'carbondata'  """).collect
      sql(s"""insert into test1 select 'xx',1""").collect
      sql(s"""alter table test1 drop columns (name)""").collect
      sql(s"""select name from test1""").collect
    }
    sql(s"""drop table if exists test1""").collect
  }


  //Check delete column for measure column
  test("DeleteCol_001_02", Include) {
    intercept[Exception] {
      sql(s"""create table test1 (name string, id int) stored by 'carbondata'""").collect
      sql(s"""insert into test1 select 'xx',1""").collect
      sql(s"""alter table test1 drop columns (id)""").collect
      sql(s"""select id from test1""").collect
    }
    sql(s"""drop table if exists test1""").collect
  }


  //Check delete column for measure and dimension column
  test("DeleteCol_001_03", Include) {
    intercept[Exception] {
      sql(s"""create table test1 (name string, country string, upd_time timestamp, id int) stored by 'carbondata'""").collect
      sql(s"""insert into test1 select 'xx','yy',current_timestamp,1""").collect
      sql(s"""alter table test1 drop columns (id,name)""").collect
      sql(s"""select id,name  from test1""").collect
    }
    sql(s"""drop table if exists test1""").collect
  }


  //Check delete column for multiple column
  test("DeleteCol_001_04", Include) {
    intercept[Exception] {
      sql(s"""create table test1 (name string, country string, upd_time timestamp, id int) stored by 'carbondata' """).collect
      sql(s"""insert into test1 select 'xx','yy',current_timestamp,1""").collect
      sql(s"""alter table test1 drop columns (name, upd_time)""").collect
      sql(s"""select name, upd_time from test1""").collect
    }
    sql(s"""drop table if exists test1""").collect
  }


  //Check delete column for all columns
  test("DeleteCol_001_05", Include) {
    sql(s"""create table test1 (name string, country string, upd_time timestamp, id int) stored by 'carbondata'""").collect
    sql(s"""insert into test1 select 'xx','yy',current_timestamp,1""").collect
    sql(s"""alter table test1 drop columns (name, upd_time, country,id)""").collect
    sql(s"""drop table if exists test1""").collect
  }


  //Check delete column for include dictionary column
  test("DeleteCol_001_06", Include) {
    intercept[Exception] {
      sql(s"""create table test1 (name string, id int) stored by 'carbondata' """).collect
      sql(s"""insert into test1 select 'xx',1""").collect
      sql(s"""alter table test1 drop columns (id)""").collect
      sql(s"""select id from test1""").collect
    }
    sql(s"""drop table if exists test1""").collect
  }


  //Check delete column for timestamp column
  test("DeleteCol_001_08", Include) {
    intercept[Exception] {
      sql(s"""create table test1 (name string, country string, upd_time timestamp, id int) stored by 'carbondata'""").collect
      sql(s"""insert into test1 select 'xx','yy',current_timestamp,1""").collect
      sql(s"""alter table test1 drop columns (upd_time)""").collect
      sql(s"""select upd_time from test1""").collect
    }
    sql(s"""drop table if exists test1""").collect
  }


  //Check the drop of added column will remove the column from table
  test("DeleteCol_001_09_1", Include) {
     sql(s"""create table test1 (name string, country string, upd_time timestamp, id int) stored by 'carbondata'""").collect
   sql(s"""insert into test1 select 'xx','yy',current_timestamp,1""").collect
   sql(s"""alter table test1 add columns (name2 string)""").collect
   sql(s"""insert into test1 select 'xx','yy',current_timestamp,1,'abc'""").collect
    checkAnswer(s"""select count(id) from test1 where name2 = 'abc'""",
      Seq(Row(1)), "AlterTableTestCase_DeleteCol_001_09_1")
     sql(s"""drop table if exists test1""").collect
  }


  //Check the drop of added column will remove the column from table
  test("DeleteCol_001_09_2", Include) {
    intercept[Exception] {
     sql(s"""create table test1 (name string, country string, upd_time timestamp, id int) stored by 'carbondata'""").collect
     sql(s"""insert into test1 select 'xx','yy',current_timestamp,1""").collect
     sql(s"""alter table test1 add columns (name2 string)""").collect
     sql(s"""insert into test1 select 'xx','yy',current_timestamp,1,'abc'""").collect
     sql(s"""alter table test1 drop columns (name2)""").collect
     sql(s"""select count(id) from test1 where name2 = 'abc'""").collect
    }
     sql(s"""drop table if exists test1""").collect
  }


  //Drop a column and add it again with a default value
  test("DeleteCol_001_10", Include) {
     sql(s"""create table test1 (name string, country string, upd_time timestamp, id int) stored by 'carbondata'""").collect
   sql(s"""insert into test1 select 'xx','yy',current_timestamp,1""").collect
   sql(s"""alter table test1 drop columns (id)""").collect
   sql(s"""alter table test1 add columns (id bigint) tblproperties('default.value.id'='999')""").collect
    checkAnswer(s"""select id from test1""",
      Seq(Row(999)), "AlterTableTestCase_DeleteCol_001_10")
     sql(s"""drop table if exists test1""").collect
  }


  //Drop a column and add it again with a default value
  test("DeleteCol_001_11", Include) {
    sql(s"""drop table if exists test1""").collect
     sql(s"""create table test1 (name string, country string, upd_time timestamp, id int) stored by 'carbondata'""").collect
   sql(s"""insert into test1 select 'xx','yy',current_timestamp,1""").collect
   sql(s"""alter table test1 drop columns (id)""").collect
   sql(s"""insert into test1 select 'a','china',current_timestamp""").collect
   sql(s"""alter table test1 add columns (id bigint)  tblproperties('default.value.id'='999')""").collect
    checkAnswer(s"""select id from test1""",
      Seq(Row(999), Row(999)), "AlterTableTestCase_DeleteCol_001_11")
     sql(s"""drop table if exists test1""").collect
  }


  //Check add column for multiple column adds
  test("AddColumn_001_01", Include) {
     sql(s"""drop table if exists test1""").collect
   sql(s"""create table test1 (name string, id int) stored by 'carbondata'""").collect
   sql(s"""insert into test1 select 'xx',1""").collect
   sql(s"""ALTER TABLE test1 ADD COLUMNS (upd_time timestamp, country string)""").collect
    checkAnswer(s"""select upd_time, country from test1""",
      Seq(Row(null,null)), "AlterTableTestCase_AddColumn_001_01")
     sql(s"""drop table if exists test1""").collect
  }


  //Check add column for dimension column and add table property to set default value
  test("AddColumn_001_02", Include) {
    sql(s"""drop table if exists test1""").collect
     sql(s"""create table test1 (name string, id int) stored by 'carbondata'""").collect
   sql(s"""insert into test1 select 'xx',1""").collect
   sql(s"""insert into test1 select 'xx',12""").collect
   sql(s"""ALTER TABLE test1 ADD COLUMNS (country string) TBLPROPERTIES('DEFAULT.VALUE.country'='China')""").collect
    checkAnswer(s"""select count(country) from test1""",
      Seq(Row(2)), "AlterTableTestCase_AddColumn_001_02")
     sql(s"""drop table if exists test1""").collect
  }


  //Check add column to add a measure column
  test("AddColumn_001_03", Include) {
    sql(s"""drop table if exists test1""").collect
     sql(s"""create table test1 (name string, id int) stored by 'carbondata'""").collect
   sql(s"""insert into test1 select 'xx',1""").collect
   sql(s"""ALTER TABLE test1 ADD COLUMNS (id1 int)""").collect
    checkAnswer(s"""select id1 from test1""",
      Seq(Row(null)), "AlterTableTestCase_AddColumn_001_03")
     sql(s"""drop table if exists test1""").collect
  }


  //Check add column to add a measure column added with dictionary include
  test("AddColumn_001_04", Include) {
     sql(s"""drop table if exists test1""").collect
   sql(s"""create table test1 (name string, id int) stored by 'carbondata'""").collect
   sql(s"""insert into test1 select 'xx',1""").collect
   sql(s"""insert into test1 select 'xx',11""").collect
   sql(s"""ALTER TABLE test1 ADD COLUMNS (id1 int) """).collect
    checkAnswer(s"""select id1 from test1""",
      Seq(Row(null), Row(null)), "AlterTableTestCase_AddColumn_001_04")
     sql(s"""drop table if exists test1""").collect
  }


  //Check add column to add a measure column initialized with default value
  ignore("AddColumn_001_05", Include) {
    sql(s"""drop table if exists test1""").collect
     sql(s"""create table test1 (name string, id int) stored by 'carbondata'""").collect
   sql(s"""insert into test1 select 'xx',1""").collect
   sql(s"""insert into test1 select 'xx',11""").collect
   sql(s"""ALTER TABLE test1 ADD COLUMNS (price decimal(10,6)) TBLPROPERTIES('DEFAULT.VALUE.price'='11.111')""").collect
    checkAnswer(s"""select sum(price) from test1 where price = 11.111""",
      Seq(Row(22.222000)), "AlterTableTestCase_AddColumn_001_05")
     sql(s"""drop table if exists test1""").collect
  }


  //Check add column to add a measure column initialized with default value which does not suite the data type
  test("AddColumn_001_06", Include) {
    sql(s"""drop table if exists test1""").collect
     sql(s"""create table test1 (name string, id int) stored by 'carbondata'""").collect
   sql(s"""insert into test1 select 'xx',1""").collect
   sql(s"""ALTER TABLE test1 ADD COLUMNS (price bigint) TBLPROPERTIES('DEFAULT.VALUE.Price'='1.1')""").collect
    checkAnswer(s"""select price from test1""",
      Seq(Row(null)), "AlterTableTestCase_AddColumn_001_06")
     sql(s"""drop table if exists test1""").collect
  }


  //Check add column to add a measure column initialized with default value on a empty table
  test("AddColumn_001_07", Include) {
    sql(s"""drop table if exists test1""").collect
     sql(s"""create table test1 (name string, id int) stored by 'carbondata'""").collect
   sql(s"""insert into test1 select 'xx',1""").collect
   sql(s"""ALTER TABLE test1 ADD COLUMNS (price bigint) TBLPROPERTIES('DEFAULT.VALUE.Price'='11')""").collect
    checkAnswer(s"""select count(id) from test1 where price = 11""",
      Seq(Row(1)), "AlterTableTestCase_AddColumn_001_07")
     sql(s"""drop table if exists test1""").collect
  }


  //Check add column to add a dim and measure column
  test("AddColumn_001_08", Include) {
     sql(s"""drop table if exists test1""").collect
   sql(s"""create table test1 (name string, id int) stored by 'carbondata'""").collect
   sql(s"""insert into test1 select 'xx',1""").collect
   sql(s"""ALTER TABLE test1 ADD COLUMNS (id1 int, country string) """).collect
    checkAnswer(s"""select id1, country from test1""",
      Seq(Row(null,null)), "AlterTableTestCase_AddColumn_001_08")
     sql(s"""drop table if exists test1""").collect
  }


  //Check add column for measure and make it dictionary column
  test("AddColumn_001_09", Include) {
     sql(s"""drop table if exists test1""").collect
   sql(s"""create table test1 (name string) stored by 'carbondata'""").collect
   sql(s"""insert into test1 select 'xx'""").collect
   sql(s"""ALTER TABLE test1 ADD COLUMNS (Id int)  """).collect
    checkAnswer(s"""select id from test1""",
      Seq(Row(null)), "AlterTableTestCase_AddColumn_001_09")
     sql(s"""drop table if exists test1""").collect
  }


  //Check add column to add columns and exclude the dim col from dictionary
  test("AddColumn_001_10", Include) {
    sql(s"""drop table if exists test1""").collect
     sql(s"""create table test1 (name string) stored by 'carbondata'""").collect
   sql(s"""insert into test1 select 'xx'""").collect
   sql(s"""ALTER TABLE test1 ADD COLUMNS (upd_time timestamp, country string) """).collect
    checkAnswer(s"""select country, upd_time from test1""",
      Seq(Row(null,null)), "AlterTableTestCase_AddColumn_001_10")
     sql(s"""drop table if exists test1""").collect
  }


  //Check add column to add a timestamp column
  test("AddColumn_001_11", Include) {
    sql(s"""drop table if exists test1""").collect
     sql(s"""create table test1 (name string, id int) stored by 'carbondata'""").collect
   sql(s"""insert into test1 select 'xx',1""").collect
   sql(s"""ALTER TABLE test1 ADD COLUMNS (upd_time timestamp)""").collect
    checkAnswer(s"""select upd_time from test1""",
      Seq(Row(null)), "AlterTableTestCase_AddColumn_001_11")
     sql(s"""drop table if exists test1""").collect
  }


  //Check add column with option default value is given for an existing column
  test("AddColumn_001_14", Include) {
    intercept[Exception] {
      sql(s"""drop table if exists test1""").collect
      sql(s"""create table test1 (name string) stored by 'carbondata'""").collect
      sql(s"""insert into test1 select 'xx'""").collect
      sql(s"""ALTER TABLE test1 ADD COLUMNS (Id int) TBLPROPERTIES('default.value.name'='yy')""").collect
    }
    sql(s"""drop table if exists test1""").collect
  }


  //check alter column for small decimal to big decimal
  test("AlterData_001_02", Include) {
    sql(s"""drop table if exists test1""").collect
     sql(s"""create table test1 (name string, price decimal(3,2)) stored by 'carbondata'""").collect
   sql(s"""insert into test1 select 'xx',1.2""").collect
   sql(s"""alter table test1 change price price decimal(10,7)""").collect
   sql(s"""insert into test1 select 'xx2',999.9999999""").collect
    checkAnswer(s"""select name from test1 where price = 999.9999999""",
      Seq(Row("xx2")), "AlterTableTestCase_AlterData_001_02")
     sql(s"""drop table if exists test1""").collect
  }


  //check drop table after table rename using new name
  test("DropTable_001_01", Include) {
    sql(s"""drop table if exists test1""").collect
     sql(s"""create table test1 (name string, price decimal(3,2)) stored by 'carbondata'""").collect
   sql(s"""insert into test1 select 'xx',1.2""").collect
   sql(s"""alter table test1 rename to test2""").collect
    sql(s"""drop table test2""").collect
  }


  //check drop table after table rename using old name
  test("DropTable_001_02", Include) {
    intercept[Exception] {
      sql(s"""drop table if exists test1""").collect
      sql(s"""create table test1 (name string, price decimal(3,2)) stored by 'carbondata'""").collect
      sql(s"""insert into test1 select 'xx',1.2""").collect
      sql(s"""alter table test1 rename to test2""").collect
      sql(s"""drop table test1""").collect
    }
    sql(s"""drop table if exists test2""").collect
  }


  //check drop table after table rename using new name, after table load
  test("DropTable_001_03", Include) {
     sql(s"""create table test1 (name string, price decimal(3,2)) stored by 'carbondata'""").collect
   sql(s"""insert into test1 select 'xx',1.2""").collect
   sql(s"""alter table test1 rename to test2""").collect
   sql(s"""insert into test2 select 'yy',1""").collect
    sql(s"""drop table test2""").collect

  }


  //check drop table after alter table name, using new name when table is empty
  test("DropTable_001_04", Include) {
    sql(s"""drop table if exists test1""").collect
     sql(s"""create table test1 (name string, price decimal(3,2)) stored by 'carbondata'""").collect
   sql(s"""alter table test1 rename to test2""").collect
    sql(s"""drop table test2""").collect

  }


  //check drop table when table is altered by adding columns
  test("DropTable_001_05", Include) {
     sql(s"""drop table if exists test1""").collect
   sql(s"""create table test1 (name string, id int) stored by 'carbondata'  """).collect
   sql(s"""insert into test1 select 'xx',1""").collect
   sql(s"""ALTER TABLE test1 ADD COLUMNS (upd_time timestamp, country string) TBLPROPERTIES( 'DEFAULT.VALUE.country'='China')""").collect
   sql(s"""insert into test1 select 'yy',1,current_timestamp,'xx'""").collect
    sql(s"""drop table if exists test1""").collect

  }


  //Check schema changes and carbon dictionary additions for alter table when new column added
  test("StorageFi_001_02", Include) {
    sql(s"""drop table if exists test1""").collect
     sql(s"""create table test1 (country string, name string) stored by 'carbondata' """).collect
   sql(s"""insert into test1 select 'xx','uu'""").collect
    sql(s"""alter table test1 add columns (price decimal(10,4)) tblproperties('DEFAULT.VALUE.price'='11.111')""").collect
     sql(s"""drop table if exists test1""").collect
  }


  //Check dictionary cache is loaded with new added column when query is run
  ignore("Dictionary_001_01", Include) {
    sql(s"""drop table if exists test1""").collect
     sql(s"""create table test1 (name string, id decimal(3,2),country string) stored by 'carbondata' """).collect
   sql(s"""insert into test1 select 'xx',1.22,'china'""").collect
   sql(s"""alter table test1 add columns (price decimal(10,4)) tblproperties('DEFAULT.VALUE.price'='11.111')""").collect
    checkAnswer(s"""select * from test1""",
      Seq(Row("xx",1.22,"china",11.1110)), "AlterTableTestCase_Dictionary_001_01")
     sql(s"""drop table if exists test1""").collect
  }


  //Check if dropped column is removed from driver side LRU cache
  test("Dictionary_001_02", Include) {
    sql(s"""drop table if exists test1""").collect
     sql(s"""create table test1 (name string, id decimal(3,2),country string) stored by 'carbondata' """).collect
   sql(s"""insert into test1 select 'xx',1.22,'china'""").collect
   sql(s"""alter table test1 drop columns (country)""").collect
    checkAnswer(s"""select * from test1""",
      Seq(Row("xx",1.22)), "AlterTableTestCase_Dictionary_001_02")
     sql(s"""drop table if exists test1""").collect
  }


  //Check if dropped column is removed from driver side LRU cache at driver side
  test("Dictionary_001_03", Include) {
    sql(s"""drop table if exists test1""").collect
     sql(s"""create table test1 (name string, id decimal(3,2),country string) stored by 'carbondata' """).collect
   sql(s"""insert into test1 select 'xx',1.22,'china'""").collect
   sql(s"""alter table test1 drop columns(country)""").collect
    checkAnswer(s"""select * from test1""",
      Seq(Row("xx",1.22)), "AlterTableTestCase_Dictionary_001_03")
     sql(s"""drop table if exists test1""").collect
  }


  //Check table load works fine after alter table name
  test("Dataload_001_01", Include) {
     sql(s"""drop table if exists t_carbn01t""").collect
   sql(s"""drop table if exists t_carbn01""").collect
   sql(s"""create table default.t_carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table default.t_carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
   sql(s"""alter table t_carbn01 rename to t_carbn01t""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table default.t_carbn01t options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
     sql(s"""drop table if exists t_carbn01t""").collect
  }


  //Check table load into old table after alter table name
  test("Dataload_001_02", Include) {
    sql(s"""drop table if exists default.t_carbn01""").collect
     sql(s"""create table default.t_carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table default.t_carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
   sql(s"""alter table t_carbn01 rename to t_carbn01t""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table default.t_carbn01t options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
    checkAnswer(s"""select count(item_name) from t_carbn01t""",
      Seq(Row(20)), "AlterTableTestCase_Dataload_001_02")
     sql(s"""drop table if exists t_carbn01t""").collect
  }


  //Check table load works fine after alter table name
  test("Dataload_001_03", Include) {
    sql(s"""drop table if exists default.t_carbn01""").collect
     sql(s"""create table default.t_carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table default.t_carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
   sql(s"""alter table t_carbn01 change Profit Profit Decimal(10,4)""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table default.t_carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
     sql(s"""drop table if exists t_carbn01""").collect
  }


  //Check table load works fine after alter table name
  test("Dataload_001_04", Include) {
    sql(s"""drop table if exists default.t_carbn01""").collect
     sql(s"""create table default.t_carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table default.t_carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
   sql(s"""alter table t_carbn01 add columns (item_code1 string, item_code2 string)""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive2.csv' INTO table default.t_carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date,item_code1, item_code2')""").collect
    checkAnswer(s"""select count(item_name) from t_carbn01""",
      Seq(Row(20)), "AlterTableTestCase_Dataload_001_04")
     sql(s"""drop table if exists t_carbn01""").collect
  }


  //Check table load works fine after alter table name
  test("Dataload_001_05", Include) {
    sql(s"""drop table if exists default.t_carbn01""").collect
     sql(s"""create table default.t_carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table default.t_carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
   sql(s"""alter table t_carbn01 drop columns (Update_time, create_date)""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive2.csv' INTO table default.t_carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name')""").collect
    checkAnswer(s"""select count(item_name) from t_carbn01""",
      Seq(Row(20)), "AlterTableTestCase_Dataload_001_05")
     sql(s"""drop table if exists t_carbn01""").collect
  }


  //Check if alter table(add column) is supported when data load is happening
  test("Concurrent_alter_001_01", Include) {
    sql(s"""drop table if exists default.t_carbn01""").collect
     sql(s"""create table default.t_carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table default.t_carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
   sql(s"""alter table t_carbn01 add columns (item_code1 string, item_code2 string)""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive2.csv' INTO table default.t_carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date,item_code1,item_code2')""").collect
     sql(s"""drop table if exists t_carbn01""").collect
  }


  //Check if alter table(delete column) is supported when data load is happening
  test("Concurrent_alter_001_02", Include) {
    sql(s"""drop table if exists default.t_carbn01""").collect
     sql(s"""create table default.t_carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table default.t_carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
    sql(s"""alter table t_carbn01 drop columns (Update_time, create_date)""").collect
     sql(s"""drop table if exists t_carbn01""").collect
  }


  //Check if alter table(change column) is supported when data load is happening
  test("Concurrent_alter_001_03", Include) {
    sql(s"""drop table if exists default.t_carbn01""").collect
     sql(s"""create table default.t_carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table default.t_carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
    sql(s"""alter table t_carbn01 change Profit Profit Decimal(10,4)""").collect
     sql(s"""drop table if exists t_carbn01""").collect
  }


  //Check if alter table(rename) is supported when data load is happening
  test("Concurrent_alter_001_04", Include) {
    sql(s"""drop table if exists default.t_carbn01""").collect
     sql(s"""create table default.t_carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table default.t_carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
    sql(s"""alter table t_carbn01 rename to t_carbn01t""").collect
     sql(s"""drop table if exists t_carbn01t""").collect
  }


  //check table insert works fine after alter table to add a column
  test("Insertint_001_03", Include) {
    sql(s"""drop table if exists default.t_carbn01""").collect
    sql(s"""drop table if exists default.t_carbn02""").collect
     sql(s"""create table default.t_carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table default.t_carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
   sql(s"""create table default.t_carbn02(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""alter table t_carbn02 add columns (item_name1 string)""").collect
   sql(s"""insert into t_carbn02 select *, 'xxx' from t_carbn01""").collect
    sql(s"""Select count(*) from t_carbn02""").collect

     sql(s"""drop table if exists t_carbn01""").collect
   sql(s"""drop table if exists t_carbn02""").collect
  }


  //check table insert works fine after alter table to add a column
  test("Insertint_001_04", Include) {
    sql(s"""drop table if exists default.t_carbn01""").collect
    sql(s"""drop table if exists default.t_carbn02""").collect
     sql(s"""create table default.t_carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1.csv' INTO table default.t_carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
   sql(s"""create table default.t_carbn02(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""alter table t_carbn02 change Profit Profit decimal(10,4)""").collect
   sql(s"""insert into t_carbn02 select * from t_carbn01""").collect
    sql(s"""Select count(*) from t_carbn02""").collect

     sql(s"""drop table if exists t_carbn01""").collect
   sql(s"""drop table if exists t_carbn02""").collect
  }


  //check table insert works fine after alter table to drop columns
  test("Insertint_001_05", Include) {
    sql(s"""drop table if exists test1""").collect
     sql(s"""create table test2 (country string, name string, state_id int,id int) stored by 'carbondata' """).collect
   sql(s"""create table test1 (country string, state_id int) stored by 'carbondata' """).collect
   sql(s"""insert into test1 select 'xx',1""").collect
   sql(s"""alter table test2 drop columns (name, id)""").collect
   sql(s"""insert into test2 select * from test1""").collect
    checkAnswer(s"""Select count(*) from test2""",
      Seq(Row(1)), "AlterTableTestCase_Insertint_001_05")
     sql(s"""drop table if exists test2""").collect
   sql(s"""drop table if exists test1""").collect
  }


  //Check show segments on old table After altering the Table name.
  test("Showsegme_001_01", Include) {
    intercept[Exception] {
      sql(s"""create table test1 (country string, id int) stored by 'carbondata'""").collect
      sql(s"""alter table test1 rename to test2""").collect
      sql(s"""show segments for table test1""").collect
    }
    sql(s"""drop table if exists test2""").collect
  }


  //Check vertical compaction on old table after altering the table name
  test("Compaction_001_01", Include) {
     sql(s"""drop table if exists test1""").collect
   sql(s"""drop table if exists test2""").collect
   sql(s"""create table test1(name string, id int) stored by 'carbondata'""").collect
   sql(s"""insert into test1 select 'xx',1""").collect
   sql(s"""insert into test1 select 'xe',2""").collect
   sql(s"""insert into test1 select 'xr',3""").collect
   sql(s"""alter table test1 rename to test2""").collect
    sql(s"""alter table test2 compact 'minor'""").collect
     sql(s"""drop table if exists test2""").collect
  }


  //Check vertical compaction on new table when all segments are created before alter table name.
  test("Compaction_001_02", Include) {
     sql(s"""drop table if exists test1""").collect
   sql(s"""drop table if exists test2""").collect
   sql(s"""create table test1(name string, id int) stored by 'carbondata'""").collect
   sql(s"""insert into test1 select 'xx',1""").collect
   sql(s"""insert into test1 select 'xe',2""").collect
   sql(s"""insert into test1 select 'xr',3""").collect
   sql(s"""alter table test1 rename to test2""").collect
   sql(s"""alter table test2 compact 'minor'""").collect
    checkAnswer(s"""select name from test2 where id =2""",
      Seq(Row("xe")), "AlterTableTestCase_Compaction_001_02")
     sql(s"""drop table if exists test2""").collect
  }


  //Check vertical compaction on new table when some of the segments are created after altering the table name
  test("Compaction_001_03", Include) {
     sql(s"""drop table if exists test1""").collect
   sql(s"""drop table if exists test2""").collect
   sql(s"""create table test1(name string, id int) stored by 'carbondata'""").collect
   sql(s"""insert into test1 select 'xx',1""").collect
   sql(s"""insert into test1 select 'xe',2""").collect
   sql(s"""alter table test1 rename to test2""").collect
   sql(s"""insert into test2 select 'xr',3""").collect
   sql(s"""alter table test2 compact 'minor'""").collect
    checkAnswer(s"""select name from test2 where id =2""",
      Seq(Row("xe")), "AlterTableTestCase_Compaction_001_03")
     sql(s"""drop table if exists test2""").collect
  }


  //Check vertical compaction on new table after altering the table name multiple times and and segments created after alter
  test("Compaction_001_04", Include) {
     sql(s"""drop table if exists test1""").collect
   sql(s"""drop table if exists test2""").collect
    sql(s"""drop table if exists test3""").collect
   sql(s"""create table test1(name string, id int) stored by 'carbondata'""").collect
   sql(s"""insert into test1 select 'xx',1""").collect
   sql(s"""alter table test1 rename to test2""").collect
   sql(s"""insert into test2 select 'xe',2""").collect
   sql(s"""alter table test2 rename to test3""").collect
   sql(s"""insert into test3 select 'xr',3""").collect
   sql(s"""alter table test3 compact 'minor'""").collect
    checkAnswer(s"""select name from test3 where id =2""",
      Seq(Row("xe")), "AlterTableTestCase_Compaction_001_04")
     sql(s"""drop table if exists test3""").collect
  }


  //Check vertical compaction(major) on new table name when part of the segments are created before altering the table name
  test("Compaction_001_05", Include) {
     sql(s"""drop table if exists test1""").collect
   sql(s"""drop table if exists test2""").collect
   sql(s"""create table test1(name string, id int) stored by 'carbondata'""").collect
   sql(s"""insert into test1 select 'xx',1""").collect
   sql(s"""insert into test1 select 'xe',2""").collect
   sql(s"""alter table test1 rename to test2""").collect
   sql(s"""insert into test2 select 'xr',3""").collect
   sql(s"""alter table test2 compact 'major'""").collect
    sql(s"""select name from test2 where id =2""").collect

     sql(s"""drop table if exists test2""").collect
  }


  //Check vertical compaction when all segments are created before drop column, check dropped column is not used in the compation
  test("Compaction_001_06", Include) {
    intercept[Exception] {
      sql(s"""drop table if exists test1""").collect
      sql(s"""drop table if exists test2""").collect
      sql(s"""create table test1(name string, country string, id int) stored by 'carbondata'""").collect
      sql(s"""insert into test1 select 'xx','china',1""").collect
      sql(s"""insert into test1 select 'xe','china',2""").collect
      sql(s"""insert into test1 select 'xe','china',3""").collect
      sql(s"""alter table test1 drop columns (country)""").collect
      sql(s"""alter table test1 compact 'minor'""").collect
      sql(s"""select country from test1 where country='china'""").collect
    }
    sql(s"""drop table if exists test1""").collect
  }


  //Check vertical compaction when some of the segments are created before drop column, check dropped column is not used in the compation
  test("Compaction_001_07", Include) {
    intercept[Exception] {
      sql(s"""drop table if exists test1""").collect
      sql(s"""drop table if exists test2""").collect
      sql(s"""create table test1(name string, country string, id int) stored by 'carbondata'""").collect
      sql(s"""insert into test1 select 'xx','china',1""").collect
      sql(s"""insert into test1 select 'xe','china',2""").collect
      sql(s"""alter table test1 drop columns (country)""").collect
      sql(s"""insert into test1 select 'xe',3""").collect
      sql(s"""alter table test1 compact 'minor'""").collect
      sql(s"""select country from test1 where country='china'""").collect
    }
    sql(s"""drop table if exists test1""").collect
  }


  //Check vertical compaction for multiple drop column, check dropped column is not used in the compation
  test("Compaction_001_08", Include) {
    intercept[Exception] {
      sql(s"""drop table if exists test1""").collect
      sql(s"""drop table if exists test2""").collect
      sql(s"""create table test1(name string, country string, id int) stored by 'carbondata'""").collect
      sql(s"""insert into test1 select 'xx','china',1""").collect
      sql(s"""alter table test1 drop columns (country)""").collect
      sql(s"""insert into test1 select 'xe',3""").collect
      sql(s"""alter table test1 drop columns (id)""").collect
      sql(s"""insert into test1 select 'xe'""").collect
      sql(s"""alter table test1 compact 'minor'""").collect
      sql(s"""select country from test1 where id=1""").collect
    }
    sql(s"""drop table if exists test1""").collect
  }


  //Check vertical compaction on altered table for column add, when all segments crreated before table alter. Ensure added column in the compacted segment
  test("Compaction_001_09", Include) {
     sql(s"""drop table if exists test1""").collect
   sql(s"""drop table if exists test2""").collect
   sql(s"""create table test1(name string) stored by 'carbondata'""").collect
   sql(s"""insert into test1 select 'xx3'""").collect
   sql(s"""insert into test1 select 'xx2'""").collect
   sql(s"""insert into test1 select 'xx1'""").collect
   sql(s"""alter table test1 add columns (country string)""").collect
   sql(s"""alter table test1 compact 'minor'""").collect
    checkAnswer(s"""select country from test1 group by country""",
      Seq(Row(null)), "AlterTableTestCase_Compaction_001_09")
     sql(s"""drop table if exists test1""").collect
  }


  //Check vertical compaction on altered table for column add, when some of the segments crreated before table alter. Ensure added column in the compacted segment
  test("Compaction_001_10", Include) {
    sql(s"""drop table if exists test1""").collect
     sql(s"""create table test1(name string) stored by 'carbondata'""").collect
   sql(s"""insert into test1 select 'xx1'""").collect
   sql(s"""insert into test1 select 'xx2'""").collect
   sql(s"""alter table test1 add columns (country string)""").collect
   sql(s"""insert into test1 select 'xx1','china'""").collect
   sql(s"""alter table test1 compact 'minor'""").collect
    checkAnswer(s"""select country from test1 group by country""",
      Seq(Row(null), Row("china")), "AlterTableTestCase_Compaction_001_10")
     sql(s"""drop table if exists test1""").collect
  }


  //Check vertical compaction on multiple altered table for column add, when some of the segments crreated after table alter. Ensure added column in the compacted segment
  test("Compaction_001_11", Include) {
    sql(s"""drop table if exists test1""").collect
     sql(s"""create table test1(name string) stored by 'carbondata'""").collect
   sql(s"""insert into test1 select 'xx1'""").collect
   sql(s"""insert into test1 select 'xx2'""").collect
   sql(s"""alter table test1 add columns (id int)  """).collect
   sql(s"""insert into test1 select 'xx1',1""").collect
   sql(s"""alter table test1 add columns (country string)""").collect
   sql(s"""insert into test1 select 'xx1',1, 'china'""").collect
   sql(s"""alter table test1 compact 'minor'""").collect
    checkAnswer(s"""select country from test1 group by country""",
      Seq(Row(null), Row("china")), "AlterTableTestCase_Compaction_001_11")
     sql(s"""drop table if exists test1""").collect
  }


  //Check vertical compaction on altered table for change column datatype, when some of the segments crreated after table alter. Ensure added column in the compacted segment
  test("Compaction_001_12", Include) {
    sql(s"""drop table if exists default.test1""").collect
     sql(s"""create table test1(name string, id int) stored by 'carbondata'""").collect
   sql(s"""insert into test1 select 'xx1',1""").collect
   sql(s"""insert into test1 select 'xx2',2""").collect
   sql(s"""alter table test1 change id id bigint """).collect
   sql(s"""insert into test1 select 'xx2',2999999999""").collect
   sql(s"""alter table test1 compact 'minor'""").collect
    checkAnswer(s"""select id from test1""",
      Seq(Row(1),Row(2), Row(2999999999L)), "AlterTableTestCase_Compaction_001_12")
     sql(s"""drop table if exists test1""").collect
  }

  test("Compaction_001_13", Include) {
    sql("drop table if exists no_table")
    var ex = intercept[MalformedCarbonCommandException] {
      sql("alter table no_table compact 'major'")
    }
    assertResult("Table or view 'no_table' not found in database 'default' or not carbon fileformat")(ex.getMessage)
  }


  //Check bad record locaion isnot changed when table name is altered
  test("BadRecords_001_01", Include) {
    sql(s"""drop table if exists default.t_carbn01""").collect
     sql(s"""create table default.t_carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1_Bad.csv' INTO table default.t_carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\','BAD_RECORDS_LOGGER_ENABLE'='true', 'BAD_RECORDS_ACTION'='REDIRECT', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
   sql(s"""alter table default.t_carbn01 rename to default.t_carbn01t""").collect
    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1_Bad.csv' INTO table default.t_carbn01t options ('DELIMITER'=',', 'QUOTECHAR'='\','BAD_RECORDS_LOGGER_ENABLE'='true', 'BAD_RECORDS_ACTION'='REDIRECT', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
     sql(s"""drop table if exists default.t_carbn01t""").collect
  }


  //Check bad record locaion isnot changed when table name is altered
  test("BadRecords_001_02", Include) {
    sql(s"""drop table if exists default.t_carbn01""").collect
     sql(s"""create table default.t_carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/T_Hive1_Bad.csv' INTO table default.t_carbn01 options ('DELIMITER'=',', 'QUOTECHAR'='\','BAD_RECORDS_LOGGER_ENABLE'='true', 'BAD_RECORDS_ACTION'='REDIRECT', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""").collect
    sql(s"""alter table t_carbn01 drop columns (item_name)""").collect
     sql(s"""drop table if exists default.t_carbn01""").collect
  }


  //Check for bad record handling while latering the table if added column is set with default value which is a bad record
  test("BadRecords_001_03", Include) {
    sql(s"""drop table if exists test1""").collect
     sql(s"""create table test1 (name string, id int) stored by 'carbondata'""").collect
   sql(s"""insert into test1 select 'xx',1""").collect
   sql(s"""insert into test1 select 'xx',12""").collect
   sql(s"""ALTER TABLE test1 ADD COLUMNS (id2 int) TBLPROPERTIES('include_dictionary'='id2','DEFAULT.VALUE.id2'='China')""").collect
    checkAnswer(s"""select * from test1 where id = 1""",
      Seq(Row("xx",1,null)), "AlterTableTestCase_BadRecords_001_03")
     sql(s"""drop table if exists test1""").collect
  }


  //Check delete segment is not allowed on old table name when table name is altered
  test("DeleteSeg_001_01", Include) {
    intercept[Exception] {
      sql(s"""create table test1 (name string, id int) stored by 'carbondata'""").collect
      sql(s"""insert into test1 select 'xx',1""").collect
      sql(s"""insert into test1 select 'xx',12""").collect
      sql(s"""alter table test1 rename to test2""").collect
      sql(s"""delete from table test1 where segment.id in (0)""").collect
    }
    sql(s"""drop table if exists test2""").collect
  }


  //Check delete segment is allowed on new table name when table name is altered
  test("DeleteSeg_001_02", Include) {
    sql(s"""drop table if exists test1""").collect
     sql(s"""create table test1 (name string, id int) stored by 'carbondata'""").collect
   sql(s"""insert into test1 select 'xx',1""").collect
   sql(s"""insert into test1 select 'xx',12""").collect
   sql(s"""alter table test1 rename to test2""").collect
   sql(s"""delete from table test2 where segment.id in (0)""").collect
    checkAnswer(s"""Select * from test2""",
      Seq(Row("xx",12)), "AlterTableTestCase_DeleteSeg_001_02")
     sql(s"""drop table if exists test2""").collect
  }


  //Check alter the table name,alter the table name again with first name and fire Select query
  test("AlterTable-001-AltersameTablename-001-TC001", Include) {
     sql(s"""drop table  if exists uniqdata""").collect
   sql(s"""drop table  if exists uniqdata1""").collect
   sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' """).collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""alter table uniqdata RENAME TO  uniqdata1""").collect
   sql(s"""alter table uniqdata1 RENAME TO uniqdata""").collect
    sql(s"""select * from uniqdata where cust_name like 'Cust%'""").collect

     sql(s"""drop table  if exists uniqdata""").collect
   sql(s"""drop table  if exists uniqdata1""").collect
  }


  //Check select query after alter the int to Bigint and decimal Lower Precision to higher precision
  test("AlterTable-007-selectquery-001-TC002", Include) {
     sql(s"""CREATE TABLE uniqdata1 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' """).collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata1 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""ALTER TABLE uniqdata1 CHANGE CUST_ID CUST_ID BIGINT""").collect
    sql(s"""select * from uniqdata1 where cust_name like 'Cust%'""").collect

     sql(s"""drop table  if exists uniqdata1""").collect
  }


  //Check select query after alter from lower to higher precision
  test("AlterTable-008-selectquery-001-TC003", Include) {
     sql(s"""CREATE TABLE uniqdata1 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' """).collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata1 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_LOGGER_ENABLE'='TRUE', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""ALTER TABLE uniqdata1 CHANGE decimal_column1 decimal_column1 DECIMAL(31,11)""").collect
    sql(s"""select * from uniqdata1 where cust_name like 'Cust%'""").collect

     sql(s"""drop table  if exists uniqdata2""").collect
  }


  //Check add column on Decimal,Timestamp,int,string,Bigint
  test("AlterTable-002-001-TC-004", Include) {
     sql(s"""drop table if exists uniqdata59""").collect
   sql(s"""CREATE TABLE uniqdata59 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' """).collect
    sql(s"""ALTER TABLE uniqdata59 ADD COLUMNS (a1 int,a2 int,a3 decimal,a4 Bigint,a5 String,a6 timestamp,a7 Bigint,a8 decimal(10,2),a9 timestamp,a10 String,a11 string,a12 string,a13 string,a14 string,a15 string,a16 string,a17 string,a18 string,a19 string,a20 string,a21 string,a22 string,a23 string,a24 string,a25 string,a26 string,a27 string,a28 string,a29 string,a30 string,a31 string,a32 string,a33 string,a34 string,a35 string,a36 string,a37 string,a38 string,a39 string,a40 string,a41 string,a42 string,a43 string,a44 string,a45 string,a46 string,a47 string,a48 string,a49 string,a50 string,a51 string,a52 string,a53 string,a54 string,a55 string,a56 string,a57 string,a58 string,a59 string,a60 string,a61 string,a62 string,a63 string,a64 string,a65 string,a66 string,a67 string,a68 string,a69 string,a70 string,a71 string,a72 string,a73 string,a74 string,a75 string,a76 string,a77 string,a78 string,a79 string,a80 string,a81 string,a82 string,a83 string,a84 string,a85 string,a86 string,a87 string,a88 string) """).collect
     sql(s"""drop table  if exists uniqdata59""").collect
  }

  test("Alter table add column for hive table for spark version above 2.1") {
    sql("drop table if exists alter_hive")
    sql("create table alter_hive(name string)")
    if(SPARK_VERSION.startsWith("2.1")) {
      val exception = intercept[MalformedCarbonCommandException] {
        sql("alter table alter_hive add columns(add string)")
      }
      assert(exception.getMessage.contains("Unsupported alter operation on hive table"))
    } else if (SparkUtil.isSparkVersionXandAbove("2.2")) {
      sql("alter table alter_hive add columns(add string)")
      sql("alter table alter_hive add columns (var map<string, string>)")
      sql("insert into alter_hive select 'abc','banglore',map('age','10','birth','2020')")
      checkAnswer(
        sql("select * from alter_hive"),
        Seq(Row("abc", "banglore", Map("age" -> "10", "birth" -> "2020")))
      )
    }
  }

  test("Alter table add column for hive partitioned table for spark version above 2.1") {
    sql("drop table if exists alter_hive")
    sql("create table alter_hive(name string) stored as rcfile partitioned by (dt string)")
    if (SparkUtil.isSparkVersionXandAbove("2.2")) {
      sql("alter table alter_hive add columns(add string)")
      sql("alter table alter_hive add columns (var map<string, string>)")
      sql("alter table alter_hive add columns (loves array<string>)")
      sql(
        s"""
           |insert into alter_hive partition(dt='par')
           |select 'abc', 'banglore', map('age', '10', 'birth', '2020'), array('a', 'b', 'c')
         """.stripMargin)
      checkAnswer(
        sql("select * from alter_hive where dt='par'"),
        Seq(Row("abc", "banglore", Map("age" -> "10", "birth" -> "2020"), Seq("a", "b", "c"), "par"))
      )
    }
  }

  test("Test drop columns not present in the table") {
    sql("drop table if exists test1")
    sql("create table test1(col1 int) stored by 'carbondata'")
    val exception = intercept[ProcessMetaDataException] {
      sql("alter table test1 drop columns(name)")
    }
    assert(exception.getMessage.contains("Column name does not exists in the table default.test1"))
    sql("drop table if exists test1")
  }

  val prop = CarbonProperties.getInstance()
  val p1 = prop.getProperty("carbon.horizontal.compaction.enable", CarbonCommonConstants.CARBON_HORIZONTAL_COMPACTION_ENABLE_DEFAULT)
  val p2 = prop.getProperty("carbon.horizontal.update.compaction.threshold", CarbonCommonConstants.DEFAULT_UPDATE_DELTAFILE_COUNT_THRESHOLD_IUD_COMPACTION)
  val p3 = prop.getProperty("carbon.horizontal.delete.compaction.threshold", CarbonCommonConstants.DEFAULT_DELETE_DELTAFILE_COUNT_THRESHOLD_IUD_COMPACTION)
  val p4 = prop.getProperty("carbon.compaction.level.threshold", CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD)
  val p5 = prop.getProperty("carbon.enable.auto.load.merge", CarbonCommonConstants.DEFAULT_ENABLE_AUTO_LOAD_MERGE)
  val p6 = prop.getProperty("carbon.bad.records.action", LoggerAction.FORCE.name())

  override protected def beforeAll() {
    // Adding new properties
    prop.addProperty("carbon.horizontal.compaction.enable", "true")
    prop.addProperty("carbon.horizontal.update.compaction.threshold", "1")
    prop.addProperty("carbon.horizontal.delete.compaction.threshold", "1")
    prop.addProperty("carbon.compaction.level.threshold", "2,1")
    prop.addProperty("carbon.enable.auto.load.merge", "false")
    prop.addProperty("carbon.bad.records.action", "FORCE")
  }

  override def afterAll: Unit = {
    //Reverting to old
    prop.addProperty("carbon.horizontal.compaction.enable", p1)
    prop.addProperty("carbon.horizontal.update.compaction.threshold", p2)
    prop.addProperty("carbon.horizontal.delete.compaction.threshold", p3)
    prop.addProperty("carbon.compaction.level.threshold", p4)
    prop.addProperty("carbon.enable.auto.load.merge", p5)
    prop.addProperty("carbon.bad.records.action", p6)
    sql("drop table if exists test2")
    sql("drop table if exists test1")
  }
}