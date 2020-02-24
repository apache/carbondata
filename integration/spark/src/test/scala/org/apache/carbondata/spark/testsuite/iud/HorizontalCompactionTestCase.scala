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

package org.apache.carbondata.spark.testsuite.iud

import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfterAll
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.{CarbonFile, CarbonFileFilter}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.test.util.QueryTest

class HorizontalCompactionTestCase extends QueryTest with BeforeAndAfterAll {
  override def beforeAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER , "false")
    sql("""drop database if exists iud4 cascade""")
    sql("""create database iud4""")
    sql("""use iud4""")
    sql(
      """create table iud4.dest (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/comp1.csv' INTO table iud4.dest""")
    sql(
      """create table iud4.source2 (c11 string,c22 int,c33 string,c55 string, c66 int) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/source3.csv' INTO table iud4.source2""")
    sql("""create table iud4.other (c1 string,c2 int) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/other.csv' INTO table iud4.other""")
    sql(
      """create table iud4.hdest (c1 string,c2 int,c3 string,c5 string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/comp1.csv' INTO table iud4.hdest""")
    sql(
      """CREATE TABLE iud4.update_01(imei string,age int,task bigint,num double,level decimal(10,3),name string)STORED AS carbondata """)
    sql(
      s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/update01.csv' INTO TABLE iud4.update_01 OPTIONS('BAD_RECORDS_LOGGER_ENABLE' = 'FALSE', 'BAD_RECORDS_ACTION' = 'FORCE') """)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_HORIZONTAL_COMPACTION_ENABLE, "true")
  }



  test("test IUD Horizontal Compaction Update Alter Clean.") {
    sql("""drop database if exists iud4 cascade""")
    sql("""create database iud4""")
    sql("""use iud4""")
    sql(
      """create table dest2 (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")

    sql(s"""load data local inpath '$resourcesPath/IUD/comp1.csv' INTO table dest2""")
    sql(s"""load data local inpath '$resourcesPath/IUD/comp2.csv' INTO table dest2""")
    sql(s"""load data local inpath '$resourcesPath/IUD/comp3.csv' INTO table dest2""")
    sql(s"""load data local inpath '$resourcesPath/IUD/comp4.csv' INTO table dest2""")
    sql(
      """create table source2 (c11 string,c22 int,c33 string,c55 string, c66 int) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/source3.csv' INTO table source2""")
    sql(
      """update dest2 d set (d.c3, d.c5 ) = (select s.c33,s.c55 from source2 s where d.c1 = s.c11 and s.c22 < 3 or (s.c22 > 10 and s.c22 < 13) or (s.c22 > 20 and s.c22 < 23) or (s.c22 > 30 and s.c22 < 33))""")
      .show()
    sql(
      """update dest2 d set (d.c3, d.c5 ) = (select s.c33,s.c55 from source2 s where d.c1 = s.c11 and (s.c22 > 3 and s.c22 < 5) or (s.c22 > 13 and s.c22 < 15) or (s.c22 > 23 and s.c22 < 25) or (s.c22 > 33 and s.c22 < 35))""")
      .show()
    sql(
      """update dest2 d set (d.c3, d.c5 ) = (select s.c33,s.c55 from source2 s where d.c1 = s.c11 and (s.c22 > 5 and c22 < 8) or (s.c22 > 15 and s.c22 < 18 ) or (s.c22 > 25 and c22 < 28) or (s.c22 > 35 and c22 < 38))""")
      .show()
    sql("""alter table dest2 compact 'minor'""")
    sql("""clean files for table dest2""")
    checkAnswer(
      sql("""select c1,c2,c3,c5 from dest2 order by c2"""),
      Seq(Row("a", 1, "MGM", "Disco"),
        Row("b", 2, "RGK", "Music"),
        Row("c", 3, "cc", "ccc"),
        Row("d", 4, "YDY", "Weather"),
        Row("e", 5, "ee", "eee"),
        Row("f", 6, "ff", "fff"),
        Row("g", 7, "YTY", "Hello"),
        Row("h", 8, "hh", "hhh"),
        Row("i", 9, "ii", "iii"),
        Row("j", 10, "jj", "jjj"),
        Row("a", 11, "MGM", "Disco"),
        Row("b", 12, "RGK", "Music"),
        Row("c", 13, "cc", "ccc"),
        Row("d", 14, "YDY", "Weather"),
        Row("e", 15, "ee", "eee"),
        Row("f", 16, "ff", "fff"),
        Row("g", 17, "YTY", "Hello"),
        Row("h", 18, "hh", "hhh"),
        Row("i", 19, "ii", "iii"),
        Row("j", 20, "jj", "jjj"),
        Row("a", 21, "MGM", "Disco"),
        Row("b", 22, "RGK", "Music"),
        Row("c", 23, "cc", "ccc"),
        Row("d", 24, "YDY", "Weather"),
        Row("e", 25, "ee", "eee"),
        Row("f", 26, "ff", "fff"),
        Row("g", 27, "YTY", "Hello"),
        Row("h", 28, "hh", "hhh"),
        Row("i", 29, "ii", "iii"),
        Row("j", 30, "jj", "jjj"),
        Row("a", 31, "MGM", "Disco"),
        Row("b", 32, "RGK", "Music"),
        Row("c", 33, "cc", "ccc"),
        Row("d", 34, "YDY", "Weather"),
        Row("e", 35, "ee", "eee"),
        Row("f", 36, "ff", "fff"),
        Row("g", 37, "YTY", "Hello"),
        Row("h", 38, "hh", "hhh"),
        Row("i", 39, "ii", "iii"),
        Row("j", 40, "jj", "jjj"))
    )
    sql("""drop table dest2""")
  }


  test("test IUD Horizontal Compaction Delete") {
    sql("""drop database if exists iud4 cascade""")
    sql("""create database iud4""")
    sql("""use iud4""")
    sql(
      """create table dest2 (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql(s"""load data local inpath '$resourcesPath/IUD/comp1.csv' INTO table dest2""")
    sql(s"""load data local inpath '$resourcesPath/IUD/comp2.csv' INTO table dest2""")
    sql(s"""load data local inpath '$resourcesPath/IUD/comp3.csv' INTO table dest2""")
    sql(s"""load data local inpath '$resourcesPath/IUD/comp4.csv' INTO table dest2""")
    sql("""select * from dest2""")
    sql(
      """create table source2 (c11 string,c22 int,c33 string,c55 string, c66 int) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/source3.csv' INTO table source2""")
    sql("""select * from source2""")
    sql("""delete from dest2 where (c2 < 3) or (c2 > 10 and c2 < 13) or (c2 > 20 and c2 < 23) or (c2 > 30 and c2 < 33)""").show()
    sql("""select * from dest2 order by 2""")
    sql("""delete from dest2 where (c2 > 3 and c2 < 5) or (c2 > 13 and c2 < 15) or (c2 > 23 and c2 < 25) or (c2 > 33 and c2 < 35)""").show()
    sql("""select * from dest2 order by 2""")
    sql("""delete from dest2 where (c2 > 5 and c2 < 8) or (c2 > 15 and c2 < 18 ) or (c2 > 25 and c2 < 28) or (c2 > 35 and c2 < 38)""").show()
    sql("""clean files for table dest2""")
    checkAnswer(
      sql("""select c1,c2,c3,c5 from dest2 order by c2"""),
      Seq(Row("c", 3, "cc", "ccc"),
        Row("e", 5, "ee", "eee"),
        Row("h", 8, "hh", "hhh"),
        Row("i", 9, "ii", "iii"),
        Row("j", 10, "jj", "jjj"),
        Row("c", 13, "cc", "ccc"),
        Row("e", 15, "ee", "eee"),
        Row("h", 18, "hh", "hhh"),
        Row("i", 19, "ii", "iii"),
        Row("j", 20, "jj", "jjj"),
        Row("c", 23, "cc", "ccc"),
        Row("e", 25, "ee", "eee"),
        Row("h", 28, "hh", "hhh"),
        Row("i", 29, "ii", "iii"),
        Row("j", 30, "jj", "jjj"),
        Row("c", 33, "cc", "ccc"),
        Row("e", 35, "ee", "eee"),
        Row("h", 38, "hh", "hhh"),
        Row("i", 39, "ii", "iii"),
        Row("j", 40, "jj", "jjj"))
    )
    sql("""drop table dest2""")
  }

  test("test IUD Horizontal Compaction Multiple Update Vertical Compaction and Clean") {
    sql("""drop database if exists iud4 cascade""")
    sql("""create database iud4""")
    sql("""use iud4""")
    sql(
      """create table dest2 (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql(s"""load data local inpath '$resourcesPath/IUD/comp1.csv' INTO table dest2""")
    sql(s"""load data local inpath '$resourcesPath/IUD/comp2.csv' INTO table dest2""")
    sql(s"""load data local inpath '$resourcesPath/IUD/comp3.csv' INTO table dest2""")
    sql(s"""load data local inpath '$resourcesPath/IUD/comp4.csv' INTO table dest2""")
    sql(
      """create table source2 (c11 string,c22 int,c33 string,c55 string, c66 int) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/source3.csv' INTO table source2""")
    sql("""update dest2 d set (d.c3, d.c5 ) = (select s.c33,s.c55 from source2 s where d.c1 = s.c11 and s.c22 < 3 or (s.c22 > 10 and s.c22 < 13) or (s.c22 > 20 and s.c22 < 23) or (s.c22 > 30 and s.c22 < 33))""").show()
    sql("""update dest2 d set (d.c3, d.c5 ) = (select s.c11,s.c66 from source2 s where d.c1 = s.c11 and s.c22 < 3 or (s.c22 > 10 and s.c22 < 13) or (s.c22 > 20 and s.c22 < 23) or (s.c22 > 30 and s.c22 < 33))""").show()
    sql("""update dest2 d set (d.c3, d.c5 ) = (select s.c33,s.c55 from source2 s where d.c1 = s.c11 and (s.c22 > 3 and s.c22 < 5) or (s.c22 > 13 and s.c22 < 15) or (s.c22 > 23 and s.c22 < 25) or (s.c22 > 33 and s.c22 < 35))""").show()
    sql("""update dest2 d set (d.c3, d.c5 ) = (select s.c11,s.c66 from source2 s where d.c1 = s.c11 and (s.c22 > 3 and s.c22 < 5) or (s.c22 > 13 and s.c22 < 15) or (s.c22 > 23 and s.c22 < 25) or (s.c22 > 33 and s.c22 < 35))""").show()
    sql("""update dest2 d set (d.c3, d.c5 ) = (select s.c33,s.c55 from source2 s where d.c1 = s.c11 and (s.c22 > 5 and c22 < 8) or (s.c22 > 15 and s.c22 < 18 ) or (s.c22 > 25 and c22 < 28) or (s.c22 > 35 and c22 < 38))""").show()
    sql("""update dest2 d set (d.c3, d.c5 ) = (select s.c11,s.c66 from source2 s where d.c1 = s.c11 and (s.c22 > 5 and c22 < 8) or (s.c22 > 15 and s.c22 < 18 ) or (s.c22 > 25 and c22 < 28) or (s.c22 > 35 and c22 < 38))""").show()
    sql("""alter table dest2 compact 'major'""")
    sql("""clean files for table dest2""")
    checkAnswer(
      sql("""select c1,c2,c3,c5 from dest2 order by c2"""),
      Seq(Row("a", 1, "a", "10"),
        Row("b", 2, "b", "8"),
        Row("c", 3, "cc", "ccc"),
        Row("d", 4, "d", "9"),
        Row("e", 5, "ee", "eee"),
        Row("f", 6, "ff", "fff"),
        Row("g", 7, "g", "12"),
        Row("h", 8, "hh", "hhh"),
        Row("i", 9, "ii", "iii"),
        Row("j", 10, "jj", "jjj"),
        Row("a", 11, "a", "10"),
        Row("b", 12, "b", "8"),
        Row("c", 13, "cc", "ccc"),
        Row("d", 14, "d", "9"),
        Row("e", 15, "ee", "eee"),
        Row("f", 16, "ff", "fff"),
        Row("g", 17, "g", "12"),
        Row("h", 18, "hh", "hhh"),
        Row("i", 19, "ii", "iii"),
        Row("j", 20, "jj", "jjj"),
        Row("a", 21, "a", "10"),
        Row("b", 22, "b", "8"),
        Row("c", 23, "cc", "ccc"),
        Row("d", 24, "d", "9"),
        Row("e", 25, "ee", "eee"),
        Row("f", 26, "ff", "fff"),
        Row("g", 27, "g", "12"),
        Row("h", 28, "hh", "hhh"),
        Row("i", 29, "ii", "iii"),
        Row("j", 30, "jj", "jjj"),
        Row("a", 31, "a", "10"),
        Row("b", 32, "b", "8"),
        Row("c", 33, "cc", "ccc"),
        Row("d", 34, "d", "9"),
        Row("e", 35, "ee", "eee"),
        Row("f", 36, "ff", "fff"),
        Row("g", 37, "g", "12"),
        Row("h", 38, "hh", "hhh"),
        Row("i", 39, "ii", "iii"),
        Row("j", 40, "jj", "jjj"))
    )
    sql("""drop table dest2""")
    sql("""drop table source2""")
    sql("""drop database iud4 cascade""")
  }

  test("test IUD Horizontal Compaction Update Delete and Clean") {
    sql("""drop database if exists iud4 cascade""")
    sql("""create database iud4""")
    sql("""use iud4""")
    sql(
      """create table dest2 (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql(s"""load data local inpath '$resourcesPath/IUD/comp1.csv' INTO table dest2""")
    sql(s"""load data local inpath '$resourcesPath/IUD/comp2.csv' INTO table dest2""")
    sql(s"""load data local inpath '$resourcesPath/IUD/comp3.csv' INTO table dest2""")
    sql(s"""load data local inpath '$resourcesPath/IUD/comp4.csv' INTO table dest2""")
    sql(
      """create table source2 (c11 string,c22 int,c33 string,c55 string, c66 int) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/source3.csv' INTO table source2""")
    sql("""update dest2 d set (d.c3, d.c5 ) = (select s.c33,s.c55 from source2 s where d.c1 = s.c11 and s.c22 < 3 or (s.c22 > 10 and s.c22 < 13) or (s.c22 > 20 and s.c22 < 23) or (s.c22 > 30 and s.c22 < 33))""").show()
    sql("""delete from dest2 where (c2 < 2) or (c2 > 10 and c2 < 13) or (c2 > 20 and c2 < 23) or (c2 > 30 and c2 < 33)""").show()
    sql("""delete from dest2 where (c2 > 3 and c2 < 5) or (c2 > 13 and c2 < 15) or (c2 > 23 and c2 < 25) or (c2 > 33 and c2 < 35)""").show()
    sql("""delete from dest2 where (c2 > 5 and c2 < 8) or (c2 > 15 and c2 < 18 ) or (c2 > 25 and c2 < 28) or (c2 > 35 and c2 < 38)""").show()
    sql("""clean files for table dest2""")
    checkAnswer(
      sql("""select c1,c2,c3,c5 from dest2 order by c2"""),
      Seq(Row("b", 2, "RGK", "Music"),
        Row("c", 3, "cc", "ccc"),
        Row("e", 5, "ee", "eee"),
        Row("h", 8, "hh", "hhh"),
        Row("i", 9, "ii", "iii"),
        Row("j", 10, "jj", "jjj"),
        Row("c", 13, "cc", "ccc"),
        Row("e", 15, "ee", "eee"),
        Row("h", 18, "hh", "hhh"),
        Row("i", 19, "ii", "iii"),
        Row("j", 20, "jj", "jjj"),
        Row("c", 23, "cc", "ccc"),
        Row("e", 25, "ee", "eee"),
        Row("h", 28, "hh", "hhh"),
        Row("i", 29, "ii", "iii"),
        Row("j", 30, "jj", "jjj"),
        Row("c", 33, "cc", "ccc"),
        Row("e", 35, "ee", "eee"),
        Row("h", 38, "hh", "hhh"),
        Row("i", 39, "ii", "iii"),
        Row("j", 40, "jj", "jjj"))
    )
    sql("""drop table dest2""")
  }

  test("test IUD Horizontal Compaction Check Column Cardinality") {
    sql("""drop database if exists iud4 cascade""")
    sql("""create database iud4""")
    sql("""use iud4""")
    sql(
      """create table T_Carbn01(Active_status String,Item_type_cd INT,Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep DOUBLE,Discount_price DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String,Update_time TIMESTAMP,Create_date String)STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/T_Hive1.csv' INTO table t_carbn01 options ('BAD_RECORDS_LOGGER_ENABLE' = 'FALSE', 'BAD_RECORDS_ACTION' = 'FORCE','DELIMITER'=',', 'QUOTECHAR'='\', 'FILEHEADER'='Active_status,Item_type_cd,Qty_day_avg,Qty_total,Sell_price,Sell_pricep,Discount_price,Profit,Item_code,Item_name,Outlet_name,Update_time,Create_date')""")
    sql("""update t_carbn01 set (item_code) = ('Orange') where item_type_cd = 14""").show()
    sql("""update t_carbn01 set (item_code) = ('Banana') where item_type_cd = 2""").show()
    sql("""delete from t_carbn01 where item_code in ('RE3423ee','Orange','Banana')""").show()
    checkAnswer(
      sql("""select item_code from t_carbn01 where item_code not in ('RE3423ee','Orange','Banana')"""),
      Seq(Row("SAD423ee"),
        Row("DE3423ee"),
        Row("SE3423ee"),
        Row("SE3423ee"),
        Row("SE3423ee"),
        Row("SE3423ee"))
    )
    sql("""drop table t_carbn01""")
  }


  test("test IUD Horizontal Compaction Segment Delete Test Case") {
    sql("""drop database if exists iud4 cascade""")
    sql("""create database iud4""")
    sql("""use iud4""")
    sql(
      """create table dest2 (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql(s"""load data local inpath '$resourcesPath/IUD/comp1.csv' INTO table dest2""")
    sql(s"""load data local inpath '$resourcesPath/IUD/comp2.csv' INTO table dest2""")
    sql(s"""load data local inpath '$resourcesPath/IUD/comp3.csv' INTO table dest2""")
    sql(s"""load data local inpath '$resourcesPath/IUD/comp4.csv' INTO table dest2""")
    sql(
      """delete from dest2 where (c2 < 3) or (c2 > 10 and c2 < 13) or (c2 > 20 and c2 < 23) or (c2 > 30 and c2 < 33)""").show()
    sql("""delete from table dest2 where segment.id in (0) """)
    sql("""clean files for table dest2""")
    sql(
      """update dest2 set (c5) = ('8RAM size') where (c2 > 3 and c2 < 5) or (c2 > 13 and c2 < 15) or (c2 > 23 and c2 < 25) or (c2 > 33 and c2 < 35)""")
      .show()
    checkAnswer(
      sql("""select count(*) from dest2"""),
      Seq(Row(24))
    )
    sql("""drop table dest2""")
  }

  test("test case full table delete") {
    sql("""drop database if exists iud4 cascade""")
    sql("""create database iud4""")
    sql("""use iud4""")
    sql(
      """create table dest2 (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql(s"""load data local inpath '$resourcesPath/IUD/comp1.csv' INTO table dest2""")
    sql(s"""load data local inpath '$resourcesPath/IUD/comp2.csv' INTO table dest2""")
    sql(s"""load data local inpath '$resourcesPath/IUD/comp3.csv' INTO table dest2""")
    sql(s"""load data local inpath '$resourcesPath/IUD/comp4.csv' INTO table dest2""")
    sql("""delete from dest2 where c2 < 41""").show()
    sql("""alter table dest2 compact 'major'""")
    checkAnswer(
      sql("""select count(*) from dest2"""),
      Seq(Row(0))
    )
    sql("""drop table dest2""")
  }
  test("test the compaction after alter command") // As per bug Carbondata-2016
  {
      sql(
        "CREATE TABLE CUSTOMER1 ( C_CUSTKEY INT , C_NAME STRING , C_ADDRESS STRING , C_NATIONKEY INT , C_PHONE STRING , C_ACCTBAL DECIMAL(15,2) , C_MKTSEGMENT STRING , C_COMMENT STRING) STORED AS carbondata")

      sql(
        "insert into customer1 values(1,'vandana','noida',1,'123456789',45987.78,'hello','comment')")

      sql(
        "insert into customer1 values(2,'vandana','noida',2,'123456789',487.78,'hello','comment')")

      sql(
        " insert into customer1 values(3,'geetika','delhi',3,'123456789',487897.78,'hello','comment')")

      sql(
        "insert into customer1 values(4,'sangeeta','delhi',3,'123456789',48789.78,'hello','comment')")

      sql(
        "alter table customer1 add columns (shortfield short) TBLPROPERTIES ('DEFAULT.VALUE.shortfield'='32767')")

      sql(
        "alter table customer1 add columns (intfield int) TBLPROPERTIES ('DEFAULT.VALUE.intfield'='2147483647')")

      sql(
        "alter table customer1 add columns (longfield bigint) TBLPROPERTIES ('DEFAULT.VALUE.longfield'='9223372036854775807')")

      sql("alter table customer1 compact 'minor' ").show()

    checkAnswer(sql("select shortfield from customer1"),Seq(Row(32767),Row(32767),Row(32767),Row(32767)))
    checkAnswer(sql("select intfield from customer1"),Seq(Row(2147483647),Row(2147483647),Row(2147483647),Row(2147483647)))
    checkAnswer(sql("select longfield from customer1"),Seq(Row(9223372036854775807L),Row(9223372036854775807L),Row(9223372036854775807L),Row(9223372036854775807L)))

  }

  def getDeltaFiles(carbonFile: CarbonFile, fileSuffix: String): Array[CarbonFile] = {
    return carbonFile.listFiles(new CarbonFileFilter {
      override def accept(file: CarbonFile): Boolean = {
        file.getName.endsWith(fileSuffix)
      }
    })
  }

  test("[CARBONDATA-3483] Don't require update.lock and compaction.lock again when execute 'IUD_UPDDEL_DELTA' compaction") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER , "true")
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_UPDATE_SEGMENT_PARALLELISM, "1")
    sql("""drop database if exists iud10 cascade""")
    sql("""create database iud10""")
    sql("""use iud10""")

    sql(
      """create table dest10 (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql(s"""load data local inpath '$resourcesPath/IUD/comp1.csv' INTO table dest10""")

    val carbonTable = CarbonEnv.getCarbonTable(Some("iud10"), "dest10")(sqlContext.sparkSession)
    val identifier = carbonTable.getAbsoluteTableIdentifier()
    val dataFilesDir = CarbonTablePath.getSegmentPath(identifier.getTablePath, "0")
    val carbonFile =
        FileFactory.getCarbonFile(dataFilesDir)

    var updateDeltaFiles = getDeltaFiles(carbonFile, CarbonCommonConstants.UPDATE_INDEX_FILE_EXT)
    var deletaDeltaFiles = getDeltaFiles(carbonFile, CarbonCommonConstants.DELETE_DELTA_FILE_EXT)
    assert(updateDeltaFiles.length == 0)
    assert(deletaDeltaFiles.length == 0)

    sql("""update dest10 set (c1, c3) = ('update_a', 'update_aa') where c2 = 3 or c2 = 6""").show()

    updateDeltaFiles = getDeltaFiles(carbonFile, CarbonCommonConstants.UPDATE_INDEX_FILE_EXT)
    deletaDeltaFiles = getDeltaFiles(carbonFile, CarbonCommonConstants.DELETE_DELTA_FILE_EXT)
    // just update once, there is no horizontal compaction at this time
    assert(updateDeltaFiles.length == 1)
    assert(deletaDeltaFiles.length == 1)

    sql("""update dest10 set (c1, c3) = ('update_a', 'update_aa') where c2 = 5 or c2 = 8""").show()

    updateDeltaFiles = getDeltaFiles(carbonFile, CarbonCommonConstants.UPDATE_INDEX_FILE_EXT)
    deletaDeltaFiles = getDeltaFiles(carbonFile, CarbonCommonConstants.DELETE_DELTA_FILE_EXT)
    // one '.carbonindex' file for last update operation
    // one '.carbonindex' file for update operation this time
    // one '.carbonindex' file for horizontal compaction
    // so there must be three '.carbonindex' files and three '.deletedelta' files
    assert(updateDeltaFiles.length == 3)
    assert(deletaDeltaFiles.length == 3)

    sql("""drop table dest10""")
    sql("""drop database if exists iud10 cascade""")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER , "false")
  }

  override def afterAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER , "true")
    sql("use default")
    sql("drop database if exists iud4 cascade")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_HORIZONTAL_COMPACTION_ENABLE , "true")
    sql("""drop table if exists t_carbn01""")
    sql("""drop table if exists customer1""")
  }

}

