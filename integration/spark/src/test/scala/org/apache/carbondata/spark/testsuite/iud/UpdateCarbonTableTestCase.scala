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

import java.io.File

import org.apache.spark.sql.test.SparkTestQueryExecutor
import org.apache.spark.sql.{AnalysisException, CarbonEnv, Row, SaveMode}
import org.scalatest.{BeforeAndAfterAll, ConfigMap}

import org.apache.carbondata.core.constants.{CarbonCommonConstants, CarbonLoadOptionConstants}
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.test.util.QueryTest

import org.apache.carbondata.common.constants.LoggerAction
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.path.CarbonTablePath

class UpdateCarbonTableTestCase extends QueryTest with BeforeAndAfterAll {
  override def beforeAll {

    sql("drop database if exists iud cascade")
    sql("create database iud")
    sql("use iud")
    sql("""create table iud.dest (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest""")
    sql("""create table iud.source2 (c11 string,c22 int,c33 string,c55 string, c66 int) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/source2.csv' INTO table iud.source2""")
    sql("""create table iud.other (c1 string,c2 int) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/other.csv' INTO table iud.other""")
    sql("""create table iud.hdest (c1 string,c2 int,c3 string,c5 string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE""").show()
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.hdest""")
    sql("""CREATE TABLE iud.update_01(imei string,age int,task bigint,num double,level decimal(10,3),name string)STORED AS carbondata """)
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/update01.csv' INTO TABLE iud.update_01 OPTIONS('BAD_RECORDS_LOGGER_ENABLE' = 'FALSE', 'BAD_RECORDS_ACTION' = 'FORCE') """)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_HORIZONTAL_COMPACTION_ENABLE , "true")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER , "true")
  }

  test("test update operation with 0 rows updation.") {
    sql("""drop table if exists iud.zerorows""").show
    sql("""create table iud.zerorows (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.zerorows""")
    sql("""update zerorows d  set (d.c2) = (d.c2 + 1) where d.c1 = 'a'""").show()
    sql("""update zerorows d  set (d.c2) = (d.c2 + 1) where d.c1 = 'xxx'""").show()
    checkAnswer(
      sql("""select c1,c2,c3,c5 from iud.zerorows"""),
      Seq(Row("a",2,"aa","aaa"),Row("b",2,"bb","bbb"),Row("c",3,"cc","ccc"),Row("d",4,"dd","ddd"),Row("e",5,"ee","eee"))
    )
    sql("""drop table iud.zerorows""").show


  }


  test("update carbon table[select from source table with where and exist]") {
    sql("""drop table if exists iud.dest11""").show
    sql("""create table iud.dest11 (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest11""")
    sql("""update iud.dest11 d set (d.c3, d.c5 ) = (select s.c33,s.c55 from iud.source2 s where d.c1 = s.c11) where 1 = 1""").show()
    checkAnswer(
      sql("""select c3,c5 from iud.dest11"""),
      Seq(Row("cc","ccc"), Row("dd","ddd"),Row("ee","eee"), Row("MGM","Disco"),Row("RGK","Music"))
    )
    sql("""drop table iud.dest11""").show
  }

  test("update with subquery having limit 1") {
    sql("drop table if exists t1")
    sql("drop table if exists t2")
    sql("create table t1 (age int, name string) STORED AS carbondata")
    sql("insert into t1 select 1, 'aa'")
    sql("insert into t1 select 3, 'bb'")
    sql("create table t2 (age int, name string) STORED AS carbondata")
    sql("insert into t2 select 3, 'Andy'")
    sql("insert into t2 select 2, 'Andy'")
    sql("insert into t2 select 1, 'aa'")
    sql("insert into t2 select 3, 'aa'")
    sql("update t1 set (age) = " +
        "(select t2.age from t2 where t2.name = 'Andy' order by  age limit 1) " +
        "where t1.age = 1 ").show(false)
    checkAnswer(sql("select * from t1"), Seq(Row(2,"aa"), Row(3,"bb")))
    sql("drop table if exists t1")
    sql("drop table if exists t2")
  }

  test("update with subquery giving 0 rows") {
    sql("drop table if exists t1")
    sql("drop table if exists t2")
    sql("create table t1 (age int, name string) STORED AS carbondata")
    sql("insert into t1 select 1, 'aa'")
    sql("create table t2 (age int, name string) STORED AS carbondata")
    sql("insert into t2 select 3, 'Andy'")
    sql("update t1 set (age) = " +
        "(select t2.age from t2 where t2.age != 3) " +
        "where t1.age = 1 ").show(false)
    // should update to null
    checkAnswer(sql("select * from t1"), Seq(Row(null,"aa")))
    sql("drop table if exists t1")
    sql("drop table if exists t2")
  }

  test("update with subquery joing with main table and limit") {
    sql("drop table if exists t1")
    sql("drop table if exists t2")
    sql("create table t1 (age int, name string) STORED AS carbondata")
    sql("insert into t1 select 1, 'Andy'")
    sql("create table t2 (age int, name string) STORED AS carbondata")
    sql("insert into t2 select 3, 'Andy'")
    intercept[AnalysisException] {
      sql("update t1 set (age) = " +
          "(select t2.age from t2 where t2.name = t1.name limit 1) " +
          "where t1.age = 1 ").show(false)
    }.getMessage.contains("Update subquery has join with maintable " +
                          "and limit leads to multiple join for each limit for each row")
    sql("drop table if exists t1")
    sql("drop table if exists t2")
  }

  test("update carbon table[using destination table columns with where and exist]") {
    sql("""drop table if exists iud.dest22""")
    sql("""create table iud.dest22 (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest22""")
    checkAnswer(
      sql("""select c2 from iud.dest22 where c1='a'"""),
      Seq(Row(1))
    )
    sql("""update dest22 d  set (d.c2) = (d.c2 + 1) where d.c1 = 'a'""").show()
    checkAnswer(
      sql("""select c2 from iud.dest22 where c1='a'"""),
      Seq(Row(2))
    )
    sql("""drop table if exists iud.dest22""")
  }

  test("update with subquery with more than one value for key") {
    sql("drop table if exists t1")
    sql("drop table if exists t2")
    sql("create table t1 (age int, name string) STORED AS carbondata")
    sql("insert into t1 select 1, 'aa'")
    sql("insert into t1 select 2, 'aa'")
    sql("create table t2 (age int, name string) STORED AS carbondata")
    sql("insert into t2 select 1, 'Andy'")
    sql("insert into t2 select 2, 'Andy'")
    sql("insert into t2 select 1, 'aa'")
    sql("insert into t2 select 3, 'aa'")
    intercept[AnalysisException] {
      sql("update t1 set (age) = (select t2.age from t2 where t2.name = 'Andy') where t1.age = 1 ").show(false)
    }.getMessage.contains("update cannot be supported for 1 to N mapping, as more than one value present for the update key")
    // test join scenario
    val exception1 = intercept[RuntimeException] {
      sql("update t1 set (age) = (select t2.age from t2 where t2.name = t1.name) ").show(false)
    }
    assertResult(
      "Update operation failed.  update cannot be supported for 1 to N mapping, as more than one " +
      "value present for the update key")(exception1.getMessage)
    // Test carbon property
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_UPDATE_CHECK_UNIQUE_VALUE, "false")
    // update  should not throw exception
    sql("update t1 set (age) = (select t2.age from t2 where t2.name = t1.name) ").show(false)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_UPDATE_CHECK_UNIQUE_VALUE, "true")
    sql("drop table if exists t1")
    sql("drop table if exists t2")
  }

  test("update carbon table without alias in set columns") {
    sql("""drop table if exists iud.dest33""")
    sql("""create table iud.dest33 (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest33""")
    sql("""update iud.dest33 d set (c3,c5 ) = (select s.c33 ,s.c55  from iud.source2 s where d.c1 = s.c11) where d.c1 = 'a'""").show()
    checkAnswer(
      sql("""select c3,c5 from iud.dest33 where c1='a'"""),
      Seq(Row("MGM","Disco"))
    )
    sql("""drop table if exists iud.dest33""")
  }

  test("update carbon table without alias in set columns with mulitple loads") {
    sql("""drop table if exists iud.dest33""")
    sql("""create table iud.dest33 (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest33""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest33""")
    sql("""update iud.dest33 d set (c3,c5 ) = (select s.c33 ,s.c55  from iud.source2 s where d.c1 = s.c11) where d.c1 = 'a'""").show()
    checkAnswer(
      sql("""select c3,c5 from iud.dest33 where c1='a'"""),
      Seq(Row("MGM","Disco"),Row("MGM","Disco"))
    )
    sql("""drop table if exists iud.dest33""")
  }

  test("update carbon table with optimized parallelism for segment") {
    sql("""drop table if exists iud.dest_opt_segment_parallelism""")
    sql(
      """create table iud.dest_opt_segment_parallelism (c1 string,c2 int,c3 string,c5 string)
        | STORED AS carbondata""".stripMargin)
    sql(
      s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv'
         | INTO table iud.dest_opt_segment_parallelism""".stripMargin)
    sql(
      s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv'
         | INTO table iud.dest_opt_segment_parallelism""".stripMargin)
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_UPDATE_SEGMENT_PARALLELISM, "3")
    sql(
      """update iud.dest_opt_segment_parallelism d
        | set (c3,c5 ) = (select s.c33 ,s.c55 from iud.source2 s where d.c1 = s.c11)
        | where d.c1 = 'a'""".stripMargin).show()
    checkAnswer(
      sql("""select c3,c5 from iud.dest_opt_segment_parallelism where c1='a'"""),
      Seq(Row("MGM","Disco"),Row("MGM","Disco"))
    )
    sql("""drop table if exists iud.dest_opt_segment_parallelism""")
  }

  test("update carbon table without alias in set three columns") {
    sql("""drop table if exists iud.dest44""")
    sql("""create table iud.dest44 (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest44""")
    sql("""update iud.dest44 d set (c1,c3,c5 ) = (select s.c11, s.c33 ,s.c55  from iud.source2 s where d.c1 = s.c11) where d.c1 = 'a'""").show()
    checkAnswer(
      sql("""select c1,c3,c5 from iud.dest44 where c1='a'"""),
      Seq(Row("a","MGM","Disco"))
    )
    sql("""drop table if exists iud.dest44""")
  }

  test("update carbon table[single column select from source with where and exist]") {
    sql("""drop table if exists iud.dest55""")
    sql("""create table iud.dest55 (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest55""")
    sql("""update iud.dest55 d set (c3)  = (select s.c33 from iud.source2 s where d.c1 = s.c11) where 1 = 1""").show()
    checkAnswer(
      sql("""select c1,c3 from iud.dest55 """),
      Seq(Row("a","MGM"),Row("b","RGK"),Row("c","cc"),Row("d","dd"),Row("e","ee"))
    )
    sql("""drop table if exists iud.dest55""")
  }

  test("update carbon table[single column SELECT from source with where and exist]") {
    sql("""drop table if exists iud.dest55""")
    sql("""create table iud.dest55 (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest55""")
    sql("""update iud.dest55 d set (c3)  = (SELECT s.c33 from iud.source2 s where d.c1 = s.c11) where 1 = 1""").show()
    checkAnswer(
      sql("""select c1,c3 from iud.dest55 """),
      Seq(Row("a","MGM"),Row("b","RGK"),Row("c","cc"),Row("d","dd"),Row("e","ee"))
    )
    sql("""drop table if exists iud.dest55""")
  }

  test("update carbon table[using destination table columns without where clause]") {
    sql("""drop table if exists iud.dest66""")
    sql("""create table iud.dest66 (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest66""")
    sql("""update iud.dest66 d set (c2, c5 ) = (c2 + 1, concat(c5 , "z"))""").show()
    checkAnswer(
      sql("""select c2,c5 from iud.dest66 """),
      Seq(Row(2,"aaaz"),Row(3,"bbbz"),Row(4,"cccz"),Row(5,"dddz"),Row(6,"eeez"))
    )
    sql("""drop table if exists iud.dest66""")
  }

  test("update carbon table[using destination table columns with where clause]") {
    sql("""drop table if exists iud.dest77""")
    sql("""create table iud.dest77 (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest77""")
    sql("""update iud.dest77 d set (c2, c5 ) = (c2 + 1, concat(c5 , "z")) where d.c3 = 'dd'""").show()
    checkAnswer(
      sql("""select c2,c5 from iud.dest77 where c3 = 'dd'"""),
      Seq(Row(5,"dddz"))
    )
    sql("""drop table if exists iud.dest77""")
  }

  test("update carbon table[using destination table( no alias) columns without where clause]") {
    sql("""drop table if exists iud.dest88""")
    sql("""create table iud.dest88 (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest88""")
    sql("""update iud.dest88  set (c2, c5 ) = (c2 + 1, concat(c5 , "y" ))""").show()
    checkAnswer(
      sql("""select c2,c5 from iud.dest88 """),
      Seq(Row(2,"aaay"),Row(3,"bbby"),Row(4,"cccy"),Row(5,"dddy"),Row(6,"eeey"))
    )
    sql("""drop table if exists iud.dest88""")
  }

  test("update carbon table[using destination table columns with hard coded value ]") {
    sql("""drop table if exists iud.dest99""")
    sql("""create table iud.dest99 (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest99""")
    sql("""update iud.dest99 d set (c2, c5 ) = (c2 + 1, "xyx")""").show()
    checkAnswer(
      sql("""select c2,c5 from iud.dest99 """),
      Seq(Row(2,"xyx"),Row(3,"xyx"),Row(4,"xyx"),Row(5,"xyx"),Row(6,"xyx"))
    )
    sql("""drop table if exists iud.dest99""")
  }

  test("update carbon tableusing destination table columns with hard coded value and where condition]") {
    sql("""drop table if exists iud.dest110""")
    sql("""create table iud.dest110 (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest110""")
    sql("""update iud.dest110 d set (c2, c5 ) = (c2 + 1, "xyx") where d.c1 = 'e'""").show()
    checkAnswer(
      sql("""select c2,c5 from iud.dest110 where c1 = 'e' """),
      Seq(Row(6,"xyx"))
    )
    sql("""drop table iud.dest110""")
  }

  test("update carbon table[using source  table columns with where and exist and no destination table condition]") {
    sql("""drop table if exists iud.dest120""")
    sql("""create table iud.dest120 (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest120""")
    sql("""update iud.dest120 d  set (c3, c5 ) = (select s.c33 ,s.c55  from iud.source2 s where d.c1 = s.c11)""").show()
    checkAnswer(
      sql("""select c3,c5 from iud.dest120 """),
      Seq(Row("MGM","Disco"),Row("RGK","Music"),Row("cc","ccc"),Row("dd","ddd"),Row("ee","eee"))
    )
    sql("""drop table iud.dest120""")
  }

  test("update carbon table[using destination table where and exist]") {
    sql("""drop table if exists iud.dest130""")
    sql("""create table iud.dest130 (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest130""")
    sql("""update iud.dest130 dd  set (c2, c5 ) = (c2 + 1, "xyx")  where dd.c1 = 'a'""").show()
    checkAnswer(
      sql("""select c2,c5 from iud.dest130 where c1 = 'a' """),
      Seq(Row(2,"xyx"))
    )
    sql("""drop table iud.dest130""")
  }

  test("update carbon table[using destination table (concat) where and exist]") {
    sql("""drop table if exists iud.dest140""")
    sql("""create table iud.dest140 (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest140""")
    sql("""update iud.dest140 d set (c2, c5 ) = (c2 + 1, concat(c5 , "z"))  where d.c1 = 'a'""").show()
    checkAnswer(
      sql("""select c2,c5 from iud.dest140 where c1 = 'a'"""),
      Seq(Row(2,"aaaz"))
    )
    sql("""drop table iud.dest140""")
  }

  test("update carbon table[using destination table (concat) with  where") {
    sql("""drop table if exists iud.dest150""")
    sql("""create table iud.dest150 (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest150""")
    sql("""update iud.dest150 d set (c5) = (concat(c5 , "z"))  where d.c1 = 'b'""").show()
    checkAnswer(
      sql("""select c5 from iud.dest150 where c1 = 'b' """),
      Seq(Row("bbbz"))
    )
    sql("""drop table iud.dest150""")
  }

  test("update table with data for datatype mismatch with column ") {
    sql("""update iud.update_01 set (imei) = ('skt') where level = 'aaa'""")
    checkAnswer(
      sql("""select * from iud.update_01 where imei = 'skt'"""),
      Seq()
    )
  }

  test("update carbon table-error[more columns in source table not allowed") {
    val exception = intercept[Exception] {
      sql("""update iud.dest d set (c2, c5 ) = (c2 + 1, concat(c5 , "z"), "abc")""").show()
    }
    assertResult("The number of columns in source table and destination table columns mismatch;")(exception.getMessage)
  }

  test("update carbon table-error[no set columns") {
    intercept[Exception] {
      sql("""update iud.dest d set () = ()""").show()
    }
  }

  test("update carbon table-error[no set columns with updated column") {
    intercept[Exception] {
      sql("""update iud.dest d set  = (c1+1)""").show()
    }
  }
  test("update carbon table-error[one set column with two updated column") {
    intercept[Exception] {
      sql("""update iud.dest  set c2 = (c2 + 1, concat(c5 , "z") )""").show()
    }
  }

  test("""update carbon [special characters  in value- test parsing logic ]""") {
    sql("""drop table if exists iud.dest160""")
    sql("""create table iud.dest160 (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest160""")
    sql("""update iud.dest160 set(c1) = ("ab\')$*)(&^)")""").show()
    sql("""update iud.dest160 set(c1) =  ('abd$asjdh$adasj$l;sdf$*)$*)(&^')""").show()
    sql("""update iud.dest160 set(c1) =("\\")""").show()
    sql("""update iud.dest160 set(c1) = ("ab\')$*)(&^)")""").show()
    sql("""update iud.dest160 d set (c3,c5)=(select s.c33,'a\\a' from iud.source2 s where d.c1 = s.c11 and d.c2 = s.c22) where  d.c2 between 1 and 3""").show()
    sql("""update iud.dest160 d set (c3,c5)=(select s.c33,'\\' from iud.source2 s where d.c1 = s.c11 and d.c2 = s.c22) where  d.c2 between 1 and 3""").show()
    sql("""update iud.dest160 d set (c3,c5)=(select s.c33,'\\a' from iud.source2 s where d.c1 = s.c11 and d.c2 = s.c22) where  d.c2 between 1 and 3""").show()
    sql("""update iud.dest160 d set (c3,c5)      =     (select s.c33,'a\\a\\' from iud.source2 s where d.c1 = s.c11 and d.c2 = s.c22) where  d.c2 between 1 and 3""").show()
    sql("""update iud.dest160 d set (c3,c5) =(select s.c33,'a\'a\\' from iud.source2 s where d.c1 = s.c11 and d.c2 = s.c22) where  d.c2 between 1 and 3""").show()
    sql("""update iud.dest160 d set (c3,c5)=(select s.c33,'\\a\'a\"' from iud.source2 s where d.c1 = s.c11 and d.c2 = s.c22) where  d.c2 between 1 and 3""").show()
    sql("""drop table iud.dest160""")
  }

  test("""update carbon [sub query, between and existing in outer condition.(Customer query ) ]""") {
    sql("""drop table if exists iud.dest170""")
    sql("""create table iud.dest170 (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest170""")
    sql("""update iud.dest170 d set (c3)=(select s.c33 from iud.source2 s where d.c1 = s.c11 and d.c2 = s.c22) where  d.c2 between 1 and 3""").show()
    checkAnswer(
      sql("""select c3 from  iud.dest170 as d where d.c2 between 1 and 3"""),
      Seq(Row("MGM"), Row("RGK"), Row("cc"))
    )
    sql("""drop table iud.dest170""")
  }

  test("""update carbon [self join select query ]""") {
    sql("""drop table if exists iud.dest171""")
    sql("""create table iud.dest171 (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest171""")
    sql("""update iud.dest171 d set (c3)=(select concat(s.c3 , "z") from iud.dest171 s where d.c2 = s.c2)""").show
    sql("""drop table if exists iud.dest172""")
    sql("""create table iud.dest172 (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest172""")
    sql("""update iud.dest172 d set (c3)=( concat(c3 , "z"))""").show
    checkAnswer(
      sql("""select c3 from  iud.dest171"""),
      sql("""select c3 from  iud.dest172""")
    )
    sql("""drop table iud.dest171""")
    sql("""drop table iud.dest172""")
  }

  test("update carbon table-error[closing bracket missed") {
    intercept[Exception] {
      sql("""update iud.dest d set (c2) = (194""").show()
    }
  }

  test("update carbon table-error[starting bracket missed") {
    intercept[Exception] {
      sql("""update iud.dest d set (c2) = 194)""").show()
    }
  }

  test("update carbon table-error[missing starting and closing bracket") {
    intercept[Exception] {
      sql("""update iud.dest d set (c2) = 194""").show()
    }
  }

  test("test create table with column name as tupleID"){
    intercept[Exception] {
      sql("CREATE table carbontable (empno int, tupleID String, " +
          "designation String, doj Timestamp, workgroupcategory int, " +
          "workgroupcategoryname String, deptno int, deptname String, projectcode int, " +
          "projectjoindate Timestamp, projectenddate Timestamp, attendance int, " +
          "utilization int,salary int) STORED AS carbondata ")
    }
  }

  test("test show segment after updating data : JIRA-1411,JIRA-1414") {
    sql("""drop table if exists iud.show_segment""").show
    sql("""create table iud.show_segment (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.show_segment""")
    val before_update = sql("""show segments for table iud.show_segment""").collect()
    sql("""update iud.show_segment d set (d.c3, d.c5 ) = (select s.c33,s.c55 from iud.source2 s where d.c1 = s.c11) where 1 = 1""").show()
    val after_update = sql("""show segments for table iud.show_segment""")
    checkAnswer(
      after_update,
      before_update
    )
    sql("""drop table if exists iud.show_segment""").show
  }

  test("Failure of update operation due to bad record with proper error message") {
    try {
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, "FAIL")
      val errorMessage = intercept[Exception] {
        sql("drop table if exists update_with_bad_record")
        sql("create table update_with_bad_record(item int, name String) STORED AS carbondata")
        sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/IUD/bad_record.csv' into table " +
            s"update_with_bad_record")
        sql("update update_with_bad_record set (item)=(3.45)").show()
        sql("drop table if exists update_with_bad_record")
      }
      assert(errorMessage.getMessage
        .contains("Update operation failed"))
    } finally {
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, "FORCE")
    }
  }

  test("More records after update operation ") {
    sql("DROP TABLE IF EXISTS carbon1")
    import sqlContext.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 36000)
      .map(x => (x+"a", "b", x))
      .toDF("c1", "c2", "c3")
    df.write
      .format("carbondata")
      .option("tableName", "carbon1")
      .option("tempCSV", "true")
      .option("compress", "true")
      .mode(SaveMode.Overwrite)
      .save()

    checkAnswer(sql("select count(*) from carbon1"), Seq(Row(36000)))

    sql("update carbon1 set (c1)=('test123') where c1='9999a'").show()

    checkAnswer(sql("select count(*) from carbon1"), Seq(Row(36000)))

    checkAnswer(sql("select * from carbon1 where c1 = 'test123'"), Row("test123","b",9999))

    sql("DROP TABLE IF EXISTS carbon1")
  }

  test("""CARBONDATA-1445 carbon.update.persist.enable=false it will fail to update data""") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_UPDATE_PERSIST_ENABLE, "false")
    import sqlContext.implicits._
    val df = sqlContext.sparkContext.parallelize(0 to 50)
      .map(x => ("a", x.toString, (x % 2).toString, x, x.toLong, x * 2))
      .toDF("stringField1", "stringField2", "stringField3", "intField", "longField", "int2Field")
    sql("DROP TABLE IF EXISTS study_carbondata ")
    sql(s""" CREATE TABLE IF NOT EXISTS study_carbondata (
           |    stringField1          string,
           |    stringField2          string,
           |    stringField3          string,
           |    intField              int,
           |    longField             bigint,
           |    int2Field             int) STORED AS carbondata""".stripMargin)
    df.write
      .format("carbondata")
      .option("tableName", "study_carbondata")
      .option("compress", "true")  // just valid when tempCSV is true
      .option("tempCSV", "false")
      .option("sort_scope", "LOCAL_SORT")
      .mode(SaveMode.Append)
      .save()
    sql("""
      UPDATE study_carbondata a
          SET (a.stringField1, a.stringField2) = (concat(a.stringField1 , "_test" ), concat(a.stringField2 , "_test" ))
      WHERE a.stringField2 = '1'
      """).show()
    assert(sql("select stringField1 from study_carbondata where stringField2 = '1_test'").collect().length == 1)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_UPDATE_PERSIST_ENABLE, "true")
    sql("DROP TABLE IF EXISTS study_carbondata ")
  }

  test("update table in carbondata with rand() ") {

    sql("""CREATE TABLE iud.rand(imei string,age int,task bigint,num double,level decimal(10,3),name string)STORED AS carbondata """)
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/update01.csv' INTO TABLE iud.rand OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,age,task,num,level,name')""").collect

    sql("select substring(name,1,2 ) , name ,getTupleId() as tupleId , rand()  from  iud.rand").show(100)

    sql("select name , substring(name,1,2 ) ,getTupleId() as tupleId , num , rand() from  iud.rand").show(100)

    sql("Update  rand set (num) = (rand())").show()

    sql("select num from rand").show()

    sql("Update  rand set (num) = (rand(9))").show()

    sql("select num from rand").show()

    sql("Update  rand set (name) = ('Lily')").show()

    sql("select name from rand").show()

    sql("select name ,  num from  iud.rand").show(100)

    sql("select  imei , age , name , num  from  iud.rand").show(100)

    sql("select rand() , getTupleId() as tupleId from  iud.rand").show(100)

    sql("select * from  iud.rand").show(100)

    sql("select  imei , rand() , num from  iud.rand").show(100)

    sql("select  name , rand()  from  iud.rand").show(100)

    sql("DROP TABLE IF EXISTS iud.rand")
  }

  test("Update operation on carbon table with persist false") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_UPDATE_PERSIST_ENABLE, "false")
    sql("drop database if exists carbon1 cascade")
    sql(s"create database carbon1 location '$dblocation'")
    sql("use carbon1")
    sql("""CREATE TABLE carbontable(id int, name string, city string, age int)
         STORED AS carbondata""")
    val testData = s"$resourcesPath/sample.csv"
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table carbontable")
    // update operation
    sql("""update carbon1.carbontable d  set (d.id) = (d.id + 1) where d.id > 2""").show()
    checkAnswer(
      sql("select count(*) from carbontable"),
      Seq(Row(6))
    )
    sql("drop table carbontable")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_UPDATE_PERSIST_ENABLE,
        CarbonCommonConstants.CARBON_UPDATE_PERSIST_ENABLE_DEFAULT)
  }

  test("partition test update operation with 0 rows updation.") {
    sql("""drop table if exists iud.zerorows_part""").show
    sql("""create table iud.zerorows_part (c1 string,c2 int,c5 string) PARTITIONED BY(c3 string) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.zerorows_part""")
    sql("""update iud.zerorows_part d  set (d.c2) = (d.c2 + 1) where d.c1 = 'a'""").show()
    sql("""update iud.zerorows_part d  set (d.c2) = (d.c2 + 1) where d.c1 = 'xxx'""").show()
    checkAnswer(
      sql("""select c1,c2,c3,c5 from iud.zerorows_part"""),
      Seq(Row("a",2,"aa","aaa"),Row("b",2,"bb","bbb"),Row("c",3,"cc","ccc"),Row("d",4,"dd","ddd"),Row("e",5,"ee","eee"))
    )
    sql("""drop table iud.zerorows_part""").show

  }


  test("partition update carbon table[select from source table with where and exist]") {
    sql("""drop table if exists iud.dest11_part""").show
    sql("""create table iud.dest11_part (c1 string,c2 int,c5 string) PARTITIONED BY(c3 string) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest11_part""")
    sql("""update iud.dest11_part d set (d.c3, d.c5 ) = (select s.c33,s.c55 from iud.source2 s where d.c1 = s.c11) where 1 = 1""").show()
    checkAnswer(
      sql("""select c3,c5 from iud.dest11_part"""),
      Seq(Row("cc","ccc"), Row("dd","ddd"),Row("ee","eee"), Row("MGM","Disco"),Row("RGK","Music"))
    )
    sql("""drop table iud.dest11_part""").show
  }

  test("partition update carbon table[using destination table columns with where and exist]") {
    sql("""drop table if exists iud.dest22_part""")
    sql("""create table iud.dest22_part (c1 string,c2 int,c5 string) PARTITIONED BY(c3 string) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest22_part""")
    checkAnswer(
      sql("""select c2 from iud.dest22_part where c1='a'"""),
      Seq(Row(1))
    )
    sql("""update iud.dest22_part d  set (d.c2) = (d.c2 + 1) where d.c1 = 'a'""").show()
    checkAnswer(
      sql("""select c2 from iud.dest22_part where c1='a'"""),
      Seq(Row(2))
    )
    sql("""drop table if exists iud.dest22_part""")
  }

  test("partition update carbon table without alias in set columns") {
    sql("""drop table if exists iud.dest33_part""")
    sql("""create table iud.dest33_part (c2 int,c3 string,c5 string) PARTITIONED BY(c1 string) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest33_part""")
    sql("""update iud.dest33_part d set (c3,c5 ) = (select s.c33 ,s.c55  from iud.source2 s where d.c1 = s.c11) where d.c1 = 'a'""").show()
    checkAnswer(
      sql("""select c3,c5 from iud.dest33_part where c1='a'"""),
      Seq(Row("MGM","Disco"))
    )
    sql("""drop table if exists iud.dest33_part""")
  }

  test("partition update carbon table without alias in set columns with mulitple loads") {
    sql("""drop table if exists iud.dest33_part""")
    sql("""create table iud.dest33_part (c1 string,c2 int,c5 string) PARTITIONED BY(c3 string) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest33_part""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest33_part""")
    sql("""update iud.dest33_part d set (c3,c5 ) = (select s.c33 ,s.c55  from iud.source2 s where d.c1 = s.c11) where d.c1 = 'a'""").show()
    checkAnswer(
      sql("""select c3,c5 from iud.dest33_part where c1='a'"""),
      Seq(Row("MGM","Disco"),Row("MGM","Disco"))
    )
    sql("""drop table if exists iud.dest33_part""")
  }

  test("test create table with tupleid as column name") {
    try {
      sql("create table create_with_tupleid_column(item int, tupleId String) " +
          "STORED AS carbondata")
    } catch {
      case ex: Exception =>
        assert(ex.getMessage.contains("not allowed in column name while creating table"))
    }
  }

  test("test create table with position reference as column name") {
    try {
      sql(
        "create table create_with_positionReference_column(item int, positionReference String) " +
        "STORED AS carbondata")
    } catch {
      case ex: Exception =>
        assert(ex.getMessage.contains("not allowed in column name while creating table"))
    }
  }

  test("test create table with position id as column name") {
    try {
      sql(
        "create table create_with_positionid_column(item int, positionId String) " +
        "STORED AS carbondata")
    } catch {
      case ex: Exception =>
        assert(ex.getMessage.contains("not allowed in column name while creating table"))
    }
  }

  test("empty folder creation after compaction and update") {
    sql("drop table if exists t")
    sql("create table t (c1 string, c2 string, c3 int, c4 string) STORED AS carbondata")
    sql("insert into t select 'asd','sdf',1,'dfg'")
    sql("insert into t select 'asdf','sadf',2,'dafg'")
    sql("insert into t select 'asdq','sqdf',3,'dqfg'")
    sql("insert into t select 'aswd','sdfw',4,'dfgw'")
    sql("insert into t select 'aesd','sdef',5,'dfge'")
    sql("alter table t compact 'minor'")
    sql("clean files for table t")
    sql("delete from t where c3 = 2").show()
    sql("update t set(c4) = ('yyy') where c3 = 3").show()
    checkAnswer(sql("select count(*) from t where c4 = 'yyy'"), Seq(Row(1)))
    val f = new File(dblocation + CarbonCommonConstants.FILE_SEPARATOR +
                     CarbonCommonConstants.FILE_SEPARATOR + "t" +
                     CarbonCommonConstants.FILE_SEPARATOR + "Fact" +
                     CarbonCommonConstants.FILE_SEPARATOR + "Part0")
    if (!FileFactory.isFileExist(
      CarbonTablePath.getSegmentFilesLocation(
        dblocation + CarbonCommonConstants.FILE_SEPARATOR +
        CarbonCommonConstants.FILE_SEPARATOR + "t"))) {
      assert(f.list().length == 2)
    }
  }
  test("test sentences func in update statement") {
    sql("drop table if exists senten")
    sql("create table senten(name string, comment string) STORED AS carbondata")
    sql("insert into senten select 'aaa','comment for aaa'")
    sql("insert into senten select 'bbb','comment for bbb'")
    sql("select * from senten").show()
    val errorMessage = intercept[Exception] {
      sql("update senten set(comment)=(sentences('Hello there! How are you?'))").show()
    }.getMessage
    errorMessage
      .contains("Unsupported data type: Array")
    sql("drop table if exists senten")
  }

  test("block updating table which has index datamap") {
    sql("use iud")
    sql("drop table if exists test_dm_index")

    sql("create table test_dm_index (a string, b string, c string) STORED AS carbondata")
    sql("insert into test_dm_index select 'ccc','bbb','ccc'")

    sql(
      s"""
         | CREATE DATAMAP dm_test_dm_index ON TABLE test_dm_index
         | USING 'bloomfilter'
         | DMProperties('INDEX_COLUMNS'='a', 'BLOOM_SIZE'='640000')
      """.stripMargin)

    assert(intercept[UnsupportedOperationException] {
      sql("update test_dm_index set(a) = ('aaa') where a = 'ccc'")
    }.getMessage.contains("Update operation is not supported for table which has index datamaps"))

    sql("drop table if exists test_dm_index")
  }

  test("flat folder carbon table without alias in set columns with mulitple loads") {
    sql("""drop table if exists iud.dest33_flat""")
    sql("""create table iud.dest33_part (c1 string,c2 int,c5 string, c3 string) STORED AS carbondata TBLPROPERTIES('flat_folder'='true')""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest33_part""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest33_part""")
    sql("""update iud.dest33_part d set (c3,c5 ) = (select s.c33 ,s.c55  from iud.source2 s where d.c1 = s.c11) where d.c1 = 'a'""").show()
    checkAnswer(
      sql("""select c3,c5 from iud.dest33_part where c1='a'"""),
      Seq(Row("MGM","Disco"),Row("MGM","Disco"))
    )
    sql("""drop table if exists iud.dest33_part""")
  }

  test("check data after update with row.filter pushdown as false") {
    sql("""drop table if exists iud.dest33_flat""")
    sql(
      """create table iud.dest33_part (c1 int,c2 string, c3 short) STORED AS carbondata"""
        .stripMargin)
    sql(
      s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/negativevalue.csv' INTO table iud
         |.dest33_part options('header'='false')""".stripMargin)
    sql(
      """update iud.dest33_part d set (c1) = (5) where d.c1 = 0""".stripMargin).show()
    checkAnswer(sql("select c3 from iud.dest33_part"), Seq(Row(-300), Row(0), Row(-200), Row(700)
      , Row(100), Row(-100), Row(null)))
    sql("""drop table if exists iud.dest33_part""")
  }

  test("[CARBONDATA-3477] deal line break chars correctly after 'select' in 'update ... select columns' sql") {
    sql("""drop table if exists iud.dest11""").show
    sql("""create table iud.dest11 (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest11""")
    sql("""update iud.dest11 d set (d.c3, d.c5 ) = (select
           s.c33,s.c55 from iud.source2 s where d.c1 = s.c11) where 1 = 1""").show()
    checkAnswer(
      sql("""select c3,c5 from iud.dest11"""),
      Seq(Row("cc","ccc"), Row("dd","ddd"),Row("ee","eee"), Row("MGM","Disco"),Row("RGK","Music"))
    )
    sql("update iud.dest11 d set (d.c3, d.c5 ) = (select\ns.c33,s.c66 from iud.source2 s where d.c1 = s.c11) where 1 = 1").show()
    checkAnswer(
      sql("""select c3,c5 from iud.dest11"""),
      Seq(Row("cc","ccc"), Row("dd","ddd"),Row("ee","eee"), Row("MGM","10"),Row("RGK","8"))
    )
    sql("update iud.dest11 d set (d.c3, d.c5 ) = (select\r\ns.c55,s.c66 from iud.source2 s where d.c1 = s.c11) where 1 = 1").show()
    checkAnswer(
      sql("""select c3,c5 from iud.dest11"""),
      Seq(Row("cc","ccc"), Row("dd","ddd"),Row("ee","eee"), Row("Disco","10"),Row("Music","8"))
    )
    sql("update iud.dest11 d set (d.c3, d.c5 ) = (select\rs.c33,s.c66 from iud.source2 s where d.c1 = s.c11) where 1 = 1").show()
    checkAnswer(
      sql("""select c3,c5 from iud.dest11"""),
      Seq(Row("cc","ccc"), Row("dd","ddd"),Row("ee","eee"), Row("MGM","10"),Row("RGK","8"))
    )
    sql("update iud.dest11 d set (d.c3, d.c5 ) = (select\ts.c33,s.c55 from iud.source2 s where d.c1 = s.c11) where 1 = 1").show()
    checkAnswer(
      sql("""select c3,c5 from iud.dest11"""),
      Seq(Row("cc","ccc"), Row("dd","ddd"),Row("ee","eee"), Row("MGM","Disco"),Row("RGK","Music"))
    )
    sql("update iud.dest11 d set (d.c3, d.c5 ) = (select\t\ns.c33,s.c55 from iud.source2 s where d.c1 = s.c11) where 1 = 1").show()
    checkAnswer(
      sql("""select c3,c5 from iud.dest11"""),
      Seq(Row("cc","ccc"), Row("dd","ddd"),Row("ee","eee"), Row("MGM","Disco"),Row("RGK","Music"))
    )
    sql("""drop table iud.dest11""").show
  }

  test("[CARBONDATA-3491] Return updated/deleted rows count when execute update/delete sql") {
    sql("drop table if exists test_return_row_count")
    sql("drop table if exists test_return_row_count_source")

    sql("create table test_return_row_count (a string, b string, c string) STORED AS carbondata").show()
    sql("insert into test_return_row_count select 'bbb','bbb','ccc'").show()
    sql("insert into test_return_row_count select 'ccc','bbb','ccc'").show()
    sql("insert into test_return_row_count select 'ccc','bbb','ccc'").show()

    sql("create table test_return_row_count_source (a string, b string, c string) STORED AS carbondata").show()
    sql("insert into test_return_row_count_source select 'aaa','eee','ccc'").show()
    sql("insert into test_return_row_count_source select 'bbb','bbb','ccc'").show()
    sql("insert into test_return_row_count_source select 'ccc','bbb','ccc'").show()
    sql("insert into test_return_row_count_source select 'ccc','bbb','ccc'").show()

    checkAnswer(sql("update test_return_row_count set (b) = ('ddd') where a = 'ccc'"),
        Seq(Row(2))
    )
    checkAnswer(sql("select * from test_return_row_count"),
        Seq(Row("bbb", "bbb", "ccc"), Row("ccc", "ddd", "ccc"), Row("ccc", "ddd", "ccc"))
    )

    checkAnswer(sql("update test_return_row_count t set (t.b) = (select s.b from test_return_row_count_source s where s.a = 'aaa') where t.a = 'ccc'"),
        Seq(Row(2))
    )
    checkAnswer(sql("select * from test_return_row_count"),
        Seq(Row("bbb", "bbb", "ccc"), Row("ccc", "eee", "ccc"), Row("ccc", "eee", "ccc"))
    )

    sql("drop table if exists test_return_row_count")
    sql("drop table if exists test_return_row_count_source")
  }

  override def afterAll {
    sql("use default")
    sql("drop database  if exists iud cascade")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_HORIZONTAL_COMPACTION_ENABLE , "true")
      .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, LoggerAction.FORCE.name)
      .addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER , "true")
      .addProperty(CarbonCommonConstants.CARBON_UPDATE_SEGMENT_PARALLELISM, "1")
  }
}
