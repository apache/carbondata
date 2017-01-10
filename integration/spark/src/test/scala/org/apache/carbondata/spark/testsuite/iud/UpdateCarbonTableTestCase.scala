/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.carbondata.spark.testsuite.iud

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class UpdateCarbonTableTestCase extends QueryTest with BeforeAndAfterAll {
  override def beforeAll {

    sql("drop database if exists iud cascade")
    sql("create database iud")
    sql("use iud")
    sql("""create table iud.dest (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""").show()
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest""")
    sql("""create table iud.source2 (c11 string,c22 int,c33 string,c55 string, c66 int) STORED BY 'org.apache.carbondata.format'""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/source2.csv' INTO table iud.source2""")
    sql("""create table iud.other (c1 string,c2 int) STORED BY 'org.apache.carbondata.format'""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/other.csv' INTO table iud.other""")
    sql("""create table iud.hdest (c1 string,c2 int,c3 string,c5 string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE""").show()
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.hdest""")
    sql("""CREATE TABLE iud.update_01(imei string,age int,task bigint,num double,level decimal(10,3),name string)STORED BY 'org.apache.carbondata.format' """)
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/update01.csv' INTO TABLE iud.update_01 OPTIONS('BAD_RECORDS_LOGGER_ENABLE' = 'FALSE', 'BAD_RECORDS_ACTION' = 'FORCE') """)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.isHorizontalCompactionEnabled , "true")
  }


  test("test update operation with 0 rows updation.") {
    sql("""drop table iud.zerorows""").show
    sql("""create table iud.zerorows (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""").show()
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
      sql("""drop table iud.dest11""").show
      sql("""create table iud.dest11 (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""").show()
      sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest11""")
      sql("""update iud.dest11 d set (d.c3, d.c5 ) = (select s.c33,s.c55 from iud.source2 s where d.c1 = s.c11) where 1 = 1""").show()
      checkAnswer(
        sql("""select c3,c5 from iud.dest11"""),
        Seq(Row("cc","ccc"), Row("dd","ddd"),Row("ee","eee"), Row("MGM","Disco"),Row("RGK","Music"))
      )
      sql("""drop table iud.dest11""").show
   }

   test("update carbon table[using destination table columns with where and exist]") {
    sql("""drop table iud.dest22""").show
    sql("""create table iud.dest22 (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""").show()
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
    sql("""drop table iud.dest22""").show
   }

   test("update carbon table without alias in set columns") {
      sql("""drop table iud.dest33""").show
      sql("""create table iud.dest33 (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""").show()
      sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest33""")
      sql("""update iud.dest33 d set (c3,c5 ) = (select s.c33 ,s.c55  from iud.source2 s where d.c1 = s.c11) where d.c1 = 'a'""").show()
      checkAnswer(
        sql("""select c3,c5 from iud.dest33 where c1='a'"""),
        Seq(Row("MGM","Disco"))
      )
      sql("""drop table iud.dest33""").show
  }

   test("update carbon table without alias in set three columns") {
     sql("""drop table iud.dest44""").show
     sql("""create table iud.dest44 (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""").show()
     sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest44""")
     sql("""update iud.dest44 d set (c1,c3,c5 ) = (select s.c11, s.c33 ,s.c55  from iud.source2 s where d.c1 = s.c11) where d.c1 = 'a'""").show()
     checkAnswer(
       sql("""select c1,c3,c5 from iud.dest44 where c1='a'"""),
       Seq(Row("a","MGM","Disco"))
     )
     sql("""drop table iud.dest44""").show
   }

   test("update carbon table[single column select from source with where and exist]") {
      sql("""drop table iud.dest55""").show
      sql("""create table iud.dest55 (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""").show()
      sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest55""")
     sql("""update iud.dest55 d set (c3)  = (select s.c33 from iud.source2 s where d.c1 = s.c11) where 1 = 1""").show()
      checkAnswer(
        sql("""select c1,c3 from iud.dest55 """),
        Seq(Row("a","MGM"),Row("b","RGK"),Row("c","cc"),Row("d","dd"),Row("e","ee"))
      )
      sql("""drop table iud.dest55""").show
   }

  test("update carbon table[single column SELECT from source with where and exist]") {
    sql("""drop table iud.dest55""").show
    sql("""create table iud.dest55 (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""").show()
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest55""")
    sql("""update iud.dest55 d set (c3)  = (SELECT s.c33 from iud.source2 s where d.c1 = s.c11) where 1 = 1""").show()
    checkAnswer(
      sql("""select c1,c3 from iud.dest55 """),
      Seq(Row("a","MGM"),Row("b","RGK"),Row("c","cc"),Row("d","dd"),Row("e","ee"))
    )
    sql("""drop table iud.dest55""").show
  }

   test("update carbon table[using destination table columns without where clause]") {
     sql("""drop table iud.dest66""").show
     sql("""create table iud.dest66 (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""").show()
     sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest66""")
     sql("""update iud.dest66 d set (c2, c5 ) = (c2 + 1, concat(c5 , "z"))""").show()
     checkAnswer(
       sql("""select c2,c5 from iud.dest66 """),
       Seq(Row(2,"aaaz"),Row(3,"bbbz"),Row(4,"cccz"),Row(5,"dddz"),Row(6,"eeez"))
     )
     sql("""drop table iud.dest66""").show
   }

   test("update carbon table[using destination table columns with where clause]") {
       sql("""drop table iud.dest77""").show
       sql("""create table iud.dest77 (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""").show()
       sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest77""")
       sql("""update iud.dest77 d set (c2, c5 ) = (c2 + 1, concat(c5 , "z")) where d.c3 = 'dd'""").show()
       checkAnswer(
         sql("""select c2,c5 from iud.dest77 where c3 = 'dd'"""),
         Seq(Row(5,"dddz"))
       )
       sql("""drop table iud.dest77""").show
   }

   test("update carbon table[using destination table( no alias) columns without where clause]") {
     sql("""drop table iud.dest88""").show
     sql("""create table iud.dest88 (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""").show()
     sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest88""")
     sql("""update iud.dest88  set (c2, c5 ) = (c2 + 1, concat(c5 , "y" ))""").show()
     checkAnswer(
       sql("""select c2,c5 from iud.dest88 """),
       Seq(Row(2,"aaay"),Row(3,"bbby"),Row(4,"cccy"),Row(5,"dddy"),Row(6,"eeey"))
     )
     sql("""drop table iud.dest88""").show
   }

   test("update carbon table[using destination table columns with hard coded value ]") {
     sql("""drop table iud.dest99""").show
     sql("""create table iud.dest99 (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""").show()
     sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest99""")
     sql("""update iud.dest99 d set (c2, c5 ) = (c2 + 1, "xyx")""").show()
     checkAnswer(
       sql("""select c2,c5 from iud.dest99 """),
       Seq(Row(2,"xyx"),Row(3,"xyx"),Row(4,"xyx"),Row(5,"xyx"),Row(6,"xyx"))
     )
     sql("""drop table iud.dest99""").show
   }

   test("update carbon tableusing destination table columns with hard coded value and where condition]") {
     sql("""drop table iud.dest110""").show
     sql("""create table iud.dest110 (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""").show()
     sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest110""")
     sql("""update iud.dest110 d set (c2, c5 ) = (c2 + 1, "xyx") where d.c1 = 'e'""").show()
     checkAnswer(
       sql("""select c2,c5 from iud.dest110 where c1 = 'e' """),
       Seq(Row(6,"xyx"))
     )
     sql("""drop table iud.dest110""").show
   }

   test("update carbon table[using source  table columns with where and exist and no destination table condition]") {
     sql("""drop table iud.dest120""").show
     sql("""create table iud.dest120 (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""").show()
     sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest120""")
     sql("""update iud.dest120 d  set (c3, c5 ) = (select s.c33 ,s.c55  from iud.source2 s where d.c1 = s.c11)""").show()
     checkAnswer(
       sql("""select c3,c5 from iud.dest120 """),
       Seq(Row("MGM","Disco"),Row("RGK","Music"),Row("cc","ccc"),Row("dd","ddd"),Row("ee","eee"))
     )
     sql("""drop table iud.dest120""").show
   }

   test("update carbon table[using destination table where and exist]") {
     sql("""drop table iud.dest130""").show
     sql("""create table iud.dest130 (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""").show()
     sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest130""")
     sql("""update iud.dest130 dd  set (c2, c5 ) = (c2 + 1, "xyx")  where dd.c1 = 'a'""").show()
     checkAnswer(
       sql("""select c2,c5 from iud.dest130 where c1 = 'a' """),
       Seq(Row(2,"xyx"))
     )
     sql("""drop table iud.dest130""").show
   }

   test("update carbon table[using destination table (concat) where and exist]") {
     sql("""drop table iud.dest140""").show
     sql("""create table iud.dest140 (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""").show()
     sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest140""")
     sql("""update iud.dest140 d set (c2, c5 ) = (c2 + 1, concat(c5 , "z"))  where d.c1 = 'a'""").show()
     checkAnswer(
       sql("""select c2,c5 from iud.dest140 where c1 = 'a'"""),
       Seq(Row(2,"aaaz"))
     )
     sql("""drop table iud.dest140""").show
   }

   test("update carbon table[using destination table (concat) with  where") {
     sql("""drop table iud.dest150""").show
     sql("""create table iud.dest150 (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""").show()
     sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest150""")
     sql("""update iud.dest150 d set (c5) = (concat(c5 , "z"))  where d.c1 = 'b'""").show()
     checkAnswer(
       sql("""select c5 from iud.dest150 where c1 = 'b' """),
       Seq(Row("bbbz"))
     )
     sql("""drop table iud.dest150""").show
   }

  test("update table with data for datatype mismatch with column ") {
    sql("""update iud.update_01 set (imei) = ('skt') where level = 'aaa'""")
    checkAnswer(
      sql("""select * from iud.update_01 where imei = 'skt'"""),
      Seq()
    )
  }

   test("update carbon table-error[more columns in source table not allowed") {
     try {
       sql("""update iud.dest d set (c2, c5 ) = (c2 + 1, concat(c5 , "z"), "abc")""").show()
       assert(false)
     } catch {
       case ex: Exception =>
         assertResult("Number of source and destination columns are not matching")(ex.getMessage)
     }
   }
   test("update carbon table-error[no set columns") {
     try {
       sql("""update iud.dest d set () = ()""").show()
       assert(false)
     } catch {
       case ex: org.apache.spark.sql.AnalysisException =>
         assert(true)
     }
   }
   test("update carbon table-error[no set columns with updated column") {
     try {
       sql("""update iud.dest d set  = (c1+1)""").show()
       assert(false)
     } catch {
       case ex: org.apache.spark.sql.AnalysisException =>
         assert(true)
     }
   }
   test("update carbon table-error[one set column with two updated column") {
     try {
       sql("""update iud.dest  set c2 = (c2 + 1, concat(c5 , "z") )""").show()
       assert(false)
     } catch {
       case ex: org.apache.spark.sql.AnalysisException =>
         assert(true)
     }
   }

 test("""update carbon [special characters  in value- test parsing logic ]""") {
    sql("""drop table iud.dest160""").show
    sql("""create table iud.dest160 (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""").show()
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
    sql("""drop table iud.dest160""").show
  }

  test("""update carbon [sub query, between and existing in outer condition.(Customer query ) ]""") {
    sql("""drop table iud.dest170""").show
    sql("""create table iud.dest170 (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""").show()
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest170""")
    sql("""update iud.dest170 d set (c3)=(select s.c33 from iud.source2 s where d.c1 = s.c11 and d.c2 = s.c22) where  d.c2 between 1 and 3""").show()
    checkAnswer(
      sql("""select c3 from  iud.dest170 as d where d.c2 between 1 and 3"""),
      Seq(Row("MGM"), Row("RGK"), Row("cc"))
    )
    sql("""drop table iud.dest170""").show
  }

  test("""update carbon [self join select query ]""") {
    sql("""drop table iud.dest171""").show
    sql("""create table iud.dest171 (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""").show()
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest171""")
    sql("""update iud.dest171 d set (c3)=(select concat(s.c3 , "z") from iud.dest171 s where d.c2 = s.c2)""").show
    sql("""drop table iud.dest172""").show
    sql("""create table iud.dest172 (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""").show()
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud.dest172""")
    sql("""update iud.dest172 d set (c3)=( concat(c3 , "z"))""").show
    checkAnswer(
      sql("""select c3 from  iud.dest171"""),
      sql("""select c3 from  iud.dest172""")
    )
    sql("""drop table iud.dest171""").show
    sql("""drop table iud.dest172""").show
  }

  test("update carbon table-error[closing bracket missed") {
    try {
      sql("""update iud.dest d set (c2) = (194""").show()
      assert(false)
    } catch {
      case ex: Exception =>
        assert(true)
    }
  }

  test("update carbon table-error[starting bracket missed") {
    try {
      sql("""update iud.dest d set (c2) = 194)""").show()
      assert(false)
    } catch {
      case ex: Exception =>
        assert(true)
    }
  }

  test("update carbon table-error[missing starting and closing bracket") {
    try {
      sql("""update iud.dest d set (c2) = 194""").show()
      assert(false)
    } catch {
      case ex: Exception =>
        assert(true)
    }
  }

  test("test create table with column name as tupleID"){
    try {
      sql("CREATE table carbontable (empno int, tupleID String, " +
          "designation String, doj Timestamp, workgroupcategory int, " +
          "workgroupcategoryname String, deptno int, deptname String, projectcode int, " +
          "projectjoindate Timestamp, projectenddate Timestamp, attendance int, " +
          "utilization int,salary int) STORED BY 'org.apache.carbondata.format' " +
          "TBLPROPERTIES('DICTIONARY_INCLUDE'='empno,workgroupcategory,deptno,projectcode'," +
          "'DICTIONARY_EXCLUDE'='empname')")
    }catch  {
      case ex: Exception => assert(true)
    }
  }


  override def afterAll {
    sql("use default")
    sql("drop database  if exists iud cascade")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.isHorizontalCompactionEnabled , "true")
  }
}