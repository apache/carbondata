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
package org.apache.carbondata.spark.testsuite.allqueries

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.Spark2QueryTest
import org.scalatest.BeforeAndAfterAll
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

import scala.collection.mutable

class InsertIntoCarbonTableSpark2TestCase extends Spark2QueryTest with BeforeAndAfterAll {
  override def beforeAll: Unit = {
    sql("drop table if exists OneRowTable")
  }

  test("insert select one row") {
    sql("create table OneRowTable(col1 string, col2 string, col3 int, col4 double) stored by 'carbondata'")
    sql("insert into OneRowTable select '0.1', 'a.b', 1, 1.2")
    checkAnswer(sql("select * from OneRowTable"), Seq(Row("0.1", "a.b", 1, 1.2)))
  }

  test("test array insert complex "){

    sql("DROP TABLE IF EXISTS testarrayinsert")

    sql("CREATE TABLE  testarrayinsert (f1 int ,f2 array<array<String>>,f3 array<int>) STORED BY 'org.apache.carbondata.format' ").show

    sql("insert into testarrayinsert values (1 , 'ab:ab1$bc:bc1$cd:cd1:cd2' , '1$2$3') ")

    sql("insert into testarrayinsert values (2 , 'ef:ef1$gh:gh11$ij:ij1:ij2' , '1$2$3') ")

    sql("select * from testarrayinsert").show

    checkAnswer(sql("select f2[0],f3 from testarrayinsert where f1=1 "),Seq(Row(mutable.ArraySeq("ab" , "ab1"),mutable.ArraySeq(1,2,3))))

    checkAnswer(sql("select f2[0],f3 from testarrayinsert where f1=2 "),Seq(Row(mutable.ArraySeq("ef" , "ef1"),mutable.ArraySeq(1,2,3))))

    sql("DROP TABLE IF EXISTS testarrayinsert")
  }

  test("test array insert with select "){

    sql("DROP TABLE IF EXISTS arrayselectinsert")

    sql("CREATE TABLE  testarrayinsert (f1 int ,f2 array<array<String>>,f3 array<int>) STORED BY 'org.apache.carbondata.format' ").show

    sql("insert into testarrayinsert values (1 , 'ab:ab1$bc:bc1$cd:cd1:cd2' , '1$2$3') ")

    sql("insert into testarrayinsert values (2 , 'ef:ef1$gh:gh11$ij:ij1:ij2' , '1$2$3') ")

    sql("insert into testarrayinsert select * from testarrayinsert where f1 =1 ")

    sql("select * from testarrayinsert").show

    checkAnswer(sql("select f2[0],f3 from testarrayinsert where f1=1 "),Seq(Row(mutable.ArraySeq("ab" , "ab1"),mutable.ArraySeq(1,2,3))
      , Row(mutable.ArraySeq("ab" , "ab1"),mutable.ArraySeq(1,2,3))))

    sql("DROP TABLE IF EXISTS testarrayinsert")
  }


  test("insert table with struct data ") {
    sql("DROP TABLE IF EXISTS st")
    sql("create table st (id int, structelem struct<id1:int, structelem: struct<id2:int, name:string>>)" +
      "stored by 'carbondata'").show
    sql("insert into st values (1 , '1$1:aa')").show
    sql("insert into st values (2 , '2$2:bb')").show
    sql("insert into st values (3 , '3$3:cc')").show
    sql("insert into st values (4 , '4$4:ff')").show
    sql("insert into st values (5 , '5$5:ee')").show
    checkAnswer(sql("select count(*) from st  ")
      ,Seq(Row(5)))
    sql("DROP TABLE IF EXISTS st")
  }

  override def afterAll {
    sql("drop table if exists OneRowTable")
  }

}
