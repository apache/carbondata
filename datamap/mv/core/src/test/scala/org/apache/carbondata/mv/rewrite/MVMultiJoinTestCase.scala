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
package org.apache.carbondata.mv.rewrite

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class MVMultiJoinTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll(){
    drop
    sql("create table dim_table(name string,age int,height int) using carbondata")
    sql("create table sdr_table(name varchar(20), score int) using carbondata")
    sql("create table areas(aid int, title string, pid int) using carbondata")
  }

  override def afterAll(){
    drop
  }

  test("test mv self join") {
    sql("insert into areas select 130000, 'hebei', null")
    sql("insert into areas select 130100, 'shijiazhuang', 130000")
    sql("insert into areas select 130400, 'handan', 130000")
    sql("insert into areas select 410000, 'henan', null")
    sql("insert into areas select 410300, 'luoyang', 410000")

    val mvSQL =
      s"""select p.title,c.title
         |from areas as p
         |inner join areas as c on c.pid=p.aid
         |where p.title = 'hebei'
       """.stripMargin
    sql("create datamap table_mv using 'mv' as " +
        "select p.title,c.title,c.pid,p.aid from areas as p inner join areas as c on " +
        "c.pid=p.aid where p.title = 'hebei'")
    val frame = sql(mvSQL)
    assert(TestUtil.verifyMVDataMap(frame.queryExecution.optimizedPlan, "table_mv"))
    checkAnswer(frame, Seq(Row("hebei","shijiazhuang"), Row("hebei","handan")))
  }

  test("test mv two join tables are same") {
    sql("drop datamap if exists table_mv")

    sql("insert into dim_table select 'tom',20,170")
    sql("insert into dim_table select 'lily',30,160")
    sql("insert into sdr_table select 'tom',70")
    sql("insert into sdr_table select 'tom',50")
    sql("insert into sdr_table select 'lily',80")

    val mvSQL =
      s"""select sdr.name,sum(sdr.score),dim.age,dim_other.height from sdr_table sdr
         | left join dim_table dim on sdr.name = dim.name
         | left join dim_table dim_other on sdr.name = dim_other.name
         | group by sdr.name,dim.age,dim_other.height
       """.stripMargin
    sql("create datamap table_mv using 'mv' as " + "select sdr.name,sum(sdr.score),dim.age,dim_other.height,count(dim.name) as c1, count(dim_other.name) as c2 from sdr_table sdr left join dim_table dim on sdr.name = dim.name left join dim_table dim_other on sdr.name = dim_other.name group by sdr.name,dim.age,dim_other.height")
    val frame = sql(mvSQL)
    assert(TestUtil.verifyMVDataMap(frame.queryExecution.optimizedPlan, "table_mv"))
    checkAnswer(frame, Seq(Row("lily",80,30,160),Row("tom",120,20,170)))
  }

  def drop: Unit ={
    sql("drop table if exists areas")
    sql("drop table if exists dim_table")
    sql("drop table if exists sdr_table")
    sql("drop datamap if exists table_mv")
  }

}
