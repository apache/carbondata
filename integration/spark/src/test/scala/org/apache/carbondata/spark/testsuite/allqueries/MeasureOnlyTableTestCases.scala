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

import java.io.File

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.test.util.QueryTest

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
  * Test Class for all query on multiple datatypes
  *
  */
class MeasureOnlyTableTestCases extends QueryTest with BeforeAndAfterAll {

  val rootPath = new File(this.getClass.getResource("/").getPath
    + "../../../..").getCanonicalPath
  val path = s"$rootPath/examples/spark/src/main/resources/data.csv"
  override def beforeAll {
    clean
    sql(s"""
               | CREATE TABLE carbon_table(
               | shortField SMALLINT,
               | intField INT,
               | bigintField BIGINT,
               | doubleField DOUBLE,
               | floatField FLOAT,
               | decimalField DECIMAL(18,2)
               | )
               | STORED AS carbondata
             """.stripMargin)

          val path = s"$rootPath/examples/spark/src/main/resources/data.csv"

          sql(
            s"""
               | LOAD DATA LOCAL INPATH '$path'
               | INTO TABLE carbon_table
               | OPTIONS('FILEHEADER'='shortField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData',
               | 'COMPLEX_DELIMITER_LEVEL_1'='#')
             """.stripMargin)



    sql("create table if not exists carbon_table_hive (shortField SMALLINT,intField INT," +
      "bigintField BIGINT,doubleField DOUBLE,stringField STRING,timestampField TIMESTAMP," +
      "decimalField DECIMAL(18,2),dateField DATE,charField CHAR(5),floatField FLOAT,complexData ARRAY<STRING>)row format delimited fields terminated by ','")
    sql(s"""LOAD DATA LOCAL INPATH '$path' INTO table carbon_table_hive""")
  }

  def clean {
    sql("drop table if exists carbon_table")
    sql("drop table if exists carbon_table_hive")
  }

  override def afterAll {
    clean
  }

  test("SELECT sum(intField) FROM carbon_table where intField > 10") {
    checkAnswer(
      sql("SELECT sum(intField) FROM carbon_table where intField > 10"),
      sql("SELECT sum(intField) FROM carbon_table_hive where intField > 10")
    )
  }

  test("SELECT sum(intField),sum(doubleField) FROM carbon_table where intField > 10 OR doubleField > 10") {
    checkAnswer(
      sql("SELECT sum(intField),sum(doubleField) FROM carbon_table where intField > 10 OR " +
      "doubleField > 10"),
      sql("SELECT sum(intField),sum(doubleField) FROM carbon_table_hive where intField > 10 OR " +
        "doubleField > 10")
    )
  }

  test("SELECT sum(decimalField) FROM carbon_table") {
    checkAnswer(
      sql("SELECT sum(decimalField) FROM carbon_table"),
      sql("SELECT sum(decimalField) FROM carbon_table_hive")
    )
  }

  test("SELECT count(*), sum(intField) FROM carbon_table where intField > 10") {
    checkAnswer(
    sql("SELECT count(*), sum(intField) FROM carbon_table where intField > 10"),
      sql("SELECT count(*), sum(intField) FROM carbon_table_hive where intField > 10")
    )
  }

  test("SELECT count(*), sum(decimalField) b FROM carbon_table order by b") {
    checkAnswer(
    sql("SELECT count(*), sum(decimalField) b FROM carbon_table order by b"),
      sql("SELECT count(*), sum(decimalField) b FROM carbon_table_hive order by b")
    )
  }

  test("SELECT intField, sum(floatField) total FROM carbon_table group by intField order by " +
    "total") {
    checkAnswer(
    sql("SELECT intField, sum(floatField) total FROM carbon_table group by intField order by " +
      "total"),
      sql("SELECT intField, sum(floatField) total FROM carbon_table_hive group by intField order " +
        "by total")
    )
  }

  test("select shortField, avg(intField+ 10) as a from carbon_table group by shortField") {
    checkAnswer(
      sql("select shortField, avg(intField+ 10) as a from carbon_table group by shortField"),
      sql("select shortField, avg(intField+ 10) as a from carbon_table_hive group by shortField")
    )
  }

  test("select shortField, avg(intField+ 10) as a from carbon_table group by shortField order by " +
    "a") {
    checkAnswer(
      sql("select shortField, avg(intField+ 10) as a from carbon_table group by shortField order " +
        "by a"),
      sql("select shortField, avg(intField+ 10) as a from carbon_table_hive group by shortField " +
        "order by a")
    )
  }

  test("select shortField, avg(intField+ intField) as a from carbon_table group by shortField " +
    "order by a") {
    checkAnswer(
      sql("select shortField, avg(intField+ intField) as a from carbon_table group by shortField order " +
        "by a"),
      sql("select shortField, avg(intField+ intField) as a from carbon_table_hive group by " +
        "shortField order by a")
    )

  }

  test("select shortField, count(intField+ 10) as a from carbon_table group by shortField order " +
    "by a") {
    checkAnswer(
      sql("select shortField, count(intField+ 10) as a from carbon_table group by shortField " +
        "order by a"),
      sql("select shortField, count(intField+ 10) as a from carbon_table_hive group by shortField" +
        " order by a")
    )
  }

  test("select shortField, min(intField+ 10) as a from carbon_table group by shortField order " +
    "by a") {
    checkAnswer(
      sql("select shortField, min(intField+ 10) as a from carbon_table group by shortField " +
        "order by a"),
      sql("select shortField, min(intField+ 10) as a from carbon_table_hive group by shortField " +
        "order by a")
    )
  }

  test("select shortField, max(intField+ 10) as a from carbon_table group by shortField order " +
    "by a") {
    checkAnswer(
      sql("select shortField, count(intField+ 10) as a from carbon_table group by shortField " +
        "order by a"),
      sql("select shortField, count(intField+ 10) as a from carbon_table_hive group by shortField" +
        " order by a")
    )
  }

  test("select shortField, sum(distinct intField) + 10 as a from carbon_table group by shortField" +
    "order by a") {
    checkAnswer(
      sql("select shortField, sum(distinct intField) + 10 as a from carbon_table group by " +
        "shortField order by a"),
      sql("select shortField, sum(distinct intField) + 10 as a from carbon_table_hive group by " +
        "shortField order by a")
    )
  }

  test("select sum(doubleField) + 7.28 as a, intField from carbon_table group by intField") {
    checkAnswer(
      sql("select sum(doubleField) + 7.28 as a, intField from carbon_table group by intField"),
      sql("select sum(doubleField) + 7.28 as a, intField from carbon_table_hive group by intField")
    )
  }

  test("select count(floatField) + 7.28 a, intField from carbon_table group by intField") {
    checkAnswer(
      sql("select count(floatField) + 7.28 a, intField from carbon_table group by intField"),
      sql("select count(floatField) + 7.28 a, intField from carbon_table_hive group by intField")
    )
  }

  test("select count(distinct floatField) + 7.28 a, intField from carbon_table group by " +
    "intField") {
    checkAnswer(
      sql("select count(distinct floatField) + 7.28 a, intField from carbon_table group by intField"),
      sql("select count(distinct floatField) + 7.28 a, intField from carbon_table_hive group" +
        " by intField")
    )
  }

  test("select count (if(doubleField>100,NULL,doubleField))  a from carbon_table") {
    checkAnswer(
      sql("select count (if(doubleField>100,NULL,doubleField))  a from carbon_table"),
      sql("select count (if(doubleField>100,NULL,doubleField))  a from carbon_table_hive")
    )
  }

  test("select count (if(decimalField>100,NULL,decimalField))  a from carbon_table") {
    checkAnswer(
      sql("select count (if(decimalField>100,NULL,decimalField))  a from carbon_table"),
      sql("select count (if(decimalField>100,NULL,decimalField))  a from carbon_table_hive")
    )
  }


  test("select avg (if(floatField>100,NULL,floatField))  a from carbon_table") {
    checkAnswer(
      sql("select avg (if(floatField>100,NULL,floatField))  a from carbon_table"),
      sql("select avg (if(floatField>100,NULL,floatField))  a from carbon_table_hive")
    )
  }

  test("select min (if(intField>100,NULL,intField))  a from carbon_table") {
    checkAnswer(
      sql("select min (if(intField>3,NULL,intField))  a from carbon_table"),
      sql("select min (if(intField>3,NULL,intField))  a from carbon_table_hive")
    )
  }

  test("select max (if(intField>5,NULL,intField))  a from carbon_table")({
    checkAnswer(
      sql("select max (if(intField>5,NULL,intField))  a from carbon_table"),
      sql("select max (if(intField>5,NULL,intField))  a from carbon_table_hive")
    )
  })

  test("select variance(doubleField) as a from carbon_table")({
    checkAnswer(
      sql("select variance(doubleField) as a from carbon_table"),
      sql("select variance(doubleField) as a from carbon_table_hive")
    )
  })

  test("select var_samp(doubleField) as a  from carbon_table")({
    checkAnswer(
      sql("select var_samp(doubleField) as a  from carbon_table"),
      sql("select var_samp(doubleField) as a  from carbon_table_hive")
    )
  })

  test("select stddev_pop(doubleField) as a  from carbon_table")({
    checkAnswer(
      sql("select stddev_pop(doubleField) as a  from carbon_table"),
      sql("select stddev_pop(doubleField) as a  from carbon_table_hive")
    )
  })

  //TC_106
  test("select stddev_samp(doubleField)  as a from carbon_table")({
    checkAnswer(
      sql("select stddev_samp(doubleField)  as a from carbon_table"),
      sql("select stddev_samp(doubleField)  as a from carbon_table_hive")
    )
  })

  test("select covar_pop(doubleField,doubleField) as a  from carbon_table")({
    checkAnswer(
      sql("select covar_pop(doubleField,doubleField) as a  from carbon_table"),
      sql("select covar_pop(doubleField,doubleField) as a  from carbon_table_hive")
    )
  })

  test("select covar_samp(doubleField,doubleField) as a  from carbon_table")({
    checkAnswer(
      sql("select covar_samp(doubleField,doubleField) as a  from carbon_table"),
      sql("select covar_samp(doubleField,doubleField) as a  from carbon_table_hive")
    )
  })

  test("select corr(doubleField,doubleField)  as a from carbon_table")({
    checkAnswer(
      sql("select corr(doubleField,doubleField)  as a from carbon_table"),
      sql("select corr(doubleField,doubleField)  as a from carbon_table_hive")
    )
  })

  test("select percentile(bigintField,0.2) as  a  from carbon_table")({
    checkAnswer(
      sql("select percentile(bigintField,0.2) as  a  from carbon_table"),
      sql("select percentile(bigintField,0.2) as  a  from carbon_table_hive"))
  })

  test("select last(doubleField) a from carbon_table")({
    checkAnswer(
      sql("select last(doubleField) a from carbon_table"),
      sql("select last(doubleField) a from carbon_table_hive")
    )
  })

  test("select intField from carbon_table where carbon_table.intField IN (3,2)")({
    checkAnswer(
      sql("select intField from carbon_table where carbon_table.intField IN (3,2)"),
      sql("select intField from carbon_table_hive where carbon_table_hive.intField IN (3,2)")
    )
  })

  test("select intField from carbon_table where carbon_table.intField NOT IN (3,2)")({
    checkAnswer(
      sql("select intField from carbon_table where carbon_table.intField NOT IN (3,2)"),
      sql("select intField from carbon_table_hive where carbon_table_hive.intField NOT IN (3,2)")
    )
  })

  test("select intField,sum(floatField) a from carbon_table group by intField order by a " +
    "desc")({
    checkAnswer(
      sql("select intField,sum(floatField) a from carbon_table group by intField order by " +
        "a desc"),
      sql("select intField,sum(floatField) a from carbon_table_hive group by intField order by " +
        "a desc")
    )
  })

  test("select intField,sum(floatField) a from carbon_table group by intField order by a" +
    " asc")({
    checkAnswer(
      sql("select intField,sum(floatField) a from carbon_table group by intField order by " +
        "a asc"),
      sql("select intField,sum(floatField) a from carbon_table_hive group by intField order by " +
        "a asc")
    )
  })

  test("select doubleField from carbon_table where doubleField NOT BETWEEN intField AND floatField")({
    checkAnswer(
      sql("select doubleField from carbon_table where doubleField NOT BETWEEN intField AND floatField"),
      sql("select doubleField from carbon_table_hive where doubleField NOT BETWEEN intField AND " +
        "floatField")
    )
  })

  test("select cast(doubleField as int) as a from carbon_table limit 10")({
    checkAnswer(
      sql("select cast(doubleField as int) as a from carbon_table limit 10"),
      sql("select cast(doubleField as int) as a from carbon_table_hive limit 10")
    )
  })

  test("select percentile_approx(1, 0.5 ,5000) from carbon_table")({
    checkAnswer(
      sql("select percentile_approx(1, 0.5 ,5000) from carbon_table"),
      sql("select percentile_approx(1, 0.5 ,5000) from carbon_table_hive")
    )
  })

  test("CARBONDATA-60-union-defect")({
    sql("drop table if exists carbonunion")
    import sqlContext.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 1000).map(x => (x, (x+100))).toDF("c1", "c2")
    df.createOrReplaceTempView("sparkunion")
    df.write
      .format("carbondata")
      .mode(SaveMode.Overwrite)
      .option("tableName", "carbonunion")
      .save()
    checkAnswer(
      sql("select c1,count(c1) from (select c1 as c1,c2 as c2 from carbonunion union all select c2 as c1,c1 as c2 from carbonunion)t where c1=200 group by c1"),
      sql("select c1,count(c1) from (select c1 as c1,c2 as c2 from sparkunion union all select c2 as c1,c1 as c2 from sparkunion)t where c1=200 group by c1"))
    sql("drop table if exists carbonunion")
  })

  test("select b.intField from carbon_table a join carbon_table b on a.intField=b.intField")({
    checkAnswer(
      sql("select b.intField from carbon_table a join carbon_table b on a.intField=b.intField"),
      sql("select b.intField from carbon_table_hive a join carbon_table_hive b on a.intField=b.intField"))
  })
}