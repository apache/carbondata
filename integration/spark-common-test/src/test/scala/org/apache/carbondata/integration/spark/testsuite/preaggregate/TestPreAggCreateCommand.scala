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

package org.apache.carbondata.integration.spark.testsuite.preaggregate

import java.io.File
import java.util
import java.util.concurrent.{Callable, ExecutorService, Executors, TimeUnit}

import scala.collection.JavaConverters._

import org.apache.spark.sql.{AnalysisException, CarbonDatasourceHadoopRelation, Row}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.test.util.CarbonQueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.common.exceptions.sql.{MalformedCarbonCommandException, MalformedDataMapCommandException}
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider.TIMESERIES
import org.apache.carbondata.core.util.CarbonProperties

class TestPreAggCreateCommand extends CarbonQueryTest with BeforeAndAfterAll {

  val timeSeries = TIMESERIES.toString

  override def beforeAll {
    sql("drop database if exists otherDB cascade")
    sql("drop table if exists PreAggMain")
    sql("drop table if exists PreAggMain1")
    sql("drop table if exists maintable")
    sql("drop table if exists showTables")
    sql("drop table if exists Preagg_twodb")
    sql("create table preaggMain (a string, b string, c string) stored by 'carbondata'")
    sql("create table preaggMain1 (a string, b string, c string) stored by 'carbondata' tblProperties('DICTIONARY_INCLUDE' = 'a')")
    sql("create table maintable (column1 int, column6 string, column5 string, column2 string, column3 int, column4 int) stored by 'carbondata' tblproperties('dictionary_include'='column1,column6', 'dictionary_exclude'='column3,column5')")
  }

  test("test pre agg create table 1") {
    sql("create datamap preagg1 on table PreAggMain using 'preaggregate' as select a,sum(b) from PreAggMain group by a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain_preagg1"), true, "preaggmain_a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain_preagg1"), true, "preaggmain_b_sum")
    sql("drop datamap preagg1 on table PreAggMain")
  }

  test("test pre agg create table 2") {
    dropDataMaps("PreAggMain", "preagg2")
    sql("create datamap preagg2 on table PreAggMain using 'preaggregate' as select a as a1,sum(b) as udfsum from PreAggMain group by a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain_preagg2"), true, "preaggmain_a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain_preagg2"), true, "preaggmain_b_sum")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain_preagg2"), false, "preaggmain_a1")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain_preagg2"), false, "preaggmain_udfsum")
    sql("drop datamap preagg2 on table PreAggMain")
  }

  test("test pre agg create table 5") {
    sql("create datamap preagg11 on table PreAggMain1 using 'preaggregate'as select a,sum(b) from PreAggMain1 group by a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain1_preagg11"), true, "preaggmain1_a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain1_preagg11"), true, "preaggmain1_b_sum")
    sql("DESCRIBE FORMATTED PreAggMain1_preagg11").show(100, false)
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain1_preagg11"), true, "Dictionary")
    sql("drop datamap preagg11 on table PreAggMain1")
  }

  test("test pre agg create table 6") {
    sql("create datamap preagg12 on table PreAggMain1 using 'preaggregate' as select a as a1,sum(b) as sum from PreAggMain1 group by a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain1_preagg12"), true, "preaggmain1_a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain1_preagg12"), true, "preaggmain1_b_sum")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain1_preagg12"), false, "preaggmain1_a1")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain1_preagg12"), false, "preaggmain1_sum")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain1_preagg12"), true, "Dictionary")
    sql("drop datamap preagg12 on table PreAggMain1")
  }

  test("test pre agg create table 8") {
    sql("create datamap preagg14 on table PreAggMain1 using 'preaggregate' as select a as a1,sum(b) as sum from PreAggMain1 group by a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain1_preagg14"), true, "preaggmain1_a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain1_preagg14"), true, "preaggmain1_b_sum")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain1_preagg14"), false, "preaggmain1_a1")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain1_preagg14"), false, "preaggmain1_sum")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain1_preagg14"), true, "Dictionary")
    sql("drop datamap preagg14 on table PreAggMain1")
  }

  test("test pre agg create table 9") {
    sql("create datamap preagg15 on table PreAggMain using 'preaggregate' as select a,avg(b) from PreAggMain group by a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain_preagg15"), true, "preaggmain_a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain_preagg15"), true, "preaggmain_b_sum")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain_preagg15"), true, "preaggmain_b_count")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain_preagg15"), false, "preaggmain2_b_avg")
    sql("drop datamap preagg15 on table PreAggMain")
  }

  test("test pre agg create table 10") {
    sql("create datamap preagg16 on table PreAggMain using 'preaggregate' as select a as a1,max(b) from PreAggMain group by a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain_preagg16"), true, "preaggmain_a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain_preagg16"), true, "preaggmain_b_max")
    sql("drop datamap preagg16 on table PreAggMain")
  }

  test("test pre agg create table 11") {
    sql("create datamap preagg17 on table PreAggMain using 'preaggregate' as select a,min(b) from PreAggMain group by a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain_preagg17"), true, "preaggmain_a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain_preagg17"), true, "preaggmain_b_min")
    sql("drop datamap preagg17 on table PreAggMain")
  }

  test("test pre agg create table 12") {
    sql("create datamap preagg18 on table PreAggMain using 'preaggregate' as select a as a1,count(b) from PreAggMain group by a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain_preagg18"), true, "preaggmain_a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain_preagg18"), true, "preaggmain_b_count")
    sql("drop datamap preagg18 on table PreAggMain")
  }

  test("test pre agg create table 13") {
    val exception: Exception = intercept[MalformedCarbonCommandException] {
      sql(
        s"""
           | create datamap preagg19 on table PreAggMain
           | using 'preaggregate'
           | as select a as a1,count(distinct b)
           | from PreAggMain group by a
         """.stripMargin)
    }
    assert(exception.getMessage.equals("Distinct is not supported On Pre Aggregation"))
  }

  test("test pre agg create table 14") {
    val exception = intercept[MalformedCarbonCommandException] {
      sql(
        s"""
           | create datamap preagg20 on table PreAggMain
           | using 'preaggregate'
           | as select a as a1,sum(distinct b) from PreAggMain
           | group by a
         """.stripMargin)
    }
    assert(exception.getMessage.equals("Distinct is not supported On Pre Aggregation"))
  }

  test("test pre agg create table 15: don't support where") {
    intercept[Exception] {
      sql(
        s"""
           | create datamap preagg21 on table PreAggMain
           | using 'preaggregate'
           | as select a as a1,sum(b)
           | from PreAggMain
           | where a='vishal'
           | group by a
         """.stripMargin)
    }
  }

  test("test pre agg create table 16") {
    sql("create datamap agg0 on table mainTable using 'preaggregate' as select column4, sum(column4) from maintable group by column4")
    val df = sql("select * from maintable_agg0")
    val carbontable = getCarbonTable(df.queryExecution.analyzed)
    assert(carbontable.getAllMeasures.size()==2)
    assert(carbontable.getAllDimensions.size()==0)
    sql("drop datamap agg0 on table maintable")
  }

  test("test pre agg create table 17") {
    sql("create datamap agg0 on table mainTable using 'preaggregate' as select column1, sum(column1),column6, sum(column6) from maintable group by column6,column1")
    val df = sql("select * from maintable_agg0")
    val carbontable = getCarbonTable(df.queryExecution.analyzed)
    assert(carbontable.getAllMeasures.size()==2)
    assert(carbontable.getAllDimensions.size()==2)
    carbontable.getAllDimensions.asScala.foreach{ f =>
      assert(f.getEncoder.contains(Encoding.DICTIONARY))
    }
    sql("drop datamap agg0 on table maintable")
  }

  test("test pre agg create table 18") {
    sql("create datamap agg0 on table mainTable using 'preaggregate' as select column1, count(column1),column6, count(column6) from maintable group by column6,column1")
    val df = sql("select * from maintable_agg0")
    val carbontable = getCarbonTable(df.queryExecution.analyzed)
    assert(carbontable.getAllMeasures.size()==2)
    assert(carbontable.getAllDimensions.size()==2)
    carbontable.getAllDimensions.asScala.foreach{ f =>
      assert(f.getEncoder.contains(Encoding.DICTIONARY))
    }
    sql("drop datamap agg0 on table maintable")
  }

  test("test pre agg create table 19") {
    sql("create datamap agg0 on table mainTable using 'preaggregate' as select column3, sum(column3),column5, sum(column5) from maintable group by column3,column5")
    val df = sql("select * from maintable_agg0")
    val carbontable = getCarbonTable(df.queryExecution.analyzed)
    assert(carbontable.getAllMeasures.size()==2)
    assert(carbontable.getAllDimensions.size()==2)
    carbontable.getAllDimensions.asScala.foreach{ f =>
      assert(!f.getEncoder.contains(Encoding.DICTIONARY))
    }
    sql("drop datamap agg0 on table maintable")
  }

  test("test pre agg create table 20") {
    sql("create datamap agg0 on table mainTable using 'preaggregate' as select column3, sum(column3),column5, sum(column5) from maintable group by column3,column5,column2")
    val df = sql("select * from maintable_agg0")
    val carbontable = getCarbonTable(df.queryExecution.analyzed)
    assert(carbontable.getAllMeasures.size()==2)
    assert(carbontable.getAllDimensions.size()==3)
    carbontable.getAllDimensions.asScala.foreach{ f =>
      assert(!f.getEncoder.contains(Encoding.DICTIONARY))
    }
    sql("drop datamap agg0 on table maintable")
  }

  test("remove agg tables from show table command") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_SHOW_DATAMAPS,"false")
    sql("DROP TABLE IF EXISTS tbl_1")
    sql("DROP TABLE IF EXISTS sparktable")
    sql("create table if not exists  tbl_1(imei string,age int,mac string ,prodate timestamp,update timestamp,gamepoint double,contrid double) stored by 'carbondata' ")
    sql("create table if not exists sparktable(a int,b string)")
    sql(
      s"""create datamap preagg_sum on table tbl_1 using 'preaggregate' as select mac,avg(age) from tbl_1 group by mac"""
        .stripMargin)
    sql(
      "create datamap agg2 on table tbl_1 using 'preaggregate' as select prodate, mac from tbl_1 group by prodate,mac")
    checkExistence(sql("show tables"), false, "tbl_1_preagg_sum","tbl_1_agg2_day","tbl_1_agg2_hour","tbl_1_agg2_month","tbl_1_agg2_year")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_SHOW_DATAMAPS,CarbonCommonConstants.CARBON_SHOW_DATAMAPS_DEFAULT)
  }

  test("test pre agg create table 22: create with preaggregate and granularity") {
    sql("DROP TABLE IF EXISTS maintabletime")
    sql(
      """
        | CREATE TABLE maintabletime(year INT,month INT,name STRING,salary INT,dob STRING)
        | STORED BY 'carbondata'
        | TBLPROPERTIES(
        |   'SORT_SCOPE'='Global_sort',
        |   'TABLE_BLOCKSIZE'='23',
        |   'SORT_COLUMNS'='month,year,name')
      """.stripMargin)
    sql("INSERT INTO maintabletime SELECT 10,11,'x',12,'2014-01-01 00:00:00'")
    sql(
      s"""
         | CREATE DATAMAP agg0 ON TABLE maintabletime
         | USING 'preaggregate'
         | AS SELECT dob,name FROM maintabletime
         | GROUP BY dob,name
       """.stripMargin)
    val e = intercept[MalformedCarbonCommandException] {
      sql(
        s"""
           | CREATE DATAMAP agg1 ON TABLE maintabletime
           | USING 'preaggregate'
           | DMPROPERTIES (
           |  'EVENT_TIME'='dob',
           |  'SECOND_GRANULARITY'='1')
           | AS SELECT dob,name FROM maintabletime
           | GROUP BY dob,name
       """.stripMargin)
    }
    assert(e.getMessage.contains("Only 'path', 'partitioning' and 'long_string_columns' dmproperties "
      + "are allowed for this datamap"))
    sql("DROP TABLE IF EXISTS maintabletime")
  }

  test("test pre agg create table 22: using invalid datamap provider") {
    sql("DROP DATAMAP IF EXISTS agg0 ON TABLE maintable")

    val e = intercept[MalformedDataMapCommandException] {
      sql(
        """
          | CREATE DATAMAP agg0 ON TABLE mainTable
          | USING 'abc'
          | AS SELECT column3, SUM(column3),column5, SUM(column5)
          | FROM maintable
          | GROUP BY column3,column5,column2
        """.stripMargin)
    }
    assert(e.getMessage.contains(s"DataMap 'abc' not found"))
    sql("DROP DATAMAP IF EXISTS agg0 ON TABLE maintable")
  }

  test("test pre agg create table 24: remove agg tables from show table command") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_SHOW_DATAMAPS, "false")
    sql("DROP TABLE IF EXISTS tbl_1")
    sql("create table if not exists  tbl_1(imei string,age int,mac string ,prodate timestamp,update timestamp,gamepoint double,contrid double) stored by 'carbondata' ")
    sql("create datamap agg1 on table tbl_1 using 'preaggregate' as select mac, sum(age) from tbl_1 group by mac")
    sql("create table if not exists  sparktable(imei string,age int,mac string ,prodate timestamp,update timestamp,gamepoint double,contrid double) ")
    checkExistence(sql("show tables"), false, "tbl_1_agg1")
    checkExistence(sql("show tables"), true, "sparktable", "tbl_1")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_SHOW_DATAMAPS, CarbonCommonConstants.CARBON_SHOW_DATAMAPS_DEFAULT)
  }

  test("test pre agg create table 25: remove TimeSeries agg tables from show table command") {
    sql("DROP TABLE IF EXISTS tbl_1")
    sql("create table if not exists  tbl_1(imei string,age int,mac string ,prodate timestamp,update timestamp,gamepoint double,contrid double) stored by 'carbondata' ")
    sql(
      "create datamap agg2 on table tbl_1 using 'preaggregate' as select prodate, mac from tbl_1 group by prodate,mac")
    checkExistence(sql("show tables"), false, "tbl_1_agg2_day","tbl_1_agg2_hour","tbl_1_agg2_month","tbl_1_agg2_year")
  }

  test("test pre agg create table 21: should support 'if not exists'") {
    sql(
      """
        | CREATE DATAMAP IF NOT EXISTS agg0 ON TABLE mainTable
        | USING 'preaggregate'
        | AS SELECT
        |   column3,
        |   sum(column3),
        |   column5,
        |   sum(column5)
        | FROM maintable
        | GROUP BY column3,column5,column2
      """.stripMargin)
    sql("DROP DATAMAP IF EXISTS agg0 ON TABLE maintable")
  }

  test("test pre agg create table 22: don't support create datamap if exists'") {
    val e: Exception = intercept[AnalysisException] {
      sql(
        """
          | CREATE DATAMAP IF EXISTS agg0 ON TABLE mainTable
          | USING 'preaggregate'
          | AS SELECT
          |   column3,
          |   sum(column3),
          |   column5,
          |   sum(column5)
          | FROM maintable
          | GROUP BY column3,column5,column2
        """.stripMargin)
      assert(true)
    }
    assert(e.getMessage.contains("identifier matching regex"))
    sql("DROP DATAMAP IF EXISTS agg0 ON TABLE maintable")
  }

  test("test show tables filtered with datamaps") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_SHOW_DATAMAPS,"false")
    sql("create datamap preagg1 on table PreAggMain using 'preaggregate' as select a,sum(b) from PreAggMain group by a")
    sql("show tables").show()
    checkExistence(sql("show tables"), false, "preaggmain_preagg1")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_SHOW_DATAMAPS,CarbonCommonConstants.CARBON_SHOW_DATAMAPS_DEFAULT)
    checkExistence(sql("show tables"), true, "preaggmain_preagg1")
  }

  test("test create main and preagg table of same name in two database") {
    sql("drop table if exists Preagg_twodb")
    sql("create table Preagg_twodb(name string, age int) stored by 'carbondata'")
    sql("create datamap sameName on table Preagg_twodb using 'preaggregate' as select sum(age) from Preagg_twodb")
    sql("create database otherDB")
    sql("use otherDB")
    sql("drop table if exists Preagg_twodb")
    sql("create table Preagg_twodb(name string, age int) stored by 'carbondata'")
    try {
      sql(
        "create datamap sameName on table Preagg_twodb using 'preaggregate' as select sum(age) from Preagg_twodb")
      assert(true)
    } catch {
      case ex: Exception =>
        assert(false)
    }
    sql("use default")
  }

  // TODO: to be confirmed
  test("test pre agg create table 26") {
    sql("drop datamap if exists preagg2 on table PreAggMain")
    sql("create datamap preagg2 on table PreAggMain using 'preaggregate' as select a as a1,sum(b) from PreAggMain group by a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain_preagg2"), true, "preaggmain_a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain_preagg2"), true, "preaggmain_b_sum")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain_preagg2"), false, "preaggmain_a1")
    intercept[Exception] {
      sql("select a1 from PreAggMain_preagg2").show()
    }
    sql("drop datamap if exists preagg2 on table PreAggMain")
  }

  test("test pre agg create table 27: select * and no group by") {
    intercept[Exception] {
      sql(
        """
          | CREATE DATAMAP IF NOT EXISTS agg0 ON TABLE mainTable
          | USING 'preaggregate'
          | AS SELECT *  FROM maintable
        """.stripMargin)
    }
  }

  // TODO : to be confirmed
  test("test pre agg create table 28: select *") {
    intercept[Exception] {
      sql(
        """
          | CREATE DATAMAP IF NOT EXISTS agg0 ON TABLE mainTable
          | USING 'preaggregate'
          | AS SELECT *  FROM maintable
          | group by a
        """.stripMargin)
    }
  }

  test("test pre agg create table 29") {
    intercept[Exception] {
      sql(
        s"""
           | create datamap preagg21 on table PreAggMain2
           | using 'preaggregate'
           | as select a as a1,sum(b)
           | from PreAggMain2
           | where a>'vishal'
           | group by a
         """.stripMargin)
    }
  }

  test("test pre agg create table 30: DESCRIBE FORMATTED") {
    dropDataMaps("PreAggMain", "preagg2")
    intercept[Exception] {
      sql("DESCRIBE FORMATTED PreAggMain_preagg2").show()
    }
  }

  test("test codegen issue with preaggregate") {
    sql("DROP TABLE IF EXISTS PreAggMain")
    sql("CREATE TABLE PreAggMain (id Int, date date, country string, phonetype string, " +
        "serialname String,salary int ) STORED BY 'org.apache.carbondata.format' " +
        "tblproperties('dictionary_include'='country')")
    sql("create datamap PreAggSum on table PreAggMain using 'preaggregate' as " +
        "select country,sum(salary) as sum from PreAggMain group by country")
    sql("create datamap PreAggAvg on table PreAggMain using 'preaggregate' as " +
        "select country,avg(salary) as avg from PreAggMain group by country")
    sql("create datamap PreAggCount on table PreAggMain using 'preaggregate' as " +
        "select country,count(salary) as count from PreAggMain group by country")
    sql("create datamap PreAggMin on table PreAggMain using 'preaggregate' as " +
        "select country,min(salary) as min from PreAggMain group by country")
    sql("create datamap PreAggMax on table PreAggMain using 'preaggregate' as " +
        "select country,max(salary) as max from PreAggMain group by country")
    sql(s"LOAD DATA INPATH '$integrationPath/spark-common-test/src/test/resources/source.csv' " +
        s"into table PreAggMain")
    checkExistence(sql("select t1.country,sum(id) from PreAggMain t1 join (select " +
                       "country as newcountry,sum(salary) as sum from PreAggMain group by country)" +
                       "t2 on t1.country=t2.newcountry group by country"), true, "france")
    sql("DROP TABLE IF EXISTS PreAggMain")
  }

  // TODO: Need to Fix
  ignore("test creation of multiple preaggregate of same name concurrently") {
    sql("DROP TABLE IF EXISTS tbl_concurr")
    sql(
      "create table if not exists  tbl_concurr(imei string,age int,mac string ,prodate timestamp," +
      "update timestamp,gamepoint double,contrid double) stored by 'carbondata' ")

    var executorService: ExecutorService = Executors.newCachedThreadPool()
    val tasks = new util.ArrayList[Callable[String]]()
    var i = 0
    val count = 5
    while (i < count) {
      tasks
        .add(new QueryTask(
          s"""create datamap agg_concu1 on table tbl_concurr using
             |'preaggregate' as select prodate, mac from tbl_concurr group by prodate,mac"""
            .stripMargin))
      i = i + 1
    }
    executorService.invokeAll(tasks).asScala
    executorService.awaitTermination(5, TimeUnit.MINUTES)
    checkExistence(sql("show tables"), true, "agg_concu1", "tbl_concurr")
    executorService.shutdown()
  }

  class QueryTask(query: String) extends Callable[String] {
    override def call(): String = {
      var result = "SUCCESS"
      try {
        sql(query).collect()
      } catch {
        case exception: Exception => LOGGER.error(exception.getMessage)
      }
      result
    }
  }


  def getCarbonTable(plan: LogicalPlan) : CarbonTable ={
    var carbonTable : CarbonTable = null
    plan.transform {
      // first check if any preaTable1 scala function is applied it is present is in plan
      // then call is from create preaTable1regate table class so no need to transform the query plan
      case ca:CarbonRelation =>
        if (ca.isInstanceOf[CarbonDatasourceHadoopRelation]) {
          val relation = ca.asInstanceOf[CarbonDatasourceHadoopRelation]
          carbonTable = relation.carbonTable
        }
        ca
      case logicalRelation:LogicalRelation =>
        if(logicalRelation.relation.isInstanceOf[CarbonDatasourceHadoopRelation]) {
          val relation = logicalRelation.relation.asInstanceOf[CarbonDatasourceHadoopRelation]
          carbonTable = relation.carbonTable
        }
        logicalRelation
    }
    carbonTable
  }

  override def afterAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_SHOW_DATAMAPS,
        CarbonCommonConstants.CARBON_SHOW_DATAMAPS_DEFAULT)
    sql("drop database if exists otherDB cascade")
    sql("drop table if exists maintable")
    sql("drop table if exists PreAggMain")
    sql("drop table if exists PreAggMain1")
    sql("drop table if exists maintabletime")
    sql("drop table if exists showTables")
    sql("drop table if exists Preagg_twodb")
    sql("DROP TABLE IF EXISTS tbl_1")
    sql("DROP TABLE IF EXISTS sparktable")
  }
}
