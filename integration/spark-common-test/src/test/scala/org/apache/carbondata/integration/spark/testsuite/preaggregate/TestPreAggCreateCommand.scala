package org.apache.carbondata.integration.spark.testsuite.preaggregate

import org.apache.spark.sql.CarbonDatasourceHadoopRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll
import scala.collection.JavaConverters._

import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.table.CarbonTable

class TestPreAggCreateCommand extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table if exists PreAggMain")
    sql("drop table if exists PreAggMain1")
    sql("drop table if exists PreAggMain2")
    sql("drop table if exists maintable")
    sql("create table preaggMain (a string, b string, c string) stored by 'carbondata'")
    sql("create table preaggMain1 (a string, b string, c string) stored by 'carbondata' tblProperties('DICTIONARY_INCLUDE' = 'a')")
    sql("create table preaggMain2 (a string, b string, c string) stored by 'carbondata'")
    sql("create table maintable (column1 int, column6 string, column5 string, column2 string, column3 int, column4 int) stored by 'carbondata' tblproperties('dictionary_include'='column1,column6', 'dictionary_exclude'='column3,column5')")

  }


  test("test pre agg create table 1") {
    sql("create datamap preagg1 on table PreAggMain using 'preaggregate' as select a,sum(b) from PreAggMain group by a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain_preagg1"), true, "preaggmain_a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain_preagg1"), true, "preaggmain_b_sum")
    sql("drop datamap preagg1 on table PreAggMain")
  }

  test("test pre agg create table 2") {
    sql("create datamap preagg2 on table PreAggMain using 'preaggregate' as select a as a1,sum(b) from PreAggMain group by a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain_preagg2"), true, "preaggmain_a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain_preagg2"), true, "preaggmain_b_sum")
    sql("drop datamap preagg2 on table PreAggMain")
  }

  test("test pre agg create table 3") {
    sql("create datamap preagg3 on table PreAggMain using 'preaggregate' as select a,sum(b) as sum from PreAggMain group by a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain_preagg3"), true, "preaggmain_a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain_preagg3"), true, "preaggmain_b_sum")
    sql("drop datamap preagg3 on table PreAggMain")
  }

  test("test pre agg create table 4") {
    sql("create datamap preagg4 on table PreAggMain using 'preaggregate' as select a as a1,sum(b) as sum from PreAggMain group by a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain_preagg4"), true, "preaggmain_a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain_preagg4"), true, "preaggmain_b_sum")
    sql("drop datamap preagg4 on table PreAggMain")
  }


  test("test pre agg create table 5") {
    sql("create datamap preagg11 on table PreAggMain1 using 'preaggregate'as select a,sum(b) from PreAggMain1 group by a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain1_preagg11"), true, "preaggmain1_a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain1_preagg11"), true, "preaggmain1_b_sum")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain1_preagg11"), true, "DICTIONARY")
    sql("drop datamap preagg11 on table PreAggMain1")
  }

  test("test pre agg create table 6") {
    sql("create datamap preagg12 on table PreAggMain1 using 'preaggregate' as select a as a1,sum(b) from PreAggMain1 group by a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain1_preagg12"), true, "preaggmain1_a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain1_preagg12"), true, "preaggmain1_b_sum")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain1_preagg12"), true, "DICTIONARY")
    sql("drop datamap preagg12 on table PreAggMain1")
  }

  test("test pre agg create table 7") {
    sql("create datamap preagg13 on table PreAggMain1 using 'preaggregate' as select a,sum(b) as sum from PreAggMain1 group by a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain1_preagg13"), true, "preaggmain1_a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain1_preagg13"), true, "preaggmain1_b_sum")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain1_preagg13"), true, "DICTIONARY")
    sql("drop datamap preagg13 on table PreAggMain1")
  }

  test("test pre agg create table 8") {
    sql("create datamap preagg14 on table PreAggMain1 using 'preaggregate' as select a as a1,sum(b) as sum from PreAggMain1 group by a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain1_preagg14"), true, "preaggmain1_a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain1_preagg14"), true, "preaggmain1_b_sum")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain1_preagg14"), true, "DICTIONARY")
    sql("drop datamap preagg14 on table PreAggMain1")
  }


  test("test pre agg create table 9") {
    sql("create datamap preagg15 on table PreAggMain2 using 'preaggregate' as select a,avg(b) from PreAggMain2 group by a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain2_preagg15"), true, "preaggmain2_a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain2_preagg15"), true, "preaggmain2_b_sum")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain2_preagg15"), true, "preaggmain2_b_count")
    sql("drop datamap preagg15 on table PreAggMain2")
  }

  test("test pre agg create table 10") {
    sql("create datamap preagg16 on table PreAggMain2 using 'preaggregate' as select a as a1,max(b) from PreAggMain2 group by a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain2_preagg16"), true, "preaggmain2_a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain2_preagg16"), true, "preaggmain2_b_max")
    sql("drop datamap preagg16 on table PreAggMain2")
  }

  test("test pre agg create table 11") {
    sql("create datamap preagg17 on table PreAggMain2 using 'preaggregate' as select a,min(b) from PreAggMain2 group by a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain2_preagg17"), true, "preaggmain2_a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain2_preagg17"), true, "preaggmain2_b_min")
    sql("drop datamap preagg17 on table PreAggMain2")
  }

  test("test pre agg create table 12") {
    sql("create datamap preagg18 on table PreAggMain2 using 'preaggregate' as select a as a1,count(b) from PreAggMain2 group by a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain2_preagg18"), true, "preaggmain2_a")
    checkExistence(sql("DESCRIBE FORMATTED PreAggMain2_preagg18"), true, "preaggmain2_b_count")
    sql("drop datamap preagg18 on table PreAggMain2")
  }

  test("test pre agg create table 13") {
    try {
      sql(
        "create datamap preagg19 on table PreAggMain2 using 'preaggregate' as select a as a1,count(distinct b) from PreAggMain2 group by a")
      assert(false)
    } catch {
      case _: Exception =>
        assert(true)
    }
  }

  test("test pre agg create table 14") {
    try {
      sql(
        "create datamap preagg20 on table PreAggMain2 using 'preaggregate' as select a as a1,sum(distinct b) from PreAggMain2 group by a")
      assert(false)
    } catch {
      case _: Exception =>
        assert(true)
    }
  }

  test("test pre agg create table 15") {
    try {
      sql(
        "create datamap preagg21 on table PreAggMain2 using 'preaggregate' as select a as a1,sum(b) from PreAggMain2 where a='vishal' group by a")
      assert(false)
    } catch {
      case _: Exception =>
        assert(true)
    }
  }

  test("test pre agg create table 16") {
    sql("create datamap agg0 on table mainTable using 'preaggregate' as select column4, sum(column4) from maintable group by column4")
    val df = sql("select * from maintable_agg0")
    val carbontable = getCarbontable(df.queryExecution.analyzed)
    assert(carbontable.getAllMeasures.size()==2)
    assert(carbontable.getAllDimensions.size()==0)
    sql("drop datamap agg0 on table maintable")
  }

  test("test pre agg create table 17") {
    sql("create datamap agg0 on table mainTable using 'preaggregate' as select column1, sum(column1),column6, sum(column6) from maintable group by column6,column1")
    val df = sql("select * from maintable_agg0")
    val carbontable = getCarbontable(df.queryExecution.analyzed)
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
    val carbontable = getCarbontable(df.queryExecution.analyzed)
    assert(carbontable.getAllMeasures.size()==1)
    assert(carbontable.getAllDimensions.size()==4)
    carbontable.getAllDimensions.asScala.foreach{ f =>
      assert(f.getEncoder.contains(Encoding.DICTIONARY))
    }
    sql("drop datamap agg0 on table maintable")
  }

  test("test pre agg create table 19") {
    sql("create datamap agg0 on table mainTable using 'preaggregate' as select column3, sum(column3),column5, sum(column5) from maintable group by column3,column5")
    val df = sql("select * from maintable_agg0")
    val carbontable = getCarbontable(df.queryExecution.analyzed)
    assert(carbontable.getAllMeasures.size()==2)
    assert(carbontable.getAllDimensions.size()==2)
    carbontable.getAllDimensions.asScala.foreach{ f =>
      assert(!f.getEncoder.contains(Encoding.DICTIONARY))
    }
    sql("drop datamap agg0 on table maintable")
  }


  def getCarbontable(plan: LogicalPlan) : CarbonTable ={
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
    sql("drop table if exists maintable")
    sql("drop table if exists PreAggMain")
    sql("drop table if exists PreAggMain1")
    sql("drop table if exists PreAggMain2")
  }
}
