package org.apache.carbondata.integration.spark.testsuite.preaggregate

import org.apache.spark.sql.CarbonDatasourceHadoopRelation
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Union}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, Ignore}


@Ignore
class TestPreAggStreaming extends QueryTest with BeforeAndAfterAll {
  override def beforeAll: Unit = {
    sql("drop table if exists mainTable")
    sql("CREATE TABLE mainTable(id int, name string, city string, age string) STORED BY 'org.apache.carbondata.format'")
    sql("create datamap agg0 on table mainTable using 'preaggregate' as select name from mainTable group by name")
    sql("create datamap agg1 on table mainTable using 'preaggregate' as select name,sum(age) from mainTable group by name")
    sql("create datamap agg2 on table mainTable using 'preaggregate' as select name,avg(age) from mainTable group by name")
    sql("create datamap agg3 on table mainTable using 'preaggregate' as select name,sum(CASE WHEN age=35 THEN id ELSE 0 END) from mainTable group by name")
  }

  test("Test Streaming table plan with only projection column") {
    val df = sql("select name from maintable group by name")
    assert(validateStreamingTablePlan(df.queryExecution.analyzed))
  }

  test("Test Streaming table plan with only projection column and order by") {
    val df = sql("select name from maintable group by name")
    assert(validateStreamingTablePlan(df.queryExecution.analyzed))
  }

  test("Test Streaming table plan with sum aggregator") {
    val df = sql("select name, sum(age) from maintable group by name order by name")
    assert(validateStreamingTablePlan(df.queryExecution.analyzed))
  }

  test("Test Streaming table plan with sum aggregator and order by") {
    val df = sql("select name, sum(age) from maintable group by name order by name")
    assert(validateStreamingTablePlan(df.queryExecution.analyzed))
  }

  test("Test Streaming table plan with avg aggregator") {
    val df = sql("select name, avg(age) from maintable group by name")
    assert(validateStreamingTablePlan(df.queryExecution.analyzed))
  }

  test("Test Streaming table plan with expression ") {
    val df = sql("select name, sum(CASE WHEN age=35 THEN id ELSE 0 END) from maintable group by name order by name")
    assert(validateStreamingTablePlan(df.queryExecution.analyzed))
  }

  /**
   * Below method will be used validate whether plan is already updated in case of streaming table
   * In case of streaming table it will add UnionNode to get the data from fact and aggregate both
   * as aggregate table will be updated after each handoff.
   * So if plan is already updated no need to transform the plan again
   * @param logicalPlan
   * query plan
   * @return whether need to update the query plan or not
   */
  def validateStreamingTablePlan(logicalPlan: LogicalPlan) : Boolean = {
    var isChildTableExists: Boolean = false
    logicalPlan.transform {
      case union @ Union(Seq(plan1, plan2)) =>
        plan2.collect{
          case logicalRelation: LogicalRelation if
          logicalRelation.relation.isInstanceOf[CarbonDatasourceHadoopRelation] &&
          logicalRelation.relation.asInstanceOf[CarbonDatasourceHadoopRelation].carbonTable
            .isChildDataMap =>
            isChildTableExists = false
        }
        union
    }
    isChildTableExists
  }

  override def afterAll: Unit = {
    sql("drop table if exists mainTable")
  }
}
