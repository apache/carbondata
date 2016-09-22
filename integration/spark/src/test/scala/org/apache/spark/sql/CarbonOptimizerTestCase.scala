package org.apache.spark.sql

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.scalatest.BeforeAndAfterAll

/**
  * Created by c00318382 on 2016/9/18.
  */
class CarbonOptimizerTestCase extends QueryTest with BeforeAndAfterAll {
  override def beforeAll: Unit = {
    sql("drop table if exists carbonTable")
    sql("drop table if exists hiveTable")
    sql(
      """CREATE TABLE carbonTable (empno int, empname String, designation String, doj String,
        workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        projectcode int, projectjoindate String, projectenddate String,attendance double,
        utilization double,salary double) STORED BY 'org.apache.carbondata.format'""")
    sql(
      """CREATE TABLE hiveTable (empno int, empname String, designation String, doj String,
        workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        projectcode int, projectjoindate String, projectenddate String,attendance double,
        utilization double,salary double) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""")
  }

  def checkPlan(sqlString: String): Unit = {
    val plan = sql(sqlString).queryExecution.optimizedPlan
    println(plan)
    // CarbonDictionaryCatalystDecoder need contain a carbon relation child
    var hasCarbon = true
    plan transformDown {
      case decoder: CarbonDictionaryCatalystDecoder =>
        val hasCarbonRelation = decoder.collectFirst {
          case l: LogicalRelation if l.relation.isInstanceOf[CarbonDatasourceRelation] => l
        } match {
          case Some(_) => true
          case None => false
        }
        if (!hasCarbonRelation) {
          hasCarbon = false
        }
        decoder
    }
    assert(hasCarbon)
  }

  test("Carbon table union Hive table") {
    checkPlan("""select empname from carbonTable
         union
        select empname from hiveTable""")
  }

  test("Hive table union Carbon table") {
    checkPlan("""select empname from hiveTable
         union
        select empname from carbonTable""")
  }

  test("Carbon table join Hive table") {
    checkPlan(
      """select ct1.empname, ct1.designation
         from carbonTable ct1
         join hiveTable ht1 on ct1.empname = ht1.empname """)
  }

  test("Hive table join Carbon table") {
    checkPlan(
      """select ct1.empname, ct1.designation
         from hiveTable ht1
         join carbonTable ct1 on ct1.empname = ht1.empname """)
  }

  override def afterAll: Unit = {
    sql("drop table if exists carbonTable")
    sql("drop table if exists hiveTable")
  }
}
