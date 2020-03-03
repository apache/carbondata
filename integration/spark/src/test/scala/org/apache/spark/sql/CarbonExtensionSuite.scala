package org.apache.spark.sql

import org.apache.spark.sql.execution.strategy.DDLStrategy
import org.apache.spark.sql.parser.CarbonExtensionSqlParser
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class CarbonExtensionSuite extends QueryTest with BeforeAndAfterAll {

  var session: SparkSession = null

  val sparkCommands = Array("select 2 > 1")

  val carbonCommands = Array("show STREAMS")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    session = sqlContext.sparkSession
  }

  test("test parser injection") {
    assert(session.sessionState.sqlParser.isInstanceOf[CarbonExtensionSqlParser])
    (carbonCommands ++ sparkCommands) foreach (command =>
      session.sql(command).show)
  }

  test("test strategy injection") {
    assert(session.sessionState.planner.strategies.filter(_.isInstanceOf[DDLStrategy]).length == 1)
    session.sql("create table if not exists table1 (column1 String) using carbondata ").show
  }
}
