package org.apache.spark.sql

import org.apache.spark.sql.execution.strategy.DDLStrategy
import org.apache.spark.sql.parser.CarbonExtensionSqlParser
import org.apache.spark.sql.test.util.PlanTest
import org.scalatest.BeforeAndAfterAll

class CarbonExtensionSuite extends PlanTest with BeforeAndAfterAll {

  var session: SparkSession = null

  val sparkCommands = Array("select 2 > 1")

  val carbonCommands = Array("show STREAMS")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    session = SparkSession
      .builder()
      .appName("parserApp")
      .master("local")
      .config("spark.sql.extensions", "org.apache.spark.sql.CarbonExtensions")
      .getOrCreate()
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
