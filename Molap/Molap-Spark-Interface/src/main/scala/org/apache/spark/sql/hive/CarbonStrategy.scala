package org.apache.spark.sql.hive


import org.apache.spark.sql.catalyst.ParserDialect
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.DDLException
import org.apache.spark.sql.{MolapSqlDDLParser, SQLContext, Strategy}

/**
  * @author V71149
  */
private[sql] object CarbonStrategy {
  def getStrategy(context: SQLContext): Strategy = {
    new OlapStrategies(context).OlapCubeScans
  }
}

private[spark] class CarbonSQLDialect extends HiveQLDialect {

  @transient
  protected val sqlParser = new MolapSqlDDLParser

  override def parse(sqlText: String): LogicalPlan = {

    try {
      sqlParser.parse(sqlText)
    } catch {
      //case ddlException: DDLException => throw ddlException
      case _ => super.parse(sqlText)
      case x: Throwable => throw x
    }
  }
}