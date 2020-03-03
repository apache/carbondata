package org.apache.spark.sql.parser

import org.apache.spark.sql.catalyst.parser.SqlBaseParser._
import org.apache.spark.sql.execution.SparkSqlAstBuilder
import org.apache.spark.sql.internal.SQLConf

class SparkSqlAstBuilderWrapper(conf: SQLConf)
  extends SparkSqlAstBuilder(conf) {

  def visitPropertyKeyValues(ctx: TablePropertyListContext): Map[String, String] = ???
}
