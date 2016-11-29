package org.apache.spark.util

import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.execution.command.DeleteLoadsByLoadDate

/**
 * delete segments before some date
 */
object DeleteSegmentByDate {

  def deleteSegmentByDate(spark: SparkSession, dbName: Option[String], tableName: String,
      dateValue: String): Unit = {
    DeleteLoadsByLoadDate(dbName, tableName, "", dateValue).run(spark)
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println(
        "Usage: TableDeleteSegmentByDate <store path> <table name> <before date value>");
      System.exit(1)
    }

    val storePath = TableAPIUtil.escape(args(0))
    val (dbName, tableName) = TableAPIUtil.parseSchemaName(TableAPIUtil.escape(args(1)))
    val dateValue = TableAPIUtil.escape(args(2))
    val spark = TableAPIUtil.spark(storePath, s"TableCleanFiles: $dbName.$tableName")
    CarbonEnv.init(spark.sqlContext)
    deleteSegmentByDate(spark, Option(dbName), tableName, dateValue)
  }
}
