package org.apache.spark.util

import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.execution.command.DeleteLoadsById

/**
 * delete segments by id list
 */
object DeleteSegmentById {

  def extractSegmentIds(segmentIds: String): Seq[String] = {
    segmentIds.split(",").toSeq
  }

  def deleteSegmentById(spark: SparkSession, dbName: Option[String], tableName: String,
      segmentIds: Seq[String]): Unit = {
    DeleteLoadsById(segmentIds, dbName, tableName).run(spark)
  }

  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      System.err.println(
        "Usage: TableDeleteSegmentByID <store path> <table name> <segment id list>");
      System.exit(1)
    }

    val storePath = TableAPIUtil.escape(args(0))
    val (dbName, tableName) = TableAPIUtil.parseSchemaName(TableAPIUtil.escape(args(1)))
    val segmentIds = extractSegmentIds(TableAPIUtil.escape(args(2)))
    val spark = TableAPIUtil.spark(storePath, s"TableDeleteSegmentById: $dbName.$tableName")
    CarbonEnv.init(spark.sqlContext)
    deleteSegmentById(spark, Option(dbName), tableName, segmentIds)
  }
}
