package org.apache.spark.sql

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.types.StructType

object MixedFormatHandlerUtil {

  def getScanForSegments(
      @transient relation: HadoopFsRelation,
      output: Seq[Attribute],
      outputSchema: StructType,
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression],
      tableIdentifier: Option[TableIdentifier]
  ): FileSourceScanExec = {
    FileSourceScanExec(
      relation,
      output,
      outputSchema,
      partitionFilters,
      dataFilters,
      tableIdentifier)
  }
}
