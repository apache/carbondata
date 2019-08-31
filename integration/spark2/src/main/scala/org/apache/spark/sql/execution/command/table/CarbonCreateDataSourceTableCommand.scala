package org.apache.spark.sql.execution.command.table

import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.execution.command.CreateDataSourceTableCommand
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.execution.datasources.{DataSource, HadoopFsRelation}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType

class CarbonCreateDataSourceTableCommand( override val table: CatalogTable,
                                               override val ignoreIfExists: Boolean)
  extends CreateDataSourceTableCommand(table, ignoreIfExists) {

  override def run(sparkSession: SparkSession): Seq[Row] = {

    assert(table.tableType != CatalogTableType.VIEW)
    assert(table.provider.isDefined)

    val sessionState = sparkSession.sessionState
    if (sessionState.catalog.tableExists(table.identifier)) {
      if (ignoreIfExists) {
        return Seq.empty[Row]
      } else {
        throw new AnalysisException(s"Table ${table.identifier.unquotedString} already exists.")
      }
    }

    // Create the relation to validate the arguments before writing the metadata to the metastore,
    // and infer the table schema and partition if users didn't specify schema in CREATE TABLE.
    val pathOption = table.storage.locationUri.map("path" -> CatalogUtils.URIToString(_))
    // Fill in some default table options from the session conf
    val tableWithDefaultOptions = table.copy(
      identifier = table.identifier.copy(
        database = Some(
          table.identifier.database.getOrElse(sessionState.catalog.getCurrentDatabase))),
      tracksPartitionsInCatalog = sessionState.conf.manageFilesourcePartitions)
    val dataSource: BaseRelation =
      DataSource(
        sparkSession = sparkSession,
        userSpecifiedSchema = if (table.schema.isEmpty) None else Some(table.schema),
        partitionColumns = table.partitionColumnNames,
        className = table.provider.get,
        bucketSpec = table.bucketSpec,
        options = table.storage.properties ++ pathOption,
        // As discussed in SPARK-19583, we don't check if the location is existed
        catalogTable = Some(tableWithDefaultOptions)).resolveRelation(checkFilesExist = false)

    val partitionColumnNames = if (table.schema.nonEmpty) {
      table.partitionColumnNames
    } else {
      // This is guaranteed in `PreprocessDDL`.
      assert(table.partitionColumnNames.isEmpty)
      dataSource match {
        case r: HadoopFsRelation => r.partitionSchema.fieldNames.toSeq
        case _ => Nil
      }
    }

    val newTable = dataSource match {
      // Since Spark 2.1, we store the inferred schema of data source in metastore, to avoid
      // inferring the schema again at read path. However if the data source has overlapped columns
      // between data and partition schema, we can't store it in metastore as it breaks the
      // assumption of table schema. Here we fallback to the behavior of Spark prior to 2.1, store
      // empty schema in metastore and infer it at runtime. Note that this also means the new
      // scalable partitioning handling feature(introduced at Spark 2.1) is disabled in this case.
      case r: HadoopFsRelation if r.overlappedPartCols.nonEmpty =>
        logWarning("It is not recommended to create a table with overlapped data and partition " +
          "columns, as Spark cannot store a valid table schema and has to infer it at runtime, " +
          "which hurts performance. Please check your data files and remove the partition " +
          "columns in it.")
        table.copy(schema = new StructType(), partitionColumnNames = Nil)

      case _ =>
        table.copy(
          schema = dataSource.schema,
          partitionColumnNames = partitionColumnNames,
          // If metastore partition management for file source tables is enabled, we start off with
          // partition provider hive, but no partitions in the metastore. The user has to call
          // `msck repair table` to populate the table partitions.
          tracksPartitionsInCatalog = partitionColumnNames.nonEmpty && false)

    }

    // We will return Nil or throw exception at the beginning if the table already exists, so when
    // we reach here, the table should not exist and we should set `ignoreIfExists` to false.
    sessionState.catalog.createTable(newTable, ignoreIfExists = false)

    Seq.empty[Row]
  }

}
