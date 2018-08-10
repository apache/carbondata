
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.hive

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.catalog.{
  CatalogRelation, CatalogTable, CatalogTableType,
  SimpleCatalogRelation
}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.{
  AlterTableRecoverPartitionsCommand, DDLUtils,
  RunnableCommand
}
import org.apache.spark.sql.execution.datasources.{DataSource, HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.sources.InsertableRelation
import org.apache.spark.sql.types.StructType

/**
 * Create table 'using carbondata' and insert the query result into it.
 * @param table the Catalog Table
 * @param mode  SaveMode:Ignore,OverWrite,ErrorIfExists,Append
 * @param query the query whose result will be insert into the new relation
 */

case class CreateCarbonSourceTableAsSelectCommand(
    table: CatalogTable,
    mode: SaveMode,
    query: LogicalPlan)
  extends RunnableCommand {

  override protected def innerChildren: Seq[LogicalPlan] = Seq(query)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    assert(table.tableType != CatalogTableType.VIEW)
    assert(table.provider.isDefined)
    assert(table.schema.isEmpty)

    val provider = table.provider.get
    val sessionState = sparkSession.sessionState
    val db = table.identifier.database.getOrElse(sessionState.catalog.getCurrentDatabase)
    val tableIdentWithDB = table.identifier.copy(database = Some(db))
    val tableName = tableIdentWithDB.unquotedString

    var createMetastoreTable = false
    var existingSchema = Option.empty[StructType]
    if (sparkSession.sessionState.catalog.tableExists(tableIdentWithDB)) {
      // Check if we need to throw an exception or just return.
      mode match {
        case SaveMode.ErrorIfExists =>
          throw new AnalysisException(s"Table $tableName already exists. " +
                                      s"If you are using saveAsTable, you can set SaveMode to " +
                                      s"SaveMode.Append to " +
                                      s"insert data into the table or set SaveMode to SaveMode" +
                                      s".Overwrite to overwrite" +
                                      s"the existing data. " +
                                      s"Or, if you are using SQL CREATE TABLE, you need to drop " +
                                      s"$tableName first.")
        case SaveMode.Ignore =>
          // Since the table already exists and the save mode is Ignore, we will just return.
          return Seq.empty[Row]
        case SaveMode.Append =>
          // Check if the specified data source match the data source of the existing table.
          val existingProvider = DataSource.lookupDataSource(provider)
          // TODO: Check that options from the resolved relation match the relation that we are
          // inserting into (i.e. using the same compression).

          // Pass a table identifier with database part, so that `lookupRelation` won't get temp
          // views unexpectedly.
          EliminateSubqueryAliases(sessionState.catalog.lookupRelation(tableIdentWithDB)) match {
            case l@LogicalRelation(_: InsertableRelation | _: HadoopFsRelation, _, _) =>
              // check if the file formats match
              l.relation match {
                case r: HadoopFsRelation if r.fileFormat.getClass != existingProvider =>
                  throw new AnalysisException(
                    s"The file format of the existing table $tableName is " +
                    s"`${ r.fileFormat.getClass.getName }`. It doesn't match the specified " +
                    s"format `$provider`")
                case _ =>
              }
              if (query.schema.size != l.schema.size) {
                throw new AnalysisException(
                  s"The column number of the existing schema[${ l.schema }] " +
                  s"doesn't match the data schema[${ query.schema }]'s")
              }
              existingSchema = Some(l.schema)
            case s: SimpleCatalogRelation if DDLUtils.isDatasourceTable(s.metadata) =>
              existingSchema = Some(s.metadata.schema)
            case c: CatalogRelation if c.catalogTable.provider == Some(DDLUtils.HIVE_PROVIDER) =>
              throw new AnalysisException("Saving data in the Hive serde table " +
                                          s"${ c.catalogTable.identifier } is not supported yet. " +
                                          s"Please use the insertInto() API as an alternative..")
            case o =>
              throw new AnalysisException(s"Saving data in ${ o.toString } is not supported.")
          }
        case SaveMode.Overwrite =>
          sessionState.catalog.dropTable(tableIdentWithDB, ignoreIfNotExists = true, purge = false)
          // Need to create the table again.
          createMetastoreTable = true
      }
    } else {
      // The table does not exist. We need to create it in metastore.
      createMetastoreTable = true
    }

    val data = Dataset.ofRows(sparkSession, query)
    val df = existingSchema match {
      // If we are inserting into an existing table, just use the existing schema.
      case Some(s) => data.selectExpr(s.fieldNames: _*)
      case None => data
    }

    val tableLocation = if (table.tableType == CatalogTableType.MANAGED) {
      Some(sessionState.catalog.defaultTablePath(table.identifier))
    } else {
      table.storage.locationUri
    }

    // Create the relation based on the data of df.
    val pathOption = tableLocation.map("path" -> _)
    val dataSource = DataSource(
      sparkSession,
      className = provider,
      partitionColumns = table.partitionColumnNames,
      bucketSpec = table.bucketSpec,
      options = table.storage.properties ++ pathOption,
      catalogTable = Some(table))

    val result = try {
      dataSource.write(mode, df)
    } catch {
      case ex: AnalysisException =>
        logError(s"Failed to write to table $tableName in $mode mode", ex)
        throw ex
    }
    result match {
      case fs: HadoopFsRelation if table.partitionColumnNames.nonEmpty &&
                                   sparkSession.sqlContext.conf.manageFilesourcePartitions =>
        // Need to recover partitions into the metastore so our saved data is visible.
        sparkSession.sessionState.executePlan(
          AlterTableRecoverPartitionsCommand(table.identifier)).toRdd
      case _ =>
    }

    // Refresh the cache of the table in the catalog.
    sessionState.catalog.refreshTable(tableIdentWithDB)
    Seq.empty[Row]
  }
}
