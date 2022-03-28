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

package org.apache.spark.sql.catalog

import java.io.{FileNotFoundException, IOException}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogTable}

import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.fileoperations.FileWriteOperation
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.reader.ThriftReader
import org.apache.carbondata.core.reader.ThriftReader.TBaseCreator
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.core.writer.ThriftWriter
import org.apache.carbondata.format.{TableInfo => ExternalTableInfo}

private[catalog] object CarbonCatalogImpl extends CarbonCatalog {

  def createTable(tableDefinition: CatalogTable,
      ignoreIfExists: Boolean,
      validateLocation: Boolean = true)(sparkSession: SparkSession): Unit = {
    sparkSession.sessionState.catalog.createTable(tableDefinition, ignoreIfExists, validateLocation)
  }

  def tableExists(name: TableIdentifier)(sparkSession: SparkSession): Boolean = {
    sparkSession.sessionState.catalog.tableExists(name)
  }

  override def getDatabaseMetadata(name: String)(sparkSession: SparkSession): CatalogDatabase = {
    sparkSession.sessionState.catalog.getDatabaseMetadata(name)
  }

  override def listDatabases()(sparkSession: SparkSession): Seq[String] = {
    sparkSession.sessionState.catalog.listDatabases()
  }

  override def listTables(dbName: String)(sparkSession: SparkSession): Seq[TableIdentifier] = {
    sparkSession.sessionState.catalog.listTables(dbName)
  }

  override def getTableMetadata(tableIdentifier: TableIdentifier)
    (sparkSession: SparkSession): CatalogTable = {
    sparkSession.sessionState.catalog.getTableMetadata(tableIdentifier)
  }

  override def dropTable(name: TableIdentifier, ignoreIfNotExists: Boolean,
      purge: Boolean)(sparkSession: SparkSession): Unit = {
    sparkSession.sessionState.catalog.dropTable(name, ignoreIfNotExists = true, purge = false)
  }

  override def getSchema(schemaFilePath: String): ExternalTableInfo = {
    if (FileFactory.isFileExist(schemaFilePath)) {
      val createTBase: ThriftReader.TBaseCreator = new TBaseCreator {
        override def create() = new ExternalTableInfo()
      }
      val thriftReader = new ThriftReader(schemaFilePath,
        createTBase,
        FileFactory.getConfiguration)
      thriftReader.open()
      val tableInfo = thriftReader.read.asInstanceOf[ExternalTableInfo]
      thriftReader.close()
      tableInfo
    } else {
      throw new FileNotFoundException("Schema file does not exist: " + schemaFilePath)
    }
  }

  override def getLastSchemaModificationTime(schemaFilePath: String): Long = {
    val schemaFile = FileFactory.getCarbonFile(schemaFilePath)
    if (schemaFile.exists()) {
      schemaFile.getLastModifiedTime
    } else {
      -1L
    }
  }

  /**
   * This method will write the schema thrift file in carbon store and load table metadata.
   */
  def saveSchema(identifier: AbsoluteTableIdentifier, thriftTableInfo: ExternalTableInfo): Long = {
    val schemaMetadataPath = CarbonTablePath.getMetadataPath(identifier.getTablePath)
    if (!FileFactory.isFileExist(schemaMetadataPath)) {
      val isDirCreated = FileFactory
        .mkdirs(schemaMetadataPath, FileFactory.getConfiguration)
      if (!isDirCreated) {
        throw new IOException(s"Failed to create the metadata directory $schemaMetadataPath")
      }
    }
    val schemaFilePath = CarbonTablePath.getSchemaFilePath(identifier.getTablePath)
    val thriftWriter = new ThriftWriter(schemaFilePath, false)
    thriftWriter.open(FileWriteOperation.OVERWRITE)
    thriftWriter.write(thriftTableInfo)
    thriftWriter.close()
    val modifiedTime = System.currentTimeMillis()
    FileFactory.getCarbonFile(schemaFilePath).setLastModifiedTime(modifiedTime)
    modifiedTime
  }

  //   def alterTable(wrapperTableInfo: TableInfo)(sparkSession: SparkSession): Unit = {
  //    val schemaParts = CarbonUtil.convertToMultiGsonStrings(wrapperTableInfo, "=", "'", "")
  //    val hiveClient = sparkSession
  //      .sessionState
  //      .catalog
  //      .externalCatalog.asInstanceOf[HiveExternalCatalog]
  //      .client
  //    hiveClient.runSqlHive(s"ALTER TABLE " +
  //      s"`${wrapperTableInfo.getDatabaseName}`.`${wrapperTableInfo.getFactTable.getTableName}`
  //      " +
  //      s"SET SERDEPROPERTIES($schemaParts)")
  //  }
}
