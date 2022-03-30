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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogTable}

import org.apache.carbondata.core.catalog.BaseCatalog

trait CarbonCatalog extends BaseCatalog {

  def createTable(tableDefinition: CatalogTable,
      ignoreIfExists: Boolean,
      validateLocation: Boolean = true)(sparkSession: SparkSession): Unit

  def tableExists(name: TableIdentifier)(sparkSession: SparkSession): Boolean

  def getDatabaseMetadata(name: String)(sparkSession: SparkSession): CatalogDatabase

  def getTableMetadata(tableIdentifier: TableIdentifier)(sparkSession: SparkSession): CatalogTable

  def listDatabases()(sparkSession: SparkSession): Seq[String]

  def listTables(dbName: String)(sparkSession: SparkSession): Seq[TableIdentifier]

  def dropTable(name: TableIdentifier,
      ignoreIfNotExists: Boolean,
      purge: Boolean)(sparkSession: SparkSession): Unit

  /**
   * This method will write the schema thrift file in carbon store and load table metadata
   */


  //  def alterTable(wrapperTableInfo: TableInfo)(sparkSession: SparkSession): Unit
}
