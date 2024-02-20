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
package org.apache.carbondata.spark.testsuite.tpc

import org.apache.spark.sql.{SaveMode, SQLContext}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.execution.datasources.{CreateTable, DataSource}
import org.apache.spark.sql.types.{StructField, StructType}

case class Table(name: String, partitionColumns: Seq[String], fields: StructField*) {
  val tempViewName = s"${name}_tmp"

  val schema = StructType(fields)

  val delimiter = "|"

  def createTemporaryTable(sqlContext: SQLContext,
                           location: String,
                           format: String): Unit = {
    sqlContext.read.format(format)
      .option("delimiter", delimiter)
      .schema(schema)
      .load(location)
      .createOrReplaceTempView(tempViewName)
  }

  def createTable(sqlContext: SQLContext, format: String, database: String): Unit = {
    val tableIdent = TableIdentifier(name, Some(database))
    val storage = DataSource.buildStorageFormatFromOptions(Map.empty)
    val tableDesc = CatalogTable(
      identifier = tableIdent,
      tableType = CatalogTableType.MANAGED,
      storage = storage,
      schema = schema,
      partitionColumnNames = partitionColumns,
      provider = Some(format)
    )
    val plan = CreateTable(tableDesc, SaveMode.Ignore, None)
    sqlContext.sparkSession.sessionState.executePlan(plan).toRdd
  }

  def loadDataFromTempView(sqlContext: SQLContext,
                           database: String): Unit = {
    val fieldName = (fields
      .filter(attr => !partitionColumns.contains(attr.name))
      .map { attr => attr.name }
      ++ partitionColumns).mkString(",")
    sqlContext.sparkSession
      .sql(s"insert into ${database}.${name} " +
        s"select $fieldName from ${tempViewName}")
  }
}
