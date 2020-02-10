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

import java.util
import java.util.{ArrayList, Arrays, List}

import org.apache.hadoop.hive.ql.exec.UDF
import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}
import org.apache.spark.sql.secondaryindex.util.{CarbonInternalScalaUtil, FileInternalUtil, IndexTableUtil}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.schema.indextable.IndexTableInfo
import org.apache.carbondata.core.metadata.schema.table.CarbonTable


/**
 * This class contains all carbon hive metadata related utilities
 */
object CarbonHiveMetadataUtil {

  @transient
  val LOGGER = LogServiceFactory.getLogService(CarbonHiveMetadataUtil.getClass.getName)


  /**
   * This method invalidates the table from HiveMetastoreCatalog before dropping table
   *
   * @param databaseName
   * @param tableName
   * @param sparkSession
   */
  def invalidateAndDropTable(databaseName: String,
      tableName: String,
      sparkSession: SparkSession): Unit = {
    try {
      val tabelIdentifier = TableIdentifier(tableName, Some(databaseName))
      sparkSession.sessionState.catalog.dropTable(tabelIdentifier, true, false)
    } catch {
      case e: Exception =>
        LOGGER.error(
          s"Error While deleting the table $databaseName.$tableName during drop carbon table" +
          e.getMessage)
        throw e
    }
  }

  def refreshTable(dbName: String, tableName: String, sparkSession: SparkSession): Unit = {
    val tableWithDb = dbName + "." + tableName
    val tableIdent = sparkSession.sessionState.sqlParser.parseTableIdentifier(tableWithDb)
    sparkSession.sessionState.catalog.refreshTable(tableIdent)
  }

  /**
   * This method invalidates the table from HiveMetastoreCatalog before dropping table and also
   * removes the index table info from parent carbon table.
   *
   * @param indexTableIdentifier
   * @param indexInfo
   * @param parentCarbonTable
   * @param sparkSession
   */
  def invalidateAndUpdateIndexInfo(indexTableIdentifier: TableIdentifier,
      indexInfo: String, parentCarbonTable: CarbonTable)(sparkSession: SparkSession): Unit = {
    val catalog = CarbonEnv.getInstance(sparkSession).carbonMetaStore
    val dbName = indexTableIdentifier.database
      .getOrElse(CarbonCommonConstants.DATABASE_DEFAULT_NAME)
    val tableName = indexTableIdentifier.table
    try {
      if (indexInfo != null) {
        removeIndexInfoFromParentTable(indexInfo,
          parentCarbonTable,
          dbName,
          tableName)(sparkSession)
      }
    } catch {
      case e: Exception =>
        LOGGER.error(
          s"Error While deleting the table $dbName.$tableName during drop carbon table" +
          e.getMessage)
    }
  }

  def removeIndexInfoFromParentTable(indexInfo: String,
      parentCarbonTable: CarbonTable,
      dbName: String,
      tableName: String)(sparkSession: SparkSession): Unit = {
    val parentTableName = parentCarbonTable.getTableName
    val newIndexInfo = removeIndexTable(indexInfo, dbName, tableName)
    CarbonInternalScalaUtil.removeIndexTableInfo(parentCarbonTable, tableName)
    sparkSession.sql(
      s"""ALTER TABLE $dbName.$parentTableName SET SERDEPROPERTIES ('indexInfo'='$newIndexInfo')
        """.stripMargin)
    FileInternalUtil.touchSchemaFileTimestamp(dbName, parentTableName,
      parentCarbonTable.getTablePath, System.currentTimeMillis())
    FileInternalUtil.touchStoreTimeStamp()
    refreshTable(dbName, parentTableName, sparkSession)
  }

  /**
   * removes index table info from parent table properties
   *
   * @param gsonData
   * @param dbName
   * @param tableName
   * @return
   */
  def removeIndexTable(gsonData: String, dbName: String, tableName: String): String = {
    val indexTableInfos: Array[IndexTableInfo] = IndexTableInfo.fromGson(gsonData)
    if (null == indexTableInfos) {
      IndexTableInfo.toGson(Array())
    } else {
      val indexTables = indexTableInfos.toList
        .filterNot(indexTable => indexTable.getDatabaseName.equalsIgnoreCase(dbName) &&
                                 indexTable.getTableName.equalsIgnoreCase(tableName))
      IndexTableInfo.toGson(indexTables.toArray)
    }
  }

  def transformToRemoveNI(expression: Expression): Expression = {
    val newExpWithoutNI = expression.transform {
      case hiveUDF: HiveSimpleUDF if hiveUDF.function.isInstanceOf[NonIndexUDFExpression] =>
        hiveUDF.asInstanceOf[HiveSimpleUDF].children.head
      case scalaUDF: ScalaUDF if "NI".equalsIgnoreCase(scalaUDF.udfName.get) =>
        scalaUDF.children.head
    }
    newExpWithoutNI
  }

  def checkNIUDF(condition: Expression): Boolean = {
    condition match {
      case hiveUDF: HiveSimpleUDF if hiveUDF.function.isInstanceOf[NonIndexUDFExpression] => true
      case scalaUDF: ScalaUDF if "NI".equalsIgnoreCase(scalaUDF.udfName.get) => true
      case _ => false
    }
  }

  def getNIChildren(condition: Expression): Expression = {
    condition.asInstanceOf[HiveSimpleUDF].children.head
  }
}

private class NonIndexUDFExpression extends UDF {
  def evaluate(input: Any): Boolean = true
}
