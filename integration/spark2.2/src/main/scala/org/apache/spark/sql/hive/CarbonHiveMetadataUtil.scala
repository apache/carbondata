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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier

import org.apache.carbondata.common.logging.LogServiceFactory


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
        LOGGER.audit(
          s"Error While deleting the table $databaseName.$tableName during drop carbon table" +
          e.getMessage)
    }
  }

}
