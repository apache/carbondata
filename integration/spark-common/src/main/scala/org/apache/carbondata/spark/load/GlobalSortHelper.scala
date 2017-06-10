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

package org.apache.carbondata.spark.load

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.CarbonTableIdentifier
import org.apache.carbondata.processing.model.CarbonLoadModel
import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException
import org.apache.carbondata.processing.surrogatekeysgenerator.csvbased.BadRecordsLogger
import org.apache.spark.util.LongAccumulator

object GlobalSortHelper {
  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def badRecordsLogger(loadModel: CarbonLoadModel, badRecordsAccum: LongAccumulator): Unit = {
    val key = new CarbonTableIdentifier(loadModel.getDatabaseName, loadModel.getTableName, null).getBadRecordLoggerKey
    if (null != BadRecordsLogger.hasBadRecord(key)) {
      LOGGER.error("Data Load is partially success for table " + loadModel.getTableName)
      badRecordsAccum.add(1)
    } else {
      LOGGER.info("Data loading is successful for table " + loadModel.getTableName)
    }
  }

  def tryWithSafeFinally[T](tableName: String, block: => T)
      (finallyBlock: => Unit): Unit = {
    try {
      block
    } catch {
      case e: CarbonDataLoadingException => throw e
      case e: Exception =>
        LOGGER.error(e, "Data Loading failed for table " + tableName)
        throw new CarbonDataLoadingException("Data Loading failed for table " + tableName, e)
    } finally {
      finallyBlock
    }
  }
}
