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

package org.apache.carbondata.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.{CarbonAppendableStreamSink, Sink}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.locks.{CarbonLockFactory, LockUsage}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.path.CarbonStorePath
import org.apache.carbondata.streaming.segment.StreamSegmentManager

/**
 * Stream sink factory
 */
object StreamSinkFactory {

  private val LOGGER = LogServiceFactory.getLogService(StreamSinkFactory.getClass.getCanonicalName)

  def createStreamTableSink(
      sparkSession: SparkSession,
      carbonTable: CarbonTable,
      parameters: Map[String, String]): Sink = {
      validateParameters(parameters)
      // default is carbon appended stream sink
      new CarbonAppendableStreamSink(
        sparkSession,
        carbonTable,
        getStreamSegmentId(carbonTable),
        parameters)
  }

  private def validateParameters(parameters: Map[String, String]): Unit = {
    // TODO require to validate parameters
  }

  /**
   * get current stream segment id
   * @return
   */
  private def getStreamSegmentId(carbonTable: CarbonTable): String = {
    val segmentId = StreamSegmentManager.getOrCreateStreamSegment(carbonTable)
    val carbonTablePath = CarbonStorePath
      .getCarbonTablePath(carbonTable.getAbsoluteTableIdentifier)
    val segmentDir = carbonTablePath.getSegmentDir("0", segmentId)
    val fileType = FileFactory.getFileType(segmentDir)
    if (FileFactory.isFileExist(segmentDir, fileType)) {
      // recover fault
      StreamSegmentManager.tryRecoverSegmentFromFault(segmentDir)
    } else {
      FileFactory.mkdirs(segmentDir, fileType)
    }
    segmentId
  }
}
