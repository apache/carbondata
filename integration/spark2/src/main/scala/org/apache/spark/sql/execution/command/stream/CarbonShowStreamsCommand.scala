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

package org.apache.spark.sql.execution.command.stream

import java.util.Date
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.MetadataCommand
import org.apache.spark.sql.types.StringType

import org.apache.carbondata.stream.StreamJobManager

/**
 * Show all streams created or on a specified table
 */
case class CarbonShowStreamsCommand(
    tableOp: Option[TableIdentifier]
) extends MetadataCommand {
  override def output: Seq[Attribute] = {
    Seq(AttributeReference("Stream Name", StringType, nullable = false)(),
      AttributeReference("JobId", StringType, nullable = false)(),
      AttributeReference("Status", StringType, nullable = false)(),
      AttributeReference("Source", StringType, nullable = false)(),
      AttributeReference("Sink", StringType, nullable = false)(),
      AttributeReference("Start Time", StringType, nullable = false)(),
      AttributeReference("Time Elapse", StringType, nullable = false)())
  }

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val jobs = tableOp match {
      case None => StreamJobManager.getAllJobs.toSeq
      case Some(table) =>
        val carbonTable = CarbonEnv.getCarbonTable(table.database, table.table)(sparkSession)
        setAuditTable(carbonTable)
        StreamJobManager.getAllJobs.filter { job =>
          job.sinkTable.equalsIgnoreCase(carbonTable.getTableName) &&
          job.sinkDb.equalsIgnoreCase(carbonTable.getDatabaseName)
        }.toSeq
    }

    jobs.map { job =>
      val elapsedTime = System.currentTimeMillis() - job.startTime
      Row(
        job.streamName,
        job.streamingQuery.id.toString,
        if (job.streamingQuery.isActive) "RUNNING" else "FAILED",
        s"${ job.sourceDb }.${ job.sourceTable }",
        s"${ job.sinkDb }.${ job.sinkTable }",
        new Date(job.startTime).toString,
        String.format(
          "%s days, %s hours, %s min, %s sec",
          TimeUnit.MILLISECONDS.toDays(elapsedTime).toString,
          TimeUnit.MILLISECONDS.toHours(elapsedTime).toString,
          TimeUnit.MILLISECONDS.toMinutes(elapsedTime).toString,
          TimeUnit.MILLISECONDS.toSeconds(elapsedTime).toString)
      )
    }
  }

  override protected def opName: String = "SHOW STREAMS"
}
