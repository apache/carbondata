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

package org.apache.carbondata.stream

import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, TimeUnit}

import scala.collection.JavaConverters._

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.carbondata.execution.datasources.CarbonSparkDataSourceUtil
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.{StructField, StructType}

import org.apache.carbondata.common.exceptions.NoSuchStreamException
import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.spark.StreamingOption
import org.apache.carbondata.streaming.CarbonStreamException

/**
 * A manager to start and stop a stream job for StreamSQL.
 * This stream job is only available to the driver memory and not persisted
 * so other drivers cannot see ongoing stream jobs.
 */
object StreamJobManager {
  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  // map of stream name to job desc
  private val jobs = new ConcurrentHashMap[String, StreamJobDesc]()

  private def validateSourceTable(source: CarbonTable): Unit = {
    if (!source.isStreamingSource) {
      throw new MalformedCarbonCommandException(
        s"Table ${source.getTableName} is not streaming source table " +
        "('streaming' tblproperty is not 'source')")
    }
  }

  private def validateSinkTable(validateQuerySchema: Boolean,
                                querySchema: StructType, sink: CarbonTable): Unit = {
    if (!sink.isStreamingSink) {
      throw new MalformedCarbonCommandException(
        s"Table ${sink.getTableName} is not streaming sink table " +
        "('streaming' tblproperty is not 'sink' or 'true')")
    }
    if (validateQuerySchema) {
      val fields = sink.getCreateOrderColumn().asScala.map { column =>
        StructField(
          column.getColName,
          CarbonSparkDataSourceUtil.convertCarbonToSparkDataType(column.getDataType))
      }
      if (!querySchema.equals(StructType(fields))) {
        throw new MalformedCarbonCommandException(
          s"Schema of table ${sink.getTableName} does not match query output")
      }
    }
  }

  /**
   * Start a spark streaming job
   * @param sparkSession session instance
   * @param ifNotExists if not exists is set or not
   * @param streamName name of the stream
   * @param sourceTable stream source table
   * @param sinkTable sink table to insert to
   * @param query query string
   * @param streamDf dataframe that containing the query from stream source table
   * @param options options provided by user
   * @return Job ID
   */
  def startStream(
      sparkSession: SparkSession,
      ifNotExists: Boolean,
      streamName: String,
      sourceTable: CarbonTable,
      sinkTable: CarbonTable,
      query: String,
      streamDf: DataFrame,
      options: StreamingOption): String = {
    val latch = new CountDownLatch(1)
    var exception: Throwable = null
    var job: StreamingQuery = null

    if (jobs.containsKey(streamName)) {
      if (ifNotExists) {
        return jobs.get(streamName).streamingQuery.id.toString
      } else {
        throw new MalformedCarbonCommandException(s"Stream Name $streamName already exists")
      }
    }

    validateSourceTable(sourceTable)

    // for kafka and socket stream source, the source table schema is one string column only
    // so we do not validate the query schema against the sink table schema
    val isKafka = Option(sourceTable.getFormat).contains("kafka")
    val isSocket = Option(sourceTable.getFormat).contains("socket")
    validateSinkTable(!(isKafka || isSocket), streamDf.schema, sinkTable)

    // start a new thread to run the streaming ingest job, the job will be running
    // until user stops it by STOP STREAM JOB
    val thread = new Thread(new Runnable {
      override def run(): Unit = {
        try {
          job = streamDf.writeStream
            .format("carbondata")
            .trigger(options.trigger)
            .option("checkpointLocation", options.checkpointLocation(sinkTable.getTablePath))
            .option("dateformat", options.dateFormat)
            .option("timestampformat", options.timeStampFormat)
            .option("carbon.stream.parser", options.rowParser)
            .option("dbName", sinkTable.getDatabaseName)
            .option("tableName", sinkTable.getTableName)
            .option("bad_record_path", options.badRecordsPath)
            .option("bad_records_action", options.badRecordsAction)
            .option("bad_records_logger_enable", options.badRecordsLogger)
            .option("is_empty_bad_record", options.isEmptyBadRecord)
            .options(options.remainingOption)
            .start()
          latch.countDown()
          job.awaitTermination()
        } catch {
          case e: Throwable =>
            LOGGER.error(e)
            exception = e
            latch.countDown()
        }
      }
    })
    thread.start()

    // wait for max 10 seconds for the streaming job to start
    if (latch.await(10, TimeUnit.SECONDS)) {
      if (exception != null) {
        throw exception
      }

      jobs.put(
        streamName,
        StreamJobDesc(job, streamName, sourceTable.getDatabaseName, sourceTable.getTableName,
          sinkTable.getDatabaseName, sinkTable.getTableName, query, thread))

      job.id.toString
    } else {
      thread.interrupt()
      throw new CarbonStreamException("Streaming job takes too long to start")
    }
  }

  /**
   * Stop a streaming job
   * @param streamName name of the stream
   * @param ifExists if exists is set or not
   */
  def stopStream(streamName: String, ifExists: Boolean): Unit = {
    if (jobs.containsKey(streamName)) {
      val jobDesc = jobs.get(streamName)
      jobDesc.streamingQuery.stop()
      jobDesc.thread.interrupt()
      jobs.remove(streamName)
    } else {
      if (!ifExists) {
        throw new NoSuchStreamException(streamName)
      }
    }
  }

  /**
   * Return all running jobs
   * @return running jobs
   */
  def getAllJobs: Set[StreamJobDesc] = jobs.values.asScala.toSet

}

/**
 * A job description for the StreamSQL job
 */
private[stream] case class StreamJobDesc(
    streamingQuery: StreamingQuery,
    streamName: String,
    sourceDb: String,
    sourceTable: String,
    sinkDb: String,
    sinkTable: String,
    query: String,
    thread: Thread,
    startTime: Long = System.currentTimeMillis()
)
