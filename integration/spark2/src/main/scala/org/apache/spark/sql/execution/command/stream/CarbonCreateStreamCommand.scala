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

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.DataCommand
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.streaming.StreamingRelation
import org.apache.spark.sql.types.StringType

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.spark.StreamingOption
import org.apache.carbondata.spark.util.SparkDataTypeConverterImpl
import org.apache.carbondata.stream.StreamJobManager

/**
 * This command will start a Spark streaming job to insert rows from source to sink
 */
case class CarbonCreateStreamCommand(
    streamName: String,
    sinkDbName: Option[String],
    sinkTableName: String,
    optionMap: Map[String, String],
    query: String
) extends DataCommand {

  override def output: Seq[Attribute] =
    Seq(AttributeReference("Stream Name", StringType, nullable = false)(),
      AttributeReference("JobId", StringType, nullable = false)(),
      AttributeReference("Status", StringType, nullable = false)())

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    val df = sparkSession.sql(query)
    var sourceTable: CarbonTable = null
    // replace the LogicalRelation in the plan with StreamingRelation
    val streamLp = df.logicalPlan transform {
      case r@LogicalRelation(relation, output, _)
        if relation.isInstanceOf[CarbonDatasourceHadoopRelation] &&
           relation.asInstanceOf[CarbonDatasourceHadoopRelation].carbonTable.isStreamingSource =>
        sourceTable = relation.asInstanceOf[CarbonDatasourceHadoopRelation].carbonTable
        validateFormat(sourceTable)
        val tblproperty = sourceTable.getTableInfo.getFactTable.getTableProperties
        val cols = sourceTable.getTableInfo.getFactTable.getListOfColumns.asScala.toArray
        val sortedCols = cols.filter(_.getSchemaOrdinal != -1)
          .sortWith(_.getSchemaOrdinal < _.getSchemaOrdinal)
        val schema = SparkDataTypeConverterImpl.convertToSparkSchema(sortedCols)
        val streamRelation = sparkSession.readStream
          .schema(schema)
          .format(tblproperty.get("format"))
          .load(tblproperty.get("path"))
          .logicalPlan
          .asInstanceOf[StreamingRelation]

        // Since SparkSQL analyzer will match the UUID in attribute,
        // create a new StreamRelation and re-use the same attribute from LogicalRelation
        StreamingRelation(streamRelation.dataSource, streamRelation.sourceName, output)
      case plan: LogicalPlan => plan
    }

    if (sourceTable == null) {
      throw new MalformedCarbonCommandException("Must specify stream source table in select query")
    }

    val jobId = StreamJobManager.startStream(
      sparkSession = sparkSession,
      streamName = streamName,
      sourceTable = sourceTable,
      sinkTable = CarbonEnv.getCarbonTable(sinkDbName, sinkTableName)(sparkSession),
      query = query,
      streamDf = Dataset.ofRows(sparkSession, streamLp),
      options = new StreamingOption(optionMap)
    )
    Seq(Row(streamName, jobId, "RUNNING"))
  }

  private def validateFormat(sourceTable: CarbonTable): Unit = {
    val format = sourceTable.getTableInfo.getFactTable.getTableProperties.get("format")
    if (format == null) {
      throw new MalformedCarbonCommandException("Streaming from carbon file is not supported")
    }
    format match {
      case "csv" =>
      case "text" =>
      case "json" =>
      case "parquet" =>
      case "kafka" =>
      case "socket" =>
      case other =>
        throw new MalformedCarbonCommandException(s"Streaming from $format is not supported")
    }
  }

}
