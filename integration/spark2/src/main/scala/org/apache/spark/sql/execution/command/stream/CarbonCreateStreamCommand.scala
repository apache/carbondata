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
import scala.collection.mutable

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, ExprId, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.execution.command.DataCommand
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.streaming.StreamingRelation
import org.apache.spark.sql.types.{StringType, StructType}

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.processing.loading.csvinput.CSVInputFormat
import org.apache.carbondata.spark.StreamingOption
import org.apache.carbondata.spark.util.{CarbonSparkUtil, Util}
import org.apache.carbondata.stream.StreamJobManager

/**
 * This command will start a Spark streaming job to insert rows from source to sink
 */
case class CarbonCreateStreamCommand(
    streamName: String,
    sinkDbName: Option[String],
    sinkTableName: String,
    ifNotExists: Boolean,
    optionMap: Map[String, String],
    query: String
) extends DataCommand {

  override def output: Seq[Attribute] =
    Seq(AttributeReference("Stream Name", StringType, nullable = false)(),
      AttributeReference("JobId", StringType, nullable = false)(),
      AttributeReference("Status", StringType, nullable = false)())

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    setAuditTable(CarbonEnv.getDatabaseName(sinkDbName)(sparkSession), sinkTableName)
    setAuditInfo(Map("streamName" -> streamName, "query" -> query) ++ optionMap)
    val inputQuery = sparkSession.sql(query)
    val sourceTableSeq = inputQuery.logicalPlan collect {
      case r: LogicalRelation
        if r.relation.isInstanceOf[CarbonDatasourceHadoopRelation] &&
           r.relation.asInstanceOf[CarbonDatasourceHadoopRelation].carbonTable.isStreamingSource =>
        r.relation.asInstanceOf[CarbonDatasourceHadoopRelation].carbonTable
    }
    if (sourceTableSeq.isEmpty) {
      throw new MalformedCarbonCommandException(
        "Must specify stream source table in the stream query")
    }
    if (sourceTableSeq.size > 1) {
      throw new MalformedCarbonCommandException(
        "Stream query on more than one stream source table is not supported")
    }
    val sourceTable = sourceTableSeq.head

    val tblProperty = sourceTable.getTableInfo.getFactTable.getTableProperties
    val format = sourceTable.getFormat
    if (format == null) {
      throw new MalformedCarbonCommandException("Streaming from carbon file is not supported")
    }
    val updatedQuery = if (format.equals("kafka")) {
      shouldHaveProperty(tblProperty, "kafka.bootstrap.servers", sourceTable)
      shouldHaveProperty(tblProperty, "subscribe", sourceTable)
      createPlan(sparkSession, inputQuery, sourceTable, "kafka", tblProperty.asScala)
    } else if (format.equals("socket")) {
      shouldHaveProperty(tblProperty, "host", sourceTable)
      shouldHaveProperty(tblProperty, "port", sourceTable)
      createPlan(sparkSession, inputQuery, sourceTable, "socket", tblProperty.asScala)
    } else {
      // Replace the logical relation with a streaming relation created
      // from the stream source table
      inputQuery.logicalPlan transform {
        case r: LogicalRelation
          if r.relation.isInstanceOf[CarbonDatasourceHadoopRelation] &&
             r.relation.asInstanceOf[CarbonDatasourceHadoopRelation].carbonTable.isStreamingSource
        => prepareStreamingRelation(sparkSession, r)
        case plan: LogicalPlan => plan
      }
    }

    if (sourceTable == null) {
      throw new MalformedCarbonCommandException("Must specify stream source table in the query")
    }

    // add CSV row parser if user does not specify
    val newMap = mutable.Map[String, String]()
    optionMap.foreach(x => newMap(x._1) = x._2)
    newMap(CSVInputFormat.DELIMITER) = tblProperty.asScala.getOrElse("delimiter", ",")

    // start the streaming job
    val jobId = StreamJobManager.startStream(
      sparkSession = sparkSession,
      ifNotExists = ifNotExists,
      streamName = streamName,
      sourceTable = sourceTable,
      sinkTable = CarbonEnv.getCarbonTable(sinkDbName, sinkTableName)(sparkSession),
      query = query,
      streamDf = Dataset.ofRows(sparkSession, updatedQuery),
      options = new StreamingOption(newMap.toMap)
    )
    Seq(Row(streamName, jobId, "RUNNING"))
  }

  /**
   * Create a new plan for the stream query on kafka and Socket source table.
   * This is required because we need to convert the schema of the data stored in kafka
   * The returned logical plan contains the complete plan tree of original plan with
   * logical relation replaced with a streaming relation.
   *
   * @param sparkSession spark session
   * @param inputQuery stream query from user
   * @param sourceTable source table (kafka table)
   * @param sourceName source name, kafka or socket
   * @param tblProperty table property of source table
   * @return a new logical plan
   */
  private def createPlan(
      sparkSession: SparkSession,
      inputQuery: DataFrame,
      sourceTable: CarbonTable,
      sourceName: String,
      tblProperty: mutable.Map[String, String]): LogicalPlan = {
    // We follow 3 steps to generate new plan
    // 1. replace the logical relation in stream query with streaming relation
    // 2. collect the new ExprId generated
    // 3. update the stream query plan with the new ExprId generated, to make the plan consistent

    // exprList is used for UDF to extract the data from the 'value' column in kafka
    val columnNames = Util.convertToSparkSchema(sourceTable).fieldNames
    val exprList = columnNames.zipWithIndex.map {
      case (columnName, i) =>
        s"case when size(_values) > $i then _values[$i] else null end AS $columnName"
    }

    val aliasMap = new util.HashMap[String, ExprId]()
    val updatedQuery = inputQuery.logicalPlan transform {
      case r: LogicalRelation
        if r.relation.isInstanceOf[CarbonDatasourceHadoopRelation] &&
           r.relation.asInstanceOf[CarbonDatasourceHadoopRelation].carbonTable.isStreamingSource =>
        // for kafka stream source, get the 'value' column and split it by using UDF
        val plan = sparkSession.readStream
          .format(sourceName)
          .options(tblProperty)
          .load()
          .selectExpr("CAST(value as string) as _value")
        val recordFormat = tblProperty.getOrElse("record_format", "csv")
        val newPlan = recordFormat match {
            case "csv" =>
              val delimiter = tblProperty.getOrElse("delimiter", ",")
              plan.selectExpr(
                s"split(_value, '${CarbonSparkUtil.delimiterConverter4Udf(delimiter)}') as _values")
                .selectExpr(exprList: _*)
                .logicalPlan
            case "json" =>
              import org.apache.spark.sql.functions._
              plan
                .select(from_json(col("_value"), Util.convertToSparkSchema(sourceTable)) as "_data")
                .select("_data.*")
                .logicalPlan
          }

        // collect the newly generated ExprId
        newPlan collect {
          case p@Project(projectList, child) =>
            projectList.map { expr =>
              aliasMap.put(expr.name, expr.exprId)
            }
            p
        }
        newPlan
      case plan: LogicalPlan => plan
    }

    // transform the stream plan to replace all attribute with the collected ExprId
    val transFormedPlan = updatedQuery transform {
      case p@Project(projectList: Seq[NamedExpression], child) =>
        val newProjectList = projectList.map { expr =>
          val newExpr = expr transform {
            case attribute: Attribute =>
              val exprId: ExprId = aliasMap.get(attribute.name)
              if (exprId != null) {
                if (exprId.id != attribute.exprId.id) {
                  AttributeReference(
                    attribute.name, attribute.dataType, attribute.nullable,
                    attribute.metadata)(exprId, attribute.qualifier)
                } else {
                  attribute
                }
              } else {
                attribute
              }
          }
          newExpr.asInstanceOf[NamedExpression]
        }
        Project(newProjectList, child)
      case f@Filter(condition: Expression, child) =>
        val newCondition = condition transform {
          case attribute: Attribute =>
            val exprId: ExprId = aliasMap.get(attribute.name)
            if (exprId != null) {
              if (exprId.id != attribute.exprId.id) {
                AttributeReference(
                  attribute.name, attribute.dataType, attribute.nullable,
                  attribute.metadata)(exprId, attribute.qualifier)
              } else {
                attribute
              }
            } else {
              attribute
            }
        }
        Filter(newCondition, child)
    }
    transFormedPlan
  }

  /**
   * Create a streaming relation from the input logical relation (source table)
   *
   * @param sparkSession spark session
   * @param logicalRelation source table to convert
   * @return sourceTable and its streaming relation
   */
  private def prepareStreamingRelation(
      sparkSession: SparkSession,
      logicalRelation: LogicalRelation): StreamingRelation = {
    val sourceTable = logicalRelation.relation
      .asInstanceOf[CarbonDatasourceHadoopRelation].carbonTable
    val tblProperty = sourceTable.getTableInfo.getFactTable.getTableProperties
    val format = sourceTable.getFormat
    if (format == null) {
      throw new MalformedCarbonCommandException("Streaming from carbon file is not supported")
    }
    val streamReader = sparkSession
      .readStream
      .schema(getSparkSchema(sourceTable))
      .format(format)
    val dataFrame: DataFrame = format match {
      case "csv" | "text" | "json" | "parquet" =>
        shouldHaveProperty(tblProperty, "path", sourceTable)
        streamReader.load(tblProperty.get("path"))
      case other =>
        throw new MalformedCarbonCommandException(s"Streaming from $format is not supported")
    }
    val streamRelation = dataFrame.logicalPlan.asInstanceOf[StreamingRelation]

    // Since SparkSQL analyzer will match the UUID in attribute,
    // create a new StreamRelation and re-use the same attribute from LogicalRelation
    StreamingRelation(streamRelation.dataSource, streamRelation.sourceName, logicalRelation.output)
  }

  private def shouldHaveProperty(
      tblProperty: java.util.Map[String, String],
      propertyName: String,
      sourceTable: CarbonTable) : Unit = {
    if (!tblProperty.containsKey(propertyName)) {
      throw new MalformedCarbonCommandException(
        s"tblproperty '$propertyName' should be provided for stream source " +
        s"${sourceTable.getDatabaseName}.${sourceTable.getTableName}")
    }
  }

  private def getSparkSchema(sourceTable: CarbonTable): StructType = {
    val cols = sourceTable.getTableInfo.getFactTable.getListOfColumns.asScala.toArray
    val sortedCols = cols.filter(_.getSchemaOrdinal != -1)
      .sortWith(_.getSchemaOrdinal < _.getSchemaOrdinal)
    Util.convertToSparkSchema(sourceTable, sortedCols, false)
  }

  override protected def opName: String = "CREATE STREAM"
}
