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

package org.apache.carbondata.spark

import scala.collection.JavaConverters._

import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.{InputSplit, Job}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.util.{SparkSQLUtil, SparkTypeConverter}
import org.apache.spark.CarbonInputMetrics
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.datastore.row.CarbonRow
import org.apache.carbondata.core.metadata.datatype.{StructField, StructType}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.hadoop.CarbonProjection
import org.apache.carbondata.hadoop.api.{CarbonInputFormat, CarbonTableInputFormat,
  CarbonTableOutputFormat}
import org.apache.carbondata.processing.loading.constants.DataLoadProcessorConstants
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.util.TableOptionConstant
import org.apache.carbondata.spark.rdd.CarbonScanRDD
import org.apache.carbondata.store.CarbonRowReadSupport

/**
 * utils of segment
 */
object SegmentUtils {

  /**
   * get splits of specified segments
   */
  def splitsOfSegments(
      sparkSession: SparkSession,
      carbonTable: CarbonTable,
      segments: Array[Segment]
  ): java.util.List[InputSplit] = {
    val jobConf = new JobConf(SparkSQLUtil.sessionState(sparkSession).newHadoopConf())
    SparkHadoopUtil.get.addCredentials(jobConf)
    val job = Job.getInstance(jobConf)
    val conf = job.getConfiguration
    CarbonInputFormat.setTablePath(conf, carbonTable.getTablePath)
    CarbonInputFormat.setTableInfo(conf, carbonTable.getTableInfo)
    CarbonInputFormat.setDatabaseName(conf, carbonTable.getDatabaseName)
    CarbonInputFormat.setTableName(conf, carbonTable.getTableName)
    CarbonInputFormat.setQuerySegment(conf, segments.map(_.getSegmentNo).mkString(","))
    new CarbonTableInputFormat[Object].getSplits(job)
  }

  /**
   * create DataFrame basing on specified segments
   */
  def dataFrameOfSegments(
      sparkSession: SparkSession,
      carbonTable: CarbonTable,
      segments: Array[Segment]
  ): DataFrame = {
    val columns = carbonTable
      .getCreateOrderColumn(carbonTable.getTableName)
      .asScala
      .map(_.getColName)
      .toArray
    val schema = SparkTypeConverter.createSparkSchema(carbonTable, columns)
    val rdd: RDD[Row] = new CarbonScanRDD[CarbonRow](
      sparkSession,
      columnProjection = new CarbonProjection(columns),
      null,
      carbonTable.getAbsoluteTableIdentifier,
      carbonTable.getTableInfo.serialize,
      carbonTable.getTableInfo,
      new CarbonInputMetrics,
      null,
      null,
      classOf[CarbonRowReadSupport],
      splitsOfSegments(sparkSession, carbonTable, segments)
    )
      .map { row =>
        new GenericRow(row.getData.asInstanceOf[Array[Any]])
      }
    sparkSession.createDataFrame(rdd, schema)
  }

  def getLoadModel(
      sparkSession: SparkSession,
      carbonTable: CarbonTable,
      segments: Array[Segment]
  ): CarbonLoadModel = {
    val conf = SparkSQLUtil.sessionState(sparkSession).newHadoopConf()
    CarbonTableOutputFormat.setDatabaseName(conf, carbonTable.getDatabaseName)
    CarbonTableOutputFormat.setTableName(conf, carbonTable.getTableName)
    CarbonTableOutputFormat.setCarbonTable(conf, carbonTable)
    val fieldList = carbonTable
      .getCreateOrderColumn(carbonTable.getTableName)
      .asScala
      .map { column =>
        new StructField(column.getColName, column.getDataType)
      }
    CarbonTableOutputFormat.setInputSchema(conf, new StructType(fieldList.asJava))
    val loadModel = CarbonTableOutputFormat.getLoadModel(conf)
    loadModel.setSerializationNullFormat(
      TableOptionConstant.SERIALIZATION_NULL_FORMAT.getName() + ",\\N")
    loadModel.setBadRecordsLoggerEnable(
      TableOptionConstant.BAD_RECORDS_LOGGER_ENABLE.getName() + ",false")
    loadModel.setBadRecordsAction(
      TableOptionConstant.BAD_RECORDS_ACTION.getName() + ",force")
    loadModel.setIsEmptyDataBadRecord(
      DataLoadProcessorConstants.IS_EMPTY_DATA_BAD_RECORD + ",false")
    loadModel
  }
}
