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
package org.apache.spark.sql.execution.datasources

import java.io.File
import java.util

import scala.collection.JavaConverters._
import scala.util.Random

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{DataType, StructType}

import org.apache.carbondata.core.constants.{CarbonCommonConstants, CarbonLoadOptionConstants}
import org.apache.carbondata.core.metadata.{CarbonMetadata, PartitionMapFileStore}
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.hadoop.api.{CarbonOutputCommitter, CarbonTableOutputFormat}
import org.apache.carbondata.hadoop.api.CarbonTableOutputFormat.CarbonRecordWriter
import org.apache.carbondata.processing.loading.csvinput.StringArrayWritable
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.spark.util.{DataLoadingUtil, Util}

class CarbonFileFormat
  extends FileFormat
    with DataSourceRegister
    with Logging
with Serializable {

  override def shortName(): String = "carbondata"

  override def inferSchema(sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    None
  }

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    val conf = job.getConfiguration
    conf.setClass(
      SQLConf.OUTPUT_COMMITTER_CLASS.key,
      classOf[CarbonOutputCommitter],
      classOf[CarbonOutputCommitter])
    conf.set("carbon.commit.protocol", "carbon.commit.protocol")
    sparkSession.sessionState.conf.setConfString(
      "spark.sql.sources.commitProtocolClass",
      "org.apache.spark.sql.execution.datasources.CarbonSQLHadoopMapReduceCommitProtocol")
    job.setOutputFormatClass(classOf[CarbonTableOutputFormat])
    var table = CarbonEnv.getCarbonTable(
      TableIdentifier(options("tableName"), options.get("dbName")))(sparkSession)
    val model = new CarbonLoadModel
    val carbonProperty = CarbonProperties.getInstance()
    val optionsFinal = DataLoadingUtil.getDataLoadingOptions(carbonProperty, options)
    val tableProperties = table.getTableInfo.getFactTable.getTableProperties
    optionsFinal.put("sort_scope", tableProperties.asScala.getOrElse("sort_scope",
      carbonProperty.getProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_SORT_SCOPE,
        carbonProperty.getProperty(CarbonCommonConstants.LOAD_SORT_SCOPE,
          CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT))))
    val partitionStr =
      table.getTableInfo.getFactTable.getPartitionInfo.getColumnSchemaList.asScala.map(
        _.getColumnName.toLowerCase).mkString(",")
    optionsFinal.put(
      "fileheader",
      dataSchema.fields.map(_.name.toLowerCase).mkString(",") + "," + partitionStr)
    DataLoadingUtil.buildCarbonLoadModel(
      table,
      carbonProperty,
      options,
      optionsFinal,
      model,
      conf
    )
    // Set the standard date/time format which supported by spark/hive.
    model.setTimestampformat(CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    model.setDateFormat(CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
    model.setPartitionId("0")
    model.setUseOnePass(options.getOrElse("onepass", "false").toBoolean)
    model.setDictionaryServerHost(options.getOrElse("dicthost", null))
    model.setDictionaryServerPort(options.getOrElse("dictport", "-1").toInt)
    CarbonTableOutputFormat.setLoadModel(conf, model)
    CarbonTableOutputFormat.setOverwrite(conf, options("overwrite").toBoolean)

    new OutputWriterFactory {

      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        val isCarbonUseMultiDir = CarbonProperties.getInstance().isUseMultiTempDir
        var storeLocation: Array[String] = Array[String]()
        val isCarbonUseLocalDir = CarbonProperties.getInstance()
          .getProperty("carbon.use.local.dir", "false").equalsIgnoreCase("true")
        val tmpLocationSuffix = File.separator + System.nanoTime()
        if (isCarbonUseLocalDir) {
          val yarnStoreLocations = Util.getConfiguredLocalDirs(SparkEnv.get.conf)
          if (!isCarbonUseMultiDir && null != yarnStoreLocations && yarnStoreLocations.nonEmpty) {
            // use single dir
            storeLocation = storeLocation :+
              (yarnStoreLocations(Random.nextInt(yarnStoreLocations.length)) + tmpLocationSuffix)
            if (storeLocation == null || storeLocation.isEmpty) {
              storeLocation = storeLocation :+
                (System.getProperty("java.io.tmpdir") + tmpLocationSuffix)
            }
          } else {
            // use all the yarn dirs
            storeLocation = yarnStoreLocations.map(_ + tmpLocationSuffix)
          }
        } else {
          storeLocation =
            storeLocation :+ (System.getProperty("java.io.tmpdir") + tmpLocationSuffix)
        }
        CarbonTableOutputFormat.setTempStoreLocations(context.getConfiguration, storeLocation)
        new CarbonOutputWriter(path, context, dataSchema.map(_.dataType))
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        CarbonTablePath.CARBON_DATA_EXT
      }

    }
  }
}

case class CarbonSQLHadoopMapReduceCommitProtocol(jobId: String, path: String, isAppend: Boolean)
  extends SQLHadoopMapReduceCommitProtocol(jobId, path, isAppend) {
  override def newTaskTempFileAbsPath(taskContext: TaskAttemptContext,
      absoluteDir: String,
      ext: String): String = {
    val carbonFlow = taskContext.getConfiguration.get("carbon.commit.protocol")
    if (carbonFlow != null) {
      super.newTaskTempFile(taskContext, Some(absoluteDir), ext)
    } else {
      super.newTaskTempFileAbsPath(taskContext, absoluteDir, ext)
    }
  }
}

/**
 * It is a just class to make compile between spark 2.1 and 2.2
 */
private trait AbstractCarbonOutputWriter {
  def write(row: Row): Unit = throw new UnsupportedOperationException("call writeInternal")
  def writeInternal(row: InternalRow): Unit = {
    writeCarbon(row)
  }
  def write(row: InternalRow): Unit = {
    writeCarbon(row)
  }
  def writeCarbon(row: InternalRow): Unit
}

private class CarbonOutputWriter(path: String,
    context: TaskAttemptContext,
    fieldTypes: Seq[DataType])
  extends OutputWriter with AbstractCarbonOutputWriter {
  val partitions = getPartitionsFromPath(path, context).map(ExternalCatalogUtils.unescapePathName)
  val partitionData = if (partitions.nonEmpty) {
    partitions.map(_.split("=")(1))
  } else {
    Array.empty
  }
  val writable = new StringArrayWritable()

  private val recordWriter: CarbonRecordWriter = {

    new CarbonTableOutputFormat() {
      override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
        new Path(path)
      }
    }.getRecordWriter(context).asInstanceOf[CarbonRecordWriter]
  }

  // TODO Implement writesupport interface to support writing Row directly to recordwriter
  def writeCarbon(row: InternalRow): Unit = {
    val data = new Array[String](fieldTypes.length + partitionData.length)
    var i = 0
    while (i < fieldTypes.length) {
      if (!row.isNullAt(i)) {
        data(i) = row.getString(i)
      }
      i += 1
    }
    if (partitionData.length > 0) {
      System.arraycopy(partitionData, 0, data, fieldTypes.length, partitionData.length)
    }
    writable.set(data)
    recordWriter.write(NullWritable.get(), writable)
  }


  override def writeInternal(row: InternalRow): Unit = {
    writeCarbon(row)
  }

  override def close(): Unit = {
    recordWriter.close(context)
    val loadModel = recordWriter.getLoadModel
    val segmentPath = CarbonTablePath.getSegmentPath(loadModel.getTablePath, loadModel.getSegmentId)
    // write partition info to new file.
    val partitonList = new util.ArrayList[String]()
    partitions.foreach(partitonList.add)
    new PartitionMapFileStore().writePartitionMapFile(
      segmentPath,
      loadModel.getTaskNo,
      partitonList)
  }

  def getPartitionsFromPath(path: String, attemptContext: TaskAttemptContext): Array[String] = {
    var attemptId = attemptContext.getTaskAttemptID.toString + "/"
    if (path.indexOf(attemptId) <= 0) {
      val model = CarbonTableOutputFormat.getLoadModel(attemptContext.getConfiguration)
      attemptId = model.getTableName + "/"
    }
    val str = path.substring(path.indexOf(attemptId) + attemptId.length, path.lastIndexOf("/"))
    if (str.length > 0) {
      str.split("/")
    } else {
      Array.empty
    }
  }
}
