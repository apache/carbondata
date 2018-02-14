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
import java.text.SimpleDateFormat
import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._
import scala.collection.mutable
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
import org.apache.spark.sql.types._

import org.apache.carbondata.core.constants.{CarbonCommonConstants, CarbonLoadOptionConstants}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.util.{CarbonProperties, DataTypeUtil}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.hadoop.api.{CarbonOutputCommitter, CarbonTableOutputFormat}
import org.apache.carbondata.hadoop.api.CarbonTableOutputFormat.CarbonRecordWriter
import org.apache.carbondata.hadoop.internal.ObjectArrayWritable
import org.apache.carbondata.hadoop.util.ObjectSerializationUtil
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.spark.util.{CarbonScalaUtil, DataLoadingUtil, Util}

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
    val table = CarbonEnv.getCarbonTable(
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
    val optionsLocal = new mutable.HashMap[String, String]()
    optionsLocal ++= options
    optionsLocal += (("header", "false"))
    DataLoadingUtil.buildCarbonLoadModel(
      table,
      carbonProperty,
      optionsLocal.toMap,
      optionsFinal,
      model,
      conf
    )
    model.setPartitionId("0")
    model.setUseOnePass(options.getOrElse("onepass", "false").toBoolean)
    model.setDictionaryServerHost(options.getOrElse("dicthost", null))
    model.setDictionaryServerPort(options.getOrElse("dictport", "-1").toInt)
    CarbonTableOutputFormat.setOverwrite(conf, options("overwrite").toBoolean)
    model.setPartitionLoad(true)
    // Set the update timestamp if user sets in case of update query. It needs to be updated
    // in load status update time
    val updateTimeStamp = options.get("updatetimestamp")
    if (updateTimeStamp.isDefined) {
      conf.set(CarbonTableOutputFormat.UPADTE_TIMESTAMP, updateTimeStamp.get)
      model.setFactTimeStamp(updateTimeStamp.get.toLong)
    }
    val staticPartition = options.getOrElse("staticpartition", null)
    if (staticPartition != null) {
      conf.set("carbon.staticpartition", staticPartition)
    }
    // In case of update query there is chance to remove the older segments, so here we can set
    // the to be deleted segments to mark as delete while updating tablestatus
    val segemntsTobeDeleted = options.get("segmentsToBeDeleted")
    if (segemntsTobeDeleted.isDefined) {
      conf.set(CarbonTableOutputFormat.SEGMENTS_TO_BE_DELETED, segemntsTobeDeleted.get)
    }
    CarbonTableOutputFormat.setLoadModel(conf, model)

    new OutputWriterFactory {

      /**
       * counter used for generating task numbers. This is used to generate unique partition numbers
       * in case of partitioning
       */
      val counter = new AtomicLong()
      val taskIdMap = new ConcurrentHashMap[String, java.lang.Long]()

      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        val isCarbonUseMultiDir = CarbonProperties.getInstance().isUseMultiTempDir
        var storeLocation: Array[String] = Array[String]()
        val isCarbonUseLocalDir = CarbonProperties.getInstance()
          .getProperty("carbon.use.local.dir", "false").equalsIgnoreCase("true")


        val taskNumber = generateTaskNumber(path, context)
        val tmpLocationSuffix =
          File.separator + "carbon" + System.nanoTime() + File.separator + taskNumber
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
        new CarbonOutputWriter(path, context, dataSchema.map(_.dataType), taskNumber)
      }

      /**
       * Generate taskid using the taskid of taskcontext and the path. It should be unique in case
       * of partition tables.
       */
      private def generateTaskNumber(path: String,
          context: TaskAttemptContext): String = {
        var partitionNumber: java.lang.Long = taskIdMap.get(path)
        if (partitionNumber == null) {
          partitionNumber = counter.incrementAndGet()
          // Generate taskid using the combination of taskid and partition number to make it unique.
          taskIdMap.put(path, partitionNumber)
        }
        val taskID = context.getTaskAttemptID.getTaskID.getId
        String.valueOf(Math.pow(10, 5).toInt + taskID) +
          String.valueOf(partitionNumber + Math.pow(10, 5).toInt)
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
    fieldTypes: Seq[DataType],
    taskNo : String)
  extends OutputWriter with AbstractCarbonOutputWriter {
  val model = CarbonTableOutputFormat.getLoadModel(context.getConfiguration)
  val partitions =
    getPartitionsFromPath(path, context, model).map(ExternalCatalogUtils.unescapePathName)
  val staticPartition: util.HashMap[String, Boolean] = {
    val staticPart = context.getConfiguration.get("carbon.staticpartition")
    if (staticPart != null) {
      ObjectSerializationUtil.convertStringToObject(
        staticPart).asInstanceOf[util.HashMap[String, Boolean]]
    } else {
      null
    }
  }
  lazy val (updatedPartitions, partitionData) = if (partitions.nonEmpty) {
    val updatedPartitions = partitions.map{ p =>
      val value = p.substring(p.indexOf("=") + 1, p.length)
      val col = p.substring(0, p.indexOf("="))
      // NUll handling case. For null hive creates with this special name
      if (value.equals("__HIVE_DEFAULT_PARTITION__")) {
        (col, null)
        // we should replace back the special string with empty value.
      } else if (value.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL)) {
        (col, "")
      } else {
        (col, value)
      }
    }

    if (staticPartition != null) {
      val table = model.getCarbonDataLoadSchema.getCarbonTable
      var timeStampformatString = model.getTimestampformat
      if (timeStampformatString.isEmpty) {
        timeStampformatString = model.getDefaultTimestampFormat
      }
      val timeFormat = new SimpleDateFormat(timeStampformatString)
      var dateFormatString = model.getDateFormat
      if (dateFormatString.isEmpty) {
        dateFormatString = model.getDefaultDateFormat
      }
      val dateFormat = new SimpleDateFormat(dateFormatString)
      val formattedPartitions = updatedPartitions.map {case (col, value) =>
        // Only convert the static partitions to the carbon format and use it while loading data
        // to carbon.
        (col, value)
      }
      (formattedPartitions, updatePartitions(formattedPartitions.map(_._2)))
    } else {
      (updatedPartitions, updatePartitions(updatedPartitions.map(_._2)))
    }
  } else {
    (Map.empty[String, String].toArray, Array.empty)
  }

  lazy val writePath = getPartitionPath(path, context, model)

  val writable = new ObjectArrayWritable

  private def updatePartitions(partitionData: Seq[String]): Array[AnyRef] = {
    model.getCarbonDataLoadSchema.getCarbonTable.getTableInfo.getFactTable.getPartitionInfo
      .getColumnSchemaList.asScala.zipWithIndex.map { case (col, index) =>

      val dataType = if (col.hasEncoding(Encoding.DICTIONARY)) {
        DataTypes.INT
      } else if (col.getDataType.equals(DataTypes.TIMESTAMP) ||
                         col.getDataType.equals(DataTypes.DATE)) {
        DataTypes.LONG
      } else {
        col.getDataType
      }
      if (staticPartition != null) {
        DataTypeUtil.getDataBasedOnDataType(
          CarbonScalaUtil.convertStaticPartitions(
            partitionData(index),
            col,
            model.getCarbonDataLoadSchema.getCarbonTable),
          dataType)
      } else {
        DataTypeUtil.getDataBasedOnDataType(partitionData(index), dataType)
      }
    }.toArray
  }

  private val recordWriter: CarbonRecordWriter = {
    context.getConfiguration.set("carbon.outputformat.taskno", taskNo)
    context.getConfiguration.set("carbon.outputformat.writepath",
      writePath + "/" + model.getFactTimeStamp + ".tmp")
    new CarbonTableOutputFormat() {
      override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
        new Path(path)
      }
    }.getRecordWriter(context).asInstanceOf[CarbonRecordWriter]
  }

  // TODO Implement writesupport interface to support writing Row directly to recordwriter
  def writeCarbon(row: InternalRow): Unit = {
    val data = new Array[AnyRef](fieldTypes.length + partitionData.length)
    var i = 0
    while (i < fieldTypes.length) {
      if (!row.isNullAt(i)) {
        fieldTypes(i) match {
          case StringType =>
            data(i) = row.getString(i)
          case d: DecimalType =>
            data(i) = row.getDecimal(i, d.precision, d.scale).toJavaBigDecimal
          case other =>
            data(i) = row.get(i, other)
        }
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
    // write partition info to new file.
    val partitonList = new util.ArrayList[String]()
    val formattedPartitions =
    // All dynamic partitions need to be converted to proper format
      CarbonScalaUtil.updatePartitions(
        updatedPartitions.toMap,
        loadModel.getCarbonDataLoadSchema.getCarbonTable)
    formattedPartitions.foreach(p => partitonList.add(p._1 + "=" + p._2))
    new SegmentFileStore().writeSegmentFile(
      loadModel.getTablePath,
      loadModel.getTaskNo,
      writePath,
      model.getFactTimeStamp + "",
      partitonList)
  }

  def getPartitionPath(path: String,
      attemptContext: TaskAttemptContext,
      model: CarbonLoadModel): String = {
    if (updatedPartitions.nonEmpty) {
      val formattedPartitions =
      // All dynamic partitions need to be converted to proper format
        CarbonScalaUtil.updatePartitions(
          updatedPartitions.toMap,
          model.getCarbonDataLoadSchema.getCarbonTable)
      val partitionstr = formattedPartitions.map{p =>
        ExternalCatalogUtils.escapePathName(p._1) + "=" + ExternalCatalogUtils.escapePathName(p._2)
      }.mkString(CarbonCommonConstants.FILE_SEPARATOR)
      model.getCarbonDataLoadSchema.getCarbonTable.getTablePath +
        CarbonCommonConstants.FILE_SEPARATOR + partitionstr
    } else {
      FileFactory.getUpdatedFilePath(path)
    }
  }

  def getPartitionsFromPath(
      path: String,
      attemptContext: TaskAttemptContext,
      model: CarbonLoadModel): Array[String] = {
    var attemptId = attemptContext.getTaskAttemptID.toString + "/"
    if (path.indexOf(attemptId) <= 0) {

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
