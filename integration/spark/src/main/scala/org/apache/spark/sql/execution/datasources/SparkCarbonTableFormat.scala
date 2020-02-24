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

import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.{Job, JobContext, TaskAttemptContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types._
import org.apache.spark.TaskContext
import org.apache.spark.sql.execution.command.management.CommonLoadUtils

import org.apache.carbondata.core.constants.{CarbonCommonConstants, CarbonLoadOptionConstants}
import org.apache.carbondata.core.datastore.compression.CompressorFactory
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.indexstore
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatusManager}
import org.apache.carbondata.core.util.{CarbonProperties, DataTypeConverter, DataTypeConverterImpl, DataTypeUtil, ObjectSerializationUtil, OutputFilesInfoHolder, ThreadLocalSessionInfo}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.hadoop.api.{CarbonOutputCommitter, CarbonTableOutputFormat}
import org.apache.carbondata.hadoop.api.CarbonTableOutputFormat.CarbonRecordWriter
import org.apache.carbondata.hadoop.internal.ObjectArrayWritable
import org.apache.carbondata.processing.loading.model.{CarbonLoadModel, CarbonLoadModelBuilder, LoadOption}
import org.apache.carbondata.processing.util.CarbonBadRecordUtil
import org.apache.carbondata.spark.util.{CarbonScalaUtil, CommonUtil}

class SparkCarbonTableFormat
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

  SparkSession.getActiveSession.get.sessionState.conf.setConfString(
    "spark.sql.sources.commitProtocolClass",
    "org.apache.spark.sql.execution.datasources.CarbonSQLHadoopMapReduceCommitProtocol")

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
    conf.set("carbondata.commit.protocol", "carbondata.commit.protocol")
    conf.set("mapreduce.task.deleteTaskAttemptPath", "false")
    conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    job.setOutputFormatClass(classOf[CarbonTableOutputFormat])
    val table = CarbonEnv.getCarbonTable(
      TableIdentifier(options("tableName"), options.get("dbName")))(sparkSession)
    val model = new CarbonLoadModel
    val columnCompressor = table.getTableInfo.getFactTable.getTableProperties.asScala
      .getOrElse(CarbonCommonConstants.COMPRESSOR,
        CompressorFactory.getInstance().getCompressor.getName)
    model.setColumnCompressor(columnCompressor)
    model.setOutputFilesInfoHolder(new OutputFilesInfoHolder())

    val carbonProperty = CarbonProperties.getInstance()
    val optionsFinal = LoadOption.fillOptionWithDefaultValue(options.asJava)
    val tableProperties = table.getTableInfo.getFactTable.getTableProperties
    optionsFinal.put("sort_scope", tableProperties.asScala.getOrElse("sort_scope",
      carbonProperty.getProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_SORT_SCOPE,
        carbonProperty.getProperty(CarbonCommonConstants.LOAD_SORT_SCOPE,
          CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT))))
    // If index handler property is configured, set flag to indicate index columns are present.
    // So that InputProcessorStepWithNoConverterImpl can generate the values for those columns,
    // convert them and then apply sort/write steps.
    val handler =
    table.getTableInfo.getFactTable.getTableProperties.get(CarbonCommonConstants.INDEX_HANDLER)
    if (handler != null) {
      val sortScope = optionsFinal.get("sort_scope")
      if (sortScope.equalsIgnoreCase(CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT)) {
        // Index handler non-schema column must be sorted
        optionsFinal.put("sort_scope", "LOCAL_SORT")
      }
      model.setIndexColumnsPresent(true)
    }
    optionsFinal
      .put("bad_record_path", CarbonBadRecordUtil.getBadRecordsPath(options.asJava, table))
    val partitionStr =
      table.getTableInfo.getFactTable.getPartitionInfo.getColumnSchemaList.asScala.map(
        _.getColumnName.toLowerCase).mkString(",")
    optionsFinal.put(
      "fileheader",
      dataSchema.fields.map(_.name.toLowerCase).mkString(",") + "," + partitionStr)
    val optionsLocal = new mutable.HashMap[String, String]()
    optionsLocal ++= options
    optionsLocal += (("header", "false"))
    new CarbonLoadModelBuilder(table).build(
      optionsLocal.toMap.asJava,
      optionsFinal,
      model,
      conf)
    CarbonTableOutputFormat.setOverwrite(conf, options("overwrite").toBoolean)
    if (options.contains("no_rearrange_of_rows")) {
      model.setLoadWithoutConverterWithoutReArrangeStep(true)
    } else {
      model.setLoadWithoutConverterStep(true)
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

    val currPartition = options.getOrElse("currentpartition", null)
    if (currPartition != null) {
      conf.set("carbon.currentpartition", currPartition)
    }
    // Update with the current in progress load.
    val currEntry = options.getOrElse("currentloadentry", null)
    if (currEntry != null) {
      val loadEntry =
        ObjectSerializationUtil.convertStringToObject(currEntry).asInstanceOf[LoadMetadataDetails]
      val details =
        SegmentStatusManager.readLoadMetadata(CarbonTablePath.getMetadataPath(model.getTablePath))
      model.setSegmentId(loadEntry.getLoadName)
      model.setFactTimeStamp(loadEntry.getLoadStartTime)
      val list = new util.ArrayList[LoadMetadataDetails](details.toList.asJava)
      list.add(loadEntry)
      model.setLoadMetadataDetails(list)
    }
    // Set the update timestamp if user sets in case of update query. It needs to be updated
    // in load status update time
    val updateTimeStamp = options.get("updatetimestamp")
    if (updateTimeStamp.isDefined) {
      conf.set(CarbonTableOutputFormat.UPADTE_TIMESTAMP, updateTimeStamp.get)
    }
    conf.set(CarbonCommonConstants.CARBON_WRITTEN_BY_APPNAME, sparkSession.sparkContext.appName)
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
        val model = CarbonTableOutputFormat.getLoadModel(context.getConfiguration)
        val appName = context.getConfiguration.get(CarbonCommonConstants.CARBON_WRITTEN_BY_APPNAME)
        CarbonProperties.getInstance().addProperty(
          CarbonCommonConstants.CARBON_WRITTEN_BY_APPNAME, appName)
        val taskNumber = generateTaskNumber(path, context, model.getSegmentId)
        val storeLocation = CommonUtil.getTempStoreLocations(taskNumber)
        CarbonTableOutputFormat.setTempStoreLocations(context.getConfiguration, storeLocation)
        new CarbonOutputWriter(path, context, dataSchema.map(_.dataType), taskNumber, model)
      }

      /**
       * Generate taskid using the taskid of taskcontext and the path. It should be unique in case
       * of partition tables.
       */
      private def generateTaskNumber(path: String,
          context: TaskAttemptContext, segmentId: String): String = {
        var partitionNumber: java.lang.Long = taskIdMap.get(path)
        if (partitionNumber == null) {
          partitionNumber = counter.incrementAndGet()
          // Generate taskid using the combination of taskid and partition number to make it unique.
          taskIdMap.put(path, partitionNumber)
        }
        val taskID = context.getTaskAttemptID.getTaskID.getId
        // In case of compaction the segmentId will be like 1.1. Therefore replacing '.' with ""
        // so that unique number can be generated
        CarbonScalaUtil.generateUniqueNumber(taskID, segmentId.replace(".", ""), partitionNumber)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        CarbonTablePath.CARBON_DATA_EXT
      }

    }
  }
  override def equals(other: Any): Boolean = other.isInstanceOf[SparkCarbonTableFormat]
}

case class CarbonSQLHadoopMapReduceCommitProtocol(jobId: String, path: String, isAppend: Boolean)
  extends SQLHadoopMapReduceCommitProtocol(jobId, path, isAppend) {

  override def setupTask(taskContext: TaskAttemptContext): Unit = {
    if (isCarbonDataFlow(taskContext.getConfiguration)) {
      ThreadLocalSessionInfo.setConfigurationToCurrentThread(taskContext.getConfiguration)
    }
    super.setupTask(taskContext)
  }

  /**
   * collect carbon partition info from TaskCommitMessage list
   */
  override def commitJob(jobContext: JobContext,
      taskCommits: Seq[TaskCommitMessage]): Unit = {
    if (isCarbonDataFlow(jobContext.getConfiguration)) {
      var dataSize = 0L
      val partitions =
        taskCommits
          .flatMap { taskCommit =>
            taskCommit.obj match {
              case (map: Map[String, String], _) =>
                val partition = map.get("carbon.partitions")
                val size = map.get("carbon.datasize")
                if (size.isDefined) {
                  dataSize = dataSize + java.lang.Long.parseLong(size.get)
                }
                if (partition.isDefined) {
                  ObjectSerializationUtil
                    .convertStringToObject(partition.get)
                    .asInstanceOf[util.ArrayList[String]]
                    .asScala
                } else {
                  Array.empty[String]
                }
              case _ => Array.empty[String]
            }
          }
          .distinct
          .toList
          .asJava

      jobContext.getConfiguration.set(
        "carbon.output.partitions.name",
        ObjectSerializationUtil.convertObjectToString(partitions))
      jobContext.getConfiguration.set("carbon.datasize", dataSize.toString)

      val newTaskCommits = taskCommits.map { taskCommit =>
        taskCommit.obj match {
          case (map: Map[String, String], set) =>
            new TaskCommitMessage(
              map
                .filterNot(e => "carbon.partitions".equals(e._1) || "carbon.datasize".equals(e._1)),
              set)
          case _ => taskCommit
        }
      }
      super
        .commitJob(jobContext, newTaskCommits)
    } else {
      super
        .commitJob(jobContext, taskCommits)
    }
  }

  /**
   * set carbon partition info into TaskCommitMessage
   */
  override def commitTask(
      taskContext: TaskAttemptContext
  ): FileCommitProtocol.TaskCommitMessage = {
    var taskMsg = super.commitTask(taskContext)
    if (isCarbonDataFlow(taskContext.getConfiguration)) {
      ThreadLocalSessionInfo.unsetAll()
      val partitions: String = taskContext.getConfiguration.get("carbon.output.partitions.name", "")
      val files = taskContext.getConfiguration.get("carbon.output.files.name", "")
      var sum = 0L
      var indexSize = 0L
      if (!StringUtils.isEmpty(files)) {
        val filesList = ObjectSerializationUtil
          .convertStringToObject(files)
          .asInstanceOf[util.ArrayList[String]]
          .asScala
        for (file <- filesList) {
          if (file.contains(".carbondata")) {
            sum += java.lang.Long.parseLong(file.substring(file.lastIndexOf(":") + 1))
          } else if (file.contains(".carbonindex")) {
            indexSize += java.lang.Long.parseLong(file.substring(file.lastIndexOf(":") + 1))
          }
        }
      }
      if (!StringUtils.isEmpty(partitions)) {
        taskMsg = taskMsg.obj match {
          case (map: Map[String, String], set) =>
            new TaskCommitMessage(
              map ++ Map("carbon.partitions" -> partitions, "carbon.datasize" -> sum.toString),
              set)
          case _ => taskMsg
        }
      }
      // Update outputMetrics with carbondata and index size
      TaskContext.get().taskMetrics().outputMetrics.setBytesWritten(sum + indexSize)
    }
    taskMsg
  }

  override def abortTask(taskContext: TaskAttemptContext): Unit = {
    super.abortTask(taskContext)
    if (isCarbonDataFlow(taskContext.getConfiguration)) {
      val files = taskContext.getConfiguration.get("carbon.output.files.name", "")
      if (!StringUtils.isEmpty(files)) {
        val filesList = ObjectSerializationUtil
          .convertStringToObject(files)
          .asInstanceOf[util.ArrayList[String]]
          .asScala
        for (file <- filesList) {
          val outputFile: String = file.substring(0, file.lastIndexOf(":"))
          if (outputFile.endsWith(CarbonTablePath.CARBON_DATA_EXT)) {
            FileFactory
              .deleteAllCarbonFilesOfDir(FileFactory
                .getCarbonFile(outputFile,
                  taskContext.getConfiguration))
          }
        }
      }
      ThreadLocalSessionInfo.unsetAll()
    }
  }

  override def newTaskTempFileAbsPath(taskContext: TaskAttemptContext,
      absoluteDir: String,
      ext: String): String = {
    if (isCarbonFileFlow(taskContext.getConfiguration) ||
        isCarbonDataFlow(taskContext.getConfiguration)) {
      super.newTaskTempFile(taskContext, Some(absoluteDir), ext)
    } else {
      super.newTaskTempFileAbsPath(taskContext, absoluteDir, ext)
    }
  }

  /**
   * set carbon temp folder to task temp folder
   */
  override def newTaskTempFile(
      taskContext: TaskAttemptContext,
      dir: Option[String],
      ext: String): String = {
    if (isCarbonDataFlow(taskContext.getConfiguration)) {
      val path = super.newTaskTempFile(taskContext, dir, ext)
      taskContext.getConfiguration.set("carbon.newTaskTempFile.path", path)
      val model = CarbonTableOutputFormat.getLoadModel(taskContext.getConfiguration)
      val partitions = CarbonOutputWriter.getPartitionsFromPath(
        path, taskContext, model).map(ExternalCatalogUtils.unescapePathName)
      val staticPartition: util.HashMap[String, Boolean] = {
        val staticPart = taskContext.getConfiguration.get("carbon.staticpartition")
        if (staticPart != null) {
          ObjectSerializationUtil.convertStringToObject(
            staticPart).asInstanceOf[util.HashMap[String, Boolean]]
        } else {
          null
        }
      }
      val converter = new DataTypeConverterImpl
      var (updatedPartitions, partitionData) = if (partitions.nonEmpty) {
        val linkedMap = mutable.LinkedHashMap[String, String]()
        val updatedPartitions = partitions.map(CarbonOutputWriter.splitPartition)
        updatedPartitions.foreach {
          case (k, v) => linkedMap.put(k, v)
        }
        (linkedMap, CarbonOutputWriter.updatePartitions(
          updatedPartitions.map(_._2), model, staticPartition, converter))
      } else {
        (mutable.LinkedHashMap.empty[String, String], Array.empty)
      }
      val currPartitions: util.List[indexstore.PartitionSpec] = {
        val currParts = taskContext.getConfiguration.get("carbon.currentpartition")
        if (currParts != null) {
          ObjectSerializationUtil.convertStringToObject(
            currParts).asInstanceOf[util.List[indexstore.PartitionSpec]]
        } else {
          new util.ArrayList[indexstore.PartitionSpec]()
        }
      }
      val writePath =
        CarbonOutputWriter.getPartitionPath(path, taskContext, model, updatedPartitions)
      writePath + "/" + model.getSegmentId + "_" + model.getFactTimeStamp + ".tmp"
    } else {
      super.newTaskTempFile(taskContext, dir, ext)
    }

  }

  private def isCarbonFileFlow(conf: Configuration): Boolean = {
    conf.get("carbon.commit.protocol") != null
  }

  private def isCarbonDataFlow(conf: Configuration): Boolean = {
    conf.get("carbondata.commit.protocol") != null
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
    nonPartitionFieldTypes: Seq[DataType],
    taskNo : String,
    model: CarbonLoadModel)
  extends OutputWriter with AbstractCarbonOutputWriter {
  var actualPath = path
  var tmpPath = context.getConfiguration.get("carbon.newTaskTempFile.path", actualPath)
  val converter = new DataTypeConverterImpl

  val partitions = CarbonOutputWriter.getPartitionsFromPath(
    tmpPath, context, model).map(ExternalCatalogUtils.unescapePathName)
  val staticPartition: util.HashMap[String, Boolean] = {
    val staticPart = context.getConfiguration.get("carbon.staticpartition")
    if (staticPart != null) {
      ObjectSerializationUtil.convertStringToObject(
        staticPart).asInstanceOf[util.HashMap[String, Boolean]]
    } else {
      null
    }
  }
  val currPartitions: util.List[indexstore.PartitionSpec] = {
    val currParts = context.getConfiguration.get("carbon.currentpartition")
    if (currParts != null) {
      ObjectSerializationUtil.convertStringToObject(
        currParts).asInstanceOf[util.List[indexstore.PartitionSpec]]
    } else {
      new util.ArrayList[indexstore.PartitionSpec]()
    }
  }
  var (updatedPartitions, partitionData) = if (partitions.nonEmpty) {
    val linkedMap = mutable.LinkedHashMap[String, String]()
    val updatedPartitions = partitions.map(CarbonOutputWriter.splitPartition)
    updatedPartitions.foreach {
      case (k, v) => linkedMap.put(k, v)
    }
    (linkedMap, CarbonOutputWriter.updatePartitions(
      updatedPartitions.map(_._2), model, staticPartition, converter))
  } else {
    val tempUpdatedPartitions = mutable.LinkedHashMap.empty[String, String]
    var tempPartitionData = Array.empty[AnyRef]
    val updatePath = CarbonOutputWriter.getPartitionPath(
      tmpPath, context, model, tempUpdatedPartitions)
    val writeSpec = new indexstore.PartitionSpec(null, updatePath)
    val index = currPartitions.indexOf(writeSpec)
    if (index > -1) {
      val spec = currPartitions.get(index)
      spec.getPartitions.asScala.map(CarbonOutputWriter.splitPartition).foreach {
        case (k, v) => tempUpdatedPartitions.put(k, v)
      }
      tempPartitionData = CarbonOutputWriter.updatePartitions(
        tempUpdatedPartitions.map(_._2).toSeq, model, staticPartition, converter)
    }
    actualPath = updatePath + "/" + model.getSegmentId + "_" + model.getFactTimeStamp + ".tmp"
    (tempUpdatedPartitions, tempPartitionData)
  }

  val writable = new ObjectArrayWritable

  private val recordWriter: CarbonRecordWriter = {
    context.getConfiguration.set("carbon.outputformat.taskno", taskNo)
    context.getConfiguration.set("carbon.outputformat.writepath", actualPath)
    new CarbonTableOutputFormat() {
      override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
        new Path(tmpPath)
      }
    }.getRecordWriter(context).asInstanceOf[CarbonRecordWriter]
  }

  // TODO Implement writesupport interface to support writing Row directly to recordwriter
  def writeCarbon(row: InternalRow): Unit = {
    val numOfColumns = nonPartitionFieldTypes.length + partitionData.length
    val data: Array[AnyRef] = CommonUtil.getObjectArrayFromInternalRowAndConvertComplexType(
      row,
      nonPartitionFieldTypes,
      numOfColumns)
    if (partitionData.length > 0) {
      System.arraycopy(partitionData, 0, data, nonPartitionFieldTypes.length, partitionData.length)
    }
    writable.set(data)
    recordWriter.write(NullWritable.get(), writable)
  }

  override def writeInternal(row: InternalRow): Unit = {
    writeCarbon(row)
  }

  override def close(): Unit = {
    recordWriter.close(context)
    ThreadLocalSessionInfo.setConfigurationToCurrentThread(context.getConfiguration)
  }
}


object CarbonOutputWriter {

  def getPartitionsFromPath(
      path: String,
      attemptContext: TaskAttemptContext,
      model: CarbonLoadModel): Array[String] = {
    val attemptId = attemptContext.getTaskAttemptID.toString + "/"
    if (path.indexOf(attemptId) > -1) {
      val str = path.substring(path.indexOf(attemptId) + attemptId.length, path.lastIndexOf("/"))
      if (str.length > 0) {
        str.split("/")
      } else {
        Array.empty
      }
    } else {
      Array.empty
    }
  }

  def splitPartition(p: String): (String, String) = {
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

  /**
   * update partition folder path
   */
  def updatePartitions(
      partitionData: Seq[String],
      model: CarbonLoadModel,
      staticPartition: util.HashMap[String, Boolean],
      converter: DataTypeConverter
  ): Array[AnyRef] = {
    model.getCarbonDataLoadSchema.getCarbonTable.getTableInfo.getFactTable.getPartitionInfo
      .getColumnSchemaList.asScala.zipWithIndex.map { case (col, index) =>

      val dataType = if (col.getDataType.equals(DataTypes.DATE)) {
        DataTypes.INT
      } else if (col.getDataType.equals(DataTypes.TIMESTAMP) ||
                 col.getDataType.equals(DataTypes.DATE)) {
        DataTypes.LONG
      } else {
        col.getDataType
      }
      if (staticPartition != null && staticPartition.get(col.getColumnName.toLowerCase)) {
        // TODO: why not use CarbonScalaUtil.convertToDateAndTimeFormats ?
        val converetedVal =
          CarbonScalaUtil.convertStaticPartitions(partitionData(index), col)
        if (col.getDataType.equals(DataTypes.DATE)) {
          converetedVal.toInt.asInstanceOf[AnyRef]
        } else {
          DataTypeUtil.getDataBasedOnDataType(
            converetedVal,
            dataType,
            converter)
        }
      } else {
        DataTypeUtil.getDataBasedOnDataType(partitionData(index), dataType, converter)
      }
    }.toArray
  }

  def getPartitionPath(path: String,
      attemptContext: TaskAttemptContext,
      model: CarbonLoadModel,
      updatedPartitions: mutable.LinkedHashMap[String, String]
  ): String = {
    if (updatedPartitions.nonEmpty) {
      // All dynamic partitions need to be converted to proper format
      val formattedPartitions = CarbonScalaUtil.updatePartitions(
        updatedPartitions.asInstanceOf[mutable.LinkedHashMap[String, String]],
        model.getCarbonDataLoadSchema.getCarbonTable)
      val partitionstr = formattedPartitions.map { p =>
        ExternalCatalogUtils.escapePathName(p._1) + "=" + ExternalCatalogUtils.escapePathName(p._2)
      }.mkString(CarbonCommonConstants.FILE_SEPARATOR)
      model.getCarbonDataLoadSchema.getCarbonTable.getTablePath +
      CarbonCommonConstants.FILE_SEPARATOR + partitionstr
    } else {
      var updatedPath = FileFactory.getUpdatedFilePath(path)
      updatedPath.substring(0, updatedPath.lastIndexOf("/"))
    }
  }
}

