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
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

import org.apache.commons.lang3.StringUtils
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
import org.apache.carbondata.core.indexstore
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentManager, SegmentStatusManager}
import org.apache.carbondata.core.util.{CarbonProperties, DataTypeConverterImpl, DataTypeUtil, ObjectSerializationUtil}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.hadoop.api.{CarbonOutputCommitter, CarbonTableOutputFormat}
import org.apache.carbondata.hadoop.api.CarbonTableOutputFormat.CarbonRecordWriter
import org.apache.carbondata.hadoop.internal.ObjectArrayWritable
import org.apache.carbondata.processing.loading.model.{CarbonLoadModel, CarbonLoadModelBuilder, LoadOption}
import org.apache.carbondata.processing.util.CarbonBadRecordUtil
import org.apache.carbondata.spark.util.{CarbonScalaUtil, SparkDataTypeConverterImpl, Util}

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
    conf.set("carbon.commit.protocol", "carbon.commit.protocol")
    job.setOutputFormatClass(classOf[CarbonTableOutputFormat])
    val table = CarbonEnv.getCarbonTable(
      TableIdentifier(options("tableName"), options.get("dbName")))(sparkSession)
    val model = new CarbonLoadModel
    val carbonProperty = CarbonProperties.getInstance()
    val optionsFinal = LoadOption.fillOptionWithDefaultValue(options.asJava)
    val tableProperties = table.getTableInfo.getFactTable.getTableProperties
    optionsFinal.put("sort_scope", tableProperties.asScala.getOrElse("sort_scope",
      carbonProperty.getProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_SORT_SCOPE,
        carbonProperty.getProperty(CarbonCommonConstants.LOAD_SORT_SCOPE,
          CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT))))
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
    model.setUseOnePass(options.getOrElse("onepass", "false").toBoolean)
    model.setDictionaryServerHost(options.getOrElse("dicthost", null))
    model.setDictionaryServerPort(options.getOrElse("dictport", "-1").toInt)
    CarbonTableOutputFormat.setOverwrite(conf, options("overwrite").toBoolean)
    model.setLoadWithoutConverterStep(true)

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
      model.setCurrentDetailVO(new SegmentManager()
        .getSegment(model.getCarbonDataLoadSchema.getCarbonTable.getAbsoluteTableIdentifier,
          model.getSegmentId))
    }
    // Set the update timestamp if user sets in case of update query. It needs to be updated
    // in load status update time
    val updateTimeStamp = options.get("updatetimestamp")
    if (updateTimeStamp.isDefined) {
      conf.set(CarbonTableOutputFormat.UPADTE_TIMESTAMP, updateTimeStamp.get)
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
        val model = CarbonTableOutputFormat.getLoadModel(context.getConfiguration)
        val isCarbonUseMultiDir = CarbonProperties.getInstance().isUseMultiTempDir
        var storeLocation: Array[String] = Array[String]()
        val isCarbonUseLocalDir = CarbonProperties.getInstance()
          .getProperty("carbon.use.local.dir", "false").equalsIgnoreCase("true")


        val taskNumber = generateTaskNumber(path, context, model.getSegmentId)
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
    taskNo : String,
    model: CarbonLoadModel)
  extends OutputWriter with AbstractCarbonOutputWriter {

  val converter = new DataTypeConverterImpl

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
  lazy val currPartitions: util.List[indexstore.PartitionSpec] = {
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
    val updatedPartitions = partitions.map(splitPartition)
    updatedPartitions.foreach {
      case (k, v) => linkedMap.put(k, v)
    }
    (linkedMap, updatePartitions(updatedPartitions.map(_._2)))
  } else {
    (mutable.LinkedHashMap.empty[String, String], Array.empty)
  }

  private def splitPartition(p: String) = {
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

  lazy val writePath = {
    val updatedPath = getPartitionPath(path, context, model)
    // in case of partition location specified by user then search the partitions from the current
    // partitions to get the corresponding partitions.
    if (partitions.isEmpty) {
      val writeSpec = new indexstore.PartitionSpec(null, updatedPath)
      val index = currPartitions.indexOf(writeSpec)
      if (index > -1) {
        val spec = currPartitions.get(index)
        spec.getPartitions.asScala.map(splitPartition).foreach {
          case (k, v) => updatedPartitions.put(k, v)
        }
        partitionData = updatePartitions(updatedPartitions.map(_._2).toSeq)
      }
    }
    updatedPath
  }

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
      if (staticPartition != null && staticPartition.get(col.getColumnName.toLowerCase)) {
        val converetedVal =
          CarbonScalaUtil.convertStaticPartitions(
            partitionData(index),
            col,
            model.getCarbonDataLoadSchema.getCarbonTable)
        if (col.hasEncoding(Encoding.DICTIONARY)) {
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

  private val recordWriter: CarbonRecordWriter = {
    context.getConfiguration.set("carbon.outputformat.taskno", taskNo)
    context.getConfiguration.set("carbon.outputformat.writepath",
      writePath + "/" + model.getSegmentId + "_" + model.getFactTimeStamp + ".tmp")
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
    // write partition info to new file.
    val partitionList = new util.ArrayList[String]()
    val formattedPartitions =
    // All dynamic partitions need to be converted to proper format
      CarbonScalaUtil.updatePartitions(
        updatedPartitions.asInstanceOf[mutable.LinkedHashMap[String, String]],
        model.getCarbonDataLoadSchema.getCarbonTable)
    formattedPartitions.foreach(p => partitionList.add(p._1 + "=" + p._2))
    SegmentFileStore.writeSegmentFile(
      model.getTablePath,
      taskNo,
      writePath,
      model.getSegmentId + "_" + model.getFactTimeStamp + "",
      partitionList)
  }

  def getPartitionPath(path: String,
      attemptContext: TaskAttemptContext,
      model: CarbonLoadModel): String = {
    if (updatedPartitions.nonEmpty) {
      val formattedPartitions =
      // All dynamic partitions need to be converted to proper format
        CarbonScalaUtil.updatePartitions(
          updatedPartitions.asInstanceOf[mutable.LinkedHashMap[String, String]],
          model.getCarbonDataLoadSchema.getCarbonTable)
      val partitionstr = formattedPartitions.map{p =>
        ExternalCatalogUtils.escapePathName(p._1) + "=" + ExternalCatalogUtils.escapePathName(p._2)
      }.mkString(CarbonCommonConstants.FILE_SEPARATOR)
      model.getCarbonDataLoadSchema.getCarbonTable.getTablePath +
        CarbonCommonConstants.FILE_SEPARATOR + partitionstr
    } else {
      var updatedPath = FileFactory.getUpdatedFilePath(path)
      updatedPath.substring(0, updatedPath.lastIndexOf("/"))
    }
  }

  def getPartitionsFromPath(
      path: String,
      attemptContext: TaskAttemptContext,
      model: CarbonLoadModel): Array[String] = {
    var attemptId = attemptContext.getTaskAttemptID.toString + "/"
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
}
