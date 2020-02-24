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

package org.apache.spark.sql.carbondata.execution.datasources

import java.net.URI
import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql._
import org.apache.spark.sql.carbondata.execution.datasources.readsupport.SparkUnsafeRowReadSuport
import org.apache.spark.sql.carbondata.execution.datasources.tasklisteners.{CarbonLoadTaskCompletionListenerImpl, CarbonQueryTaskCompletionListenerImpl}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.SparkTypeConverter
import org.apache.spark.util.SerializableConfiguration

import org.apache.carbondata.common.annotations.{InterfaceAudience, InterfaceStability}
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.converter.SparkDataTypeConverterImpl
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.DataMapFilter
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.indexstore.BlockletDetailInfo
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, ColumnarFormatVersion}
import org.apache.carbondata.core.metadata.schema.SchemaReader
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.scan.expression.{Expression => CarbonExpression}
import org.apache.carbondata.core.scan.expression.logical.AndExpression
import org.apache.carbondata.core.statusmanager.{FileFormat => CarbonFileFormatVersion}
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.hadoop.{CarbonInputSplit, CarbonProjection, CarbonRecordReader}
import org.apache.carbondata.hadoop.api.{CarbonFileInputFormat, CarbonInputFormat, CarbonTableOutputFormat}
import org.apache.carbondata.hadoop.internal.ObjectArrayWritable
import org.apache.carbondata.processing.loading.complexobjects.{ArrayObject, StructObject}
import org.apache.carbondata.spark.vectorreader.VectorizedCarbonRecordReader

/**
 * Used to read and write data stored in carbondata files to/from the spark execution engine.
 */
@InterfaceAudience.User
@InterfaceStability.Evolving
class SparkCarbonFileFormat extends FileFormat
  with DataSourceRegister
  with Logging
  with Serializable {

  @transient val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  /**
   * If user does not provide schema while reading the data then spark calls this method to infer
   * schema from the carbondata files.
   * It reads the schema present in carbondata files and return it.
   */
  override def inferSchema(sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    val conf = sparkSession.sessionState.newHadoopConf()
    val tablePath = options.get("path") match {
      case Some(path) =>
        FileFactory.checkAndAppendDefaultFs(path, conf)
      case _ if files.nonEmpty =>
        FileFactory.getUpdatedFilePath(files.head.getPath.getParent.toUri.toString)
      case _ =>
        return None
    }
    if (options.get(CarbonCommonConstants.SORT_COLUMNS).isDefined) {
      throw new UnsupportedOperationException("Cannot use sort columns during infer schema")
    }
    val tableInfo = SchemaReader.inferSchema(AbsoluteTableIdentifier.from(tablePath, "", ""),
      false, conf)
    val table = CarbonTable.buildFromTableInfo(tableInfo)
    var schema = new StructType
    val fields = tableInfo.getFactTable.getListOfColumns.asScala.map { col =>
      // TODO find better way to know its a child
      if (!col.getColumnName.contains(".")) {
        Some((col.getSchemaOrdinal,
          StructField(col.getColumnName,
            SparkTypeConverter.convertCarbonToSparkDataType(col, table))))
      } else {
        None
      }
    }.filter(_.nonEmpty).map(_.get)
    // Maintain the schema order.
    fields.sortBy(_._1).foreach(f => schema = schema.add(f._2))
    Some(schema)
  }

  /**
   * Add our own protocol to control the commit.
   */
  SparkSession.getActiveSession match {
    case Some(session) => session.sessionState.conf.setConfString(
      "spark.sql.sources.commitProtocolClass",
      "org.apache.spark.sql.carbondata.execution." +
      "datasources.CarbonSQLHadoopMapReduceCommitProtocol")
    case _ =>
  }

  /**
   * Prepares a write job and returns an [[OutputWriterFactory]].  Client side job preparation is
   * done here.
   */
  override def prepareWrite(sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {

    val conf = job.getConfiguration
    conf.set(CarbonCommonConstants.CARBON_WRITTEN_BY_APPNAME, sparkSession.sparkContext.appName)
    val model = CarbonSparkDataSourceUtil.prepareLoadModel(options, dataSchema)
    model.setLoadWithoutConverterStep(true)
    CarbonTableOutputFormat.setLoadModel(conf, model)
    conf.set(CarbonSQLHadoopMapReduceCommitProtocol.COMMIT_PROTOCOL, "true")

    new OutputWriterFactory {
      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        val updatedPath = if (path.endsWith(CarbonTablePath.CARBON_DATA_EXT)) {
          new Path(path).getParent.toString
        } else {
          path
        }
        context.getConfiguration.set("carbon.outputformat.writepath", updatedPath)
        // "jobid"+"x"+"taskid", task retry should have same task number
        context.getConfiguration.set("carbon.outputformat.taskno",
          context.getTaskAttemptID.getJobID.getJtIdentifier +
          context.getTaskAttemptID.getJobID.getId
          + 'x' + context.getTaskAttemptID.getTaskID.getId)
        new CarbonOutputWriter(path, context, dataSchema.fields)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        CarbonTablePath.CARBON_DATA_EXT
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


  /**
   * Writer class for carbondata files
   */
  private class CarbonOutputWriter(path: String,
      context: TaskAttemptContext,
      fieldTypes: Array[StructField]) extends OutputWriter with AbstractCarbonOutputWriter {

    private val writable = new ObjectArrayWritable

    private val cutOffDate = Integer.MAX_VALUE >> 1

    private val recordWriter: RecordWriter[NullWritable, ObjectArrayWritable] =
      new CarbonTableOutputFormat().getRecordWriter(context)

    Option(TaskContext.get()).foreach {c =>
      c.addTaskCompletionListener(CarbonLoadTaskCompletionListenerImpl(recordWriter, context))
    }

    /**
     * Write sparks internal row to carbondata record writer
     */
    def writeCarbon(row: InternalRow): Unit = {
      val data: Array[AnyRef] = extractData(row, fieldTypes)
      writable.set(data)
      recordWriter.write(NullWritable.get(), writable)
    }

    override def writeInternal(row: InternalRow): Unit = {
      writeCarbon(row)
    }

    /**
     * Convert the internal row to carbondata understandable object
     */
    private def extractData(row: InternalRow, fieldTypes: Array[StructField]): Array[AnyRef] = {
      val data = new Array[AnyRef](fieldTypes.length)
      var i = 0
      val len = fieldTypes.length
      while (i < len) {
        if (!row.isNullAt(i)) {
          fieldTypes(i).dataType match {
            case StringType =>
              data(i) = row.getString(i)
            case BinaryType =>
              data(i) = row.getBinary(i)
            case d: DecimalType =>
              data(i) = row.getDecimal(i, d.precision, d.scale).toJavaBigDecimal
            case s: StructType =>
              data(i) = new StructObject(extractData(row.getStruct(i, s.fields.length), s.fields))
            case a: ArrayType =>
              data(i) = new ArrayObject(extractData(row.getArray(i), a.elementType))
            case m: MapType =>
              data(i) = extractMapData(row.getMap(i), m)
            case d: DateType =>
              data(i) = (row.getInt(i) + cutOffDate).asInstanceOf[AnyRef]
            case d: TimestampType =>
              data(i) = (row.getLong(i) / 1000).asInstanceOf[AnyRef]
            case other =>
              data(i) = row.get(i, other)
          }
        } else {
          setNull(fieldTypes(i).dataType, data, i)
        }
        i += 1
      }
      data
    }

    private def extractMapData(data: AnyRef, mapType: MapType): ArrayObject = {
      val mapData = data.asInstanceOf[MapData]
      val keys = extractData(mapData.keyArray(), mapType.keyType)
      val values = extractData(mapData.valueArray(), mapType.valueType)
      new ArrayObject(keys.zip(values).map { case (key, value) =>
        new StructObject(Array(key, value))
      })
    }

    private def setNull(dataType: DataType, data: Array[AnyRef], i: Int) = {
      dataType match {
        case d: DateType =>
          // 1  as treated as null in carbon
          data(i) = 1.asInstanceOf[AnyRef]
        case _ =>
      }
    }

    /**
     * Convert the internal row to carbondata understandable object
     */
    private def extractData(row: ArrayData, dataType: DataType): Array[AnyRef] = {
      val data = new Array[AnyRef](row.numElements())
      var i = 0
      val len = data.length
      while (i < len) {
        if (!row.isNullAt(i)) {
          dataType match {
            case StringType =>
              data(i) = row.getUTF8String(i).toString
            case d: DecimalType =>
              data(i) = row.getDecimal(i, d.precision, d.scale).toJavaBigDecimal
            case s: StructType =>
              data(i) = new StructObject(extractData(row.getStruct(i, s.fields.length), s.fields))
            case a: ArrayType =>
              data(i) = new ArrayObject(extractData(row.getArray(i), a.elementType))
            case m: MapType =>
              data(i) = extractMapData(row.getMap(i), m)
            case d: DateType =>
              data(i) = (row.getInt(i) + cutOffDate).asInstanceOf[AnyRef]
            case d: TimestampType =>
              data(i) = (row.getLong(i) / 1000).asInstanceOf[AnyRef]
            case other => data(i) = row.get(i, dataType)
          }
        } else {
          setNull(dataType, data, i)
        }
        i += 1
      }
      data
    }

    override def close(): Unit = {
      recordWriter.close(context)
    }
  }

  override def shortName(): String = "carbon"

  override def toString: String = "carbon"

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other.isInstanceOf[SparkCarbonFileFormat]

  /**
   * Whether to support vector reader while reading data.
   * In case of complex types it is not required to support it
   */
  private def supportVector(sparkSession: SparkSession, schema: StructType): Boolean = {
    val vectorizedReader = {
      if (sparkSession.sqlContext.sparkSession.conf
        .contains(CarbonCommonConstants.ENABLE_VECTOR_READER)) {
        sparkSession.sqlContext.sparkSession.conf.get(CarbonCommonConstants.ENABLE_VECTOR_READER)
      } else if (System.getProperty(CarbonCommonConstants.ENABLE_VECTOR_READER) != null) {
        System.getProperty(CarbonCommonConstants.ENABLE_VECTOR_READER)
      } else {
        CarbonProperties.getInstance().getProperty(CarbonCommonConstants.ENABLE_VECTOR_READER,
          CarbonCommonConstants.ENABLE_VECTOR_READER_DEFAULT)
      }
    }
    vectorizedReader.toBoolean && schema.forall(_.dataType.isInstanceOf[AtomicType])
  }

  /**
   * Returns whether this format support returning columnar batch or not.
   */
  override def supportBatch(sparkSession: SparkSession, schema: StructType): Boolean = {
    val conf = sparkSession.sessionState.conf
    supportVector(sparkSession, schema) && conf.wholeStageEnabled &&
    schema.length <= conf.wholeStageMaxNumFields &&
    schema.forall(_.dataType.isInstanceOf[AtomicType])
  }

  /**
   * Returns a function that can be used to read a single carbondata file in as an
   * Iterator of InternalRow.
   */
  override def buildReaderWithPartitionValues(sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    val dataTypeMap = dataSchema.map(f => f.name -> f.dataType).toMap
    // Filter out the complex filters as carbon does not support them.
    val filter: Option[CarbonExpression] = filters.filterNot{ ref =>
      ref.references.exists{ p =>
        !dataTypeMap(p).isInstanceOf[AtomicType]
      }
    }.flatMap { filter =>
      CarbonSparkDataSourceUtil.createCarbonFilter(dataSchema, filter)
    }.reduceOption(new AndExpression(_, _))

    val projection = requiredSchema.map(_.name).toArray
    val carbonProjection = new CarbonProjection
    projection.foreach(carbonProjection.addColumn)

    val resultSchema = StructType(partitionSchema.fields ++ requiredSchema.fields)

    var supportBatchValue: Boolean = supportBatch(sparkSession, resultSchema)
    val readVector = supportVector(sparkSession, resultSchema) && supportBatchValue

    val model = CarbonSparkDataSourceUtil.prepareLoadModel(options, dataSchema)
    CarbonInputFormat
      .setTableInfo(hadoopConf, model.getCarbonDataLoadSchema.getCarbonTable.getTableInfo)
    CarbonInputFormat.setTransactionalTable(hadoopConf, false)
    CarbonInputFormat.setColumnProjection(hadoopConf, carbonProjection)
    filter match {
      case Some(c) => CarbonInputFormat
        .setFilterPredicates(hadoopConf,
          new DataMapFilter(model.getCarbonDataLoadSchema.getCarbonTable, c, true))
      case None => None
    }
    val format: CarbonFileInputFormat[Object] = new CarbonFileInputFormat[Object]
    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    file: PartitionedFile => {
      assert(file.partitionValues.numFields == partitionSchema.size)

      if (file.filePath.endsWith(CarbonTablePath.CARBON_DATA_EXT)) {
        val split = new CarbonInputSplit("null",
          new Path(new URI(file.filePath)).toString,
          file.start,
          file.length,
          file.locations,
          CarbonFileFormatVersion.COLUMNAR_V3)
        // It supports only from V3 version.
        split.setVersion(ColumnarFormatVersion.V3)
        val info = new BlockletDetailInfo()
        split.setDetailInfo(info)
        info.setBlockSize(file.length)
        // Read the footer offset and set.
        val reader = FileFactory.getFileHolder(FileFactory.getFileType(split.getFilePath),
          broadcastedHadoopConf.value.value)
        val buffer = reader
          .readByteBuffer(FileFactory.getUpdatedFilePath(split.getFilePath),
            file.length - 8,
            8)
        info.setBlockFooterOffset(buffer.getLong)
        info.setVersionNumber(split.getVersion.number())
        info.setUseMinMaxForPruning(true)
        reader.finish()
        val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
        val hadoopAttemptContext =
          new TaskAttemptContextImpl(broadcastedHadoopConf.value.value, attemptId)
        val model = format.createQueryModel(split, hadoopAttemptContext)
        model.setConverter(new SparkDataTypeConverterImpl)
        model.setPreFetchData(false)
        // As file format uses on heap, no need to free unsafe memory
        model.setFreeUnsafeMemory(false)
        val carbonReader = if (readVector) {
          model.setDirectVectorFill(true)
          val vectorizedReader = new VectorizedCarbonRecordReader(model,
            null,
            supportBatchValue.toString)
          vectorizedReader.initialize(split, hadoopAttemptContext)
          vectorizedReader.initBatch(MemoryMode.ON_HEAP, partitionSchema, file.partitionValues)
          logDebug(s"Appending $partitionSchema ${ file.partitionValues }")
          vectorizedReader
        } else {
          val reader = new CarbonRecordReader(model,
            new SparkUnsafeRowReadSuport(requiredSchema), broadcastedHadoopConf.value.value)
          reader.initialize(split, hadoopAttemptContext)
          reader
        }

        val iter = new RecordReaderIterator(carbonReader)
        Option(TaskContext.get()).foreach{context =>
          context.addTaskCompletionListener(
          CarbonQueryTaskCompletionListenerImpl(
            iter.asInstanceOf[RecordReaderIterator[InternalRow]]))
        }

        if (carbonReader.isInstanceOf[VectorizedCarbonRecordReader] && readVector) {
          iter.asInstanceOf[Iterator[InternalRow]]
        } else {
          val fullSchema = requiredSchema.toAttributes ++ partitionSchema.toAttributes
          val joinedRow = new JoinedRow()
          val appendPartitionColumns = GenerateUnsafeProjection.generate(fullSchema, fullSchema)
          if (partitionSchema.length == 0) {
            // There is no partition columns
            iter.asInstanceOf[Iterator[InternalRow]]
          } else {
            iter.asInstanceOf[Iterator[InternalRow]]
              .map(d => appendPartitionColumns(joinedRow(d, file.partitionValues)))
          }
        }
      }
      else {
        Iterator.empty
      }
    }
  }

}

/**
 * Since carbon writes 2 files carbondata files and index file , but spark cannot understand two
 * files so added custom protocol to copy the files in case of custom partition location.
 */
case class CarbonSQLHadoopMapReduceCommitProtocol(jobId: String, path: String, isAppend: Boolean)
  extends SQLHadoopMapReduceCommitProtocol(jobId, path, isAppend) {

  override def newTaskTempFileAbsPath(
      taskContext: TaskAttemptContext, absoluteDir: String, ext: String): String = {
    val carbonFlow = taskContext.getConfiguration.get(
      CarbonSQLHadoopMapReduceCommitProtocol.COMMIT_PROTOCOL)
    val tempPath = super.newTaskTempFileAbsPath(taskContext, absoluteDir, ext)
    // Call only in case of carbon flow.
    if (carbonFlow != null) {
      // Create subfolder with uuid and write carbondata files
      val path = new Path(tempPath)
      val uuid = path.getName.substring(0, path.getName.indexOf("-part-"))
      new Path(new Path(path.getParent, uuid), path.getName).toString
    } else {
      tempPath
    }
  }

  override def commitJob(jobContext: JobContext,
      taskCommits: Seq[FileCommitProtocol.TaskCommitMessage]): Unit = {
    val carbonFlow = jobContext.getConfiguration.get(
      CarbonSQLHadoopMapReduceCommitProtocol.COMMIT_PROTOCOL)
    var updatedTaskCommits = taskCommits
    // Call only in case of carbon flow.
    if (carbonFlow != null) {
      val (allAbsPathFiles, allPartitionPaths) =
        // spark 2.1 and 2.2 case
        if (taskCommits.exists(_.obj.isInstanceOf[Map[String, String]])) {
        (taskCommits.map(_.obj.asInstanceOf[Map[String, String]]), null)
      } else {
          // spark 2.3 and above
        taskCommits.map(_.obj.asInstanceOf[(Map[String, String], Set[String])]).unzip
      }
      val filesToMove = allAbsPathFiles.foldLeft(Map[String, String]())(_ ++ _)
      val fs = new Path(path).getFileSystem(jobContext.getConfiguration)
      // Move files from stage directory to actual location.
      filesToMove.foreach{case (src, dest) =>
        val srcPath = new Path(src)
        val name = srcPath.getName
        // Get uuid from spark's stage filename
        val uuid = name.substring(0, name.indexOf("-part-"))
        // List all the files under the uuid location
        val list = fs.listStatus(new Path(new Path(src).getParent, uuid))
        // Move all these files to actual folder.
        list.foreach{ f =>
          fs.rename(f.getPath, new Path(new Path(dest).getParent, f.getPath.getName))
        }
      }
      updatedTaskCommits = if (allPartitionPaths == null) {
        taskCommits.map(f => new FileCommitProtocol.TaskCommitMessage(Map.empty))
      } else {
        taskCommits.zipWithIndex.map{f =>
          new FileCommitProtocol.TaskCommitMessage((Map.empty, allPartitionPaths(f._2)))
        }
      }
    }
    super.commitJob(jobContext, updatedTaskCommits)
  }
}
object CarbonSQLHadoopMapReduceCommitProtocol {
  val COMMIT_PROTOCOL = "carbon.commit.protocol"
}


