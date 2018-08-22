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

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql._
import org.apache.spark.sql.carbondata.execution.datasources.readsupport.SparkUnsafeRowReadSuport
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration

import org.apache.carbondata.common.annotations.{InterfaceAudience, InterfaceStability}
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.converter.SparkDataTypeConverterImpl
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.indexstore.BlockletDetailInfo
import org.apache.carbondata.core.metadata.ColumnarFormatVersion
import org.apache.carbondata.core.reader.CarbonHeaderReader
import org.apache.carbondata.core.scan.expression.{Expression => CarbonExpression}
import org.apache.carbondata.core.scan.expression.logical.AndExpression
import org.apache.carbondata.core.statusmanager.{FileFormat => CarbonFileFormatVersion}
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.hadoop.{CarbonInputSplit, CarbonProjection, CarbonRecordReader}
import org.apache.carbondata.hadoop.api.{CarbonFileInputFormat, CarbonInputFormat, CarbonTableOutputFormat}
import org.apache.carbondata.hadoop.internal.ObjectArrayWritable
import org.apache.carbondata.processing.loading.complexobjects.{ArrayObject, StructObject}
import org.apache.carbondata.spark.vectorreader.VectorizedCarbonRecordReader

@InterfaceAudience.User
@InterfaceStability.Evolving
class SparkCarbonFileFormat extends FileFormat
  with DataSourceRegister
  with Logging
  with Serializable {

  @transient val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  override def inferSchema(sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    val filePath = if (options.isEmpty) {
      val carbondataFiles = files.seq.filter { each =>
        if (each.isFile) {
          each.getPath.getName.contains(".carbondata")
        } else {
          false
        }
      }

      if (carbondataFiles.nonEmpty) {
        carbondataFiles.head.getPath.toString
      } else {
        null
      }
    } else {
      CarbonUtil.getFilePathExternalFilePath(
        options("path"))
    }

    if (filePath == null) {
      throw new SparkException("CarbonData file is not present in the location mentioned in DDL")
    }
    val carbonHeaderReader: CarbonHeaderReader = new CarbonHeaderReader(filePath)
    val fileHeader = carbonHeaderReader.readHeader
    val table_columns: java.util.List[org.apache.carbondata.format.ColumnSchema] = fileHeader
      .getColumn_schema
    var colArray = ArrayBuffer[StructField]()
    for (i <- 0 until table_columns.size()) {
      val col = CarbonUtil.thriftColumnSchemaToWrapperColumnSchema(table_columns.get(i))
      colArray += new StructField(col.getColumnName,
        CarbonSparkDataSourceUtil.convertCarbonToSparkDataType(col.getDataType), false)
    }
    colArray.+:(Nil)

    Some(StructType(colArray))
  }

  override def prepareWrite(sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {

    val conf = job.getConfiguration

    val model = CarbonSparkDataSourceUtil.prepareLoadModel(options, dataSchema)
    model.setLoadWithoutConverterStep(true)
    CarbonTableOutputFormat.setLoadModel(conf, model)

    new OutputWriterFactory {
      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        val updatedPath = if (path.endsWith(".carbondata")) {
          new Path(path).getParent.toString
        } else {
          path
        }
        context.getConfiguration.set("carbon.outputformat.writepath", updatedPath)
        context.getConfiguration.set("carbon.outputformat.taskno", System.nanoTime() + "")
        new CarbonOutputWriter(path, context, dataSchema.fields)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        ".carbondata"
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
      fieldTypes: Array[StructField]) extends OutputWriter with AbstractCarbonOutputWriter {

    val writable = new ObjectArrayWritable

    val recordWriter: RecordWriter[NullWritable, ObjectArrayWritable] =
      new CarbonTableOutputFormat().getRecordWriter(context)


    def writeCarbon(row: InternalRow): Unit = {
      val data: Array[AnyRef] = extractData(row, fieldTypes)
      writable.set(data)
      recordWriter.write(NullWritable.get(), writable)
    }

    override def writeInternal(row: InternalRow): Unit = {
      writeCarbon(row)
    }

    private def extractData(row: InternalRow, fieldTypes: Array[StructField]): Array[AnyRef] = {
      val data = new Array[AnyRef](fieldTypes.length)
      var i = 0
      while (i < fieldTypes.length) {
        if (!row.isNullAt(i)) {
          fieldTypes(i).dataType match {
            case StringType =>
              data(i) = row.getString(i)
            case d: DecimalType =>
              data(i) = row.getDecimal(i, d.precision, d.scale).toJavaBigDecimal
            case s: StructType =>
              data(i) = new StructObject(extractData(row.getStruct(i, s.fields.length), s.fields))
            case s: ArrayType =>
              data(i) = new ArrayObject(extractData(row.getArray(i), s.elementType))
            case other =>
              data(i) = row.get(i, other)
          }
        }
        i += 1
      }
      data
    }

    private def extractData(row: ArrayData, fieldType: DataType): Array[AnyRef] = {
      val data = new Array[AnyRef](row.numElements())
      var i = 0
      while (i < data.length) {

        fieldType match {
          case d: DecimalType =>
            data(i) = row.getDecimal(i, d.precision, d.scale).toJavaBigDecimal
          case s: StructType =>
            data(i) = new StructObject(extractData(row.getStruct(i, s.fields.length), s.fields))
          case s: ArrayType =>
            data(i) = new ArrayObject(extractData(row.getArray(i), s.elementType))
          case other => data(i) = row.get(i, fieldType)
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

  def supportVector(sparkSession: SparkSession, schema: StructType): Boolean = {
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


  override def supportBatch(sparkSession: SparkSession, schema: StructType): Boolean = {
    val conf = sparkSession.sessionState.conf
    conf.wholeStageEnabled &&
    schema.length <= conf.wholeStageMaxNumFields &&
    schema.forall(_.dataType.isInstanceOf[AtomicType])
  }


  override def buildReaderWithPartitionValues(sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {
    val filter: Option[CarbonExpression] = filters.flatMap { filter =>
      CarbonSparkDataSourceUtil.createCarbonFilter(dataSchema, filter)
    }.reduceOption(new AndExpression(_, _))

    val projection = requiredSchema.map(_.name).toArray
    val carbonProjection = new CarbonProjection
    projection.foreach(carbonProjection.addColumn)

    var supportBatchValue: Boolean = false

    val resultSchema = StructType(partitionSchema.fields ++ requiredSchema.fields)
    val readVector = supportVector(sparkSession, resultSchema)
    if (readVector) {
      supportBatchValue = supportBatch(sparkSession, resultSchema)
    }
    val model = CarbonSparkDataSourceUtil.prepareLoadModel(options, dataSchema)
    CarbonInputFormat
      .setTableInfo(hadoopConf, model.getCarbonDataLoadSchema.getCarbonTable.getTableInfo)
    CarbonInputFormat.setTransactionalTable(hadoopConf, false)
    CarbonInputFormat.setColumnProjection(hadoopConf, carbonProjection)
    filter match {
      case Some(c) => CarbonInputFormat.setFilterPredicates(hadoopConf, c)
      case None => None
    }
    val format: CarbonFileInputFormat[Object] = new CarbonFileInputFormat[Object]
    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    file: PartitionedFile => {
      assert(file.partitionValues.numFields == partitionSchema.size)

      if (!(file.filePath.endsWith(CarbonTablePath.INDEX_FILE_EXT) ||
            file.filePath.endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT))) {
        val split = new CarbonInputSplit("null",
          new Path(file.filePath),
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
        val reader = FileFactory.getFileHolder(FileFactory.getFileType(file.filePath))
        val buffer = reader
          .readByteBuffer(FileFactory.getUpdatedFilePath(file.filePath), file.length - 8, 8)
        info.setBlockFooterOffset(buffer.getLong)
        info.setVersionNumber(split.getVersion.number())
        info.setUseMinMaxForPruning(true)
        reader.finish()
        val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
        val hadoopAttemptContext =
          new TaskAttemptContextImpl(broadcastedHadoopConf.value.value, attemptId)
        val model = format.createQueryModel(split, hadoopAttemptContext)
        model.setConverter(new SparkDataTypeConverterImpl)
        val carbonReader = if (readVector) {
          val vectorizedReader = new VectorizedCarbonRecordReader(model,
            null,
            supportBatchValue.toString)
          vectorizedReader.initialize(split, hadoopAttemptContext)
          vectorizedReader.initBatch(MemoryMode.ON_HEAP, partitionSchema, file.partitionValues)
          logDebug(s"Appending $partitionSchema ${ file.partitionValues }")
          vectorizedReader
        } else {
          val reader = new CarbonRecordReader(model,
            new SparkUnsafeRowReadSuport(requiredSchema), null)
          reader.initialize(split, hadoopAttemptContext)
          reader
        }

        val iter = new RecordReaderIterator(carbonReader)
        Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => iter.close()))

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


