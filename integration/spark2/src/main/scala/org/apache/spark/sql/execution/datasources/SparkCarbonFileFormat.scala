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

import java.net.URI

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.optimizer.CarbonFilters
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types._

import org.apache.carbondata.common.annotations.{InterfaceAudience, InterfaceStability}
import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.{DataMapChooser, DataMapStoreManager, Segment}
import org.apache.carbondata.core.indexstore.blockletindex.SegmentIndexFileStore
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonMetadata, ColumnarFormatVersion}
import org.apache.carbondata.core.readcommitter.LatestFilesReadCommittedScope
import org.apache.carbondata.core.reader.CarbonHeaderReader
import org.apache.carbondata.core.scan.expression.Expression
import org.apache.carbondata.core.scan.expression.logical.AndExpression
import org.apache.carbondata.core.scan.model.QueryModel
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.hadoop.{CarbonInputSplit, CarbonProjection, CarbonRecordReader, InputMetricsStats}
import org.apache.carbondata.hadoop.api.{CarbonFileInputFormat, CarbonInputFormat, CarbonTableOutputFormat}
import org.apache.carbondata.hadoop.internal.ObjectArrayWritable
import org.apache.carbondata.sdk.file.{CarbonWriterUtil, Field, Schema}
import org.apache.carbondata.spark.util.CarbonScalaUtil

@InterfaceAudience.User
@InterfaceStability.Evolving
class SparkCarbonFileFormat extends FileFormat
  with DataSourceRegister
  with Logging
  with Serializable {

  @transient val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getName)

  override def inferSchema(sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    val filePaths = if (options.isEmpty) {
      val carbondataFiles = files.seq.filter { each =>
        if (each.isFile) {
          each.getPath.getName.contains(".carbondata")
        } else {
          false
        }
      }

      carbondataFiles.map { each =>
        each.getPath.toString
      }.toList.asJava
    } else {
      CarbonUtil.getFilePathExternalFilePath(
        options("path"))
    }

    if (filePaths.size() == 0) {
      throw new SparkException("CarbonData file is not present in the location mentioned in DDL")
    }
    val carbonHeaderReader: CarbonHeaderReader = new CarbonHeaderReader(filePaths.get(0))
    val fileHeader = carbonHeaderReader.readHeader
    val table_columns: java.util.List[org.apache.carbondata.format.ColumnSchema] = fileHeader
      .getColumn_schema
    var colArray = ArrayBuffer[StructField]()
    for (i <- 0 to table_columns.size() - 1) {
      val col = CarbonUtil.thriftColumnSchemaToWrapperColumnSchema(table_columns.get(i))
      colArray += new StructField(col.getColumnName,
        CarbonScalaUtil.convertCarbonToSparkDataType(col.getDataType), false)
    }
    colArray.+:(Nil)

    Some(StructType(colArray))
  }

  override def prepareWrite(sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {

    val conf = job.getConfiguration
    val schema = new Schema(dataSchema.fields.map {
      field =>
        new Field(field.name, field.dataType.typeName)
    })
    try {
      val model = CarbonWriterUtil.getInstance()
        .createLoadModel(options("path"),
          false,
          System.currentTimeMillis(),
          options.getOrElse("taskNo", null),
          options.asJava,
          schema)
      model.setFactFilePath(options("path"))
      CarbonTableOutputFormat.setLoadModel(conf, model)
    } catch {
      case e: Exception =>
        throw new SparkException("Writing failed: ", e)
    }


    new OutputWriterFactory {
      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {

        context.getConfiguration.set("carbon.outputformat.writepath", options("path"))
        new CarbonOutputWriter(path, context, dataSchema.fields.map(_.dataType))
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        ""
      }
    }
  }

  private class CarbonOutputWriter(path: String,
      context: TaskAttemptContext,
      fieldTypes: Seq[DataType]) extends OutputWriter {

    val writable = new ObjectArrayWritable

    val recordWriter: RecordWriter[NullWritable, ObjectArrayWritable] =
      new CarbonTableOutputFormat().getRecordWriter(context)

    override def write(row: InternalRow): Unit = {
      val data = new Array[AnyRef](fieldTypes.length)
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
      writable.set(data)
      recordWriter.write(NullWritable.get(), writable)
    }

    override def close(): Unit = {
      recordWriter.close(context)
    }
  }

  override def shortName(): String = "carbonfile"

  override def toString: String = "carbonfile"

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
    vectorizedReader.toBoolean
  }


  override def supportBatch(sparkSession: SparkSession, schema: StructType): Boolean = {
    val conf = sparkSession.sessionState.conf
    conf.wholeStageEnabled &&
    schema.length <= conf.wholeStageMaxNumFields &&
    schema.forall(_.dataType.isInstanceOf[AtomicType])
  }


  def createVectorizedCarbonRecordReader(queryModel: QueryModel,
      inputMetricsStats: InputMetricsStats, enableBatch: String): RecordReader[Void, Object] = {
    val name = "org.apache.carbondata.spark.vectorreader.VectorizedCarbonRecordReader"
    try {
      val cons = Class.forName(name).getDeclaredConstructors
      cons.head.setAccessible(true)
      cons.head.newInstance(queryModel, inputMetricsStats, enableBatch)
        .asInstanceOf[RecordReader[Void, Object]]
    } catch {
      case e: Exception =>
        LOGGER.error(e)
        null
    }
  }

  override def buildReaderWithPartitionValues(sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {

    val filter : Option[Expression] = filters.flatMap { filter =>
      CarbonFilters.createCarbonFilter(dataSchema, filter)
    }.reduceOption(new AndExpression(_, _))

    val projection = requiredSchema.map(_.name).toArray
    val carbonProjection = new CarbonProjection
    projection.foreach(carbonProjection.addColumn)

    val conf = new Configuration()
    val jobConf = new JobConf(conf)
    SparkHadoopUtil.get.addCredentials(jobConf)
    val job = Job.getInstance(jobConf)
    var supportBatchValue: Boolean = false

    val readVector = supportVector(sparkSession, dataSchema)
    if (readVector) {
      supportBatchValue = supportBatch(sparkSession, dataSchema)
    }

    CarbonInputFormat.setTableName(job.getConfiguration, "externaldummy")
    CarbonInputFormat.setDatabaseName(job.getConfiguration, "default")
    CarbonMetadata.getInstance.removeTable("default_externaldummy")
    val format: CarbonFileInputFormat[Object] = new CarbonFileInputFormat[Object]

    file: PartitionedFile => {
      assert(file.partitionValues.numFields == partitionSchema.size)

      if (file.filePath.endsWith(CarbonCommonConstants.FACT_FILE_EXT)) {
        val fileSplit =
          new FileSplit(new Path(new URI(file.filePath)), file.start, file.length, Array.empty)

        val conf1 = new Configuration()
        val tablePath = options("path")
        lazy val identifier: AbsoluteTableIdentifier = AbsoluteTableIdentifier.from(
          tablePath,
          "default",
          "externaldummy")
        val split = CarbonInputSplit.from("null", "0", fileSplit, ColumnarFormatVersion.V3, null)


        val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
        conf1.set("mapreduce.input.carboninputformat.tableName", "externaldummy")
        conf1.set("mapreduce.input.carboninputformat.databaseName", "default")
        conf1.set("mapreduce.input.fileinputformat.inputdir", tablePath)
        CarbonInputFormat.setColumnProjection(conf1, carbonProjection)
        filter match {
          case Some(c) => CarbonInputFormat.setFilterPredicates(conf1, c)
          case None => None
        }
        val attemptContext = new TaskAttemptContextImpl(conf1, attemptId)

        val model = format.createQueryModel(split, attemptContext)

        val readCommittedScope = new LatestFilesReadCommittedScope(
          identifier.getTablePath)

        val segments = new java.util.ArrayList[Segment]()
        val seg = new Segment(readCommittedScope.getSegmentList.head.getLoadName,
          null,
          readCommittedScope)
        segments.add(seg)

        val indexFiles = new SegmentIndexFileStore().getIndexFilesFromSegment(tablePath)
        if (indexFiles.size() == 0) {
          throw new SparkException("Index file not present to read the carbondata file")
        }

        val tab = model.getTable
        DataMapStoreManager.getInstance().clearDataMaps(identifier)

        val dataMapExprWrapper = new DataMapChooser(tab).choose(
          model.getFilterExpressionResolverTree)

        // TODO : handle the partition for CarbonFileLevelFormat
        val prunedBlocklets = dataMapExprWrapper.prune(segments, null)

        val detailInfo = prunedBlocklets.get(0).getDetailInfo
        detailInfo.readColumnSchema(detailInfo.getColumnSchemaBinary)
        split.setDetailInfo(detailInfo)

        val carbonReader = if (readVector) {
          val vectorizedReader = createVectorizedCarbonRecordReader(model,
            null,
            supportBatchValue.toString)
          vectorizedReader.initialize(split, attemptContext)
          logDebug(s"Appending $partitionSchema ${ file.partitionValues }")
          vectorizedReader
        } else {
          val reader = new CarbonRecordReader(model,
            format.getReadSupportClass(attemptContext.getConfiguration), null)
          reader.initialize(split, attemptContext)
          reader
        }

        val iter = new RecordReaderIterator(carbonReader)
        Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => iter.close()))

        iter.asInstanceOf[Iterator[InternalRow]]
      }
      else {
        Iterator.empty
      }
    }
  }
}


