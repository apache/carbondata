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

package org.apache.spark.sql

import java.net.URI
import java.util

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.{ParquetInputFormat, ParquetRecordReader}
import org.apache.parquet.hadoop.codec.CodecConfig
import org.apache.spark.{TaskContext, TaskKilledException}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, JoinedRow, UnsafeRow}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.util.CompressionCodecs
import org.apache.spark.sql.execution.DataSourceScanExec
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.{ParquetLogRedirector, ParquetOutputWriter, ParquetReadSupport, VectorizedParquetRecordReader}
import org.apache.spark.sql.execution.datasources.text.{TextOptions, TextOutputWriter}
import org.apache.spark.sql.optimizer.CarbonFilters
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, Filter, RelationProvider}
import org.apache.spark.sql.types.{AtomicType, IntegerType, StructField, StructType}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.{DataMapStoreManager, TableDataMap}
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, ColumnarFormatVersion}
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema
import org.apache.carbondata.core.reader.CarbonHeaderReader
import org.apache.carbondata.core.scan.expression.logical.AndExpression
import org.apache.carbondata.core.scan.model.QueryModel
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil, TaskMetricsMap, ThreadLocalSessionInfo}
import org.apache.carbondata.hadoop.api.{CarbonFileInputFormat, CarbonTableInputFormat, DataMapJob}
import org.apache.carbondata.hadoop.streaming.CarbonStreamRecordReader
import org.apache.carbondata.hadoop.{CarbonInputSplit, CarbonProjection, CarbonRecordReader, InputMetricsStats}
import org.apache.carbondata.spark.CarbonOption
import org.apache.carbondata.spark.rdd.{CarbonSparkPartition, SparkDataMapJob}
import org.apache.carbondata.spark.util.CarbonScalaUtil


class CarbonFileLevelFormat extends FileFormat
  with DataSourceRegister
  with Logging
  with Serializable {

  @transient val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  override def inferSchema(sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    val filePaths = CarbonUtil.getFilePathExternalFilePath(
      options.get("path").get)
    // + "/Fact/Part0/Segment_null")
    val carbonHeaderReader: CarbonHeaderReader = new CarbonHeaderReader(filePaths.get(0))
    val fileHeader = carbonHeaderReader.readHeader
    val table_columns: java.util.List[org.apache.carbondata.format.ColumnSchema] = fileHeader
      .getColumn_schema
    var colArray = ArrayBuffer[StructField]()
    // CatalystSqlParser.parseDataType(schema).asInstanceOf[StructType]
    for (i <- 0 to table_columns.size() - 1) {
      val col = CarbonUtil.thriftColumnSchmeaToWrapperColumnSchema(table_columns.get(i))
      colArray += (new StructField(col.getColumnName,
        CarbonScalaUtil.convertCarbonToSparkDataType(col.getDataType), false))
    }
    colArray.+:(Nil)

    Some(StructType(colArray))
  }

  override def prepareWrite(sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {

    new OutputWriterFactory {
      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        new TextOutputWriter(path, dataSchema, context)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        ".txt" + CodecStreams.getCompressionExtension(context)
      }
    }
  }

  override def shortName(): String = "CarbonDataFileFormat"

  override def toString: String = "CarbonDataFileFormat"

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other.isInstanceOf[CarbonFileLevelFormat]

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

    val conf = sparkSession.sessionState.conf
    conf.wholeStageEnabled && vectorizedReader.toBoolean &&
    schema.length <= conf.wholeStageMaxNumFields &&
    schema.forall(_.dataType.isInstanceOf[AtomicType])
  }

  def createVectorizedCarbonRecordReader(queryModel: QueryModel,
      inputMetricsStats: InputMetricsStats): RecordReader[Void, Object] = {
    val name = "org.apache.carbondata.spark.vectorreader.VectorizedCarbonRecordReader"
    try {
      val cons = Class.forName(name).getDeclaredConstructors
      cons.head.setAccessible(true)
      cons.head.newInstance(queryModel, inputMetricsStats).asInstanceOf[RecordReader[Void, Object]]
    } catch {
      case e: Exception =>
        LOGGER.error(e)
        null
    }
  }


//  private def createFileInputFormat(conf: Configuration): CarbonFileInputFormat[Object] = {
//    val format = new CarbonFileInputFormat[Object]
//    CarbonFileInputFormat.setTablePath(conf,
//      identifier.appendWithLocalPrefix(identifier.getTablePath))
//    CarbonFileInputFormat.setQuerySegment(conf, identifier)
//    CarbonFileInputFormat.setFilterPredicates(conf, filterExpression)
//    CarbonFileInputFormat.setColumnProjection(conf, columnProjection)
//    CarbonFileInputFormat.setDataMapJob(conf, new SparkDataMapJob)
//    if (CarbonProperties.getInstance().getProperty(
//      CarbonCommonConstants.USE_DISTRIBUTED_DATAMAP,
//      CarbonCommonConstants.USE_DISTRIBUTED_DATAMAP_DEFAULT).toBoolean) {
//      CarbonTableInputFormat.setDataMapJob(conf, new SparkDataMapJob)
//    }
//
//    // when validate segments is disabled in thread local update it to CarbonTableInputFormat
//    val carbonSessionInfo = ThreadLocalSessionInfo.getCarbonSessionInfo
//    if (carbonSessionInfo != null) {
//      CarbonTableInputFormat.setValidateSegmentsToAccess(conf, carbonSessionInfo.getSessionParams
//        .getProperty(CarbonCommonConstants.VALIDATE_CARBON_INPUT_SEGMENTS +
//                     identifier.getCarbonTableIdentifier.getDatabaseName + "." +
//                     identifier.getCarbonTableIdentifier.getTableName, "true").toBoolean)
//    }
//    format
//  }


  override def buildReaderWithPartitionValues(sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {

    val filter = filters.flatMap { filter =>
      CarbonFilters.createCarbonFilter(dataSchema, filter)
    }.reduceOption(new AndExpression(_, _))

    val projection = requiredSchema.map(_.name).toArray
    val carbonProjection = new CarbonProjection
    projection.foreach(carbonProjection.addColumn)

    val conf = new Configuration()
    val jobConf = new JobConf(conf)
    SparkHadoopUtil.get.addCredentials(jobConf)
    val job = Job.getInstance(jobConf)

    val readVector = supportVector(sparkSession, dataSchema)
    CarbonFileInputFormat.setTableName(job.getConfiguration, "dummyexternal")
    CarbonFileInputFormat.setDatabaseName(job.getConfiguration, "default")
    val dataMapJob: DataMapJob = CarbonFileInputFormat.getDataMapJob(job.getConfiguration)
    val format = new CarbonFileInputFormat[Object]



    // val inputSplits = format.getSplits(job).asInstanceOf[CarbonSparkPartition].split.value

    (file: PartitionedFile) => {
      assert(file.partitionValues.numFields == partitionSchema.size)

      val fileSplit =
        new FileSplit(new Path(new URI(file.filePath)), file.start, file.length, Array.empty)


      val path : String = options.get("path").get
      val endindex : Int = path.indexOf("Fact") - 1
      val tablePath = path.substring(0, endindex)
      lazy val identifier: AbsoluteTableIdentifier = AbsoluteTableIdentifier.from(
        tablePath,
        "default",
        "externaldummy")

      val split = CarbonInputSplit.from("null", "0", fileSplit, ColumnarFormatVersion.V3 , null)
      val blockletMap: TableDataMap = DataMapStoreManager.getInstance
        .chooseDataMap(identifier)

      // val split = format.getSplits(job).asInstanceOf[CarbonSparkPartition].split.value

      //split.setDetailInfo()
      val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
      val conf1 = new Configuration()
      conf1.set("mapreduce.input.carboninputformat.tableName", "externaldummy")
      conf1.set("mapreduce.input.carboninputformat.databaseName", "default")
      conf1.set("mapreduce.input.fileinputformat.inputdir", options.get("path").get)
      CarbonFileInputFormat.setColumnProjection(conf1, carbonProjection)
      val attemptContext = new TaskAttemptContextImpl(conf1, attemptId)

      val model = format.createQueryModel(split, attemptContext)

      var segments  = new java.util.ArrayList[String]()
      segments.add("null")
      var partition  = new java.util.ArrayList[String]()

      val prunedBlocklets = blockletMap
        .prune(segments, model.getFilterExpressionResolverTree, partition)
     //  split.setDetailInfo(prunedBlocklets.get
      //      val carbonTable = createTable(dataSchema).getMeta
      //      val queryModel = carbonTable.createQuery(projection, filter)

      //      // Try to push down filters when filter push-down is enabled.
      //      // Notice: This push-down is RowGroups level, not individual records.
      //      if (pushed.isDefined) {
      //        ParquetInputFormat.setFilterPredicate(hadoopAttemptContext.getConfiguration, pushed.get)
      //      }
      split.setDetailInfo(prunedBlocklets.get(0).getDetailInfo)
      val carbonReader = if (readVector) {
        val vectorizedReader = createVectorizedCarbonRecordReader(model, null)
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
      // UnsafeRowParquetRecordReader appends the columns internally to avoid another copy.
      //      if (carbonReader.isInstanceOf[VectorizedCarbonRecordReader] && readVector) {
      //        iter.asInstanceOf[Iterator[InternalRow]]
      //      } else {
      //        val fullSchema = requiredSchema.toAttributes ++ partitionSchema.toAttributes
      //        val joinedRow = new JoinedRow()
      //        val appendPartitionColumns = GenerateUnsafeProjection.generate(fullSchema, fullSchema)
      //
      //        // This is a horrible erasure hack...  if we type the iterator above, then it actually check
      //        // the type in next() and we get a class cast exception.  If we make that function return
      //        // Object, then we can defer the cast until later!
      //        if (partitionSchema.length == 0) {
      //          // There is no partition columns
      //          iter.asInstanceOf[Iterator[InternalRow]]
      //        } else {
      //          iter.asInstanceOf[Iterator[InternalRow]]
      //            .map(d => appendPartitionColumns(joinedRow(d, file.partitionValues)))
      //        }
      //      }
    }
  }


//  override def buildReader(
//      sparkSession: SparkSession,
//      dataSchema: StructType,
//      partitionSchema: StructType,
//      requiredSchema: StructType,
//      filters: Seq[Filter],
//      options: Map[String, String],
//      hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {
//
//    val filter = filters.flatMap { filter =>
//      CarbonFilters.createCarbonFilter(dataSchema, filter)
//    }.reduceOption(new AndExpression(_, _))
//
//    val projection = requiredSchema.map(_.name).toArray
//
//    val conf = new Configuration()
//    val jobConf = new JobConf(conf)
//    SparkHadoopUtil.get.addCredentials(jobConf)
//    val job = Job.getInstance(jobConf)
//
//    val readVector = supportVector(sparkSession, dataSchema)
//    val format = new CarbonFileInputFormat[Object]
//    // val inputSplits = format.getSplits(job).asInstanceOf[CarbonSparkPartition].split.value
//
//    (file: PartitionedFile) => {
//      assert(file.partitionValues.numFields == partitionSchema.size)
//
//      val fileSplit =
//        new FileSplit(new Path(new URI(file.filePath)), file.start, file.length, Array.empty)
//
//      val split = CarbonInputSplit.from("null", "0", fileSplit, ColumnarFormatVersion.V3 , null)
//
//      val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
//      val attemptContext = new TaskAttemptContextImpl(new Configuration(), attemptId)
//      val model = format.createQueryModel(split, attemptContext)
////      val carbonTable = createTable(dataSchema).getMeta
////      val queryModel = carbonTable.createQuery(projection, filter)
//
////      // Try to push down filters when filter push-down is enabled.
////      // Notice: This push-down is RowGroups level, not individual records.
////      if (pushed.isDefined) {
////        ParquetInputFormat.setFilterPredicate(hadoopAttemptContext.getConfiguration, pushed.get)
////      }
//        val carbonReader = if (readVector) {
//          val vectorizedReader = createVectorizedCarbonRecordReader(model, null)
//          vectorizedReader.initialize(split, attemptContext)
//          logDebug(s"Appending $partitionSchema ${ file.partitionValues }")
//          vectorizedReader
//        } else {
//          val reader = new CarbonRecordReader(model,
//            format.getReadSupportClass(attemptContext.getConfiguration), null)
//          reader.initialize(split, attemptContext)
//          reader
//       }
//
//      val iter = new RecordReaderIterator(carbonReader)
//      Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => iter.close()))
//
//      iter.asInstanceOf[Iterator[InternalRow]]
//      // UnsafeRowParquetRecordReader appends the columns internally to avoid another copy.
////      if (carbonReader.isInstanceOf[VectorizedCarbonRecordReader] && readVector) {
////        iter.asInstanceOf[Iterator[InternalRow]]
////      } else {
////        val fullSchema = requiredSchema.toAttributes ++ partitionSchema.toAttributes
////        val joinedRow = new JoinedRow()
////        val appendPartitionColumns = GenerateUnsafeProjection.generate(fullSchema, fullSchema)
////
////        // This is a horrible erasure hack...  if we type the iterator above, then it actually check
////        // the type in next() and we get a class cast exception.  If we make that function return
////        // Object, then we can defer the cast until later!
////        if (partitionSchema.length == 0) {
////          // There is no partition columns
////          iter.asInstanceOf[Iterator[InternalRow]]
////        } else {
////          iter.asInstanceOf[Iterator[InternalRow]]
////            .map(d => appendPartitionColumns(joinedRow(d, file.partitionValues)))
////        }
////      }
//    }
//  }
//
////    (file: PartitionedFile) => if (inputSplits.getAllSplits.size() > 0) {
////      val recordReader = format.createRecordReader(inputSplits, attemptContext)
////
////      recordReader.initialize(inputSplits, attemptContext)
////
////      new Iterator[Any] {
////        private var havePair = false
////        private var finished = false
////
////
////        override def hasNext: Boolean = {
////          if (!finished && !havePair) {
////            finished = !recordReader.nextKeyValue
////            havePair = !finished
////          }
////          !finished
////        }
////
////        override def next(): Any = {
////          if (!hasNext) {
////            throw new java.util.NoSuchElementException("End of stream")
////          }
////          havePair = false
////          val value = recordReader.getCurrentValue
////          value
////        }
////
////        private def close() {
////          TaskMetricsMap.getInstance().updateReadBytes(Thread.currentThread().getId)
////        }
////      }
////    } else {
////      new Iterator[Any] {
////        override def hasNext: Boolean = false
////
////        override def next(): Any = throw new java.util.NoSuchElementException("End of stream")
////      }
////    }
////
////    iterator.asInstanceOf[Iterator[InternalRow]]

}


