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
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.text.TextOutputWriter
import org.apache.spark.sql.optimizer.CarbonFilters
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.{AtomicType, StructField, StructType}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.{DataMapStoreManager, Segment, TableDataMap}
import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.indexstore.blockletindex.SegmentIndexFileStore
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, ColumnarFormatVersion}
import org.apache.carbondata.core.reader.CarbonHeaderReader
import org.apache.carbondata.core.scan.expression.Expression
import org.apache.carbondata.core.scan.expression.logical.AndExpression
import org.apache.carbondata.core.scan.model.QueryModel
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.hadoop.{CarbonInputSplit, CarbonProjection, CarbonRecordReader, InputMetricsStats}
import org.apache.carbondata.hadoop.api.{CarbonFileInputFormat, DataMapJob}
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
    if (filePaths.size() == 0) {
      throw new SparkException("CarbonData file is not present in the location mentioned in DDL")
    }
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
      hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {

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

    CarbonFileInputFormat.setTableName(job.getConfiguration, "dummyexternal")
    CarbonFileInputFormat.setDatabaseName(job.getConfiguration, "default")
    // CarbonFileInputFormat.setColumnProjection(conf, columnProjection)
    val dataMapJob: DataMapJob = CarbonFileInputFormat.getDataMapJob(job.getConfiguration)
    val format = new CarbonFileInputFormat[Object]

    (file: PartitionedFile) => {
      assert(file.partitionValues.numFields == partitionSchema.size)

      if (file.filePath.endsWith(CarbonCommonConstants.FACT_FILE_EXT)) {
        val fileSplit =
          new FileSplit(new Path(new URI(file.filePath)), file.start, file.length, Array.empty)

        val path: String = options.get("path").get
        val endindex: Int = path.indexOf("Fact") - 1
        val tablePath = path.substring(0, endindex)
        lazy val identifier: AbsoluteTableIdentifier = AbsoluteTableIdentifier.from(
          tablePath,
          "default",
          "externaldummy")
        val split = CarbonInputSplit.from("null", "0", fileSplit, ColumnarFormatVersion.V3, null)
        val blockletMap: TableDataMap = DataMapStoreManager.getInstance
          .chooseDataMap(identifier)

        val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
        val conf1 = new Configuration()
        conf1.set("mapreduce.input.carboninputformat.tableName", "externaldummy")
        conf1.set("mapreduce.input.carboninputformat.databaseName", "default")
        conf1.set("mapreduce.input.fileinputformat.inputdir", tablePath)
        CarbonFileInputFormat.setColumnProjection(conf1, carbonProjection)
        filter match {
          case Some(c) => CarbonFileInputFormat.setFilterPredicates(conf1, c)
          case None => None
        }
        val attemptContext = new TaskAttemptContextImpl(conf1, attemptId)

        val model = format.createQueryModel(split, attemptContext)

        var segments= new java.util.ArrayList[Segment]()
        val seg = new Segment("null", null)
        segments.add(seg)
        var partition : java.util.List[PartitionSpec] = new java.util.ArrayList[PartitionSpec]()
//      TODO : handle the partition for CarbonFileLevelFormat
//      partition = getPartitionsToPrune

        // clean the blocklet
        blockletMap.clear()
        val segmentPath = CarbonTablePath.getSegmentPath(identifier.getTablePath(), "null")
        val indexFiles = new SegmentIndexFileStore().getIndexFilesFromSegment(segmentPath)
        if (indexFiles.size() == 0) {
          throw new SparkException("Index file not present to read the carbondata file")
        }
        val prunedBlocklets = blockletMap
          .prune(segments, model.getFilterExpressionResolverTree, null)

        val detailInfo = prunedBlocklets.get(0).getDetailInfo
        detailInfo.readColumnSchema(detailInfo.getColumnSchemaBinary)
        split.setDetailInfo(detailInfo)

        val carbonReader = if (readVector) {
          // val batchSupport = supportBatch(sparkSession, dataSchema)
          val vectorizedReader = createVectorizedCarbonRecordReader(model,
            null,
            supportBatchValue.toString)
          // val vectorizedReader = VectorizedCarbonRecordReader()
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
        // to do : input PartitionedFile itself should have only .carbondata file.
        // because don't know the side effects of returning Iterator.empty here
      }
    }
  }
}


