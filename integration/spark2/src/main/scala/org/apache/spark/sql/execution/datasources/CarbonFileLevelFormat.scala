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
import java.text.SimpleDateFormat
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.optimizer.CarbonFilters
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.{DataType, StructType}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.datatype.StructField
import org.apache.carbondata.core.scan.model.QueryModel
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.hadoop.CarbonRecordReader
import org.apache.carbondata.hadoop.api.{CarbonFileOutputFormat, CarbonInputFormat, CarbonOutputCommitter, CarbonTableOutputFormat}
import org.apache.carbondata.hadoop.api.CarbonTableOutputFormat.CarbonRecordWriter
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil
import org.apache.carbondata.processing.loading.csvinput.StringArrayWritable
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.spark.rdd.SparkReadSupport
import org.apache.carbondata.spark.util.{CarbonScalaUtil, DataLoadingUtil, SparkDataTypeConverterImpl}
import org.apache.carbondata.store.api.{CarbonStore, SchemaBuilder, Table}

// File level format implementation that writes carbondata file to
// user specified folder, without segment folder
class CarbonFileLevelFormat
  extends FileFormat
    with DataSourceRegister
    with Logging
    with Serializable {

  override def shortName(): String = "carbonfile"

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = None

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
    job.setOutputFormatClass(classOf[CarbonFileOutputFormat])

    val table: Table = createTable(dataSchema)
    val loadModel: CarbonLoadModel = DataLoadingUtil.buildCarbonLoadModelJava(
      table.getMeta, new util.HashMap[String, String])
    CarbonTableOutputFormat.setLoadModel(conf, loadModel)

    new OutputWriterFactory {

      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {

        val filePath = new URI(path).getRawPath
        CarbonFileOutputFormat.setWriteTempFile(context, filePath)
        new CarbonFileOutputWriter(path, context, dataSchema.map(_.dataType))
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        CarbonTablePath.CARBON_DATA_EXT
      }

    }
  }

  private def createTable(dataSchema: StructType): Table = {
    // TODO: prepare a default loadmodel and set it to configuration
    val schemaBuilder = SchemaBuilder.newInstance()
    dataSchema.fields.foreach { field =>
      schemaBuilder.addColumn(
        new StructField(field.name, CarbonScalaUtil.convertSparkToCarbonDataType(field.dataType)),
        false)
    }
    CarbonStore.build().createTable("_temp_table", schemaBuilder.create(), "./_temp")
  }

  override def buildReader(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {

    def readFile(file: PartitionedFile): Iterator[InternalRow] = {
      if (!file.filePath.endsWith(CarbonCommonConstants.FACT_FILE_EXT)) {
        return new Iterator[InternalRow]{
          override def hasNext: Boolean = false
          override def next(): InternalRow = null
        }
      }
      val projection = requiredSchema.map(_.name).toArray
      val filter = CarbonFilters.createFilter(dataSchema, filters.toArray).orNull

      val carbonTable = createTable(dataSchema).getMeta
      val queryModel = carbonTable.createQuery(projection, filter)
      queryModel.setConverter(new SparkDataTypeConverterImpl)
      queryModel.setFilePath(file.filePath)
      val readSupport = CarbonInputFormat.getReadSupport(SparkReadSupport.readSupportClass.getName)
      val reader = new CarbonRecordReader(queryModel, readSupport)
      reader.initializeForFileLevelRead(file.filePath)

      new Iterator[InternalRow] {
        private var havePair = false
        private var finished = false

        override def hasNext: Boolean = {
          if (!finished && !havePair) {
            finished = !reader.nextKeyValue
            havePair = !finished
          }
          !finished
        }

        override def next(): InternalRow = {
          if (!hasNext) {
            throw new java.util.NoSuchElementException("End of stream")
          }
          havePair = false
          reader.getCurrentValue.asInstanceOf[GenericInternalRow]
        }

      }
    }
    readFile
  }


}

private class CarbonFileOutputWriter(
    path: String,
    context: TaskAttemptContext,
    fieldTypes: Seq[DataType])
  extends OutputWriter with AbstractCarbonOutputWriter {
  val writable = new StringArrayWritable()

  private val recordWriter: CarbonRecordWriter = {

    new CarbonFileOutputFormat() {
      override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
        new Path(path)
      }
    }.getRecordWriter(context).asInstanceOf[CarbonRecordWriter]
  }

  private val timeStampformatString = CarbonProperties.getInstance()
    .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
      CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
  private val timeStampFormat = new SimpleDateFormat(timeStampformatString)
  private val dateFormatString = CarbonProperties.getInstance().getProperty(CarbonCommonConstants
    .CARBON_DATE_FORMAT, CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
  private val dateFormat = new SimpleDateFormat(dateFormatString)
  private val loadModel = CarbonTableOutputFormat.getLoadModel(context.getConfiguration)
  private val delimiterLevel1 = loadModel.getComplexDelimiterLevel1
  private val delimiterLevel2 = loadModel.getComplexDelimiterLevel2
  private val serializationNullFormat =
    loadModel.getSerializationNullFormat.split(CarbonCommonConstants.COMMA, 2)(1)

  // TODO Implement writesupport interface to support writing Row directly to recordwriter
  def writeCarbon(row: InternalRow): Unit = {
    val data = new Array[String](fieldTypes.length)
    var i = 0
    while (i < fieldTypes.length) {
      if (!row.isNullAt(i)) {
        data(i) = CarbonScalaUtil.getString(
          row.get(i, fieldTypes(i)),
          serializationNullFormat,
          delimiterLevel1,
          delimiterLevel2,
          timeStampFormat,
          dateFormat)
      }
      i += 1
    }
    writable.set(data)
    recordWriter.write(NullWritable.get(), writable)
  }


  override def writeInternal(row: InternalRow): Unit = {
    writeCarbon(row)
  }

  override def close(): Unit = {
    recordWriter.close(context)
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
