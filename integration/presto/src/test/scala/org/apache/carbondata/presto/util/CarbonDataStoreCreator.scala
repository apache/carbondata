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

package org.apache.carbondata.presto.util

import java.io._
import java.nio.charset.Charset
import java.text.SimpleDateFormat
import java.util
import java.util.concurrent.atomic.AtomicInteger
import java.util.{ArrayList, Date, UUID}

import scala.collection.JavaConversions._
import scala.collection.mutable

import com.google.gson.Gson
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.TaskAttemptID
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.mapreduce.{RecordReader, TaskType}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.compression.CompressorFactory
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.fileoperations.{AtomicFileOperationFactory, AtomicFileOperations, FileWriteOperation}
import org.apache.carbondata.core.metadata.converter.{SchemaConverter, ThriftWrapperSchemaConverterImpl}
import org.apache.carbondata.core.metadata.datatype.{DataTypes, StructField}
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, CarbonTableBuilder, TableSchemaBuilder}
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonMetadata, CarbonTableIdentifier}
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.core.writer.ThriftWriter
import org.apache.carbondata.processing.loading.DataLoadExecutor
import org.apache.carbondata.processing.loading.constants.DataLoadProcessorConstants
import org.apache.carbondata.processing.loading.csvinput.{BlockDetails, CSVInputFormat, CSVRecordReaderIterator, StringArrayWritable}
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException
import org.apache.carbondata.processing.loading.model.{CarbonDataLoadSchema, CarbonLoadModel}
import org.apache.carbondata.processing.util.TableOptionConstant

object CarbonDataStoreCreator {

  private val logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Create store without any restructure
   */
  def createCarbonStore(storePath: String, dataFilePath: String,
      useLocalDict: Boolean = false): Unit = {
    try {
      logger.info("Creating The Carbon Store")
      val dbName: String = "testdb"
      val tableName: String = "testtable"
      val absoluteTableIdentifier = AbsoluteTableIdentifier.from(
        storePath + "/" + dbName + "/" + tableName,
        new CarbonTableIdentifier(dbName,
          tableName,
          UUID.randomUUID().toString))
      val table: CarbonTable = createTable(absoluteTableIdentifier, useLocalDict)
      val schema: CarbonDataLoadSchema = new CarbonDataLoadSchema(table)
      val loadModel: CarbonLoadModel = new CarbonLoadModel()
      import scala.collection.JavaConverters._
      val columnCompressor = table.getTableInfo.getFactTable.getTableProperties.asScala
        .getOrElse(CarbonCommonConstants.COMPRESSOR,
          CompressorFactory.getInstance().getCompressor().getName())
      loadModel.setColumnCompressor(columnCompressor)
      loadModel.setCarbonDataLoadSchema(schema)
      loadModel.setDatabaseName(
        absoluteTableIdentifier.getCarbonTableIdentifier.getDatabaseName)
      loadModel.setTableName(
        absoluteTableIdentifier.getCarbonTableIdentifier.getTableName)
      loadModel.setTableName(
        absoluteTableIdentifier.getCarbonTableIdentifier.getTableName)
      loadModel.setFactFilePath(dataFilePath)
      loadModel.setCarbonTransactionalTable(table.isTransactionalTable)
      loadModel.setLoadMetadataDetails(new ArrayList[LoadMetadataDetails]())
      loadModel.setTablePath(absoluteTableIdentifier.getTablePath)
      CarbonProperties.getInstance
        .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE, "true")

      loadModel.setDefaultTimestampFormat(
        CarbonProperties.getInstance.getProperty(
          CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
          CarbonCommonConstants.CARBON_TIMESTAMP_MILLIS))
      loadModel.setDefaultDateFormat(
        CarbonProperties.getInstance.getProperty(
          CarbonCommonConstants.CARBON_DATE_FORMAT,
          CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT))
      loadModel.setSerializationNullFormat(
        TableOptionConstant.SERIALIZATION_NULL_FORMAT.getName +
        "," +
        "\\N")
      loadModel.setBadRecordsLoggerEnable(
        TableOptionConstant.BAD_RECORDS_LOGGER_ENABLE.getName +
        "," +
        "false")
      loadModel.setBadRecordsAction(
        TableOptionConstant.BAD_RECORDS_ACTION.getName + "," +
        "force")
      loadModel.setIsEmptyDataBadRecord(
        DataLoadProcessorConstants.IS_EMPTY_DATA_BAD_RECORD +
        "," +
        "true")
      loadModel.setMaxColumns("15")
      loadModel.setCsvHeader(
        "ID,date,country,name,phonetype,serialname,salary,bonus,monthlyBonus,dob,shortField,isCurrentEmployee")
      loadModel.setCsvHeaderColumns(loadModel.getCsvHeader.split(","))
      loadModel.setTaskNo("0")
      loadModel.setSegmentId("0")
      loadModel.setFactTimeStamp(System.currentTimeMillis())
      loadModel.setMaxColumns("15")
      executeGraph(loadModel, storePath)
    } catch {
      case e: Exception =>
        throw e
    }
  }

  private def createTable(absoluteTableIdentifier: AbsoluteTableIdentifier,
      useLocalDict: Boolean): CarbonTable = {

    val integer = new AtomicInteger(0)
    val schemaBuilder = new TableSchemaBuilder
    schemaBuilder.addColumn(new StructField("ID", DataTypes.INT), integer, false, false)
    schemaBuilder.addColumn(new StructField("date", DataTypes.DATE), integer, false, false)
    schemaBuilder.addColumn(new StructField("country", DataTypes.STRING), integer, false, false)
    schemaBuilder.addColumn(new StructField("name", DataTypes.STRING), integer, false, false)
    schemaBuilder.addColumn(new StructField("phonetype", DataTypes.STRING), integer, false, false)
    schemaBuilder.addColumn(new StructField("serialname", DataTypes.STRING), integer, false, false)
    schemaBuilder.addColumn(new StructField("salary", DataTypes.DOUBLE), integer, false, false)
    schemaBuilder.addColumn(new StructField("bonus", DataTypes.createDecimalType(10, 4)), integer, false, true)
    schemaBuilder.addColumn(new StructField("monthlyBonus", DataTypes.createDecimalType(18, 4)), integer, false, true)
    schemaBuilder.addColumn(new StructField("dob", DataTypes.TIMESTAMP), integer, false, true)
    schemaBuilder.addColumn(new StructField("shortField", DataTypes.SHORT), integer, false, false)
    schemaBuilder.addColumn(new StructField("isCurrentEmployee", DataTypes.BOOLEAN), integer, false, true)
    schemaBuilder.tableName(absoluteTableIdentifier.getTableName)
    val schema = schemaBuilder.build()

    val builder = new CarbonTableBuilder
    builder.databaseName(absoluteTableIdentifier.getDatabaseName)
      .tableName(absoluteTableIdentifier.getTableName)
      .tablePath(absoluteTableIdentifier.getTablePath)
      .isTransactionalTable(true)
      .tableSchema(schema)
    val carbonTable = builder.build()

    val tableInfo = carbonTable.getTableInfo
    val schemaFilePath: String = CarbonTablePath.getSchemaFilePath(
      absoluteTableIdentifier.getTablePath)
    val schemaMetadataPath: String =
      CarbonTablePath.getFolderContainingFile(schemaFilePath)
    CarbonMetadata.getInstance.loadTableMetadata(tableInfo)
    val schemaConverter: SchemaConverter =
      new ThriftWrapperSchemaConverterImpl()
    val thriftTableInfo: org.apache.carbondata.format.TableInfo =
      schemaConverter.fromWrapperToExternalTableInfo(
        tableInfo,
        tableInfo.getDatabaseName,
        tableInfo.getFactTable.getTableName)
    val schemaEvolutionEntry: org.apache.carbondata.format.SchemaEvolutionEntry =
      new org.apache.carbondata.format.SchemaEvolutionEntry(
        tableInfo.getLastUpdatedTime)
    thriftTableInfo.getFact_table.getSchema_evolution.getSchema_evolution_history
      .add(schemaEvolutionEntry)
    if (!FileFactory.isFileExist(schemaMetadataPath)) {
      FileFactory.mkdirs(schemaMetadataPath)
    }
    val thriftWriter: ThriftWriter = new ThriftWriter(schemaFilePath, false)
    thriftWriter.open()
    thriftWriter.write(thriftTableInfo)
    thriftWriter.close()
    CarbonMetadata.getInstance.getCarbonTable(tableInfo.getTableUniqueName)
  }

  private def addDictionaryValuesToDimensionSet(dims: util.List[CarbonDimension],
      dimensionIndex: mutable.Buffer[Int],
      dimensionSet: Array[util.List[String]],
      data: Array[String],
      index: Int) = {
    if (isDictionaryDefaultMember(dims, dimensionSet, index)) {
      dimensionSet(index).add(CarbonCommonConstants.MEMBER_DEFAULT_VAL)
      dimensionSet(index).add(data(dimensionIndex(index)))
    }
    else {
      if (data.length == 1) {
        dimensionSet(index).add("""\N""")
      } else {
        dimensionSet(index).add(data(dimensionIndex(index)))
      }
    }
  }

  private def isDictionaryDefaultMember(dims: util.List[CarbonDimension],
      dimensionSet: Array[util.List[String]],
      index: Int) = {
    dimensionSet(index).isEmpty && dims(index).hasEncoding(Encoding.DICTIONARY) &&
    !dims(index).hasEncoding(Encoding.DIRECT_DICTIONARY)
  }

  /**
   * Execute graph which will further load data
   *
   * @param loadModel     Carbon load model
   * @param storeLocation store location directory
   * @throws Exception
   */
  private def executeGraph(loadModel: CarbonLoadModel, storeLocation: String): Unit = {
    new File(storeLocation).mkdirs()
    val outPutLoc: String = storeLocation + "/etl"
    val databaseName: String = loadModel.getDatabaseName
    val tableName: String = loadModel.getTableName
    val tempLocationKey: String = databaseName + '_' + tableName + "_1"
    CarbonProperties.getInstance.addProperty(tempLocationKey, storeLocation)
    CarbonProperties.getInstance
      .addProperty("store_output_location", outPutLoc)
    CarbonProperties.getInstance.addProperty("send.signal.load", "false")
    CarbonProperties.getInstance
      .addProperty("carbon.is.columnar.storage", "true")
    CarbonProperties.getInstance
      .addProperty("carbon.dimension.split.value.in.columnar", "1")
    CarbonProperties.getInstance
      .addProperty("carbon.is.fullyfilled.bits", "true")
    CarbonProperties.getInstance.addProperty("is.int.based.indexer", "true")
    CarbonProperties.getInstance
      .addProperty("aggregate.columnar.keyblock", "true")
    CarbonProperties.getInstance.addProperty("is.compressed.keyblock", "false")
    CarbonProperties.getInstance
      .addProperty("carbon.direct.dictionary", "true")
    val graphPath: String = outPutLoc + File.separator + loadModel.getDatabaseName +
                            File.separator +
                            tableName +
                            File.separator +
                            0 +
                            File.separator +
                            1 +
                            File.separator +
                            tableName +
                            ".ktr"
    val path: File = new File(graphPath)
    if (path.exists()) {
      path.delete()
    }
    val blockDetails: BlockDetails = new BlockDetails(
      new Path(loadModel.getFactFilePath),
      0,
      new File(loadModel.getFactFilePath).length,
      Array("localhost"))
    val configuration: Configuration = new Configuration()
    CSVInputFormat.setCommentCharacter(configuration, loadModel.getCommentChar)
    CSVInputFormat.setCSVDelimiter(configuration, loadModel.getCsvDelimiter)
    CSVInputFormat.setEscapeCharacter(configuration, loadModel.getEscapeChar)
    CSVInputFormat.setHeaderExtractionEnabled(configuration, true)
    CSVInputFormat.setQuoteCharacter(configuration, loadModel.getQuoteChar)
    CSVInputFormat.setReadBufferSize(
      configuration,
      CarbonProperties.getInstance.getProperty(
        CarbonCommonConstants.CSV_READ_BUFFER_SIZE,
        CarbonCommonConstants.CSV_READ_BUFFER_SIZE_DEFAULT))
    CSVInputFormat.setNumberOfColumns(
      configuration,
      String.valueOf(loadModel.getCsvHeaderColumns.length))
    CSVInputFormat.setMaxColumns(configuration, "15")
    val hadoopAttemptContext: TaskAttemptContextImpl =
      new TaskAttemptContextImpl(configuration,
        new TaskAttemptID("", 1, TaskType.MAP, 0, 0))
    val format: CSVInputFormat = new CSVInputFormat()
    val recordReader: RecordReader[NullWritable, StringArrayWritable] =
      format.createRecordReader(blockDetails, hadoopAttemptContext)
    val readerIterator: CSVRecordReaderIterator = new CSVRecordReaderIterator(
      recordReader,
      blockDetails,
      hadoopAttemptContext)
    new DataLoadExecutor()
      .execute(loadModel, Array(storeLocation), Array(readerIterator))
    writeLoadMetadata(loadModel.getCarbonDataLoadSchema,
      loadModel.getTableName,
      loadModel.getTableName,
      new ArrayList[LoadMetadataDetails]())
  }

  private def writeLoadMetadata(
      schema: CarbonDataLoadSchema,
      databaseName: String,
      tableName: String,
      listOfLoadFolderDetails: util.List[LoadMetadataDetails]): Unit = {
    try {
      val loadMetadataDetails: LoadMetadataDetails = new LoadMetadataDetails()
      loadMetadataDetails.setLoadEndTime(System.currentTimeMillis())
      loadMetadataDetails.setSegmentStatus(SegmentStatus.SUCCESS)
      loadMetadataDetails.setLoadName(String.valueOf(0))
      loadMetadataDetails.setLoadStartTime(
        loadMetadataDetails.getTimeStamp(readCurrentTime()))
      listOfLoadFolderDetails.add(loadMetadataDetails)
      val dataLoadLocation: String = schema.getCarbonTable.getMetadataPath + File.separator +
                                     CarbonTablePath.TABLE_STATUS_FILE
      val gsonObjectToWrite: Gson = new Gson()
      val writeOperation: AtomicFileOperations = AtomicFileOperationFactory
        .getAtomicFileOperations(dataLoadLocation)
      val dataOutputStream =
        writeOperation.openForWrite(FileWriteOperation.OVERWRITE)
      val brWriter = new BufferedWriter(
        new OutputStreamWriter(
          dataOutputStream,
          Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)))
      val metadataInstance: String =
        gsonObjectToWrite.toJson(listOfLoadFolderDetails.toArray())
      brWriter.write(metadataInstance)
      if (Option(brWriter).isDefined) {
        brWriter.flush()
      }
      CarbonUtil.closeStreams(brWriter)
      writeOperation.close()
    }
    catch {
      case exception: Exception => logger.error(s"exception occurs $exception")
        throw new CarbonDataLoadingException("Data Loading Failed")
    }
  }

  private def readCurrentTime(): String = {
    val sdf: SimpleDateFormat = new SimpleDateFormat(
      CarbonCommonConstants.CARBON_TIMESTAMP_MILLIS)
    sdf.format(new Date())
  }

}

