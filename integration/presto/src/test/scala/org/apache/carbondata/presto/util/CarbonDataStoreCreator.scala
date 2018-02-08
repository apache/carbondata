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
import java.util.{ArrayList, Date, List, UUID}

import scala.collection.JavaConversions._

import com.google.gson.Gson
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.TaskAttemptID
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.mapreduce.{RecordReader, TaskType}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.cache.dictionary.{Dictionary, DictionaryColumnUniqueIdentifier,
ReverseDictionary}
import org.apache.carbondata.core.cache.{Cache, CacheProvider, CacheType}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.fileoperations.{AtomicFileOperations, AtomicFileOperationsImpl,
FileWriteOperation}
import org.apache.carbondata.core.metadata.converter.{SchemaConverter,
ThriftWrapperSchemaConverterImpl}
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.table.column.{CarbonColumn, CarbonDimension,
CarbonMeasure, ColumnSchema}
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, TableInfo, TableSchema}
import org.apache.carbondata.core.metadata.schema.{SchemaEvolution, SchemaEvolutionEntry}
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonMetadata,
CarbonTableIdentifier, ColumnIdentifier}
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.core.writer.sortindex.{CarbonDictionarySortIndexWriter,
CarbonDictionarySortIndexWriterImpl, CarbonDictionarySortInfo, CarbonDictionarySortInfoPreparator}
import org.apache.carbondata.core.writer.{CarbonDictionaryWriter, CarbonDictionaryWriterImpl,
ThriftWriter}
import org.apache.carbondata.processing.loading.DataLoadExecutor
import org.apache.carbondata.processing.loading.constants.DataLoadProcessorConstants
import org.apache.carbondata.processing.loading.csvinput.{BlockDetails, CSVInputFormat,
CSVRecordReaderIterator, StringArrayWritable}
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException
import org.apache.carbondata.processing.loading.model.{CarbonDataLoadSchema, CarbonLoadModel}
import org.apache.carbondata.processing.util.TableOptionConstant

object CarbonDataStoreCreator {

  private val logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Create store without any restructure
   */
  def createCarbonStore(storePath: String, dataFilePath: String): Unit = {
    try {
      logger.info("Creating The Carbon Store")
      val dbName: String = "testdb"
      val tableName: String = "testtable"
      val absoluteTableIdentifier = AbsoluteTableIdentifier.from(
        storePath + "/" + dbName + "/" + tableName,
        new CarbonTableIdentifier(dbName,
          tableName,
          UUID.randomUUID().toString))
      //   val factFilePath: String = new File(dataFilePath).getCanonicalPath
      val storeDir: File = new File(absoluteTableIdentifier.getTablePath)
      CarbonUtil.deleteFoldersAndFiles(storeDir)
      CarbonProperties.getInstance.addProperty(
        CarbonCommonConstants.STORE_LOCATION_HDFS,
        absoluteTableIdentifier.getTablePath)
      val table: CarbonTable = createTable(absoluteTableIdentifier)
      writeDictionary(dataFilePath, table, absoluteTableIdentifier)
      val schema: CarbonDataLoadSchema = new CarbonDataLoadSchema(table)
      val loadModel: CarbonLoadModel = new CarbonLoadModel()
      loadModel.setCarbonDataLoadSchema(schema)
      loadModel.setDatabaseName(
        absoluteTableIdentifier.getCarbonTableIdentifier.getDatabaseName)
      loadModel.setTableName(
        absoluteTableIdentifier.getCarbonTableIdentifier.getTableName)
      loadModel.setTableName(
        absoluteTableIdentifier.getCarbonTableIdentifier.getTableName)
      loadModel.setFactFilePath(dataFilePath)
      loadModel.setLoadMetadataDetails(new ArrayList[LoadMetadataDetails]())
      loadModel.setTablePath(absoluteTableIdentifier.getTablePath)
      CarbonProperties.getInstance
        .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE_LOADING, "true")

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
      case e: Exception => e.printStackTrace()

    }
  }

  private def createTable(absoluteTableIdentifier: AbsoluteTableIdentifier): CarbonTable = {
    val tableInfo: TableInfo = new TableInfo()
    tableInfo.setTablePath(absoluteTableIdentifier.getTablePath)
    tableInfo.setDatabaseName(
      absoluteTableIdentifier.getCarbonTableIdentifier.getDatabaseName)
    val tableSchema: TableSchema = new TableSchema()
    tableSchema.setTableName(
      absoluteTableIdentifier.getCarbonTableIdentifier.getTableName)
    val columnSchemas = new ArrayList[ColumnSchema]()
    val dictionaryEncoding: ArrayList[Encoding] = new ArrayList[Encoding]()
    dictionaryEncoding.add(Encoding.DICTIONARY)

    val invertedIndexEncoding: ArrayList[Encoding] = new ArrayList[Encoding]()
    invertedIndexEncoding.add(Encoding.INVERTED_INDEX)

    val id: ColumnSchema = new ColumnSchema()
    id.setColumnName("ID")
    id.setColumnar(true)
    id.setDataType(DataTypes.INT)
    id.setEncodingList(dictionaryEncoding)
    id.setColumnUniqueId(UUID.randomUUID().toString)
    id.setColumnReferenceId(id.getColumnUniqueId)
    id.setDimensionColumn(true)
    id.setColumnGroup(1)
    id.setSchemaOrdinal(0)
    columnSchemas.add(id)

    val directDictionaryEncoding: util.ArrayList[Encoding] = new util.ArrayList[Encoding]()
    directDictionaryEncoding.add(Encoding.DIRECT_DICTIONARY)
    directDictionaryEncoding.add(Encoding.DICTIONARY)
    directDictionaryEncoding.add(Encoding.INVERTED_INDEX)

    val date: ColumnSchema = new ColumnSchema()
    date.setColumnName("date")
    date.setColumnar(true)
    date.setDataType(DataTypes.DATE)
    date.setEncodingList(directDictionaryEncoding)
    date.setColumnUniqueId(UUID.randomUUID().toString)
    date.setDimensionColumn(true)
    date.setColumnGroup(2)
    date.setColumnReferenceId(date.getColumnUniqueId)
    date.setSchemaOrdinal(1)
    columnSchemas.add(date)

    val country: ColumnSchema = new ColumnSchema()
    country.setColumnName("country")
    country.setColumnar(true)
    country.setDataType(DataTypes.STRING)
    country.setEncodingList(dictionaryEncoding)
    country.setColumnUniqueId(UUID.randomUUID().toString)
    country.setColumnReferenceId(country.getColumnUniqueId)
    country.setDimensionColumn(true)
    country.setColumnGroup(3)
    country.setSchemaOrdinal(2)
    country.setColumnReferenceId(country.getColumnUniqueId)
    columnSchemas.add(country)

    val name: ColumnSchema = new ColumnSchema()
    name.setColumnName("name")
    name.setColumnar(true)
    name.setDataType(DataTypes.STRING)
    name.setEncodingList(dictionaryEncoding)
    name.setColumnUniqueId(UUID.randomUUID().toString)
    name.setDimensionColumn(true)
    name.setColumnGroup(4)
    name.setSchemaOrdinal(3)
    name.setColumnReferenceId(name.getColumnUniqueId)
    columnSchemas.add(name)

    val phonetype: ColumnSchema = new ColumnSchema()
    phonetype.setColumnName("phonetype")
    phonetype.setColumnar(true)
    phonetype.setDataType(DataTypes.STRING)
    phonetype.setEncodingList(dictionaryEncoding)
    phonetype.setColumnUniqueId(UUID.randomUUID().toString)
    phonetype.setDimensionColumn(true)
    phonetype.setColumnGroup(5)
    phonetype.setSchemaOrdinal(4)
    phonetype.setColumnReferenceId(phonetype.getColumnUniqueId)
    columnSchemas.add(phonetype)

    val serialname: ColumnSchema = new ColumnSchema()
    serialname.setColumnName("serialname")
    serialname.setColumnar(true)
    serialname.setDataType(DataTypes.STRING)
    serialname.setEncodingList(dictionaryEncoding)
    serialname.setColumnUniqueId(UUID.randomUUID().toString)
    serialname.setDimensionColumn(true)
    serialname.setColumnGroup(6)
    serialname.setSchemaOrdinal(5)
    serialname.setColumnReferenceId(serialname.getColumnUniqueId)
    columnSchemas.add(serialname)

    val salary: ColumnSchema = new ColumnSchema()
    salary.setColumnName("salary")
    salary.setColumnar(true)
    salary.setDataType(DataTypes.DOUBLE)
    salary.setEncodingList(dictionaryEncoding)
    salary.setColumnUniqueId(UUID.randomUUID().toString)
    salary.setDimensionColumn(true)
    salary.setColumnGroup(7)
    salary.setSchemaOrdinal(6)
    salary.setColumnReferenceId(salary.getColumnUniqueId)
    columnSchemas.add(salary)

    val bonus: ColumnSchema = new ColumnSchema()
    bonus.setColumnName("bonus")
    bonus.setColumnar(true)
    bonus.setDataType(DataTypes.createDecimalType(10, 4))
    bonus.setPrecision(10)
    bonus.setScale(4)
    bonus.setEncodingList(invertedIndexEncoding)
    bonus.setColumnUniqueId(UUID.randomUUID().toString)
    bonus.setDimensionColumn(false)
    bonus.setColumnGroup(8)
    bonus.setSchemaOrdinal(7)
    bonus.setColumnReferenceId(bonus.getColumnUniqueId)
    columnSchemas.add(bonus)

    val monthlyBonus: ColumnSchema = new ColumnSchema()
    monthlyBonus.setColumnName("monthlyBonus")
    monthlyBonus.setColumnar(true)
    monthlyBonus.setDataType(DataTypes.createDecimalType(10, 4))
    monthlyBonus.setPrecision(10)
    monthlyBonus.setScale(4)
    monthlyBonus.setSchemaOrdinal(8)
    monthlyBonus.setEncodingList(invertedIndexEncoding)
    monthlyBonus.setColumnUniqueId(UUID.randomUUID().toString)
    monthlyBonus.setDimensionColumn(false)
    monthlyBonus.setColumnGroup(9)
    monthlyBonus.setColumnReferenceId(monthlyBonus.getColumnUniqueId)
    columnSchemas.add(monthlyBonus)

    val dob: ColumnSchema = new ColumnSchema()
    dob.setColumnName("dob")
    dob.setColumnar(true)
    dob.setDataType(DataTypes.TIMESTAMP)
    dob.setEncodingList(directDictionaryEncoding)
    dob.setColumnUniqueId(UUID.randomUUID().toString)
    dob.setDimensionColumn(true)
    dob.setColumnGroup(9)
    dob.setSchemaOrdinal(9)
    dob.setColumnReferenceId(dob.getColumnUniqueId)
    columnSchemas.add(dob)

    val shortField: ColumnSchema = new ColumnSchema()
    shortField.setColumnName("shortField")
    shortField.setColumnar(true)
    shortField.setDataType(DataTypes.SHORT)
    shortField.setEncodingList(dictionaryEncoding)
    shortField.setColumnUniqueId(UUID.randomUUID().toString)
    shortField.setDimensionColumn(true)
    shortField.setColumnGroup(10)
    shortField.setSchemaOrdinal(10)
    shortField.setColumnReferenceId(shortField.getColumnUniqueId)
    columnSchemas.add(shortField)

    val isCurrentEmployee: ColumnSchema = new ColumnSchema()
    isCurrentEmployee.setColumnName("isCurrentEmployee")
    isCurrentEmployee.setColumnar(true)
    isCurrentEmployee.setDataType(DataTypes.BOOLEAN)
    isCurrentEmployee.setEncodingList(invertedIndexEncoding)
    isCurrentEmployee.setColumnUniqueId(UUID.randomUUID().toString)
    isCurrentEmployee.setDimensionColumn(false)
    isCurrentEmployee.setColumnGroup(11)
    isCurrentEmployee.setColumnReferenceId(isCurrentEmployee.getColumnUniqueId)
    columnSchemas.add(isCurrentEmployee)

    tableSchema.setListOfColumns(columnSchemas)
    val schemaEvol: SchemaEvolution = new SchemaEvolution()
    schemaEvol.setSchemaEvolutionEntryList(
      new util.ArrayList[SchemaEvolutionEntry]())
    tableSchema.setSchemaEvalution(schemaEvol)
    tableSchema.setTableId(UUID.randomUUID().toString)
    tableInfo.setTableUniqueName(
      absoluteTableIdentifier.getCarbonTableIdentifier.getTableUniqueName
    )
    tableInfo.setLastUpdatedTime(System.currentTimeMillis())
    tableInfo.setFactTable(tableSchema)
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
    val fileType: FileFactory.FileType =
      FileFactory.getFileType(schemaMetadataPath)
    if (!FileFactory.isFileExist(schemaMetadataPath, fileType)) {
      FileFactory.mkdirs(schemaMetadataPath, fileType)
    }
    val thriftWriter: ThriftWriter = new ThriftWriter(schemaFilePath, false)
    thriftWriter.open()
    thriftWriter.write(thriftTableInfo)
    thriftWriter.close()
    CarbonMetadata.getInstance.getCarbonTable(tableInfo.getTableUniqueName)
  }

  private def writeDictionary(factFilePath: String,
      table: CarbonTable,
      absoluteTableIdentifier: AbsoluteTableIdentifier): Unit = {
    val reader: BufferedReader = new BufferedReader(
      new FileReader(factFilePath))
    val header: String = reader.readLine()
    val split: Array[String] = header.split(",")
    val allCols: util.List[CarbonColumn] = new util.ArrayList[CarbonColumn]()
    val dims: util.List[CarbonDimension] =
      table.getDimensionByTableName(table.getTableName)
    allCols.addAll(dims)
    val msrs: List[CarbonMeasure] =
      table.getMeasureByTableName(table.getTableName)
    allCols.addAll(msrs)
    val set: Array[util.Set[String]] = Array.ofDim[util.Set[String]](dims.size)
    val dimsIndex = dims.map(dim => dim.getColumnSchema.getSchemaOrdinal)
    for (i <- set.indices) {
      set(i) = new util.HashSet[String]()
    }
    var line: String = reader.readLine()
    while (line != null) {
      val data: Array[String] = line.split(",")
      for (i <- set.indices) {
        set(i).add(data(dimsIndex(i)))
      }
      line = reader.readLine()
    }
    val dictCache: Cache[DictionaryColumnUniqueIdentifier, ReverseDictionary] = CacheProvider
      .getInstance.createCache(CacheType.REVERSE_DICTIONARY)

    for (i <- set.indices) {
      val columnIdentifier: ColumnIdentifier =
        new ColumnIdentifier(dims.get(i).getColumnId, null, null)

      val dictionaryColumnUniqueIdentifier: DictionaryColumnUniqueIdentifier =
        new DictionaryColumnUniqueIdentifier(
          table.getAbsoluteTableIdentifier,
          columnIdentifier,
          columnIdentifier.getDataType)
      val writer: CarbonDictionaryWriter = new CarbonDictionaryWriterImpl(
        dictionaryColumnUniqueIdentifier)
      for (value <- set(i)) {
        writer.write(value)
      }
      writer.close()
      writer.commit()
      val dict: Dictionary = dictCache
        .get(
          new DictionaryColumnUniqueIdentifier(
            absoluteTableIdentifier,
            columnIdentifier,
            dims.get(i).getDataType)
        )
        .asInstanceOf[Dictionary]
      val preparator: CarbonDictionarySortInfoPreparator =
        new CarbonDictionarySortInfoPreparator()
      val newDistinctValues: List[String] = new ArrayList[String]()
      val dictionarySortInfo: CarbonDictionarySortInfo =
        preparator.getDictionarySortInfo(newDistinctValues,
          dict,
          dims.get(i).getDataType)
      val carbonDictionaryWriter: CarbonDictionarySortIndexWriter =
        new CarbonDictionarySortIndexWriterImpl(dictionaryColumnUniqueIdentifier)
      try {
        carbonDictionaryWriter.writeSortIndex(dictionarySortInfo.getSortIndex)
        carbonDictionaryWriter.writeInvertedSortIndex(
          dictionarySortInfo.getSortIndexInverted)
      }
      catch {
        case exception: Exception =>


          logger.error(s"exception occurs $exception")
          throw new CarbonDataLoadingException("Data Loading Failed")
      }
      finally carbonDictionaryWriter.close()
    }
    reader.close()
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
    CarbonProperties.getInstance
      .addProperty("high.cardinality.value", "100000")
    CarbonProperties.getInstance.addProperty("is.compressed.keyblock", "false")
    CarbonProperties.getInstance.addProperty("carbon.leaf.node.size", "120000")
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
                                     CarbonCommonConstants.LOADMETADATA_FILENAME
      val gsonObjectToWrite: Gson = new Gson()
      val writeOperation: AtomicFileOperations = new AtomicFileOperationsImpl(
        dataLoadLocation,
        FileFactory.getFileType(dataLoadLocation))
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

