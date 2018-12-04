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
package org.apache.carbondata.hadoop.testutil;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.DataMapStoreManager;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperationFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperations;
import org.apache.carbondata.core.fileoperations.FileWriteOperation;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.ColumnIdentifier;
import org.apache.carbondata.core.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.SchemaEvolution;
import org.apache.carbondata.core.metadata.schema.SchemaEvolutionEntry;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.core.writer.CarbonDictionaryWriter;
import org.apache.carbondata.core.writer.CarbonDictionaryWriterImpl;
import org.apache.carbondata.core.writer.ThriftWriter;
import org.apache.carbondata.core.writer.sortindex.CarbonDictionarySortIndexWriter;
import org.apache.carbondata.core.writer.sortindex.CarbonDictionarySortIndexWriterImpl;
import org.apache.carbondata.core.writer.sortindex.CarbonDictionarySortInfo;
import org.apache.carbondata.core.writer.sortindex.CarbonDictionarySortInfoPreparator;
import org.apache.carbondata.processing.loading.DataLoadExecutor;
import org.apache.carbondata.processing.loading.constants.DataLoadProcessorConstants;
import org.apache.carbondata.processing.loading.csvinput.BlockDetails;
import org.apache.carbondata.processing.loading.csvinput.CSVInputFormat;
import org.apache.carbondata.processing.loading.csvinput.CSVRecordReaderIterator;
import org.apache.carbondata.processing.loading.csvinput.StringArrayWritable;
import org.apache.carbondata.processing.loading.model.CarbonDataLoadSchema;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.util.TableOptionConstant;

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.log4j.Logger;

/**
 * This class will create store file based on provided schema
 *
 */
public class StoreCreator {

  private static final Logger LOG =
      LogServiceFactory.getLogService(StoreCreator.class.getCanonicalName());
  private AbsoluteTableIdentifier absoluteTableIdentifier;
  private String storePath = null;
  private String csvPath;
  private boolean dictionary;
  private List<String> sortColumns = new ArrayList<>();

  public StoreCreator(String storePath, String csvPath) {
    this(storePath, csvPath, false);
  }

  public StoreCreator(String storePath, String csvPath, boolean dictionary) {
    this.storePath = storePath;
    this.csvPath = csvPath;
    String dbName = "testdb";
    String tableName = "testtable";
    sortColumns.add("date");
    sortColumns.add("country");
    sortColumns.add("name");
    sortColumns.add("phonetype");
    sortColumns.add("serialname");
    absoluteTableIdentifier = AbsoluteTableIdentifier.from(storePath + "/testdb/testtable",
        new CarbonTableIdentifier(dbName, tableName, UUID.randomUUID().toString()));
    this.dictionary = dictionary;
  }

  public AbsoluteTableIdentifier getAbsoluteTableIdentifier() {
    return absoluteTableIdentifier;
  }

  public static CarbonLoadModel buildCarbonLoadModel(CarbonTable table, String factFilePath,
      AbsoluteTableIdentifier absoluteTableIdentifier) {
    CarbonDataLoadSchema schema = new CarbonDataLoadSchema(table);
    CarbonLoadModel loadModel = new CarbonLoadModel();
    String columnCompressor = table.getTableInfo().getFactTable().getTableProperties().get(
        CarbonCommonConstants.COMPRESSOR);
    if (columnCompressor == null) {
      columnCompressor = CompressorFactory.getInstance().getCompressor().getName();
    }
    loadModel.setColumnCompressor(columnCompressor);
    loadModel.setCarbonDataLoadSchema(schema);
    loadModel.setDatabaseName(absoluteTableIdentifier.getCarbonTableIdentifier().getDatabaseName());
    loadModel.setTableName(absoluteTableIdentifier.getCarbonTableIdentifier().getTableName());
    loadModel.setTableName(absoluteTableIdentifier.getCarbonTableIdentifier().getTableName());
    loadModel.setFactFilePath(factFilePath);
    loadModel.setLoadMetadataDetails(new ArrayList<LoadMetadataDetails>());
    loadModel.setTablePath(absoluteTableIdentifier.getTablePath());
    loadModel.setDateFormat(null);
    loadModel.setCarbonTransactionalTable(table.isTransactionalTable());
    loadModel.setDefaultTimestampFormat(CarbonProperties.getInstance().getProperty(
        CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_MILLIS));
    loadModel.setDefaultDateFormat(CarbonProperties.getInstance().getProperty(
        CarbonCommonConstants.CARBON_DATE_FORMAT,
        CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT));
    loadModel
        .setSerializationNullFormat(
            TableOptionConstant.SERIALIZATION_NULL_FORMAT.getName() + "," + "\\N");
    loadModel
        .setBadRecordsLoggerEnable(
            TableOptionConstant.BAD_RECORDS_LOGGER_ENABLE.getName() + "," + "false");
    loadModel
        .setBadRecordsAction(
            TableOptionConstant.BAD_RECORDS_ACTION.getName() + "," + "FORCE");
    loadModel
        .setIsEmptyDataBadRecord(
            DataLoadProcessorConstants.IS_EMPTY_DATA_BAD_RECORD + "," + "false");
    loadModel.setCsvHeader("ID,date,country,name,phonetype,serialname,salary");
    loadModel.setCsvHeaderColumns(loadModel.getCsvHeader().split(","));
    loadModel.setTaskNo("0");
    loadModel.setSegmentId("0");
    loadModel.setFactTimeStamp(System.currentTimeMillis());
    loadModel.setMaxColumns("10");
    return loadModel;
  }

  /**
   * Create store without any restructure
   */
  public void createCarbonStore() throws Exception {
    CarbonLoadModel loadModel = createTableAndLoadModel();
    loadData(loadModel, storePath);
  }

  /**
   * Create store without any restructure
   */
  public void createCarbonStore(CarbonLoadModel loadModel) throws Exception {
    loadData(loadModel, storePath);
  }

  /**
   * Method to clear the data maps
   */
  public void clearDataMaps() throws IOException {
    DataMapStoreManager.getInstance().clearDataMaps(absoluteTableIdentifier);
  }

  public CarbonLoadModel createTableAndLoadModel(boolean deleteOldStore) throws Exception {
    if (deleteOldStore) {
      File storeDir = new File(storePath);
      CarbonUtil.deleteFoldersAndFiles(storeDir);
    }

    CarbonTable table = createTable(absoluteTableIdentifier);
    writeDictionary(csvPath, table);
    return buildCarbonLoadModel(table, csvPath, absoluteTableIdentifier);
  }

  public CarbonLoadModel createTableAndLoadModel() throws Exception {
    return createTableAndLoadModel(true);
  }

  public CarbonTable createTable(
      AbsoluteTableIdentifier identifier) throws IOException {
    TableInfo tableInfo = new TableInfo();
    tableInfo.setDatabaseName(identifier.getCarbonTableIdentifier().getDatabaseName());
    TableSchema tableSchema = new TableSchema();
    tableSchema.setTableName(identifier.getCarbonTableIdentifier().getTableName());
    List<ColumnSchema> columnSchemas = new ArrayList<ColumnSchema>();
    ArrayList<Encoding> encodings = new ArrayList<>();
    if (dictionary) {
      encodings.add(Encoding.DICTIONARY);
    }
    int schemaOrdinal = 0;
    ColumnSchema id = new ColumnSchema();
    id.setColumnName("id");
    id.setDataType(DataTypes.INT);
    id.setEncodingList(encodings);
    id.setColumnUniqueId(UUID.randomUUID().toString());
    id.setColumnReferenceId(id.getColumnUniqueId());
    id.setDimensionColumn(true);
    id.setSchemaOrdinal(schemaOrdinal++);
    if (sortColumns.contains(id.getColumnName())) {
      id.setSortColumn(true);
    }
    columnSchemas.add(id);

    ColumnSchema date = new ColumnSchema();
    date.setColumnName("date");
    date.setDataType(DataTypes.STRING);
    date.setEncodingList(encodings);
    date.setColumnUniqueId(UUID.randomUUID().toString());
    date.setDimensionColumn(true);
    date.setColumnReferenceId(date.getColumnUniqueId());
    date.setSchemaOrdinal(schemaOrdinal++);
    if (sortColumns.contains(date.getColumnName())) {
      date.setSortColumn(true);
    }
    columnSchemas.add(date);

    ColumnSchema country = new ColumnSchema();
    country.setColumnName("country");
    country.setDataType(DataTypes.STRING);
    country.setEncodingList(encodings);
    country.setColumnUniqueId(UUID.randomUUID().toString());
    country.setDimensionColumn(true);
    country.setSortColumn(true);
    country.setSchemaOrdinal(schemaOrdinal++);
    if (sortColumns.contains(country.getColumnName())) {
      country.setSortColumn(true);
    }
    country.setColumnReferenceId(country.getColumnUniqueId());
    columnSchemas.add(country);

    ColumnSchema name = new ColumnSchema();
    name.setColumnName("name");
    name.setDataType(DataTypes.STRING);
    name.setEncodingList(encodings);
    name.setColumnUniqueId(UUID.randomUUID().toString());
    name.setDimensionColumn(true);
    name.setSchemaOrdinal(schemaOrdinal++);
    if (sortColumns.contains(name.getColumnName())) {
      name.setSortColumn(true);
    }
    name.setColumnReferenceId(name.getColumnUniqueId());
    columnSchemas.add(name);

    ColumnSchema phonetype = new ColumnSchema();
    phonetype.setColumnName("phonetype");
    phonetype.setDataType(DataTypes.STRING);
    phonetype.setEncodingList(encodings);
    phonetype.setColumnUniqueId(UUID.randomUUID().toString());
    phonetype.setDimensionColumn(true);
    phonetype.setSchemaOrdinal(schemaOrdinal++);
    if (sortColumns.contains(phonetype.getColumnName())) {
      phonetype.setSortColumn(true);
    }
    phonetype.setColumnReferenceId(phonetype.getColumnUniqueId());
    columnSchemas.add(phonetype);

    ColumnSchema serialname = new ColumnSchema();
    serialname.setColumnName("serialname");
    serialname.setDataType(DataTypes.STRING);
    serialname.setEncodingList(encodings);
    serialname.setColumnUniqueId(UUID.randomUUID().toString());
    serialname.setDimensionColumn(true);
    serialname.setSchemaOrdinal(schemaOrdinal++);
    if (sortColumns.contains(serialname.getColumnName())) {
      serialname.setSortColumn(true);
    }
    serialname.setColumnReferenceId(serialname.getColumnUniqueId());
    columnSchemas.add(serialname);
    ColumnSchema salary = new ColumnSchema();
    salary.setColumnName("salary");
    salary.setDataType(DataTypes.INT);
    salary.setEncodingList(new ArrayList<Encoding>());
    salary.setColumnUniqueId(UUID.randomUUID().toString());
    salary.setDimensionColumn(false);
    salary.setColumnReferenceId(salary.getColumnUniqueId());
    salary.setSchemaOrdinal(schemaOrdinal++);
    columnSchemas.add(salary);

    // rearrange the column schema based on the sort order, if sort columns exists
    List<ColumnSchema> columnSchemas1 = reArrangeColumnSchema(columnSchemas);
    tableSchema.setListOfColumns(columnSchemas1);
    SchemaEvolution schemaEvol = new SchemaEvolution();
    schemaEvol.setSchemaEvolutionEntryList(new ArrayList<SchemaEvolutionEntry>());
    tableSchema.setSchemaEvolution(schemaEvol);
    tableSchema.setTableId(UUID.randomUUID().toString());
    tableInfo.setTableUniqueName(
        identifier.getCarbonTableIdentifier().getTableUniqueName()
    );
    tableInfo.setLastUpdatedTime(System.currentTimeMillis());
    tableInfo.setFactTable(tableSchema);
    tableInfo.setTablePath(identifier.getTablePath());
    String schemaFilePath = CarbonTablePath.getSchemaFilePath(identifier.getTablePath());
    String schemaMetadataPath = CarbonTablePath.getFolderContainingFile(schemaFilePath);
    CarbonMetadata.getInstance().loadTableMetadata(tableInfo);

    SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
    org.apache.carbondata.format.TableInfo thriftTableInfo =
        schemaConverter.fromWrapperToExternalTableInfo(
            tableInfo,
            tableInfo.getDatabaseName(),
            tableInfo.getFactTable().getTableName());
    org.apache.carbondata.format.SchemaEvolutionEntry schemaEvolutionEntry =
        new org.apache.carbondata.format.SchemaEvolutionEntry(tableInfo.getLastUpdatedTime());
    thriftTableInfo.getFact_table().getSchema_evolution().getSchema_evolution_history()
        .add(schemaEvolutionEntry);

    FileFactory.FileType fileType = FileFactory.getFileType(schemaMetadataPath);
    if (!FileFactory.isFileExist(schemaMetadataPath, fileType)) {
      FileFactory.mkdirs(schemaMetadataPath, fileType);
    }

    ThriftWriter thriftWriter = new ThriftWriter(schemaFilePath, false);
    thriftWriter.open();
    thriftWriter.write(thriftTableInfo);
    thriftWriter.close();
    return CarbonMetadata.getInstance().getCarbonTable(tableInfo.getTableUniqueName());
  }

  private List<ColumnSchema> reArrangeColumnSchema(List<ColumnSchema> columnSchemas) {
    List<ColumnSchema> newColumnSchema = new ArrayList<>(columnSchemas.size());
    // add sort columns first
    for (ColumnSchema columnSchema : columnSchemas) {
      if (columnSchema.isSortColumn()) {
        newColumnSchema.add(columnSchema);
      }
    }
    // add other dimension columns
    for (ColumnSchema columnSchema : columnSchemas) {
      if (!columnSchema.isSortColumn() && columnSchema.isDimensionColumn()) {
        newColumnSchema.add(columnSchema);
      }
    }
    // add measure columns
    for (ColumnSchema columnSchema : columnSchemas) {
      if (!columnSchema.isDimensionColumn()) {
        newColumnSchema.add(columnSchema);
      }
    }
    return newColumnSchema;
  }

  private void writeDictionary(String factFilePath, CarbonTable table) throws Exception {
    BufferedReader reader = new BufferedReader(new InputStreamReader(
        new FileInputStream(factFilePath), "UTF-8"));
    List<CarbonDimension> dims = table.getDimensionByTableName(table.getTableName());
    Set<String>[] set = new HashSet[dims.size()];
    for (int i = 0; i < set.length; i++) {
      set[i] = new HashSet<String>();
    }
    String line = reader.readLine();
    while (line != null) {
      String[] data = line.split(",");
      for (int i = 0; i < set.length; i++) {
        set[i].add(data[i]);
      }
      line = reader.readLine();
    }

    Cache dictCache = CacheProvider.getInstance()
        .createCache(CacheType.REVERSE_DICTIONARY);
    for (int i = 0; i < set.length; i++) {
      ColumnIdentifier columnIdentifier =
          new ColumnIdentifier(dims.get(i).getColumnId(), null, null);
      DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier =
          new DictionaryColumnUniqueIdentifier(
              table.getAbsoluteTableIdentifier(), columnIdentifier, columnIdentifier.getDataType());
      CarbonDictionaryWriter writer =
          new CarbonDictionaryWriterImpl(dictionaryColumnUniqueIdentifier);
      for (String value : set[i]) {
        writer.write(value);
      }
      writer.close();
      writer.commit();
      Dictionary dict = (Dictionary) dictCache.get(
          new DictionaryColumnUniqueIdentifier(absoluteTableIdentifier,
              columnIdentifier, dims.get(i).getDataType()));
      CarbonDictionarySortInfoPreparator preparator =
          new CarbonDictionarySortInfoPreparator();
      List<String> newDistinctValues = new ArrayList<String>();
      CarbonDictionarySortInfo dictionarySortInfo =
          preparator.getDictionarySortInfo(newDistinctValues, dict, dims.get(i).getDataType());
      CarbonDictionarySortIndexWriter carbonDictionaryWriter =
          new CarbonDictionarySortIndexWriterImpl(dictionaryColumnUniqueIdentifier);
      try {
        carbonDictionaryWriter.writeSortIndex(dictionarySortInfo.getSortIndex());
        carbonDictionaryWriter.writeInvertedSortIndex(dictionarySortInfo.getSortIndexInverted());
      } finally {
        carbonDictionaryWriter.close();
      }
    }
    reader.close();
  }

  public void setSortColumns(List<String> sortColumns) {
    this.sortColumns = sortColumns;
  }

  /**
   * Execute graph which will further load data
   *
   * @param loadModel
   * @param storeLocation
   * @throws Exception
   */
  public static void loadData(CarbonLoadModel loadModel, String storeLocation)
      throws Exception {
    if (new File(storeLocation).mkdirs()) {
      LOG.warn("mkdir is failed");
    }
    String outPutLoc = storeLocation + "/etl";
    String databaseName = loadModel.getDatabaseName();
    String tableName = loadModel.getTableName();
    String tempLocationKey = databaseName + '_' + tableName + "_1";
    CarbonProperties.getInstance().addProperty(
        tempLocationKey, storeLocation + "/" + databaseName + "/" + tableName);
    CarbonProperties.getInstance().addProperty("store_output_location", outPutLoc);
    CarbonProperties.getInstance().addProperty("send.signal.load", "false");
    CarbonProperties.getInstance().addProperty("carbon.is.columnar.storage", "true");
    CarbonProperties.getInstance().addProperty("carbon.dimension.split.value.in.columnar", "1");
    CarbonProperties.getInstance().addProperty("carbon.is.fullyfilled.bits", "true");
    CarbonProperties.getInstance().addProperty("is.int.based.indexer", "true");
    CarbonProperties.getInstance().addProperty("aggregate.columnar.keyblock", "true");
    CarbonProperties.getInstance().addProperty("is.compressed.keyblock", "false");

    String graphPath =
        outPutLoc + File.separator + loadModel.getDatabaseName() + File.separator + tableName
            + File.separator + 0 + File.separator + 1 + File.separator + tableName + ".ktr";
    File path = new File(graphPath);
    if (path.exists()) {
      if (!path.delete()) {
        LOG.warn("delete " + path + " failed");
      }
    }

    BlockDetails blockDetails = new BlockDetails(new Path(loadModel.getFactFilePath()),
        0, new File(loadModel.getFactFilePath()).length(), new String[] {"localhost"});
    Configuration configuration = new Configuration();
    CSVInputFormat.setCommentCharacter(configuration, loadModel.getCommentChar());
    CSVInputFormat.setCSVDelimiter(configuration, loadModel.getCsvDelimiter());
    CSVInputFormat.setEscapeCharacter(configuration, loadModel.getEscapeChar());
    CSVInputFormat.setHeaderExtractionEnabled(configuration, true);
    CSVInputFormat.setQuoteCharacter(configuration, loadModel.getQuoteChar());
    CSVInputFormat.setReadBufferSize(configuration,
        CarbonProperties.getInstance().getProperty(
            CarbonCommonConstants.CSV_READ_BUFFER_SIZE,
            CarbonCommonConstants.CSV_READ_BUFFER_SIZE_DEFAULT));
    CSVInputFormat.setNumberOfColumns(
        configuration, String.valueOf(loadModel.getCsvHeaderColumns().length));
    CSVInputFormat.setMaxColumns(configuration, "10");

    TaskAttemptContextImpl hadoopAttemptContext =
        new TaskAttemptContextImpl(configuration, new TaskAttemptID("", 1, TaskType.MAP, 0, 0));
    CSVInputFormat format = new CSVInputFormat();

    RecordReader<NullWritable, StringArrayWritable> recordReader =
        format.createRecordReader(blockDetails, hadoopAttemptContext);

    CSVRecordReaderIterator readerIterator =
        new CSVRecordReaderIterator(recordReader, blockDetails, hadoopAttemptContext);
    DataTypeUtil.clearFormatter();
    new DataLoadExecutor().execute(loadModel,
        new String[] {storeLocation + "/" + databaseName + "/" + tableName},
        new CarbonIterator[]{readerIterator});

    writeLoadMetadata(
        loadModel.getCarbonDataLoadSchema(), loadModel.getTableName(), loadModel.getTableName(),
        new ArrayList<LoadMetadataDetails>());
  }

  public static void writeLoadMetadata(CarbonDataLoadSchema schema, String databaseName,
      String tableName, List<LoadMetadataDetails> listOfLoadFolderDetails) throws IOException {
    LoadMetadataDetails loadMetadataDetails = new LoadMetadataDetails();
    loadMetadataDetails.setLoadEndTime(System.currentTimeMillis());
    loadMetadataDetails.setSegmentStatus(SegmentStatus.SUCCESS);
    loadMetadataDetails.setLoadName(String.valueOf(0));
    loadMetadataDetails.setLoadStartTime(loadMetadataDetails.getTimeStamp(readCurrentTime()));
    listOfLoadFolderDetails.add(loadMetadataDetails);

    String dataLoadLocation = schema.getCarbonTable().getMetadataPath() + File.separator
        + CarbonTablePath.TABLE_STATUS_FILE;

    DataOutputStream dataOutputStream;
    Gson gsonObjectToWrite = new Gson();
    BufferedWriter brWriter = null;

    AtomicFileOperations writeOperation =
        AtomicFileOperationFactory.getAtomicFileOperations(dataLoadLocation);

    try {

      dataOutputStream = writeOperation.openForWrite(FileWriteOperation.OVERWRITE);
      brWriter = new BufferedWriter(new OutputStreamWriter(dataOutputStream,
              Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));

      String metadataInstance = gsonObjectToWrite.toJson(listOfLoadFolderDetails.toArray());
      brWriter.write(metadataInstance);
    } catch (IOException ioe) {
      LOG.error("Error message: " + ioe.getLocalizedMessage());
      writeOperation.setFailed();
      throw ioe;
    } finally {
      try {
        if (null != brWriter) {
          brWriter.flush();
        }
      } catch (Exception e) {
        throw e;

      }
      CarbonUtil.closeStreams(brWriter);

    }
    writeOperation.close();

  }

  public static String readCurrentTime() {
    SimpleDateFormat sdf = new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP_MILLIS);
    String date = null;

    date = sdf.format(new Date());

    return date;
  }

  public static void main(String[] args) throws Exception {
    new StoreCreator(new File("target/store").getAbsolutePath(),
        new File("../hadoop/src/test/resources/data.csv").getCanonicalPath()).createCarbonStore();
  }

}