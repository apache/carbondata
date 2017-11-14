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
package org.apache.carbondata.processing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
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
import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperations;
import org.apache.carbondata.core.fileoperations.AtomicFileOperationsImpl;
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
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonStorePath;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.core.writer.CarbonDictionaryWriter;
import org.apache.carbondata.core.writer.CarbonDictionaryWriterImpl;
import org.apache.carbondata.core.writer.ThriftWriter;
import org.apache.carbondata.core.writer.sortindex.CarbonDictionarySortIndexWriter;
import org.apache.carbondata.core.writer.sortindex.CarbonDictionarySortIndexWriterImpl;
import org.apache.carbondata.core.writer.sortindex.CarbonDictionarySortInfo;
import org.apache.carbondata.core.writer.sortindex.CarbonDictionarySortInfoPreparator;
import org.apache.carbondata.processing.util.TableOptionConstant;
import org.apache.carbondata.processing.loading.csvinput.BlockDetails;
import org.apache.carbondata.processing.loading.csvinput.CSVInputFormat;
import org.apache.carbondata.processing.loading.csvinput.CSVRecordReaderIterator;
import org.apache.carbondata.processing.loading.csvinput.StringArrayWritable;
import org.apache.carbondata.processing.loading.model.CarbonDataLoadSchema;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.loading.DataLoadExecutor;
import org.apache.carbondata.processing.loading.constants.DataLoadProcessorConstants;

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

/**
 * This class will create store file based on provided schema
 *
 */
public class StoreCreator {

  private static AbsoluteTableIdentifier absoluteTableIdentifier;

  static {
    try {
      String storePath = new File("target/store").getCanonicalPath();
      String dbName = "testdb";
      String tableName = "testtable";
      absoluteTableIdentifier =
          new AbsoluteTableIdentifier(storePath, new CarbonTableIdentifier(dbName, tableName, UUID.randomUUID().toString()));
    } catch (IOException ex) {

    }
  }

  public static AbsoluteTableIdentifier getAbsoluteTableIdentifier() {
    return absoluteTableIdentifier;
  }

  /**
   * Create store without any restructure
   */
  public static void createCarbonStore() {
    try {
      String factFilePath = new File("../hadoop/src/test/resources/data.csv").getCanonicalPath();
      File storeDir = new File(absoluteTableIdentifier.getStorePath());
      CarbonUtil.deleteFoldersAndFiles(storeDir);
      CarbonProperties.getInstance().addProperty(CarbonCommonConstants.STORE_LOCATION_HDFS,
          absoluteTableIdentifier.getStorePath());

      CarbonTable table = createTable();
      writeDictionary(factFilePath, table);
      CarbonDataLoadSchema schema = new CarbonDataLoadSchema(table);
      CarbonLoadModel loadModel = new CarbonLoadModel();
      loadModel.setCarbonDataLoadSchema(schema);
      loadModel.setDatabaseName(absoluteTableIdentifier.getCarbonTableIdentifier().getDatabaseName());
      loadModel.setTableName(absoluteTableIdentifier.getCarbonTableIdentifier().getTableName());
      loadModel.setTableName(absoluteTableIdentifier.getCarbonTableIdentifier().getTableName());
      loadModel.setFactFilePath(factFilePath);
      loadModel.setLoadMetadataDetails(new ArrayList<LoadMetadataDetails>());
      loadModel.setStorePath(absoluteTableIdentifier.getStorePath());
      loadModel.setDateFormat(null);
      loadModel.setDefaultTimestampFormat(CarbonProperties.getInstance().getProperty(
          CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
          CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT));
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
      loadModel.setPartitionId("0");
      loadModel.setFactTimeStamp(System.currentTimeMillis());
      loadModel.setMaxColumns("10");

      loadData(loadModel, absoluteTableIdentifier.getStorePath());

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static CarbonTable createTable() throws IOException {
    TableInfo tableInfo = new TableInfo();
    tableInfo.setStorePath(absoluteTableIdentifier.getStorePath());
    tableInfo.setDatabaseName(absoluteTableIdentifier.getCarbonTableIdentifier().getDatabaseName());
    TableSchema tableSchema = new TableSchema();
    tableSchema.setTableName(absoluteTableIdentifier.getCarbonTableIdentifier().getTableName());
    List<ColumnSchema> columnSchemas = new ArrayList<ColumnSchema>();
    ArrayList<Encoding> encodings = new ArrayList<>();
    encodings.add(Encoding.DICTIONARY);
    ColumnSchema id = new ColumnSchema();
    id.setColumnName("ID");
    id.setColumnar(true);
    id.setDataType(DataTypes.INT);
    id.setEncodingList(encodings);
    id.setColumnUniqueId(UUID.randomUUID().toString());
    id.setDimensionColumn(true);
    id.setColumnGroup(1);
    columnSchemas.add(id);

    ColumnSchema date = new ColumnSchema();
    date.setColumnName("date");
    date.setColumnar(true);
    date.setDataType(DataTypes.STRING);
    date.setEncodingList(encodings);
    date.setColumnUniqueId(UUID.randomUUID().toString());
    date.setDimensionColumn(true);
    date.setColumnGroup(2);
    columnSchemas.add(date);

    ColumnSchema country = new ColumnSchema();
    country.setColumnName("country");
    country.setColumnar(true);
    country.setDataType(DataTypes.STRING);
    country.setEncodingList(encodings);
    country.setColumnUniqueId(UUID.randomUUID().toString());
    country.setDimensionColumn(true);
    country.setColumnGroup(3);
    columnSchemas.add(country);

    ColumnSchema name = new ColumnSchema();
    name.setColumnName("name");
    name.setColumnar(true);
    name.setDataType(DataTypes.STRING);
    name.setEncodingList(encodings);
    name.setColumnUniqueId(UUID.randomUUID().toString());
    name.setDimensionColumn(true);
    name.setColumnGroup(4);
    columnSchemas.add(name);

    ColumnSchema phonetype = new ColumnSchema();
    phonetype.setColumnName("phonetype");
    phonetype.setColumnar(true);
    phonetype.setDataType(DataTypes.STRING);
    phonetype.setEncodingList(encodings);
    phonetype.setColumnUniqueId(UUID.randomUUID().toString());
    phonetype.setDimensionColumn(true);
    phonetype.setColumnGroup(5);
    columnSchemas.add(phonetype);

    ColumnSchema serialname = new ColumnSchema();
    serialname.setColumnName("serialname");
    serialname.setColumnar(true);
    serialname.setDataType(DataTypes.STRING);
    serialname.setEncodingList(encodings);
    serialname.setColumnUniqueId(UUID.randomUUID().toString());
    serialname.setDimensionColumn(true);
    serialname.setColumnGroup(6);
    columnSchemas.add(serialname);

    ColumnSchema salary = new ColumnSchema();
    salary.setColumnName("salary");
    salary.setColumnar(true);
    salary.setDataType(DataTypes.INT);
    salary.setEncodingList(new ArrayList<Encoding>());
    salary.setColumnUniqueId(UUID.randomUUID().toString());
    salary.setDimensionColumn(false);
    salary.setColumnGroup(7);
    columnSchemas.add(salary);

    tableSchema.setListOfColumns(columnSchemas);
    SchemaEvolution schemaEvol = new SchemaEvolution();
    schemaEvol.setSchemaEvolutionEntryList(new ArrayList<SchemaEvolutionEntry>());
    tableSchema.setSchemaEvalution(schemaEvol);
    tableSchema.setTableId(UUID.randomUUID().toString());
    tableInfo.setTableUniqueName(
        absoluteTableIdentifier.getCarbonTableIdentifier().getDatabaseName() + "_"
            + absoluteTableIdentifier.getCarbonTableIdentifier().getTableName());
    tableInfo.setLastUpdatedTime(System.currentTimeMillis());
    tableInfo.setFactTable(tableSchema);

    CarbonTablePath carbonTablePath = CarbonStorePath
        .getCarbonTablePath(absoluteTableIdentifier.getStorePath(),
            absoluteTableIdentifier.getCarbonTableIdentifier());
    String schemaFilePath = carbonTablePath.getSchemaFilePath();
    String schemaMetadataPath = CarbonTablePath.getFolderContainingFile(schemaFilePath);
    tableInfo.setMetaDataFilepath(schemaMetadataPath);
    CarbonMetadata.getInstance().loadTableMetadata(tableInfo);

    SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
    org.apache.carbondata.format.TableInfo thriftTableInfo = schemaConverter
        .fromWrapperToExternalTableInfo(tableInfo, tableInfo.getDatabaseName(),
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

  private static void writeDictionary(String factFilePath, CarbonTable table) throws Exception {
    BufferedReader reader = new BufferedReader(new FileReader(factFilePath));
    String header = reader.readLine();
    String[] split = header.split(",");
    List<CarbonColumn> allCols = new ArrayList<CarbonColumn>();
    List<CarbonDimension> dims = table.getDimensionByTableName(table.getFactTableName());
    allCols.addAll(dims);
    List<CarbonMeasure> msrs = table.getMeasureByTableName(table.getFactTableName());
    allCols.addAll(msrs);
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
        .createCache(CacheType.REVERSE_DICTIONARY, absoluteTableIdentifier.getStorePath());
    for (int i = 0; i < set.length; i++) {
      ColumnIdentifier columnIdentifier = new ColumnIdentifier(dims.get(i).getColumnId(), null, null);
      DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier = new DictionaryColumnUniqueIdentifier(table.getCarbonTableIdentifier(), columnIdentifier, columnIdentifier.getDataType(),
          CarbonStorePath.getCarbonTablePath(table.getStorePath(), table.getCarbonTableIdentifier()));
      CarbonDictionaryWriter writer =
          new CarbonDictionaryWriterImpl(absoluteTableIdentifier.getStorePath(),
              absoluteTableIdentifier.getCarbonTableIdentifier(), dictionaryColumnUniqueIdentifier);
      for (String value : set[i]) {
        writer.write(value);
      }
      writer.close();
      writer.commit();
      Dictionary dict = (Dictionary) dictCache.get(
          new DictionaryColumnUniqueIdentifier(absoluteTableIdentifier.getCarbonTableIdentifier(),
        		  columnIdentifier, dims.get(i).getDataType(),
              CarbonStorePath.getCarbonTablePath(absoluteTableIdentifier)));
      CarbonDictionarySortInfoPreparator preparator =
          new CarbonDictionarySortInfoPreparator();
      List<String> newDistinctValues = new ArrayList<String>();
      CarbonDictionarySortInfo dictionarySortInfo =
          preparator.getDictionarySortInfo(newDistinctValues, dict, dims.get(i).getDataType());
      CarbonDictionarySortIndexWriter carbonDictionaryWriter =
          new CarbonDictionarySortIndexWriterImpl(
              absoluteTableIdentifier.getCarbonTableIdentifier(), dictionaryColumnUniqueIdentifier,
              absoluteTableIdentifier.getStorePath());
      try {
        carbonDictionaryWriter.writeSortIndex(dictionarySortInfo.getSortIndex());
        carbonDictionaryWriter.writeInvertedSortIndex(dictionarySortInfo.getSortIndexInverted());
      } finally {
        carbonDictionaryWriter.close();
      }
    }
    reader.close();
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
    new File(storeLocation).mkdirs();
    String outPutLoc = storeLocation + "/etl";
    String databaseName = loadModel.getDatabaseName();
    String tableName = loadModel.getTableName();
    String tempLocationKey = databaseName + '_' + tableName + "_1";
    CarbonProperties.getInstance().addProperty(tempLocationKey, storeLocation);
    CarbonProperties.getInstance().addProperty("store_output_location", outPutLoc);
    CarbonProperties.getInstance().addProperty("send.signal.load", "false");
    CarbonProperties.getInstance().addProperty("carbon.is.columnar.storage", "true");
    CarbonProperties.getInstance().addProperty("carbon.dimension.split.value.in.columnar", "1");
    CarbonProperties.getInstance().addProperty("carbon.is.fullyfilled.bits", "true");
    CarbonProperties.getInstance().addProperty("is.int.based.indexer", "true");
    CarbonProperties.getInstance().addProperty("aggregate.columnar.keyblock", "true");
    CarbonProperties.getInstance().addProperty("high.cardinality.value", "100000");
    CarbonProperties.getInstance().addProperty("is.compressed.keyblock", "false");
    CarbonProperties.getInstance().addProperty("carbon.leaf.node.size", "120000");

    String graphPath =
        outPutLoc + File.separator + loadModel.getDatabaseName() + File.separator + tableName
            + File.separator + 0 + File.separator + 1 + File.separator + tableName + ".ktr";
    File path = new File(graphPath);
    if (path.exists()) {
      path.delete();
    }

    BlockDetails blockDetails = new BlockDetails(new Path(loadModel.getFactFilePath()),
        0, new File(loadModel.getFactFilePath()).length(), new String[] {"localhost"});
    Configuration configuration = new Configuration();
    CSVInputFormat.setCommentCharacter(configuration, loadModel.getCommentChar());
    CSVInputFormat.setCSVDelimiter(configuration, loadModel.getCsvDelimiter());
    CSVInputFormat.setEscapeCharacter(configuration, loadModel.getEscapeChar());
    CSVInputFormat.setHeaderExtractionEnabled(configuration, true);
    CSVInputFormat.setQuoteCharacter(configuration, loadModel.getQuoteChar());
    CSVInputFormat.setReadBufferSize(configuration, CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CSV_READ_BUFFER_SIZE,
            CarbonCommonConstants.CSV_READ_BUFFER_SIZE_DEFAULT));
    CSVInputFormat.setMaxColumns(configuration, "10");
    CSVInputFormat.setNumberOfColumns(configuration, "7");

    TaskAttemptContextImpl hadoopAttemptContext = new TaskAttemptContextImpl(configuration, new TaskAttemptID("", 1, TaskType.MAP, 0, 0));
    CSVInputFormat format = new CSVInputFormat();

    RecordReader<NullWritable, StringArrayWritable> recordReader =
        format.createRecordReader(blockDetails, hadoopAttemptContext);

    CSVRecordReaderIterator readerIterator = new CSVRecordReaderIterator(recordReader, blockDetails, hadoopAttemptContext);
    String[] storeLocationArray = new String[] {storeLocation};
    new DataLoadExecutor().execute(loadModel,
        storeLocationArray,
        new CarbonIterator[]{readerIterator});

    writeLoadMetadata(loadModel.getCarbonDataLoadSchema(), loadModel.getTableName(), loadModel.getTableName(),
        new ArrayList<LoadMetadataDetails>());

    String segLocation =
        storeLocation + "/" + databaseName + "/" + tableName + "/Fact/Part0/Segment_0";
    File file = new File(segLocation);
    File factFile = null;
    File[] folderList = file.listFiles();
    File folder = null;
    for (int i = 0; i < folderList.length; i++) {
      if (folderList[i].isDirectory()) {
        folder = folderList[i];
      }
    }
    if (folder.isDirectory()) {
      File[] files = folder.listFiles();
      for (int i = 0; i < files.length; i++) {
        if (!files[i].isDirectory() && files[i].getName().startsWith("part")) {
          factFile = files[i];
          break;
        }
      }
      factFile.renameTo(new File(segLocation + "/" + factFile.getName()));
      CarbonUtil.deleteFoldersAndFiles(folder);
    }
  }

  public static void writeLoadMetadata(CarbonDataLoadSchema schema, String databaseName,
      String tableName, List<LoadMetadataDetails> listOfLoadFolderDetails) throws IOException {
    LoadMetadataDetails loadMetadataDetails = new LoadMetadataDetails();
    loadMetadataDetails.setLoadEndTime(System.currentTimeMillis());
    loadMetadataDetails.setSegmentStatus(SegmentStatus.SUCCESS);
    loadMetadataDetails.setLoadName(String.valueOf(0));
    loadMetadataDetails.setLoadStartTime(loadMetadataDetails.getTimeStamp(readCurrentTime()));
    listOfLoadFolderDetails.add(loadMetadataDetails);

    String dataLoadLocation = schema.getCarbonTable().getMetaDataFilepath() + File.separator
        + CarbonCommonConstants.LOADMETADATA_FILENAME;

    DataOutputStream dataOutputStream;
    Gson gsonObjectToWrite = new Gson();
    BufferedWriter brWriter = null;

    AtomicFileOperations writeOperation =
        new AtomicFileOperationsImpl(dataLoadLocation, FileFactory.getFileType(dataLoadLocation));

    try {

      dataOutputStream = writeOperation.openForWrite(FileWriteOperation.OVERWRITE);
      brWriter = new BufferedWriter(new OutputStreamWriter(dataOutputStream,
              Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));

      String metadataInstance = gsonObjectToWrite.toJson(listOfLoadFolderDetails.toArray());
      brWriter.write(metadataInstance);
    } catch (Exception ex) {
      throw ex;
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
    SimpleDateFormat sdf = new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP);
    String date = null;

    date = sdf.format(new Date());

    return date;
  }

  public static void main(String[] args) {
    StoreCreator.createCarbonStore();
  }

}