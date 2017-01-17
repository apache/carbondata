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
package org.apache.carbondata.hadoop.test.util;

import com.google.gson.Gson;
import org.apache.hadoop.fs.Path;

import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.processing.model.CarbonDataLoadSchema;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.ColumnIdentifier;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.datatype.DataType;
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
import org.apache.carbondata.core.util.path.CarbonStorePath;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.processing.csvreaderstep.BlockDetails;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.writer.CarbonDictionaryWriter;
import org.apache.carbondata.core.writer.CarbonDictionaryWriterImpl;
import org.apache.carbondata.core.writer.ThriftWriter;
import org.apache.carbondata.core.writer.sortindex.CarbonDictionarySortIndexWriter;
import org.apache.carbondata.core.writer.sortindex.CarbonDictionarySortIndexWriterImpl;
import org.apache.carbondata.core.writer.sortindex.CarbonDictionarySortInfo;
import org.apache.carbondata.core.writer.sortindex.CarbonDictionarySortInfoPreparator;
import org.apache.carbondata.core.fileoperations.AtomicFileOperations;
import org.apache.carbondata.core.fileoperations.AtomicFileOperationsImpl;
import org.apache.carbondata.core.fileoperations.FileWriteOperation;
import org.apache.carbondata.processing.api.dataloader.DataLoadModel;
import org.apache.carbondata.processing.api.dataloader.SchemaInfo;
import org.apache.carbondata.processing.constants.TableOptionConstant;
import org.apache.carbondata.processing.csvload.DataGraphExecuter;
import org.apache.carbondata.processing.dataprocessor.DataProcessTaskStatus;
import org.apache.carbondata.processing.dataprocessor.IDataProcessStatus;
import org.apache.carbondata.processing.graphgenerator.GraphGenerator;
import org.apache.carbondata.processing.graphgenerator.GraphGeneratorException;

import java.io.*;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * This class will create store file based on provided schema
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

      String factFilePath = new File("src/test/resources/data.csv").getCanonicalPath();
      File storeDir = new File(absoluteTableIdentifier.getStorePath());
      CarbonUtil.deleteFoldersAndFiles(storeDir);
      CarbonProperties.getInstance().addProperty(CarbonCommonConstants.STORE_LOCATION_HDFS,
          absoluteTableIdentifier.getStorePath());

      String kettleHomePath = "../processing/carbonplugins";
      CarbonTable table = createTable();
      writeDictionary(factFilePath, table);
      CarbonDataLoadSchema schema = new CarbonDataLoadSchema(table);
      LoadModel loadModel = new LoadModel();
      String partitionId = "0";
      loadModel.setSchema(schema);
      loadModel.setDatabaseName(absoluteTableIdentifier.getCarbonTableIdentifier().getDatabaseName());
      loadModel.setTableName(absoluteTableIdentifier.getCarbonTableIdentifier().getTableName());
      loadModel.setTableName(absoluteTableIdentifier.getCarbonTableIdentifier().getTableName());
      loadModel.setFactFilePath(factFilePath);
      loadModel.setLoadMetadataDetails(new ArrayList<LoadMetadataDetails>());

      executeGraph(loadModel, absoluteTableIdentifier.getStorePath(), kettleHomePath);
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
    id.setDataType(DataType.INT);
    id.setEncodingList(encodings);
    id.setColumnUniqueId(UUID.randomUUID().toString());
    id.setDimensionColumn(true);
    id.setColumnGroup(1);
    columnSchemas.add(id);

    ColumnSchema date = new ColumnSchema();
    date.setColumnName("date");
    date.setColumnar(true);
    date.setDataType(DataType.STRING);
    date.setEncodingList(encodings);
    date.setColumnUniqueId(UUID.randomUUID().toString());
    date.setDimensionColumn(true);
    date.setColumnGroup(2);
    columnSchemas.add(date);

    ColumnSchema country = new ColumnSchema();
    country.setColumnName("country");
    country.setColumnar(true);
    country.setDataType(DataType.STRING);
    country.setEncodingList(encodings);
    country.setColumnUniqueId(UUID.randomUUID().toString());
    country.setDimensionColumn(true);
    country.setColumnGroup(3);
    columnSchemas.add(country);

    ColumnSchema name = new ColumnSchema();
    name.setColumnName("name");
    name.setColumnar(true);
    name.setDataType(DataType.STRING);
    name.setEncodingList(encodings);
    name.setColumnUniqueId(UUID.randomUUID().toString());
    name.setDimensionColumn(true);
    name.setColumnGroup(4);
    columnSchemas.add(name);

    ColumnSchema phonetype = new ColumnSchema();
    phonetype.setColumnName("phonetype");
    phonetype.setColumnar(true);
    phonetype.setDataType(DataType.STRING);
    phonetype.setEncodingList(encodings);
    phonetype.setColumnUniqueId(UUID.randomUUID().toString());
    phonetype.setDimensionColumn(true);
    phonetype.setColumnGroup(5);
    columnSchemas.add(phonetype);

    ColumnSchema serialname = new ColumnSchema();
    serialname.setColumnName("serialname");
    serialname.setColumnar(true);
    serialname.setDataType(DataType.STRING);
    serialname.setEncodingList(encodings);
    serialname.setColumnUniqueId(UUID.randomUUID().toString());
    serialname.setDimensionColumn(true);
    serialname.setColumnGroup(6);
    columnSchemas.add(serialname);

    ColumnSchema salary = new ColumnSchema();
    salary.setColumnName("salary");
    salary.setColumnar(true);
    salary.setDataType(DataType.INT);
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
    tableInfo.setAggregateTableList(new ArrayList<TableSchema>());

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
      CarbonDictionaryWriter writer =
          new CarbonDictionaryWriterImpl(absoluteTableIdentifier.getStorePath(),
              absoluteTableIdentifier.getCarbonTableIdentifier(), columnIdentifier);
      for (String value : set[i]) {
        writer.write(value);
      }
      writer.close();
      writer.commit();
      Dictionary dict = (Dictionary) dictCache.get(
          new DictionaryColumnUniqueIdentifier(absoluteTableIdentifier.getCarbonTableIdentifier(),
        		  columnIdentifier, dims.get(i).getDataType()));
      CarbonDictionarySortInfoPreparator preparator =
          new CarbonDictionarySortInfoPreparator();
      List<String> newDistinctValues = new ArrayList<String>();
      CarbonDictionarySortInfo dictionarySortInfo =
          preparator.getDictionarySortInfo(newDistinctValues, dict, dims.get(i).getDataType());
      CarbonDictionarySortIndexWriter carbonDictionaryWriter =
          new CarbonDictionarySortIndexWriterImpl(
              absoluteTableIdentifier.getCarbonTableIdentifier(), columnIdentifier,
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
   * @param kettleHomePath
   * @throws Exception
   */
  public static void executeGraph(LoadModel loadModel, String storeLocation, String kettleHomePath)
      throws Exception {
    System.setProperty("KETTLE_HOME", kettleHomePath);
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

    String fileNamePrefix = "";

    String graphPath =
        outPutLoc + File.separator + loadModel.getDatabaseName() + File.separator + tableName
            + File.separator + 0 + File.separator + 1 + File.separator + tableName + ".ktr";
    File path = new File(graphPath);
    if (path.exists()) {
      path.delete();
    }

    DataProcessTaskStatus dataProcessTaskStatus = new DataProcessTaskStatus(databaseName, tableName);
    dataProcessTaskStatus.setCsvFilePath(loadModel.getFactFilePath());
    SchemaInfo info = new SchemaInfo();
    BlockDetails blockDetails = new BlockDetails(new Path(loadModel.getFactFilePath()),
        0, new File(loadModel.getFactFilePath()).length(), new String[] {"localhost"});
    GraphGenerator.blockInfo.put("qwqwq", new BlockDetails[] { blockDetails });
    dataProcessTaskStatus.setBlocksID("qwqwq");
    dataProcessTaskStatus.setEscapeCharacter("\\");
    dataProcessTaskStatus.setQuoteCharacter("\"");
    dataProcessTaskStatus.setCommentCharacter("#");
    dataProcessTaskStatus.setDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);
    info.setDatabaseName(databaseName);
    info.setTableName(tableName);
    info.setSerializationNullFormat(
        TableOptionConstant.SERIALIZATION_NULL_FORMAT.getName() + "," + "\\N");
    info.setBadRecordsLoggerEnable(
        TableOptionConstant.BAD_RECORDS_LOGGER_ENABLE.getName() + "," + "false");
    info.setBadRecordsLoggerAction(
        TableOptionConstant.BAD_RECORDS_ACTION.getName() + "," + "force");

    generateGraph(dataProcessTaskStatus, info, loadModel.getTableName(), "0", loadModel.getSchema(), null,
        loadModel.getLoadMetadataDetails());

    DataGraphExecuter graphExecuter = new DataGraphExecuter(dataProcessTaskStatus);
    graphExecuter
        .executeGraph(graphPath, info, loadModel.getSchema());
    //    LoadMetadataDetails[] loadDetails =
    //        CarbonUtil.readLoadMetadata(loadModel.schema.getCarbonTable().getMetaDataFilepath());
    writeLoadMetadata(loadModel.schema, loadModel.getTableName(), loadModel.getTableName(),
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
      //      Files.copy(factFile.toPath(), file.toPath(), REPLACE_EXISTING);
      factFile.renameTo(new File(segLocation + "/" + factFile.getName()));
      CarbonUtil.deleteFoldersAndFiles(folder);
    }
  }

  public static void writeLoadMetadata(CarbonDataLoadSchema schema, String databaseName,
      String tableName, List<LoadMetadataDetails> listOfLoadFolderDetails) throws IOException {
    LoadMetadataDetails loadMetadataDetails = new LoadMetadataDetails();
    loadMetadataDetails.setLoadStatus("SUCCESS");
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

  /**
   * generate graph
   *
   * @param dataProcessTaskStatus
   * @param info
   * @param tableName
   * @param partitionID
   * @param schema
   * @param factStoreLocation
   * @param loadMetadataDetails
   * @throws GraphGeneratorException
   */
  private static void generateGraph(IDataProcessStatus dataProcessTaskStatus, SchemaInfo info,
      String tableName, String partitionID, CarbonDataLoadSchema schema, String factStoreLocation,
      List<LoadMetadataDetails> loadMetadataDetails)
      throws GraphGeneratorException {
    DataLoadModel model = new DataLoadModel();
    model.setCsvLoad(null != dataProcessTaskStatus.getCsvFilePath() || null != dataProcessTaskStatus.getFilesToProcess());
    model.setSchemaInfo(info);
    model.setTableName(dataProcessTaskStatus.getTableName());
    model.setTaskNo("1");
    model.setBlocksID(dataProcessTaskStatus.getBlocksID());
    model.setFactTimeStamp(System.currentTimeMillis() + "");
    model.setEscapeCharacter(dataProcessTaskStatus.getEscapeCharacter());
    model.setQuoteCharacter(dataProcessTaskStatus.getQuoteCharacter());
    model.setCommentCharacter(dataProcessTaskStatus.getCommentCharacter());
    model.setDateFormat(dataProcessTaskStatus.getDateFormat());
    String outputLocation = CarbonProperties.getInstance()
        .getProperty("store_output_location", "../carbon-store/system/carbon/etl");
    GraphGenerator generator =
        new GraphGenerator(model, partitionID, factStoreLocation, schema, "0", outputLocation);
    generator.generateGraph();
  }

  public static String readCurrentTime() {
    SimpleDateFormat sdf = new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP);
    String date = null;

    date = sdf.format(new Date());

    return date;
  }

  /**
   * This is local model object used inside this class to store information related to data loading
   */
  private static class LoadModel {

    private CarbonDataLoadSchema schema;
    private String tableName;
    private String databaseName;
    private List<LoadMetadataDetails> loadMetaDetail;
    private String factFilePath;

    public void setSchema(CarbonDataLoadSchema schema) {
      this.schema = schema;
    }

    public List<LoadMetadataDetails> getLoadMetadataDetails() {
      return loadMetaDetail;
    }

    public CarbonDataLoadSchema getSchema() {
      return schema;
    }

    public String getFactFilePath() {
      return factFilePath;
    }

    public String getTableName() {
      return tableName;
    }

    public String getDatabaseName() {
      return databaseName;
    }

    public void setLoadMetadataDetails(List<LoadMetadataDetails> loadMetaDetail) {
      this.loadMetaDetail = loadMetaDetail;
    }

    public void setFactFilePath(String factFilePath) {
      this.factFilePath = factFilePath;
    }

    public void setTableName(String tableName) {
      this.tableName = tableName;
    }

    public void setDatabaseName(String databaseName) {
      this.databaseName = databaseName;
    }


  }

  public static void main(String[] args) {
    StoreCreator.createCarbonStore();
  }

}