/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.processing.csvload;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.CarbonDataLoadSchema;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.filesystem.CarbonFileFilter;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.datastorage.store.impl.FileFactory.FileType;
import org.carbondata.processing.api.dataloader.SchemaInfo;
import org.carbondata.processing.constants.DataProcessorConstants;
import org.carbondata.processing.csvreaderstep.CsvInputMeta;
import org.carbondata.processing.dataprocessor.IDataProcessStatus;
import org.carbondata.processing.etl.DataLoadingException;
import org.carbondata.processing.surrogatekeysgenerator.csvbased.BadRecordslogger;

import org.apache.commons.vfs.FileSystemException;
import org.apache.commons.vfs.FileSystemManager;
import org.apache.commons.vfs.impl.DefaultFileSystemManager;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.logging.LoggingObjectInterface;
import org.pentaho.di.core.logging.LoggingRegistry;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.core.xml.XMLHandlerCache;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.getfilenames.GetFileNamesMeta;
import org.pentaho.di.trans.steps.hadoopfileinput.HadoopFileInputMeta;
import org.pentaho.di.trans.steps.textfileinput.TextFileInputField;
import org.pentaho.hdfs.vfs.HDFSFileProvider;

public class DataGraphExecuter {
  /**
   * Comment for <code>LOGGER</code>
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DataGraphExecuter.class.getName());
  /**
   * graph transformation object
   */
  private Trans trans;
  /**
   *
   */
  private IDataProcessStatus model;

  public DataGraphExecuter(IDataProcessStatus model) {
    this.model = model;
  }

  /**
   * This method check whether then CSV file has header or not.
   *
   * @param columnNames
   * @param csvFilePath
   * @return
   */
  private boolean checkHeaderExist(String[] columnNames, String csvFilePath, String delimiter) {
    return GraphExecutionUtil.checkHeaderExist(csvFilePath, columnNames, delimiter);
  }

  /**
   * This Method checks whether csv file provided and the column name in schema are same
   * or not
   *
   * @param columnNames
   * @param csvFilePath
   * @return true if same, false otherwise.
   */
  private boolean checkCSVAndRequestedTableColumns(String[] columnNames, String csvFilePath,
      String delimiter) {
    return GraphExecutionUtil.checkCSVAndRequestedTableColumns(csvFilePath, columnNames, delimiter);
  }

  /**
   * This method returns the Columns names from the schema.
   *
   * @param schemaInfo
   * @param tableName
   * @return column names array.
   */
  private String[] getColumnNames(SchemaInfo schemaInfo, String tableName, String partitionId,
      CarbonDataLoadSchema schema) {

    Set<String> columnNames = GraphExecutionUtil.getSchemaColumnNames(schema, tableName);

    return columnNames.toArray(new String[columnNames.size()]);
  }

  /**
   * This method returns the Columns names for the dimension only from the schema.
   *
   * @param schema
   * @param schemaInfo
   * @return column names array.
   */
  private String[] getDimColumnNames(SchemaInfo schemaInfo, String factTableName,
      String dimTableName, String partitionId, CarbonDataLoadSchema schema) {

    Set<String> columnNames = GraphExecutionUtil.getDimensionColumnNames(dimTableName, schema);
    return columnNames.toArray(new String[columnNames.size()]);
  }

  private void validateCSV(SchemaInfo schemaInfo, String tableName, CarbonFile f,
      String partitionId, CarbonDataLoadSchema schema, String delimiter)
      throws DataLoadingException {

    String[] columnNames = getColumnNames(schemaInfo, tableName, partitionId, schema);

    if (!checkCSVAndRequestedTableColumns(columnNames, f.getAbsolutePath(), delimiter)) {
      LOGGER.error(
          "CSV File provided is not proper. Column names in schema and csv header are not same. "
              + "CSVFile Name : "
              + f.getName());
      throw new DataLoadingException(DataProcessorConstants.CSV_VALIDATION_ERRROR_CODE,
          "CSV File provided is not proper. Column names in schema and csv header are not same. "
              + "CSVFile Name : "
              + f.getName());
    }

    if (!checkHeaderExist(columnNames, f.getAbsolutePath(), delimiter)) {
      LOGGER.error("Header Columns are not present in the provided CSV File :" + f.getName());
      throw new DataLoadingException(DataProcessorConstants.CSV_VALIDATION_ERRROR_CODE,
          "Header Columns are not present in the provided CSV File:" + f.getName());

    }
  }

  public void executeGraph(String graphFilePath, List<String> measureColumns, SchemaInfo schemaInfo,
      String partitionId, CarbonDataLoadSchema schema) throws DataLoadingException {

    //This Method will validate the both fact and dimension csv files.
    if (!schemaInfo.isAutoAggregateRequest()) {
      validateCSVFiles(schemaInfo, partitionId, schema);
    }
    execute(graphFilePath, measureColumns, schemaInfo);
  }

  /**
   * executeGraph which generate the kettle graph
   *
   * @throws DataLoadingException
   */

  private void execute(String graphFilePath, List<String> measureColumns, SchemaInfo schemaInfo)
      throws DataLoadingException {

    //This Method will validate the both fact and dimension csv files.

    initKettleEnv();
    TransMeta transMeta = null;
    try {
      transMeta = new TransMeta(graphFilePath);
      transMeta.setFilename(graphFilePath);
      trans = new Trans(transMeta);
      if (!schemaInfo.isAutoAggregateRequest()) {
        // Register HDFS as a file system type with VFS to make HadoopFileInputMeta work
        boolean hdfsReadMode =
            model.getCsvFilePath() != null && model.getCsvFilePath().startsWith("hdfs:");
        if (hdfsReadMode) {
          try {
            FileSystemManager fsm = KettleVFS.getInstance().getFileSystemManager();
            if (fsm instanceof DefaultFileSystemManager) {
              if (!Arrays.asList(fsm.getSchemes()).contains("hdfs")
                  && !((DefaultFileSystemManager) fsm).hasProvider("hdfs")) {
                ((DefaultFileSystemManager) fsm).addProvider("hdfs", new HDFSFileProvider());
              }
            }
          } catch (FileSystemException e) {
            if (!e.getMessage().contains("Multiple providers registered for URL scheme")) {
              LOGGER.error(e,
                  e.getMessage());
            }
          }
        }

        trans.setVariable("modifiedDimNames", model.getDimTables());
        trans.setVariable("csvInputFilePath", model.getCsvFilePath());
        trans.setVariable("dimFileLocDir", model.getDimCSVDirLoc());
        if (hdfsReadMode) {
          trans.addParameterDefinition("vfs.hdfs.dfs.client.read.shortcircuit", "true", "");
          trans.addParameterDefinition("vfs.hdfs.dfs.domain.socket.path",
              "/var/lib/hadoop-hdfs-new/dn_socket", "");
          trans.addParameterDefinition("vfs.hdfs.dfs.block.local-path-access.user", "hadoop,root",
              "");
          trans.addParameterDefinition("vfs.hdfs.io.file.buffer.size", "5048576", "");
        }
        List<StepMeta> stepsMeta = trans.getTransMeta().getSteps();
        StringBuilder builder = new StringBuilder();
        StringBuilder measuresInCSVFile = new StringBuilder();
        processCsvInputMeta(measureColumns, stepsMeta, builder, measuresInCSVFile);
        processGetFileNamesMeta(stepsMeta);

        processHadoopFileInputMeta(measureColumns, stepsMeta, builder, measuresInCSVFile);
      }
      setGraphLogLevel();
      trans.execute(null);
      LOGGER.info("Graph execution is started " + graphFilePath);
      trans.waitUntilFinished();
      LOGGER.info("Graph execution is finished.");
    } catch (KettleXMLException e) {
      LOGGER.error(e,
          "Unable to start execution of graph " + e.getMessage());
      throw new DataLoadingException("Unable to start execution of graph ", e);

    } catch (KettleException e) {
      LOGGER.error(e,
          "Unable to start execution of graph " + e.getMessage());
      throw new DataLoadingException("Unable to start execution of graph ", e);
    } catch (Throwable e) {
      LOGGER.error(e,
          "Unable to start execution of graph " + e.getMessage());
      throw new DataLoadingException("Unable to start execution of graph ", e);
    }

    //Don't change the logic of creating key
    String key = model.getSchemaName() + '/' + model.getCubeName() + '_' + model.getTableName();

    if (trans.getErrors() > 0) {
      LOGGER.error("Graph Execution had errors");
      throw new DataLoadingException("Internal Errors");
    } else if (null != BadRecordslogger.hasBadRecord(key)) {
      LOGGER.error("Graph Execution is partcially success");
      throw new DataLoadingException(DataProcessorConstants.BAD_REC_FOUND,
          "Graph Execution is partcially success");
    } else {
      LOGGER.info("Graph execution task is over with No error.");
    }
    LoggingRegistry instance = LoggingRegistry.getInstance();
    Map<String, LoggingObjectInterface> map = instance.getMap();
    if (null != map) {
      for (Entry<String, LoggingObjectInterface> entry : map.entrySet()) {
        instance.removeIncludingChildren(entry.getKey());
      }
    }

    map = null;
    XMLHandlerCache.getInstance().clear();
    trans.cleanup();
    trans.eraseParameters();
    trans.killAll();
    trans = null;
  }

  /**
   * @param measureColumns
   * @param stepsMeta
   * @param builder
   * @param measuresInCSVFile
   * @throws DataLoadingException
   */
  private void processHadoopFileInputMeta(List<String> measureColumns, List<StepMeta> stepsMeta,
      StringBuilder builder, StringBuilder measuresInCSVFile) throws DataLoadingException {
    for (StepMeta step : stepsMeta) {
      if (step.getStepMetaInterface() instanceof HadoopFileInputMeta) {

        HadoopFileInputMeta stepMetaInterface = (HadoopFileInputMeta) step.getStepMetaInterface();
        if (null != model.getCsvFilePath()) {
          stepMetaInterface.setFilenameField("filename");
          stepMetaInterface.setFileName(new String[] { "${csvInputFilePath}" });
          stepMetaInterface.setDefault();
          stepMetaInterface.setEncoding("UTF-8");
          stepMetaInterface.setEnclosure("\"");
          stepMetaInterface.setHeader(true);
          stepMetaInterface.setSeparator(",");
          stepMetaInterface.setAcceptingFilenames(true);
          stepMetaInterface.setAcceptingStepName("getFileNames");
          stepMetaInterface.setFileFormat("mixed");
          stepMetaInterface.setAcceptingField("filename");

          CarbonFile csvFileToRead = GraphExecutionUtil.getCsvFileToRead(model.getCsvFilePath());
          TextFileInputField[] inputFields = GraphExecutionUtil
              .getTextInputFiles(csvFileToRead, measureColumns, builder, measuresInCSVFile, ",");
          stepMetaInterface.setInputFields(inputFields);
        } else if (model.isDirectLoad()) {
          String[] files = new String[model.getFilesToProcess().size()];
          int i = 0;
          for (String file : model.getFilesToProcess()) {
            files[i++] = file;
          }
          stepMetaInterface.setFileName(files);
          stepMetaInterface.setFilenameField("filename");
          stepMetaInterface.setDefault();
          stepMetaInterface.setEncoding("UTF-8");
          stepMetaInterface.setEnclosure("\"");
          stepMetaInterface.setHeader(true);
          stepMetaInterface.setSeparator(",");
          stepMetaInterface.setAcceptingFilenames(true);
          stepMetaInterface.setAcceptingStepName("getFileNames");
          stepMetaInterface.setFileFormat("mixed");
          stepMetaInterface.setAcceptingField("filename");

          if (null != model.getCsvHeader() && !model.getCsvHeader().isEmpty()) {
            TextFileInputField[] inputParams = GraphExecutionUtil
                .getTextInputFiles(model.getCsvHeader(), builder, measuresInCSVFile, ",");
            ((CsvInputMeta) step.getStepMetaInterface()).setInputFields(inputParams);
            ((CsvInputMeta) step.getStepMetaInterface()).setDelimiter(model.getCsvDelimiter());
            ((CsvInputMeta) step.getStepMetaInterface()).setHeaderPresent(false);

          } else if (model.getFilesToProcess().size() > 0) {
            CarbonFile csvFile =
                GraphExecutionUtil.getCsvFileToRead(model.getFilesToProcess().get(0));
            TextFileInputField[] inputFields = GraphExecutionUtil
                .getTextInputFiles(csvFile, measureColumns, builder, measuresInCSVFile,
                    model.getCsvDelimiter());
            ((CsvInputMeta) step.getStepMetaInterface()).setInputFields(inputFields);
            ((CsvInputMeta) step.getStepMetaInterface()).setDelimiter(model.getCsvDelimiter());
            ((CsvInputMeta) step.getStepMetaInterface()).setHeaderPresent(true);
          }
        }

        break;
      }
    }
  }

  /**
   * @param stepsMeta
   * @throws IOException
   */
  private void processGetFileNamesMeta(List<StepMeta> stepsMeta) throws IOException {
    for (StepMeta step : stepsMeta) {
      if (step.getStepMetaInterface() instanceof GetFileNamesMeta) {
        GetFileNamesMeta stepMetaInterface = (GetFileNamesMeta) step.getStepMetaInterface();
        if (null != model.getCsvFilePath()) {
          boolean checkIsFolder = GraphExecutionUtil.checkIsFolder(model.getCsvFilePath());
          if (checkIsFolder) {
            stepMetaInterface.setFileName(new String[] { model.getCsvFilePath() });
            stepMetaInterface.setFileMask(new String[] { ".*\\.csv$|.*\\.inprogress" });
            stepMetaInterface.setExcludeFileMask(new String[] { "1" });
          } else {
            //If absolute file path is provided for the data load and stopped in between then csv
            // file will be
            // changed to inprogress, and when next time server start then we need to check the
            // file name extension.
            // can contain .csv.inprogress file.

            FileType fileType = FileFactory.getFileType(model.getCsvFilePath());

            boolean exists = FileFactory.isFileExist(model.getCsvFilePath(), fileType);

            if (exists) {
              stepMetaInterface.setFileName(new String[] { model.getCsvFilePath() });
              stepMetaInterface.setExcludeFileMask(new String[] { null });
            } else {
              stepMetaInterface.setFileName(new String[] {
                  model.getCsvFilePath() + CarbonCommonConstants.FILE_INPROGRESS_STATUS });
              stepMetaInterface.setExcludeFileMask(new String[] { null });
            }
          }
        } else if (model.isDirectLoad()) {
          String[] files = new String[model.getFilesToProcess().size()];
          int i = 0;
          for (String file : model.getFilesToProcess()) {
            files[i++] = file;
          }
          stepMetaInterface.setFileName(files);
        }
        break;
      }
    }
  }

  /**
   * @param measureColumns
   * @param stepsMeta
   * @param builder
   * @param measuresInCSVFile
   * @throws DataLoadingException
   */
  private void processCsvInputMeta(List<String> measureColumns, List<StepMeta> stepsMeta,
      StringBuilder builder, StringBuilder measuresInCSVFile) throws DataLoadingException {
    for (StepMeta step : stepsMeta) {
      if (step.getStepMetaInterface() instanceof CsvInputMeta) {
        if (null != model.getCsvFilePath()) {
          CarbonFile csvFileToRead = GraphExecutionUtil.getCsvFileToRead(model.getCsvFilePath());
          TextFileInputField[] inputFields = GraphExecutionUtil
              .getTextInputFiles(csvFileToRead, measureColumns, builder, measuresInCSVFile, ",");
          ((CsvInputMeta) step.getStepMetaInterface()).setInputFields(inputFields);
        } else if (model.isDirectLoad()) {
          if (null != model.getCsvHeader() && !model.getCsvHeader().isEmpty()) {
            TextFileInputField[] inputFields = GraphExecutionUtil
                .getTextInputFiles(model.getCsvHeader(), builder, measuresInCSVFile, ",");
            ((CsvInputMeta) step.getStepMetaInterface()).setInputFields(inputFields);
            ((CsvInputMeta) step.getStepMetaInterface()).setDelimiter(model.getCsvDelimiter());
            ((CsvInputMeta) step.getStepMetaInterface()).setHeaderPresent(false);

          } else if (model.getFilesToProcess().size() > 0) {
            CarbonFile csvFileToRead =
                GraphExecutionUtil.getCsvFileToRead(model.getFilesToProcess().get(0));
            TextFileInputField[] inputFields = GraphExecutionUtil
                .getTextInputFiles(csvFileToRead, measureColumns, builder, measuresInCSVFile,
                    model.getCsvDelimiter());
            ((CsvInputMeta) step.getStepMetaInterface()).setInputFields(inputFields);
            ((CsvInputMeta) step.getStepMetaInterface()).setDelimiter(model.getCsvDelimiter());
            ((CsvInputMeta) step.getStepMetaInterface()).setHeaderPresent(true);
          }
        }
        break;
      }
    }
  }

  /**
   *
   */
  private void initKettleEnv() {
    try {
      KettleEnvironment.init(false);
      LOGGER.info("Kettle environment initialized");
    } catch (KettleException ke) {
      LOGGER.error("Unable to initialize Kettle Environment " + ke.getMessage());
    }
  }


  private void setGraphLogLevel() {
    trans.setLogLevel(LogLevel.NOTHING);
  }

  private void validateHeader(SchemaInfo schemaInfo, String partitionId,
      CarbonDataLoadSchema schema) throws DataLoadingException {
    String[] columnNames = getColumnNames(schemaInfo, model.getTableName(), partitionId, schema);
    String[] csvHeader = model.getCsvHeader().toLowerCase().split(",");

    List<String> csvColumnsList = new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);

    for (String column : csvHeader) {
      csvColumnsList.add(column.replaceAll("\"", "").trim());
    }

    int count = 0;

    for (String columns : columnNames) {
      if (csvColumnsList.contains(columns.toLowerCase())) {
        count++;
      }
    }

    if (count != columnNames.length) {
      LOGGER.error(
          "CSV File provided is not proper. Column names in schema and CSV header are not same.");
      throw new DataLoadingException(DataProcessorConstants.CSV_VALIDATION_ERRROR_CODE,
          "CSV File provided is not proper. Column names in schema and csv header are not same.");
    }
  }

  /**
   * This method will validate the both fact as well as dimension csv files.
   *
   * @param schemaInfo
   * @throws DataLoadingException
   */
  private void validateCSVFiles(SchemaInfo schemaInfo, String partitionId,
      CarbonDataLoadSchema schema) throws DataLoadingException {
    // Validate the Fact CSV Files.
    String csvFilePath = model.getCsvFilePath();
    if (csvFilePath != null) {
      FileType fileType = FileFactory.getFileType(csvFilePath);
      try {
        boolean exists = FileFactory.isFileExist(csvFilePath, fileType);
        if (exists && FileFactory.getCarbonFile(csvFilePath, fileType).isDirectory()) {
          CarbonFile fileDir = FileFactory.getCarbonFile(csvFilePath, fileType);
          CarbonFile[] listFiles = fileDir.listFiles(new CarbonFileFilter() {

            @Override public boolean accept(CarbonFile pathname) {
              if (pathname.getName().endsWith(CarbonCommonConstants.CSV_FILE_EXTENSION) || pathname
                  .getName().endsWith(CarbonCommonConstants.CSV_FILE_EXTENSION
                      + CarbonCommonConstants.FILE_INPROGRESS_STATUS)) {
                return true;
              }
              return false;
            }
          });

          for (CarbonFile f : listFiles) {
            validateCSV(schemaInfo, model.getTableName(), f, partitionId, schema, ",");
          }
        } else {

          if (!(csvFilePath.endsWith(CarbonCommonConstants.CSV_FILE_EXTENSION) || csvFilePath
              .endsWith(CarbonCommonConstants.CSV_FILE_EXTENSION
                  + CarbonCommonConstants.FILE_INPROGRESS_STATUS))) {
            LOGGER.error("File provided is not proper, Only csv files are allowed." + csvFilePath);
            throw new DataLoadingException(
                "File provided is not proper, Only csv files are allowed." + csvFilePath);
          }

          if (exists) {
            validateCSV(schemaInfo, model.getTableName(),
                FileFactory.getCarbonFile(csvFilePath, fileType), partitionId, schema, ",");
          } else {
            validateCSV(schemaInfo, model.getTableName(), FileFactory
                .getCarbonFile(csvFilePath + CarbonCommonConstants.FILE_INPROGRESS_STATUS,
                    fileType), partitionId, schema, ",");
          }

        }

      } catch (IOException e) {
        LOGGER.error(e,
            "Error while checking file exists" + csvFilePath);
      }
    } else if (model.isDirectLoad()) {
      if (null != model.getCsvHeader() && !model.getCsvHeader().isEmpty()) {
        validateHeader(schemaInfo, partitionId, schema);
      } else {
        for (String file : model.getFilesToProcess()) {
          try {
            FileFactory.FileType fileType = FileFactory.getFileType(file);
            if (FileFactory.isFileExist(file, fileType)) {
              validateCSV(schemaInfo, model.getTableName(),
                  FileFactory.getCarbonFile(file, fileType), partitionId, schema,
                  model.getCsvDelimiter());
            }
          } catch (IOException e) {
            LOGGER.error(e,
                "Error while checking file exists" + file);
          }
        }
      }
    }

    // Validate the Dimension CSV Files.
    String dimFilesStr = model.getDimCSVDirLoc();

    if (null != dimFilesStr && dimFilesStr.length() > 0) {
      String[] dimMapList = model.getDimCSVDirLoc().split(",");

      for (String dimFileMap : dimMapList) {
        String tableName = dimFileMap.split(":")[0];
        String dimCSVFileLoc = dimFileMap.substring(tableName.length() + 1);

        try {
          if (dimCSVFileLoc != null) {
            FileType fileType = FileFactory.getFileType(dimCSVFileLoc);
            boolean exists = FileFactory.isFileExist(dimCSVFileLoc, fileType);

            if (exists) {
              CarbonFile dimCsvFile = FileFactory.getCarbonFile(dimCSVFileLoc, fileType);

              String dimFileName = dimCsvFile.getName();

              if (dimFileName.endsWith(CarbonCommonConstants.CSV_FILE_EXTENSION)) {
                String dimTableName = dimFileMap.split(":")[0];

                validateDimensionCSV(schemaInfo, model.getTableName(), dimTableName, dimCsvFile,
                    partitionId, schema, ",");
              } else {
                LOGGER.error(
                    "Dimension table file provided to load Dimension tables is not a CSV file : "
                        + dimCSVFileLoc);
                throw new DataLoadingException(DataProcessorConstants.CSV_VALIDATION_ERRROR_CODE,
                    "Dimension table file provided to load Dimension tables is not a CSV file : "
                        + dimCSVFileLoc);
              }
            } else {
              LOGGER.error(
                  "Dimension table csv file not present in the path provided to load Dimension "
                      + "tables : "
                      + dimCSVFileLoc);
              throw new DataLoadingException(DataProcessorConstants.CSV_VALIDATION_ERRROR_CODE,
                  "Dimension table csv file not present in the path provided to load Dimension "
                      + "tables : "
                      + dimCSVFileLoc);
            }
          }
        } catch (IOException e) {
          LOGGER.error(
              "Dimension table csv file not present in the path provided to load Dimension tables"
                  + " : "
                  + dimCSVFileLoc);

          throw new DataLoadingException(DataProcessorConstants.CSV_VALIDATION_ERRROR_CODE,
              "Dimension table csv file not present in the path provided to load Dimension tables"
                  + " : "
                  + dimCSVFileLoc);
        }
      }
    }
  }

  /**
   * Validate the dimension csv files.
   *
   * @param schema
   * @param schemaInfo
   * @param dimFile
   * @throws DataLoadingException
   */
  private void validateDimensionCSV(SchemaInfo schemaInfo, String factTableName,
      String dimTableName, CarbonFile dimFile, String partitionId, CarbonDataLoadSchema schema,
      String delimiter) throws DataLoadingException {
    String[] columnNames =
        getDimColumnNames(schemaInfo, factTableName, dimTableName, partitionId, schema);

    if (null == columnNames || columnNames.length < 1) {
      return;
    }
    if (!checkAllColumnsPresent(columnNames, dimFile.getAbsolutePath(), delimiter)) {
      LOGGER.error(
          "CSV File provided is not proper. Column names in schema and csv header are not same. "
              + "CSVFile Name : "
              + dimFile.getName());
      throw new DataLoadingException(DataProcessorConstants.CSV_VALIDATION_ERRROR_CODE,
          "Dimension CSV file provided is not proper. Column names in Schema and csv header are "
              + "not same. CSVFile Name : "
              + dimFile.getName());
    }

    if (!checkDimHeaderExist(columnNames, dimFile.getAbsolutePath(), delimiter)) {
      LOGGER.error(
          "Header Columns are not present in the provided CSV File For Dimension Table Load :"
              + dimFile.getName());
      throw new DataLoadingException(DataProcessorConstants.CSV_VALIDATION_ERRROR_CODE,
          "Header Columns are not present in the provided CSV File For Dimension Table Load :"
              + dimFile.getName());

    }

  }

  /**
   * Check All the columns are present in the CSV File
   *
   * @param dimFilePath
   * @return
   */
  private boolean checkAllColumnsPresent(String[] columnNames, String dimFilePath,
      String delimiter) {
    return GraphExecutionUtil.checkCSVAndRequestedTableColumns(dimFilePath, columnNames, delimiter);
  }

  /**
   * Check the dimension csv file is having all the dimension.
   */
  private boolean checkDimHeaderExist(String[] columnNames, String dimFilePath, String delimiter) {
    return GraphExecutionUtil.checkHeaderExist(dimFilePath, columnNames, delimiter);
  }

  /**
   * Interrupts all child threads run by kettle to execute the graph
   */
  public void interruptGraphExecution() {
    LOGGER.error("Graph Execution is interrupted");
    if (null != trans) {
      trans.killAll();
      LOGGER.info("Graph execution steps are killed.");
    }
  }

}
