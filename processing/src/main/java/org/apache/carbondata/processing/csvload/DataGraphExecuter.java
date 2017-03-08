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

package org.apache.carbondata.processing.csvload;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.datastore.impl.FileFactory.FileType;
import org.apache.carbondata.processing.api.dataloader.SchemaInfo;
import org.apache.carbondata.processing.constants.DataProcessorConstants;
import org.apache.carbondata.processing.csvreaderstep.CsvInputMeta;
import org.apache.carbondata.processing.dataprocessor.IDataProcessStatus;
import org.apache.carbondata.processing.etl.DataLoadingException;
import org.apache.carbondata.processing.model.CarbonDataLoadSchema;
import org.apache.carbondata.processing.surrogatekeysgenerator.csvbased.BadRecordsLogger;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.logging.LoggingObjectInterface;
import org.pentaho.di.core.logging.LoggingRegistry;
import org.pentaho.di.core.xml.XMLHandlerCache;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.getfilenames.GetFileNamesMeta;
import org.pentaho.di.trans.steps.hadoopfileinput.HadoopFileInputMeta;
import org.pentaho.di.trans.steps.textfileinput.TextFileInputField;

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
   * This Method checks whether csv file provided and the column name in schema are same
   * or not
   *
   * @param columnNames
   * @param csvFilePath
   * @return true if same, false otherwise.
   */
  private boolean checkCSVAndRequestedTableColumns(String[] columnNames, String csvFilePath,
      String delimiter) throws IOException {
    return GraphExecutionUtil.checkCSVAndRequestedTableColumns(csvFilePath, columnNames, delimiter);
  }

  /**
   * This method returns the Columns names from the schema.
   *
   * @param tableName
   * @return column names array.
   */
  private String[] getColumnNames(String tableName, CarbonDataLoadSchema schema) {
    Set<String> columnNames = CarbonDataProcessorUtil.getSchemaColumnNames(schema, tableName);
    return columnNames.toArray(new String[columnNames.size()]);
  }

  private void validateCSV(String tableName, CarbonFile f, CarbonDataLoadSchema schema,
      String delimiter) throws DataLoadingException, IOException {

    String[] columnNames = getColumnNames(tableName, schema);

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
  }

  public void executeGraph(String graphFilePath, SchemaInfo schemaInfo, CarbonDataLoadSchema schema)
      throws DataLoadingException {

    //This Method will validate the both fact and dimension csv files.
    if (!schemaInfo.isAutoAggregateRequest() && model.getRddIteratorKey() == null) {
      validateCSVFiles(schema);
    }
    execute(graphFilePath, schemaInfo);
  }

  /**
   * executeGraph which generate the kettle graph
   *
   * @throws DataLoadingException
   */

  private void execute(String graphFilePath, SchemaInfo schemaInfo)
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
        trans.setVariable("modifiedDimNames", model.getDimTables());
        trans.setVariable("csvInputFilePath", model.getCsvFilePath());
        trans.setVariable(CarbonCommonConstants.BAD_RECORD_KEY, null);
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
        processCsvInputMeta(stepsMeta, builder, measuresInCSVFile);
        processGetFileNamesMeta(stepsMeta);

        processHadoopFileInputMeta(stepsMeta, builder, measuresInCSVFile);
      }
      setGraphLogLevel();
      trans.execute(null);
      LOGGER.info("Graph execution is started " + graphFilePath);
      trans.waitUntilFinished();
      LOGGER.info("Graph execution is finished.");
    } catch (KettleException | IOException e) {
      LOGGER.error(e, "Unable to start execution of graph " + e.getMessage());
      throw new DataLoadingException("Unable to start execution of graph ", e);
    }

    //Don't change the logic of creating key
    String key = model.getDatabaseName() + '/' + model.getTableName() + '_' + model.getTableName();

    if (trans.getErrors() > 0) {
      if (null != trans.getVariable(CarbonCommonConstants.BAD_RECORD_KEY)) {
        LOGGER.error(trans.getVariable(CarbonCommonConstants.BAD_RECORD_KEY));
        throw new DataLoadingException(
            "Data load failed due to bad record ," + trans
                .getVariable(CarbonCommonConstants.BAD_RECORD_KEY));
      }
      LOGGER.error("Graph Execution had errors");
      throw new DataLoadingException("Due to internal errors, please check logs for more details.");
    } else if (null != BadRecordsLogger.hasBadRecord(key)) {
      LOGGER.error("Data load is partially success");
      throw new DataLoadingException(DataProcessorConstants.BAD_REC_FOUND,
          "Data load is partially success");
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
   * @param stepsMeta
   * @param builder
   * @param measuresInCSVFile
   * @throws DataLoadingException
   */
  private void processHadoopFileInputMeta(List<StepMeta> stepsMeta, StringBuilder builder,
      StringBuilder measuresInCSVFile) throws DataLoadingException {
    for (StepMeta step : stepsMeta) {
      if (step.getStepMetaInterface() instanceof HadoopFileInputMeta) {

        HadoopFileInputMeta stepMetaInterface = (HadoopFileInputMeta) step.getStepMetaInterface();
        if (null != model.getCsvFilePath()) {
          stepMetaInterface.setFilenameField("filename");
          stepMetaInterface.setFileName(new String[] { "${csvInputFilePath}" });
          stepMetaInterface.setDefault();
          stepMetaInterface.setEncoding(CarbonCommonConstants.DEFAULT_CHARSET);
          stepMetaInterface.setEnclosure("\"");
          stepMetaInterface.setHeader(true);
          stepMetaInterface.setSeparator(",");
          stepMetaInterface.setAcceptingFilenames(true);
          stepMetaInterface.setAcceptingStepName("getFileNames");
          stepMetaInterface.setFileFormat("mixed");
          stepMetaInterface.setAcceptingField("filename");

          CarbonFile csvFileToRead = GraphExecutionUtil.getCsvFileToRead(model.getCsvFilePath());
          TextFileInputField[] inputFields = GraphExecutionUtil
              .getTextInputFiles(csvFileToRead, builder, measuresInCSVFile, ",");
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
          stepMetaInterface.setEncoding(CarbonCommonConstants.DEFAULT_CHARSET);
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
            ((CsvInputMeta) step.getStepMetaInterface())
              .setEscapeCharacter(model.getEscapeCharacter());
            ((CsvInputMeta) step.getStepMetaInterface()).setHeaderPresent(false);

          } else if (model.getFilesToProcess().size() > 0) {
            CarbonFile csvFile =
                GraphExecutionUtil.getCsvFileToRead(model.getFilesToProcess().get(0));
            TextFileInputField[] inputFields = GraphExecutionUtil
                .getTextInputFiles(csvFile, builder, measuresInCSVFile,
                    model.getCsvDelimiter());
            ((CsvInputMeta) step.getStepMetaInterface()).setInputFields(inputFields);
            ((CsvInputMeta) step.getStepMetaInterface()).setDelimiter(model.getCsvDelimiter());
            ((CsvInputMeta) step.getStepMetaInterface())
              .setEscapeCharacter(model.getEscapeCharacter());
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
   * @param stepsMeta
   * @param builder
   * @param measuresInCSVFile
   * @throws DataLoadingException
   */
  private void processCsvInputMeta(List<StepMeta> stepsMeta, StringBuilder builder,
      StringBuilder measuresInCSVFile) throws DataLoadingException {
    for (StepMeta step : stepsMeta) {
      if (step.getStepMetaInterface() instanceof CsvInputMeta) {
        if (null != model.getCsvFilePath() && model.getRddIteratorKey() == null) {
          CarbonFile csvFileToRead = GraphExecutionUtil.getCsvFileToRead(model.getCsvFilePath());
          TextFileInputField[] inputFields = GraphExecutionUtil
              .getTextInputFiles(csvFileToRead, builder, measuresInCSVFile, ",");
          ((CsvInputMeta) step.getStepMetaInterface()).setInputFields(inputFields);
        } else if (model.isDirectLoad()) {
          if (null != model.getCsvHeader() && !model.getCsvHeader().isEmpty()) {
            TextFileInputField[] inputFields = GraphExecutionUtil
                .getTextInputFiles(model.getCsvHeader(), builder, measuresInCSVFile, ",");
            ((CsvInputMeta) step.getStepMetaInterface()).setInputFields(inputFields);
            ((CsvInputMeta) step.getStepMetaInterface()).setDelimiter(model.getCsvDelimiter());
            ((CsvInputMeta) step.getStepMetaInterface())
                .setEscapeCharacter(model.getEscapeCharacter());
            ((CsvInputMeta) step.getStepMetaInterface()).setHeaderPresent(false);

          } else if (model.getFilesToProcess().size() > 0) {
            CarbonFile csvFileToRead =
                GraphExecutionUtil.getCsvFileToRead(model.getFilesToProcess().get(0));
            TextFileInputField[] inputFields = GraphExecutionUtil
                .getTextInputFiles(csvFileToRead, builder, measuresInCSVFile,
                    model.getCsvDelimiter());
            ((CsvInputMeta) step.getStepMetaInterface()).setInputFields(inputFields);
            ((CsvInputMeta) step.getStepMetaInterface()).setDelimiter(model.getCsvDelimiter());
            ((CsvInputMeta) step.getStepMetaInterface())
              .setEscapeCharacter(model.getEscapeCharacter());
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

  /**
   * This method will validate the both fact as well as dimension csv files.
   *
   * @param schema
   * @throws DataLoadingException
   */
  private void validateCSVFiles(CarbonDataLoadSchema schema) throws DataLoadingException {
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
            validateCSV(model.getTableName(), f, schema, ",");
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
            validateCSV(model.getTableName(),
                FileFactory.getCarbonFile(csvFilePath, fileType), schema, ",");
          } else {
            validateCSV(model.getTableName(), FileFactory
                .getCarbonFile(csvFilePath + CarbonCommonConstants.FILE_INPROGRESS_STATUS,
                    fileType), schema, ",");
          }

        }

      } catch (IOException e) {
        LOGGER.error(e,
            "Error while checking file exists" + csvFilePath);
      }
    } else if (model.isDirectLoad()) {
      if (null != model.getCsvHeader() && !model.getCsvHeader().isEmpty()) {
        if (!CarbonDataProcessorUtil
            .isHeaderValid(model.getTableName(), model.getCsvHeader(), schema, ",")) {
          LOGGER.error("CSV header provided in DDL is not proper."
              + " Column names in schema and CSV header are not the same.");
          throw new DataLoadingException(DataProcessorConstants.CSV_VALIDATION_ERRROR_CODE,
              "CSV header provided in DDL is not proper. Column names in schema and CSV header are "
                  + "not the same.");
        }
      } else {
        for (String file : model.getFilesToProcess()) {
          try {
            FileFactory.FileType fileType = FileFactory.getFileType(file);
            if (FileFactory.isFileExist(file, fileType)) {
              validateCSV(model.getTableName(),
                  FileFactory.getCarbonFile(file, fileType), schema,
                  model.getCsvDelimiter());
            }
          } catch (IOException e) {
            LOGGER.error(e,
                "Error while checking file exists" + file);
          }
        }
      }
    }
  }

}
