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

package org.apache.carbondata.hadoop.api;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.CarbonLoadOptionConstants;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.metadata.datatype.StructField;
import org.apache.carbondata.core.metadata.datatype.StructType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonThreadFactory;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.core.util.ObjectSerializationUtil;
import org.apache.carbondata.core.util.ThreadLocalSessionInfo;
import org.apache.carbondata.hadoop.internal.ObjectArrayWritable;
import org.apache.carbondata.processing.loading.ComplexDelimitersEnum;
import org.apache.carbondata.processing.loading.DataLoadExecutor;
import org.apache.carbondata.processing.loading.TableProcessingOperations;
import org.apache.carbondata.processing.loading.iterator.CarbonOutputIteratorWrapper;
import org.apache.carbondata.processing.loading.model.CarbonDataLoadSchema;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

/**
 * This is table level output format which writes the data to store in new segment. Each load
 * creates new segment folder and manages the folder through tablestatus file.
 * It also generate and writes dictionary data during load only if dictionary server is configured.
 */
// TODO Move dictionary generater which is coded in spark to MR framework.
public class CarbonTableOutputFormat extends FileOutputFormat<NullWritable, ObjectArrayWritable> {

  private static final String LOAD_MODEL = "mapreduce.carbontable.load.model";
  private static final String DATABASE_NAME = "mapreduce.carbontable.databaseName";
  private static final String TABLE_NAME = "mapreduce.carbontable.tableName";
  private static final String TABLE = "mapreduce.carbontable.table";
  private static final String TABLE_PATH = "mapreduce.carbontable.tablepath";
  private static final String INPUT_SCHEMA = "mapreduce.carbontable.inputschema";
  private static final String TEMP_STORE_LOCATIONS = "mapreduce.carbontable.tempstore.locations";
  private static final String OVERWRITE_SET = "mapreduce.carbontable.set.overwrite";
  public static final String COMPLEX_DELIMITERS = "mapreduce.carbontable.complex_delimiters";
  private static final String CARBON_TRANSACTIONAL_TABLE =
      "mapreduce.input.carboninputformat.transactional";
  public static final String SERIALIZATION_NULL_FORMAT =
      "mapreduce.carbontable.serialization.null.format";
  public static final String BAD_RECORDS_LOGGER_ENABLE =
      "mapreduce.carbontable.bad.records.logger.enable";
  public static final String BAD_RECORDS_LOGGER_ACTION =
      "mapreduce.carbontable.bad.records.logger.action";
  public static final String IS_EMPTY_DATA_BAD_RECORD =
      "mapreduce.carbontable.empty.data.bad.record";
  public static final String SKIP_EMPTY_LINE = "mapreduce.carbontable.skip.empty.line";
  public static final String SORT_SCOPE = "mapreduce.carbontable.load.sort.scope";
  public static final String BATCH_SORT_SIZE_INMB =
      "mapreduce.carbontable.batch.sort.size.inmb";
  public static final String GLOBAL_SORT_PARTITIONS =
      "mapreduce.carbontable.global.sort.partitions";
  public static final String BAD_RECORD_PATH = "mapreduce.carbontable.bad.record.path";
  public static final String DATE_FORMAT = "mapreduce.carbontable.date.format";
  public static final String TIMESTAMP_FORMAT = "mapreduce.carbontable.timestamp.format";
  public static final String IS_ONE_PASS_LOAD = "mapreduce.carbontable.one.pass.load";
  public static final String DICTIONARY_SERVER_HOST =
      "mapreduce.carbontable.dict.server.host";
  public static final String DICTIONARY_SERVER_PORT =
      "mapreduce.carbontable.dict.server.port";
  /**
   * Set the update timestamp if user sets in case of update query. It needs to be updated
   * in load status update time
   */
  public static final String UPADTE_TIMESTAMP = "mapreduce.carbontable.update.timestamp";

  /**
   * During update query we first delete the old data and then add updated data to new segment, so
   * sometimes there is a chance that complete segments needs to removed during deletion. We should
   * do 'Mark for delete' for those segments during table status update.
   */
  public static final String SEGMENTS_TO_BE_DELETED =
      "mapreduce.carbontable.segments.to.be.removed";

  /**
   * It is used only to fire events in case of any child tables to be loaded.
   */
  public static final String OPERATION_CONTEXT = "mapreduce.carbontable.operation.context";

  private static final Logger LOG =
      LogServiceFactory.getLogService(CarbonTableOutputFormat.class.getName());

  private CarbonOutputCommitter committer;

  public static void setDatabaseName(Configuration configuration, String databaseName) {
    if (null != databaseName) {
      configuration.set(DATABASE_NAME, databaseName);
    }
  }

  public static String getDatabaseName(Configuration configuration) {
    return configuration.get(DATABASE_NAME);
  }

  public static void setTableName(Configuration configuration, String tableName) {
    if (null != tableName) {
      configuration.set(TABLE_NAME, tableName);
    }
  }

  public static String getTableName(Configuration configuration) {
    return configuration.get(TABLE_NAME);
  }

  public static void setTablePath(Configuration configuration, String tablePath) {
    if (null != tablePath) {
      configuration.set(TABLE_PATH, tablePath);
    }
  }

  public static String getTablePath(Configuration configuration) {
    return configuration.get(TABLE_PATH);
  }

  public static void setCarbonTable(Configuration configuration, CarbonTable carbonTable)
      throws IOException {
    if (carbonTable != null) {
      configuration.set(TABLE,
          ObjectSerializationUtil.convertObjectToString(carbonTable.getTableInfo().serialize()));
    }
  }

  public static CarbonTable getCarbonTable(Configuration configuration) throws IOException {
    CarbonTable carbonTable = null;
    String encodedString = configuration.get(TABLE);
    if (encodedString != null) {
      byte[] bytes = (byte[]) ObjectSerializationUtil.convertStringToObject(encodedString);
      TableInfo tableInfo = TableInfo.deserialize(bytes);
      carbonTable = CarbonTable.buildFromTableInfo(tableInfo);
    }
    return carbonTable;
  }

  public static void setLoadModel(Configuration configuration, CarbonLoadModel loadModel)
      throws IOException {
    if (loadModel != null) {
      configuration.set(LOAD_MODEL, ObjectSerializationUtil.convertObjectToString(loadModel));
    }
  }

  public static void setInputSchema(Configuration configuration, StructType inputSchema)
      throws IOException {
    if (inputSchema != null && inputSchema.getFields().size() > 0) {
      configuration.set(INPUT_SCHEMA, ObjectSerializationUtil.convertObjectToString(inputSchema));
    } else {
      throw new UnsupportedOperationException("Input schema must be set");
    }
  }

  private static StructType getInputSchema(Configuration configuration) throws IOException {
    String encodedString = configuration.get(INPUT_SCHEMA);
    if (encodedString != null) {
      return (StructType) ObjectSerializationUtil.convertStringToObject(encodedString);
    }
    return null;
  }

  public static boolean isOverwriteSet(Configuration configuration) {
    String overwrite = configuration.get(OVERWRITE_SET);
    if (overwrite != null) {
      return Boolean.parseBoolean(overwrite);
    }
    return false;
  }

  public static void setOverwrite(Configuration configuration, boolean overwrite) {
    configuration.set(OVERWRITE_SET, String.valueOf(overwrite));
  }

  public static void setTempStoreLocations(Configuration configuration, String[] tempLocations)
      throws IOException {
    if (tempLocations != null && tempLocations.length > 0) {
      configuration
          .set(TEMP_STORE_LOCATIONS, ObjectSerializationUtil.convertObjectToString(tempLocations));
    }
  }

  private static String[] getTempStoreLocations(TaskAttemptContext taskAttemptContext)
      throws IOException {
    String encodedString = taskAttemptContext.getConfiguration().get(TEMP_STORE_LOCATIONS);
    if (encodedString != null) {
      return (String[]) ObjectSerializationUtil.convertStringToObject(encodedString);
    }
    return new String[] {
        System.getProperty("java.io.tmpdir") + "/" + System.nanoTime() + "_" + taskAttemptContext
            .getTaskAttemptID().toString() };
  }

  @Override
  public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException {
    if (this.committer == null) {
      Path output = getOutputPath(context);
      this.committer = new CarbonOutputCommitter(output, context);
    }
    return this.committer;
  }

  @Override
  public RecordWriter<NullWritable, ObjectArrayWritable> getRecordWriter(
      final TaskAttemptContext taskAttemptContext) throws IOException {
    final CarbonLoadModel loadModel = getLoadModel(taskAttemptContext.getConfiguration());
    String appName =
        taskAttemptContext.getConfiguration().get(CarbonCommonConstants.CARBON_WRITTEN_BY_APPNAME);
    if (null != appName) {
      CarbonProperties.getInstance()
          .addProperty(CarbonCommonConstants.CARBON_WRITTEN_BY_APPNAME, appName);
    }
    //if loadModel having taskNo already(like in SDK) then no need to overwrite
    short sdkWriterCores = loadModel.getSdkWriterCores();
    int itrSize = (sdkWriterCores > 0) ? sdkWriterCores : 1;
    final CarbonOutputIteratorWrapper[] iterators = new CarbonOutputIteratorWrapper[itrSize];
    for (int i = 0; i < itrSize; i++) {
      iterators[i] = new CarbonOutputIteratorWrapper();
    }
    if (null == loadModel.getTaskNo() || loadModel.getTaskNo().isEmpty()) {
      loadModel.setTaskNo(taskAttemptContext.getConfiguration()
          .get("carbon.outputformat.taskno", String.valueOf(System.nanoTime())));
    }
    loadModel.setDataWritePath(
        taskAttemptContext.getConfiguration().get("carbon.outputformat.writepath"));
    final String[] tempStoreLocations = getTempStoreLocations(taskAttemptContext);
    DataTypeUtil.clearFormatter();
    final DataLoadExecutor dataLoadExecutor = new DataLoadExecutor();
    final ExecutorService executorService = Executors.newFixedThreadPool(1,
        new CarbonThreadFactory("CarbonRecordWriter:" + loadModel.getTableName()));
    // It should be started in new thread as the underlying iterator uses blocking queue.
    Future future = executorService.submit(new Thread() {
      @Override public void run() {
        ThreadLocalSessionInfo.setConfigurationToCurrentThread(taskAttemptContext
            .getConfiguration());
        try {
          dataLoadExecutor
              .execute(loadModel, tempStoreLocations, iterators);
        } catch (Exception e) {
          executorService.shutdownNow();
          for (CarbonOutputIteratorWrapper iterator : iterators) {
            iterator.closeWriter(true);
          }
          try {
            dataLoadExecutor.close();
          } catch (Exception ex) {
            // As already exception happened before close() send that exception.
            throw new RuntimeException(e);
          }
          throw new RuntimeException(e);
        } finally {
          ThreadLocalSessionInfo.unsetAll();
        }
      }
    });

    if (sdkWriterCores > 0) {
      // CarbonMultiRecordWriter handles the load balancing of the write rows in round robin.
      return new CarbonMultiRecordWriter(iterators, dataLoadExecutor, loadModel, future,
          executorService);
    } else {
      return new CarbonRecordWriter(iterators[0], dataLoadExecutor, loadModel, future,
          executorService);
    }
  }

  public static CarbonLoadModel getLoadModel(Configuration conf) throws IOException {
    CarbonLoadModel model;
    String encodedString = conf.get(LOAD_MODEL);
    if (encodedString != null) {
      model = (CarbonLoadModel) ObjectSerializationUtil.convertStringToObject(encodedString);
      return model;
    }
    model = new CarbonLoadModel();
    CarbonProperties carbonProperty = CarbonProperties.getInstance();
    model.setDatabaseName(CarbonTableOutputFormat.getDatabaseName(conf));
    model.setTableName(CarbonTableOutputFormat.getTableName(conf));
    model.setCarbonTransactionalTable(true);
    CarbonTable carbonTable = getCarbonTable(conf);
    String columnCompressor = carbonTable.getTableInfo().getFactTable().getTableProperties().get(
        CarbonCommonConstants.COMPRESSOR);
    if (null == columnCompressor) {
      columnCompressor = CompressorFactory.getInstance().getCompressor().getName();
    }
    model.setColumnCompressor(columnCompressor);
    model.setCarbonDataLoadSchema(new CarbonDataLoadSchema(carbonTable));
    model.setTablePath(getTablePath(conf));
    setFileHeader(conf, model);
    model.setSerializationNullFormat(conf.get(SERIALIZATION_NULL_FORMAT, "\\N"));
    model.setBadRecordsLoggerEnable(
        conf.get(
            BAD_RECORDS_LOGGER_ENABLE,
            carbonProperty.getProperty(
                CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORDS_LOGGER_ENABLE,
                CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORDS_LOGGER_ENABLE_DEFAULT)));
    model.setBadRecordsAction(
        conf.get(
            BAD_RECORDS_LOGGER_ACTION,
            carbonProperty.getProperty(
                CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION,
                CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION_DEFAULT)));

    model.setIsEmptyDataBadRecord(
        conf.get(
            IS_EMPTY_DATA_BAD_RECORD,
            carbonProperty.getProperty(
                CarbonLoadOptionConstants.CARBON_OPTIONS_IS_EMPTY_DATA_BAD_RECORD,
                CarbonLoadOptionConstants.CARBON_OPTIONS_IS_EMPTY_DATA_BAD_RECORD_DEFAULT)));

    model.setSkipEmptyLine(
        conf.get(
            SKIP_EMPTY_LINE,
            carbonProperty.getProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_SKIP_EMPTY_LINE)));

    String complexDelim = conf.get(COMPLEX_DELIMITERS);
    if (null == complexDelim) {
      complexDelim = ComplexDelimitersEnum.COMPLEX_DELIMITERS_LEVEL_1.value() + ","
          + ComplexDelimitersEnum.COMPLEX_DELIMITERS_LEVEL_2.value() + ","
          + ComplexDelimitersEnum.COMPLEX_DELIMITERS_LEVEL_3.value();
    }
    String[] split = complexDelim.split(",");
    model.setComplexDelimiter(split[0]);
    if (split.length > 2) {
      model.setComplexDelimiter(split[1]);
      model.setComplexDelimiter(split[2]);
    } else if (split.length > 1) {
      model.setComplexDelimiter(split[1]);
    }
    model.setDateFormat(
        conf.get(
            DATE_FORMAT,
            carbonProperty.getProperty(
                CarbonLoadOptionConstants.CARBON_OPTIONS_DATEFORMAT,
                CarbonLoadOptionConstants.CARBON_OPTIONS_DATEFORMAT_DEFAULT)));

    model.setTimestampformat(
        conf.get(
            TIMESTAMP_FORMAT,
            carbonProperty.getProperty(
                CarbonLoadOptionConstants.CARBON_OPTIONS_TIMESTAMPFORMAT,
                CarbonLoadOptionConstants.CARBON_OPTIONS_TIMESTAMPFORMAT_DEFAULT)));

    model.setGlobalSortPartitions(
        conf.get(
            GLOBAL_SORT_PARTITIONS,
            carbonProperty.getProperty(
                CarbonLoadOptionConstants.CARBON_OPTIONS_GLOBAL_SORT_PARTITIONS,
                null)));

    model.setBatchSortSizeInMb(
        conf.get(
            BATCH_SORT_SIZE_INMB,
            carbonProperty.getProperty(
                CarbonLoadOptionConstants.CARBON_OPTIONS_BATCH_SORT_SIZE_INMB,
                carbonProperty.getProperty(
                    CarbonCommonConstants.LOAD_BATCH_SORT_SIZE_INMB,
                    CarbonCommonConstants.LOAD_BATCH_SORT_SIZE_INMB_DEFAULT))));

    String badRecordsPath = conf.get(BAD_RECORD_PATH);
    if (StringUtils.isEmpty(badRecordsPath)) {
      badRecordsPath =
          carbonTable.getTableInfo().getFactTable().getTableProperties().get("bad_record_path");
      if (StringUtils.isEmpty(badRecordsPath)) {
        badRecordsPath = carbonProperty
            .getProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORD_PATH, carbonProperty
                .getProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC,
                    CarbonCommonConstants.CARBON_BADRECORDS_LOC_DEFAULT_VAL));
      }
    }
    model.setBadRecordsLocation(badRecordsPath);
    model.setUseOnePass(
        conf.getBoolean(IS_ONE_PASS_LOAD,
            Boolean.parseBoolean(
                carbonProperty.getProperty(
                    CarbonLoadOptionConstants.CARBON_OPTIONS_SINGLE_PASS,
                    CarbonLoadOptionConstants.CARBON_OPTIONS_SINGLE_PASS_DEFAULT))));
    return model;
  }

  private static void setFileHeader(Configuration configuration, CarbonLoadModel model)
      throws IOException {
    StructType inputSchema = getInputSchema(configuration);
    if (inputSchema == null || inputSchema.getFields().size() == 0) {
      throw new UnsupportedOperationException("Input schema must be set");
    }
    List<StructField> fields = inputSchema.getFields();
    StringBuilder builder = new StringBuilder();
    String[] columns = new String[fields.size()];
    int i = 0;
    for (StructField field : fields) {
      builder.append(field.getFieldName());
      builder.append(",");
      columns[i++] = field.getFieldName();
    }
    String header = builder.toString();
    model.setCsvHeader(header.substring(0, header.length() - 1));
    model.setCsvHeaderColumns(columns);
  }

  public static class CarbonRecordWriter extends RecordWriter<NullWritable, ObjectArrayWritable> {

    private CarbonOutputIteratorWrapper iteratorWrapper;

    private DataLoadExecutor dataLoadExecutor;

    private CarbonLoadModel loadModel;

    private ExecutorService executorService;

    private Future future;

    private boolean isClosed;

    public CarbonRecordWriter(CarbonOutputIteratorWrapper iteratorWrapper,
        DataLoadExecutor dataLoadExecutor, CarbonLoadModel loadModel, Future future,
        ExecutorService executorService) {
      this.iteratorWrapper = iteratorWrapper;
      this.dataLoadExecutor = dataLoadExecutor;
      this.loadModel = loadModel;
      this.executorService = executorService;
      this.future = future;
    }

    @Override public void write(NullWritable aVoid, ObjectArrayWritable objects)
        throws InterruptedException {
      if (iteratorWrapper != null) {
        iteratorWrapper.write(objects.get());
      }
    }

    @Override public void close(TaskAttemptContext taskAttemptContext) throws InterruptedException {
      if (!isClosed) {
        isClosed = true;
        if (iteratorWrapper != null) {
          iteratorWrapper.closeWriter(false);
        }
        try {
          future.get();
        } catch (ExecutionException e) {
          LOG.error("Error while loading data", e);
          throw new InterruptedException(e.getMessage());
        } finally {
          executorService.shutdownNow();
          dataLoadExecutor.close();
          ThreadLocalSessionInfo.unsetAll();
          // clean up the folders and files created locally for data load operation
          TableProcessingOperations.deleteLocalDataLoadFolderLocation(loadModel, false, false);
        }
        LOG.info("Closed writer task " + taskAttemptContext.getTaskAttemptID());
      }
    }

    public CarbonLoadModel getLoadModel() {
      return loadModel;
    }
  }

  /* CarbonMultiRecordWriter takes multiple iterators
  and handles the load balancing of the write rows in round robin. */
  public static class CarbonMultiRecordWriter extends CarbonRecordWriter {

    private CarbonOutputIteratorWrapper[] iterators;

    // keep counts of number of writes called
    // and it is used to load balance each write call to one iterator.
    private AtomicLong counter;

    CarbonMultiRecordWriter(CarbonOutputIteratorWrapper[] iterators,
        DataLoadExecutor dataLoadExecutor, CarbonLoadModel loadModel, Future future,
        ExecutorService executorService) {
      super(null, dataLoadExecutor, loadModel, future, executorService);
      this.iterators = iterators;
      counter = new AtomicLong(0);
    }

    @Override public void write(NullWritable aVoid, ObjectArrayWritable objects)
        throws InterruptedException {
      int iteratorNum = (int) (counter.incrementAndGet() % iterators.length);
      synchronized (iterators[iteratorNum]) {
        iterators[iteratorNum].write(objects.get());
      }
    }

    @Override public void close(TaskAttemptContext taskAttemptContext) throws InterruptedException {
      for (int i = 0; i < iterators.length; i++) {
        synchronized (iterators[i]) {
          iterators[i].closeWriter(false);
        }
      }
      super.close(taskAttemptContext);
    }
  }
}
