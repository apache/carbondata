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

package org.carbondata.processing.store;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.carbondata.core.metadata.SliceMetaData;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.core.util.CarbonUtilException;
import org.carbondata.core.util.ValueCompressionUtil;
import org.carbondata.processing.store.writer.exception.CarbonDataWriterException;
import org.carbondata.processing.threadbasedmerger.consumer.ConsumerThread;
import org.carbondata.processing.threadbasedmerger.container.Container;
import org.carbondata.processing.threadbasedmerger.producer.ProducerThread;
import org.carbondata.processing.util.CarbonDataProcessorUtil;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

public class CarbonDataWriterStep extends BaseStep implements StepInterface {

  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonDataWriterStep.class.getName());

  /**
   * carbon data writer step data class
   */
  private CarbonDataWriterStepData data;

  /**
   * carbon data writer step meta
   */
  private CarbonDataWriterStepMeta meta;

  /**
   * tabel name
   */
  private String tableName;

  /**
   * measure count
   */
  private int measureCount;

  /**
   * index of mdkey in incoming rows
   */
  private int mdKeyIndex;

  /**
   * temp file location
   */
  private String tempFileLocation;

  /**
   * number of consumer thread
   */
  private int numberOfConsumerThreads;

  /**
   * number of producer thread
   */
  private int numberOfProducerThreads;

  /**
   * buffer size
   */
  private int bufferSize;

  /**
   * mdkey lenght
   */
  private int mdkeyLength;

  /**
   * boolean to check whether producer and cosumer based sorting is enabled
   */
  private boolean isPAndCSorting;

  /**
   * dataHandler
   */
  private CarbonFactHandler dataHandler;

  /**
   * isEmptyLoad
   */
  private boolean isEmptyLoad;

  /**
   * type
   */
  private char[] type;

  private String[] aggregators;

  /**
   * CarbonDataWriterStep Constructor to initialize the step
   *
   * @param stepMeta
   * @param stepDataInterface
   * @param copyNr
   * @param transMeta
   * @param trans
   */
  public CarbonDataWriterStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
      TransMeta transMeta, Trans trans) {
    super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
  }

  /**
   * Perform the equivalent of processing one row. Typically this means
   * reading a row from input (getRow()) and passing a row to output
   * (putRow)).
   *
   * @param smi The steps metadata to work with
   * @param sdi The steps temporary working data to work with (database
   *            connections, result sets, caches, temporary variables, etc.)
   * @return false if no more rows can be processed or an error occurred.
   * @throws KettleException
   */
  public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {

    try {
      meta = (CarbonDataWriterStepMeta) smi;

      // carbon data writer step data
      data = (CarbonDataWriterStepData) sdi;
      // get row from previous step, blocks when needed!
      Object[] row = getRow();
      if (first) {

        first = false;
        // // carbon data writer step meta
        if (null != getInputRowMeta()) {
          this.data.outputRowMeta = (RowMetaInterface) getInputRowMeta().clone();
          this.meta.getFields(data.outputRowMeta, getStepname(), null, null, this);
        }
        // set step configuration
        setStepConfiguration();
      }
      // if row is null then we will start final merging and writing the fact file
      if (null == row) {
        if (!isEmptyLoad) {
          try {
            startBTreeCreation();
          } finally {
            this.dataHandler.closeHandler();
          }
        }
        putRow(data.outputRowMeta, new Object[0]);
        setOutputDone();
        return false;
      }
      // if row is null then there is no more incoming data

    } catch (Exception ex) {
      LOGGER.error(ex);
      throw new RuntimeException(ex);
    }
    putRow(data.outputRowMeta, new Object[0]);
    return true;
  }

  /**
   * below method will be used to write the fact file
   *
   * @throws KettleException
   * @throws CarbonDataWriterException
   */
  private void startBTreeCreation() throws CarbonDataWriterException {
    try {
      if (isPAndCSorting) {
        startPAndCFinalMerge();
      } else {
        startSingleThreadFinalMerge();
      }
      this.dataHandler.finish();
    } catch (CarbonDataWriterException e) {
      LOGGER.error(e);
      throw e;
    } finally {
      this.dataHandler.closeHandler();
    }

  }

  /**
   * This method will be used to get and update the step properties which will
   * required to run this step
   *
   * @throws KettleException
   * @throws CarbonUtilException
   */
  private void setStepConfiguration() throws KettleException {
    CarbonProperties instance = CarbonProperties.getInstance();
    // get the table name
    this.tableName = meta.getTabelName();
    String inputStoreLocation = meta.getSchemaName() + File.separator + meta.getCubeName();
    // get the base store location
    String tempLocationKey = meta.getSchemaName() + '_' + meta.getCubeName();
    String baseStorelocation =
        instance.getProperty(tempLocationKey, CarbonCommonConstants.STORE_LOCATION_DEFAULT_VAL)
            + File.separator + inputStoreLocation;
    int restructFolderNumber = meta.getCurrentRestructNumber();
    if (restructFolderNumber < 0) {
      isEmptyLoad = true;
      return;
    }

    baseStorelocation = baseStorelocation + File.separator + CarbonCommonConstants.RESTRUCTRE_FOLDER
        + restructFolderNumber + File.separator + this.tableName;

    // get the current folder sequence
    int counter = CarbonUtil.checkAndReturnCurrentLoadFolderNumber(baseStorelocation);
    if (counter < 0) {
      isEmptyLoad = true;
      return;
    }
    File file = new File(baseStorelocation);
    // get the store location
    String storeLocation =
        file.getAbsolutePath() + File.separator + CarbonCommonConstants.LOAD_FOLDER + counter
            + CarbonCommonConstants.FILE_INPROGRESS_STATUS;

    SliceMetaData sliceMetaData = null;
    try {
      sliceMetaData =
          CarbonUtil.readSliceMetadata(new File(baseStorelocation), restructFolderNumber);
    } catch (CarbonUtilException e1) {
      throw new KettleException("Problem while reading the slice metadata", e1);
    }
    this.mdkeyLength = meta.getMdkeyLength();
    String[] measures = sliceMetaData.getMeasures();
    this.aggregators = sliceMetaData.getMeasuresAggregator();
    this.measureCount = measures.length;
    int NoDictionaryIndex = measureCount;
    // incremented the index of mdkey by 1 so that the earlier one is high card index.
    this.mdKeyIndex = NoDictionaryIndex + 1;
    bufferSize = CarbonCommonConstants.CARBON_PREFETCH_BUFFERSIZE;

    try {
      numberOfConsumerThreads =
          Integer.parseInt(instance.getProperty("carbon.sort.number.of.cosumer.thread", "2"));
    } catch (NumberFormatException e) {
      numberOfConsumerThreads = 2;
    }

    try {
      numberOfProducerThreads =
          Integer.parseInt(instance.getProperty("carbon.sort.number.of.producer.thread", "4"));
    } catch (NumberFormatException e) {
      numberOfProducerThreads = 4;
    }
    updateSortTempFileLocation(instance, meta.getSchemaName(), meta.getCubeName());
    isPAndCSorting = Boolean.parseBoolean(instance
        .getProperty(CarbonCommonConstants.IS_PRODUCERCONSUMER_BASED_SORTING,
            CarbonCommonConstants.PRODUCERCONSUMER_BASED_SORTING_ENABLED_DEFAULTVALUE));
    String[] aggType = sliceMetaData.getMeasuresAggregator();
    ValueCompressionModel compressionModel = getValueCompressionModel(storeLocation);
    type = new char[measureCount];
    type = compressionModel.getType();

    boolean isByteArrayInMeasure = true;

    String levelCardinalityFilePath = storeLocation + File.separator +
        CarbonCommonConstants.LEVEL_METADATA_FILE + meta.getTabelName() + ".metadata";
    int[] dimLens;
    try {
      dimLens = CarbonUtil.getCardinalityFromLevelMetadataFile(levelCardinalityFilePath);
    } catch (CarbonUtilException e1) {
      throw new KettleException("Problem while reading the cardinality from level metadata file",
          e1);
    }

    updateFactHandler(isByteArrayInMeasure, aggType, dimLens, storeLocation, compressionModel);
    try {
      dataHandler.initialise();
    } catch (CarbonDataWriterException e) {
      throw new KettleException(e);
    }
  }

  /**
   * Initialize and do work where other steps need to wait for...
   *
   * @param smi The metadata to work with
   * @param sdi The data to initialize
   * @return step initialize or not
   */
  public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
    meta = (CarbonDataWriterStepMeta) smi;
    data = (CarbonDataWriterStepData) sdi;
    return super.init(smi, sdi);
  }

  /**
   * Dispose of this step: close files, empty logs, etc.
   *
   * @param smi The metadata to work with
   * @param sdi The data to dispose of
   */
  public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
    meta = (CarbonDataWriterStepMeta) smi;
    data = (CarbonDataWriterStepData) sdi;
    super.dispose(smi, sdi);
    this.meta = null;
    this.data = null;
  }

  /**
   * This will be used to get the sort temo location
   */
  private void updateSortTempFileLocation(CarbonProperties carbonProperties, String schemaName,
      String cubeName) {
    // get the base location
    String tempLocationKey = meta.getSchemaName() + '_' + meta.getCubeName();
    String baseLocation = carbonProperties
        .getProperty(tempLocationKey, CarbonCommonConstants.STORE_LOCATION_DEFAULT_VAL);
    // get the temp file location
    this.tempFileLocation =
        baseLocation + File.separator + schemaName + File.separator + cubeName + File.separator
            + CarbonCommonConstants.SORT_TEMP_FILE_LOCATION + File.separator + this.tableName;
    LOGGER.info("temp file location" + this.tempFileLocation);
  }

  /**
   * below method will be used for single thread based merging
   *
   * @throws CarbonDataWriterException
   */
  private void startSingleThreadFinalMerge() throws CarbonDataWriterException {
    SingleThreadFinalMerger finalMergerThread =
        new SingleThreadFinalMerger(tempFileLocation, tableName, mdkeyLength, measureCount,
            mdKeyIndex, meta.isFactMdKeyInInputRow(), meta.getFactMdkeyLength(), type,
            this.aggregators, this.meta.getNoDictionaryCount());
    finalMergerThread.startFinalMerge();
    int recordCounter = 0;
    while (finalMergerThread.hasNext()) {
      dataHandler.addDataToStore(finalMergerThread.next());
      recordCounter++;
    }
    LOGGER.info("************************************************ Total number of records processed"
        + recordCounter);
    finalMergerThread.clear();
  }

  private ValueCompressionModel getValueCompressionModel(String storeLocation) {
    String measureMetaDataFileLoc =
        storeLocation + CarbonCommonConstants.MEASURE_METADATA_FILE_NAME + this.tableName
            + CarbonCommonConstants.MEASUREMETADATA_FILE_EXT;
    return ValueCompressionUtil.getValueCompressionModel(measureMetaDataFileLoc, this.measureCount);
  }

  /**
   * below method will be used to producer consumer based sorting
   */
  private void startPAndCFinalMerge() {
    ExecutorService executorService = null;
    File file = new File(tempFileLocation);
    File[] tempFiles = file.listFiles(new FileFilter() {
      @Override public boolean accept(File pathname) {
        return pathname.getName().startsWith(tableName);
      }
    });

    if (null == tempFiles || tempFiles.length < 1) {
      return;
    }
    int fileBufferSize = 64 * CarbonCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR;

    int numberOfFilesPerThreads =
        tempFiles.length / (numberOfConsumerThreads * numberOfProducerThreads);

    int leftOver = tempFiles.length % (numberOfConsumerThreads * numberOfProducerThreads);

    if (numberOfFilesPerThreads == 0 && leftOver > 0) {
      numberOfConsumerThreads = 1;
      numberOfProducerThreads = 1;
    }

    int totalNumberOfThreads =
        (numberOfConsumerThreads * numberOfProducerThreads) + numberOfConsumerThreads + 1;

    LOGGER.info("******************************************************Total Number of Threads: "
        + totalNumberOfThreads);

    executorService = Executors.newFixedThreadPool(totalNumberOfThreads);

    File[][] filesPerEachThread = new File[numberOfConsumerThreads * numberOfProducerThreads][];

    int counter = 0;
    if (numberOfFilesPerThreads > 0) {
      for (int i = 0; i < filesPerEachThread.length; i++) {
        filesPerEachThread[i] = new File[numberOfFilesPerThreads];
        System.arraycopy(tempFiles, counter, filesPerEachThread[i], 0, numberOfFilesPerThreads);
        counter += numberOfFilesPerThreads;
      }
    }
    if (leftOver > 0 && numberOfFilesPerThreads > 0) {
      int i = 0;
      while (true) {
        File[] temp = new File[filesPerEachThread[i].length + 1];
        System.arraycopy(filesPerEachThread[i], 0, temp, 0, filesPerEachThread[i].length);
        temp[temp.length - 1] = tempFiles[counter++];
        filesPerEachThread[i] = temp;
        if (counter >= tempFiles.length) {
          break;
        }

        if (counter < tempFiles.length && i >= filesPerEachThread.length) {
          i = 0;
        }
        i++;
      }
    }
    if (leftOver > 0 && numberOfFilesPerThreads <= 0) {
      filesPerEachThread = new File[numberOfConsumerThreads * numberOfProducerThreads][];

      filesPerEachThread[0] = new File[leftOver];
      System.arraycopy(tempFiles, 0, filesPerEachThread[0], 0, tempFiles.length);
    }

    for (int i = 0; i < filesPerEachThread.length; i++) {
      LOGGER.info("********************************************Number of Files for Producer: "
          + filesPerEachThread[i].length);
    }

    startPAndC(executorService, fileBufferSize, filesPerEachThread);
  }

  private void startPAndC(ExecutorService executorService, int fileBufferSize,
      File[][] filesPerEachThread) {
    List<Container> consumerContainerList = new ArrayList<Container>(numberOfConsumerThreads);
    for (int i = 0; i < numberOfConsumerThreads; i++) {
      Container container = new Container();
      consumerContainerList.add(container);
    }
    List<List<Container>> producerContainersList =
        new ArrayList<List<Container>>(numberOfConsumerThreads);
    for (int i = 0; i < numberOfConsumerThreads; i++) {
      List<Container> list = new ArrayList<Container>(numberOfProducerThreads);
      for (int j = 0; j < numberOfProducerThreads; j++) {
        Container container = new Container();
        list.add(container);
      }
      producerContainersList.add(list);
    }
    int index = 0;
    for (int i = 0; i < numberOfConsumerThreads; i++) {
      ConsumerThread c = new ConsumerThread(producerContainersList.get(i), bufferSize,
          consumerContainerList.get(i), i, this.mdKeyIndex);
      List<Container> list1 = producerContainersList.get(i);
      for (int j = 0; j < numberOfProducerThreads; j++) {
        LOGGER.info("*******************************************Submitted Producer Thread " + j);
        executorService.submit(
            new ProducerThread(filesPerEachThread[index], fileBufferSize, bufferSize,
                this.measureCount, this.mdkeyLength, list1.get(j), index,
                meta.isFactMdKeyInInputRow(), meta.getFactMdkeyLength(), type));
        index++;
      }

      LOGGER.info("************************************ Submitted consumer thread");
      executorService.submit(c);
    }
    ProducerCosumerFinalMergerThread finalMergerThread =
        new ProducerCosumerFinalMergerThread(dataHandler, this.measureCount, this.mdKeyIndex,
            consumerContainerList);
    executorService.submit(finalMergerThread);
    LOGGER.info("************************************** Submitted all the task to executer");
    executorService.shutdown();
    try {
      executorService.awaitTermination(3, TimeUnit.HOURS);
    } catch (InterruptedException e) {
      LOGGER.error(e);
    }
  }

  private void updateFactHandler(boolean isByteArrayInMeasure, String[] aggType, int[] dimLens,
      String storeLocation, ValueCompressionModel compressionModel) {
    boolean isColumnar =
        Boolean.parseBoolean(CarbonCommonConstants.IS_COLUMNAR_STORAGE_DEFAULTVALUE);

    char[] type = new char[aggType.length];
    Arrays.fill(type, 'n');
    for (int i = 0; i < aggType.length; i++) {
      if (aggType[i].equals(CarbonCommonConstants.CUSTOM) || aggType[i]
          .equals(CarbonCommonConstants.DISTINCT_COUNT)) {
        this.type[i] = 'c';
      }
    }
    CarbonFactDataHandlerModel carbonFactDataHandlerModel = getCarbonFactDataHandlerModel();
    carbonFactDataHandlerModel.setStoreLocation(storeLocation);
    carbonFactDataHandlerModel.setAggType(type);
    carbonFactDataHandlerModel.setDimLens(dimLens);
    carbonFactDataHandlerModel.setMergingRequestForCustomAgg(isByteArrayInMeasure);
    dataHandler = new CarbonFactDataHandlerColumnar(carbonFactDataHandlerModel);
  }

  /**
   * This method will create a model object for carbon fact data handler
   *
   * @return
   */
  private CarbonFactDataHandlerModel getCarbonFactDataHandlerModel() {
    CarbonFactDataHandlerModel carbonFactDataHandlerModel = new CarbonFactDataHandlerModel();
    carbonFactDataHandlerModel.setDatabaseName(meta.getSchemaName());
    carbonFactDataHandlerModel.setTableName(tableName);
    carbonFactDataHandlerModel.setMeasureCount(measureCount);
    carbonFactDataHandlerModel.setMdKeyIndex(mdKeyIndex);
    carbonFactDataHandlerModel.setMdKeyLength(mdkeyLength);
    carbonFactDataHandlerModel.setNoDictionaryCount(meta.getNoDictionaryCount());
    carbonFactDataHandlerModel.setDataWritingRequest(true);
    carbonFactDataHandlerModel.setAggLevels(meta.getAggregateLevels());
    carbonFactDataHandlerModel.setFactLevels(meta.getFactLevels());
    carbonFactDataHandlerModel
        .setFactDimLens(CarbonDataProcessorUtil.getDimLens(meta.getFactDimLensString()));
    return carbonFactDataHandlerModel;
  }
}
