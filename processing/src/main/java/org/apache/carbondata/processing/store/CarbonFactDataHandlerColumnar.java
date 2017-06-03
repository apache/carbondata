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

package org.apache.carbondata.processing.store;

import java.io.File;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.CarbonV3DataFormatConstants;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.columnar.BlockIndexerStorageForInt;
import org.apache.carbondata.core.datastore.columnar.BlockIndexerStorageForNoInvertedIndex;
import org.apache.carbondata.core.datastore.columnar.BlockIndexerStorageForNoInvertedIndexForShort;
import org.apache.carbondata.core.datastore.columnar.BlockIndexerStorageForShort;
import org.apache.carbondata.core.datastore.columnar.ColumnGroupModel;
import org.apache.carbondata.core.datastore.columnar.IndexStorage;
import org.apache.carbondata.core.datastore.compression.ValueCompressionHolder;
import org.apache.carbondata.core.datastore.compression.WriterCompressModel;
import org.apache.carbondata.core.datastore.dataholder.CarbonWriteDataHolder;
import org.apache.carbondata.core.datastore.page.FixLengthColumnPage;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.keygenerator.columnar.ColumnarSplitter;
import org.apache.carbondata.core.keygenerator.columnar.impl.MultiDimKeyVarLengthEquiSplitGenerator;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.NodeHolder;
import org.apache.carbondata.core.util.ValueCompressionUtil;
import org.apache.carbondata.processing.datatypes.GenericDataType;
import org.apache.carbondata.processing.newflow.row.CarbonRow;
import org.apache.carbondata.processing.newflow.row.WriteStepRowUtil;
import org.apache.carbondata.processing.store.colgroup.ColGroupBlockStorage;
import org.apache.carbondata.processing.store.file.FileManager;
import org.apache.carbondata.processing.store.file.IFileManagerComposite;
import org.apache.carbondata.processing.store.writer.CarbonDataWriterVo;
import org.apache.carbondata.processing.store.writer.CarbonFactDataWriter;
import org.apache.carbondata.processing.store.writer.exception.CarbonDataWriterException;
import org.apache.carbondata.processing.util.NonDictionaryUtil;

/**
 * Fact data handler class to handle the fact data
 */
public class CarbonFactDataHandlerColumnar implements CarbonFactHandler {

  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonFactDataHandlerColumnar.class.getName());

  private CarbonFactDataHandlerModel model;

  /**
   * data writer
   */
  private CarbonFactDataWriter dataWriter;
  /**
   * File manager
   */
  private IFileManagerComposite fileManager;
  /**
   * total number of entries in blocklet
   */
  private int entryCount;

  /**
   * blocklet size (for V1 and V2) or page size (for V3). A Producer thread will start to process
   * once this size of input is reached
   */
  private int blockletSize;
  /**
   * keyGenerator
   */
  private ColumnarSplitter columnarSplitter;
  /**
   * keyBlockHolder
   */
  private CarbonKeyBlockHolder[] keyBlockHolder;
  private boolean[] aggKeyBlock;
  private boolean[] isNoDictionary;
  private long processedDataCount;
  private ExecutorService producerExecutorService;
  private List<Future<Void>> producerExecutorServiceTaskList;
  private ExecutorService consumerExecutorService;
  private List<Future<Void>> consumerExecutorServiceTaskList;
  private List<CarbonRow> dataRows;
  private ColumnGroupModel colGrpModel;
  private boolean[] isUseInvertedIndex;
  /**
   * semaphore which will used for managing node holder objects
   */
  private Semaphore semaphore;
  /**
   * counter that incremented for every job submitted to data writer thread
   */
  private int writerTaskSequenceCounter;
  /**
   * a private class that will hold the data for blocklets
   */
  private BlockletDataHolder blockletDataHolder;
  /**
   * number of cores configured
   */
  private int numberOfCores;
  /**
   * integer that will be incremented for every new blocklet submitted to producer for processing
   * the data and decremented every time consumer fetches the blocklet for writing
   */
  private AtomicInteger blockletProcessingCount;
  /**
   * flag to check whether all blocklets have been finished writing
   */
  private boolean processingComplete;

  /**
   * boolean to check whether dimension
   * is of dictionary type or no dictionary type
   */
  private boolean[] isDictDimension;

  private int bucketNumber;

  private int taskExtension;

  /**
   * current data format version
   */
  private ColumnarFormatVersion version;

  /**
   * CarbonFactDataHandler constructor
   */
  public CarbonFactDataHandlerColumnar(CarbonFactDataHandlerModel model) {
    this.model = model;
    initParameters(model);

    int numDimColumns = colGrpModel.getNoOfColumnStore() + model.getNoDictionaryCount()
        + getExpandedComplexColsCount();
    this.aggKeyBlock = new boolean[numDimColumns];
    this.isNoDictionary = new boolean[numDimColumns];
    this.bucketNumber = model.getBucketId();
    this.taskExtension = model.getTaskExtension();
    this.isUseInvertedIndex = new boolean[numDimColumns];
    if (null != model.getIsUseInvertedIndex()) {
      for (int i = 0; i < isUseInvertedIndex.length; i++) {
        if (i < model.getIsUseInvertedIndex().length) {
          isUseInvertedIndex[i] = model.getIsUseInvertedIndex()[i];
        } else {
          isUseInvertedIndex[i] = true;
        }
      }
    }
    int noDictStartIndex = this.colGrpModel.getNoOfColumnStore();
    // setting true value for dims of high card
    for (int i = 0; i < model.getNoDictionaryCount(); i++) {
      this.isNoDictionary[noDictStartIndex + i] = true;
    }

    boolean isAggKeyBlock = Boolean.parseBoolean(
        CarbonProperties.getInstance().getProperty(
            CarbonCommonConstants.AGGREAGATE_COLUMNAR_KEY_BLOCK,
            CarbonCommonConstants.AGGREAGATE_COLUMNAR_KEY_BLOCK_DEFAULTVALUE));
    if (isAggKeyBlock) {
      int noDictionaryValue = Integer.parseInt(
          CarbonProperties.getInstance().getProperty(
              CarbonCommonConstants.HIGH_CARDINALITY_VALUE,
              CarbonCommonConstants.HIGH_CARDINALITY_VALUE_DEFAULTVALUE));
      int[] columnSplits = colGrpModel.getColumnSplit();
      int dimCardinalityIndex = 0;
      int aggIndex = 0;
      int[] dimLens = model.getSegmentProperties().getDimColumnsCardinality();
      for (int i = 0; i < columnSplits.length; i++) {
        if (colGrpModel.isColumnar(i) && dimLens[dimCardinalityIndex] < noDictionaryValue) {
          this.aggKeyBlock[aggIndex++] = true;
          continue;
        }
        dimCardinalityIndex += columnSplits[i];
        aggIndex++;
      }

      if (model.getDimensionCount() < dimLens.length) {
        int allColsCount = getColsCount(model.getDimensionCount());
        List<Boolean> aggKeyBlockWithComplex = new ArrayList<Boolean>(allColsCount);
        for (int i = 0; i < model.getDimensionCount(); i++) {
          GenericDataType complexDataType = model.getComplexIndexMap().get(i);
          if (complexDataType != null) {
            complexDataType.fillAggKeyBlock(aggKeyBlockWithComplex, this.aggKeyBlock);
          } else {
            aggKeyBlockWithComplex.add(this.aggKeyBlock[i]);
          }
        }
        this.aggKeyBlock = new boolean[allColsCount];
        for (int i = 0; i < allColsCount; i++) {
          this.aggKeyBlock[i] = aggKeyBlockWithComplex.get(i);
        }
      }
      aggKeyBlock = arrangeUniqueBlockType(aggKeyBlock);
    }
    version = CarbonProperties.getInstance().getFormatVersion();
  }

  private void initParameters(CarbonFactDataHandlerModel model) {

    this.colGrpModel = model.getSegmentProperties().getColumnGroupModel();

    //TODO need to pass carbon table identifier to metadata
    CarbonTable carbonTable =
        CarbonMetadata.getInstance().getCarbonTable(
            model.getDatabaseName() + CarbonCommonConstants.UNDERSCORE + model.getTableName());
    isDictDimension =
        CarbonUtil.identifyDimensionType(carbonTable.getDimensionByTableName(model.getTableName()));

    // in compaction flow the measure with decimal type will come as spark decimal.
    // need to convert it to byte array.
    if (model.isCompactionFlow()) {
      try {
        numberOfCores = Integer.parseInt(CarbonProperties.getInstance()
            .getProperty(CarbonCommonConstants.NUM_CORES_COMPACTING,
                CarbonCommonConstants.NUM_CORES_DEFAULT_VAL));
      } catch (NumberFormatException exc) {
        LOGGER.error("Configured value for property " + CarbonCommonConstants.NUM_CORES_COMPACTING
            + "is wrong.Falling back to the default value "
            + CarbonCommonConstants.NUM_CORES_DEFAULT_VAL);
        numberOfCores = Integer.parseInt(CarbonCommonConstants.NUM_CORES_DEFAULT_VAL);
      }
    } else {
      try {
        numberOfCores = Integer.parseInt(CarbonProperties.getInstance()
            .getProperty(CarbonCommonConstants.NUM_CORES_LOADING,
                CarbonCommonConstants.NUM_CORES_DEFAULT_VAL));
      } catch (NumberFormatException exc) {
        LOGGER.error("Configured value for property " + CarbonCommonConstants.NUM_CORES_LOADING
            + "is wrong.Falling back to the default value "
            + CarbonCommonConstants.NUM_CORES_DEFAULT_VAL);
        numberOfCores = Integer.parseInt(CarbonCommonConstants.NUM_CORES_DEFAULT_VAL);
      }
    }

    blockletProcessingCount = new AtomicInteger(0);
    producerExecutorService = Executors.newFixedThreadPool(numberOfCores);
    producerExecutorServiceTaskList =
        new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    LOGGER.info("Initializing writer executors");
    consumerExecutorService = Executors.newFixedThreadPool(1);
    consumerExecutorServiceTaskList = new ArrayList<>(1);
    semaphore = new Semaphore(numberOfCores);
    blockletDataHolder = new BlockletDataHolder();

    // Start the consumer which will take each blocklet/page in order and write to a file
    Consumer consumer = new Consumer(blockletDataHolder);
    consumerExecutorServiceTaskList.add(consumerExecutorService.submit(consumer));
  }

  private boolean[] arrangeUniqueBlockType(boolean[] aggKeyBlock) {
    int counter = 0;
    boolean[] uniqueBlock = new boolean[aggKeyBlock.length];
    for (int i = 0; i < isDictDimension.length; i++) {
      if (isDictDimension[i]) {
        uniqueBlock[i] = aggKeyBlock[counter++];
      } else {
        uniqueBlock[i] = false;
      }
    }
    return uniqueBlock;
  }

  private void setComplexMapSurrogateIndex(int dimensionCount) {
    int surrIndex = 0;
    for (int i = 0; i < dimensionCount; i++) {
      GenericDataType complexDataType = model.getComplexIndexMap().get(i);
      if (complexDataType != null) {
        List<GenericDataType> primitiveTypes = new ArrayList<GenericDataType>();
        complexDataType.getAllPrimitiveChildren(primitiveTypes);
        for (GenericDataType eachPrimitive : primitiveTypes) {
          eachPrimitive.setSurrogateIndex(surrIndex++);
        }
      } else {
        surrIndex++;
      }
    }
  }

  /**
   * This method will be used to get and update the step properties which will
   * required to run this step
   *
   * @throws CarbonDataWriterException
   */
  public void initialise() throws CarbonDataWriterException {
    fileManager = new FileManager();
    fileManager.setName(new File(model.getStoreLocation()).getName());
    setWritingConfiguration();
  }

  /**
   * below method will be used to add row to store
   *
   * @param row
   * @throws CarbonDataWriterException
   */
  public void addDataToStore(CarbonRow row) throws CarbonDataWriterException {
    dataRows.add(row);
    this.entryCount++;
    // if entry count reaches to leaf node size then we are ready to write
    // this to leaf node file and update the intermediate files
    if (this.entryCount == this.blockletSize) {
      try {
        semaphore.acquire();

        producerExecutorServiceTaskList.add(
            producerExecutorService.submit(
                new Producer(blockletDataHolder, dataRows, ++writerTaskSequenceCounter, false)
            )
        );
        blockletProcessingCount.incrementAndGet();
        // set the entry count to zero
        processedDataCount += entryCount;
        LOGGER.info("Total Number Of records added to store: " + processedDataCount);
        dataRows = new ArrayList<>(this.blockletSize);
        this.entryCount = 0;
      } catch (InterruptedException e) {
        LOGGER.error(e, e.getMessage());
        throw new CarbonDataWriterException(e.getMessage(), e);
      }
    }
  }

  class IndexKey {
    private int pageSize;
    byte[] currentMDKey = null;
    byte[][] currentNoDictionaryKey = null;
    byte[] startKey = null;
    byte[] endKey = null;
    byte[][] noDictStartKey = null;
    byte[][] noDictEndKey = null;
    byte[] packedNoDictStartKey = null;
    byte[] packedNoDictEndKey = null;

    IndexKey(int pageSize) {
      this.pageSize = pageSize;
    }

    /** update all keys based on the input row */
    void update(int rowId, CarbonRow row) throws KeyGenException {
      currentMDKey = WriteStepRowUtil.getMdk(row, model.getMDKeyGenerator());
      if (model.getNoDictionaryCount() > 0 || model.getComplexIndexMap().size() > 0) {
        currentNoDictionaryKey = WriteStepRowUtil.getNoDictAndComplexDimension(row);
      }
      if (rowId == 0) {
        startKey = currentMDKey;
        noDictStartKey = currentNoDictionaryKey;
      }
      endKey = currentMDKey;
      noDictEndKey = currentNoDictionaryKey;
      if (rowId == pageSize - 1) {
        finalizeKeys();
      }
    }

    /** update all keys if SORT_COLUMNS option is used when creating table */
    private void finalizeKeys() {
      // If SORT_COLUMNS is used, may need to update start/end keys since the they may
      // contains dictionary columns that are not in SORT_COLUMNS, which need to be removed from
      // start/end key
      int numberOfDictSortColumns = model.getSegmentProperties().getNumberOfDictSortColumns();
      if (numberOfDictSortColumns > 0) {
        // if SORT_COLUMNS contain dictionary columns
        int[] keySize = columnarSplitter.getBlockKeySize();
        if (keySize.length > numberOfDictSortColumns) {
          // if there are some dictionary columns that are not in SORT_COLUMNS, it will come to here
          int newMdkLength = 0;
          for (int i = 0; i < numberOfDictSortColumns; i++) {
            newMdkLength += keySize[i];
          }
          byte[] newStartKeyOfSortKey = new byte[newMdkLength];
          byte[] newEndKeyOfSortKey = new byte[newMdkLength];
          System.arraycopy(startKey, 0, newStartKeyOfSortKey, 0, newMdkLength);
          System.arraycopy(endKey, 0, newEndKeyOfSortKey, 0, newMdkLength);
          startKey = newStartKeyOfSortKey;
          endKey = newEndKeyOfSortKey;
        }
      } else {
        startKey = new byte[0];
        endKey = new byte[0];
      }

      // Do the same update for noDictionary start/end Key
      int numberOfNoDictSortColumns = model.getSegmentProperties().getNumberOfNoDictSortColumns();
      if (numberOfNoDictSortColumns > 0) {
        // if sort_columns contain no-dictionary columns
        if (noDictStartKey.length > numberOfNoDictSortColumns) {
          byte[][] newNoDictionaryStartKey = new byte[numberOfNoDictSortColumns][];
          byte[][] newNoDictionaryEndKey = new byte[numberOfNoDictSortColumns][];
          System.arraycopy(
              noDictStartKey, 0, newNoDictionaryStartKey, 0, numberOfNoDictSortColumns);
          System.arraycopy(
              noDictEndKey, 0, newNoDictionaryEndKey, 0, numberOfNoDictSortColumns);
          noDictStartKey = newNoDictionaryStartKey;
          noDictEndKey = newNoDictionaryEndKey;
        }
        packedNoDictStartKey =
            NonDictionaryUtil.packByteBufferIntoSingleByteArray(noDictStartKey);
        packedNoDictEndKey =
            NonDictionaryUtil.packByteBufferIntoSingleByteArray(noDictEndKey);
      }
    }
  }

  /**
   * generate the NodeHolder from the input rows (one page in case of V3 format)
   */
  private NodeHolder processDataRows(List<CarbonRow> dataRows)
      throws CarbonDataWriterException, KeyGenException {
    if (dataRows.size() == 0) {
      return new NodeHolder();
    }
    TablePage tablePage = new TablePage(model, dataRows.size());
    IndexKey keys = new IndexKey(dataRows.size());
    int rowId = 0;

    // convert row to columnar data
    for (CarbonRow row : dataRows) {
      tablePage.addRow(rowId, row);
      keys.update(rowId, row);
      rowId++;
    }

    // encode and compress dimensions and measure
    // TODO: To make the encoding more transparent to the user, user should be enable to specify
    // the encoding and compression method for each type when creating table.

    Codec codec = new Codec(model.getMeasureDataType());
    IndexStorage[] dimColumns = codec.encodeAndCompressDimensions(tablePage);
    Codec encodedMeasure = codec.encodeAndCompressMeasures(tablePage);

    // prepare nullBitSet for writer, remove this after writer can accept TablePage
    BitSet[] nullBitSet = new BitSet[tablePage.getMeasurePage().length];
    FixLengthColumnPage[] measurePages = tablePage.getMeasurePage();
    for (int i = 0; i < nullBitSet.length; i++) {
      nullBitSet[i] = measurePages[i].getNullBitSet();
    }

    LOGGER.info("Number Of records processed: " + dataRows.size());

    // TODO: writer interface should be modified to use TablePage
    return dataWriter.buildDataNodeHolder(dimColumns, encodedMeasure.getEncodedMeasure(),
        dataRows.size(), keys.startKey, keys.endKey, encodedMeasure.getCompressionModel(),
        keys.packedNoDictStartKey, keys.packedNoDictEndKey, nullBitSet);
  }

  /**
   * below method will be used to finish the data handler
   *
   * @throws CarbonDataWriterException
   */
  public void finish() throws CarbonDataWriterException {
    // still some data is present in stores if entryCount is more
    // than 0
    try {
      semaphore.acquire();
      producerExecutorServiceTaskList.add(producerExecutorService
          .submit(new Producer(blockletDataHolder, dataRows, ++writerTaskSequenceCounter, true)));
      blockletProcessingCount.incrementAndGet();
      processedDataCount += entryCount;
      closeWriterExecutionService(producerExecutorService);
      processWriteTaskSubmitList(producerExecutorServiceTaskList);
      processingComplete = true;
    } catch (InterruptedException e) {
      LOGGER.error(e, e.getMessage());
      throw new CarbonDataWriterException(e.getMessage(), e);
    }
  }

  /**
   * This method will close writer execution service and get the node holders and
   * add them to node holder list
   *
   * @param service the service to shutdown
   * @throws CarbonDataWriterException
   */
  private void closeWriterExecutionService(ExecutorService service)
      throws CarbonDataWriterException {
    try {
      service.shutdown();
      service.awaitTermination(1, TimeUnit.DAYS);
    } catch (InterruptedException e) {
      LOGGER.error(e, e.getMessage());
      throw new CarbonDataWriterException(e.getMessage());
    }
  }

  /**
   * This method will iterate through future task list and check if any exception
   * occurred during the thread execution
   *
   * @param taskList
   * @throws CarbonDataWriterException
   */
  private void processWriteTaskSubmitList(List<Future<Void>> taskList)
      throws CarbonDataWriterException {
    for (int i = 0; i < taskList.size(); i++) {
      try {
        taskList.get(i).get();
      } catch (InterruptedException | ExecutionException e) {
        LOGGER.error(e, e.getMessage());
        throw new CarbonDataWriterException(e.getMessage(), e);
      }
    }
  }

  private int getColsCount(int columnSplit) {
    int count = 0;
    for (int i = 0; i < columnSplit; i++) {
      GenericDataType complexDataType = model.getComplexIndexMap().get(i);
      if (complexDataType != null) {
        count += complexDataType.getColsCount();
      } else count++;
    }
    return count;
  }

  // return the number of complex column after complex columns are expanded
  private int getExpandedComplexColsCount() {
    int count = 0;
    int dictDimensionCount = model.getDimensionCount();
    for (int i = 0; i < dictDimensionCount; i++) {
      GenericDataType complexDataType = model.getComplexIndexMap().get(i);
      if (complexDataType != null) {
        count += complexDataType.getColsCount();
      }
    }
    return count;
  }

  // return the number of complex column
  private int getComplexColumnCount() {
    return model.getComplexIndexMap().size();
  }

  /**
   * below method will be used to close the handler
   */
  public void closeHandler() throws CarbonDataWriterException {
    if (null != this.dataWriter) {
      // wait until all blocklets have been finished writing
      while (blockletProcessingCount.get() > 0) {
        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          throw new CarbonDataWriterException(e.getMessage());
        }
      }
      consumerExecutorService.shutdownNow();
      processWriteTaskSubmitList(consumerExecutorServiceTaskList);
      this.dataWriter.writeBlockletInfoToFile();
      LOGGER.info("All blocklets have been finished writing");
      // close all the open stream for both the files
      this.dataWriter.closeWriter();
    }
    this.dataWriter = null;
    this.keyBlockHolder = null;
  }

  /**
   * Below method will be to configure fact file writing configuration
   *
   * @throws CarbonDataWriterException
   */
  private void setWritingConfiguration() throws CarbonDataWriterException {
    // get blocklet size
    this.blockletSize = Integer.parseInt(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.BLOCKLET_SIZE,
            CarbonCommonConstants.BLOCKLET_SIZE_DEFAULT_VAL));
    if (version == ColumnarFormatVersion.V3) {
      this.blockletSize = Integer.parseInt(CarbonProperties.getInstance()
          .getProperty(CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE,
              CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_DEFAULT));
    }
    LOGGER.info("Number of rows per column blocklet " + blockletSize);
    dataRows = new ArrayList<>(this.blockletSize);
    int dimSet =
        Integer.parseInt(CarbonCommonConstants.DIMENSION_SPLIT_VALUE_IN_COLUMNAR_DEFAULTVALUE);
    // if atleast one dimension is present then initialize column splitter otherwise null
    int noOfColStore = colGrpModel.getNoOfColumnStore();
    int[] keyBlockSize = new int[noOfColStore + getExpandedComplexColsCount()];

    if (model.getDimLens().length > 0) {
      //Using Variable length variable split generator
      //This will help in splitting mdkey to columns. variable split is required because all
      // columns which are part of
      //row store will be in single column store
      //e.g if {0,1,2,3,4,5} is dimension and {0,1,2) is row store dimension
      //than below splitter will return column as {0,1,2}{3}{4}{5}
      this.columnarSplitter = model.getSegmentProperties().getFixedLengthKeySplitter();
      System.arraycopy(columnarSplitter.getBlockKeySize(), 0, keyBlockSize, 0, noOfColStore);
      this.keyBlockHolder =
          new CarbonKeyBlockHolder[this.columnarSplitter.getBlockKeySize().length];
    } else {
      this.keyBlockHolder = new CarbonKeyBlockHolder[0];
    }

    for (int i = 0; i < keyBlockHolder.length; i++) {
      this.keyBlockHolder[i] = new CarbonKeyBlockHolder(blockletSize);
      this.keyBlockHolder[i].resetCounter();
    }

    // agg type
    List<Integer> otherMeasureIndexList =
        new ArrayList<Integer>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    List<Integer> customMeasureIndexList =
        new ArrayList<Integer>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    DataType[] type = model.getMeasureDataType();
    for (int j = 0; j < type.length; j++) {
      if (type[j] != DataType.BYTE && type[j] != DataType.DECIMAL) {
        otherMeasureIndexList.add(j);
      } else {
        customMeasureIndexList.add(j);
      }
    }

    int[] otherMeasureIndex = new int[otherMeasureIndexList.size()];
    int[] customMeasureIndex = new int[customMeasureIndexList.size()];
    for (int i = 0; i < otherMeasureIndex.length; i++) {
      otherMeasureIndex[i] = otherMeasureIndexList.get(i);
    }
    for (int i = 0; i < customMeasureIndex.length; i++) {
      customMeasureIndex[i] = customMeasureIndexList.get(i);
    }
    setComplexMapSurrogateIndex(model.getDimensionCount());
    int[] blockKeySize = getBlockKeySizeWithComplexTypes(new MultiDimKeyVarLengthEquiSplitGenerator(
        CarbonUtil.getIncrementedCardinalityFullyFilled(model.getDimLens().clone()), (byte) dimSet)
        .getBlockKeySize());
    System.arraycopy(blockKeySize, noOfColStore, keyBlockSize, noOfColStore,
        blockKeySize.length - noOfColStore);
    this.dataWriter = getFactDataWriter(keyBlockSize);
    this.dataWriter.setIsNoDictionary(isNoDictionary);
    // initialize the channel;
    this.dataWriter.initializeWriter();
    //initializeColGrpMinMax();
  }

  /**
   * This method combines primitive dimensions with complex metadata columns
   *
   * @param primitiveBlockKeySize
   * @return all dimensions cardinality including complex dimension metadata column
   */
  private int[] getBlockKeySizeWithComplexTypes(int[] primitiveBlockKeySize) {
    int allColsCount = getExpandedComplexColsCount();
    int[] blockKeySizeWithComplexTypes =
        new int[this.colGrpModel.getNoOfColumnStore() + allColsCount];

    List<Integer> blockKeySizeWithComplex =
        new ArrayList<Integer>(blockKeySizeWithComplexTypes.length);
    int dictDimensionCount = model.getDimensionCount();
    for (int i = 0; i < dictDimensionCount; i++) {
      GenericDataType complexDataType = model.getComplexIndexMap().get(i);
      if (complexDataType != null) {
        complexDataType.fillBlockKeySize(blockKeySizeWithComplex, primitiveBlockKeySize);
      } else {
        blockKeySizeWithComplex.add(primitiveBlockKeySize[i]);
      }
    }
    for (int i = 0; i < blockKeySizeWithComplexTypes.length; i++) {
      blockKeySizeWithComplexTypes[i] = blockKeySizeWithComplex.get(i);
    }

    return blockKeySizeWithComplexTypes;
  }

  /**
   * Below method will be used to get the fact data writer instance
   *
   * @param keyBlockSize
   * @return data writer instance
   */
  private CarbonFactDataWriter<?> getFactDataWriter(int[] keyBlockSize) {
    return CarbonDataWriterFactory.getInstance()
        .getFactDataWriter(version, getDataWriterVo(keyBlockSize));
  }

  /**
   * Below method will be used to get the writer vo
   *
   * @param keyBlockSize size of each key block
   * @return data writer vo object
   */
  private CarbonDataWriterVo getDataWriterVo(int[] keyBlockSize) {
    CarbonDataWriterVo carbonDataWriterVo = new CarbonDataWriterVo();
    carbonDataWriterVo.setStoreLocation(model.getStoreLocation());
    carbonDataWriterVo.setMeasureCount(model.getMeasureCount());
    carbonDataWriterVo.setTableName(model.getTableName());
    carbonDataWriterVo.setKeyBlockSize(keyBlockSize);
    carbonDataWriterVo.setFileManager(fileManager);
    carbonDataWriterVo.setAggBlocks(aggKeyBlock);
    carbonDataWriterVo.setIsComplexType(isComplexTypes());
    carbonDataWriterVo.setNoDictionaryCount(model.getNoDictionaryCount());
    carbonDataWriterVo.setCarbonDataFileAttributes(model.getCarbonDataFileAttributes());
    carbonDataWriterVo.setDatabaseName(model.getDatabaseName());
    carbonDataWriterVo.setWrapperColumnSchemaList(model.getWrapperColumnSchema());
    carbonDataWriterVo.setIsDictionaryColumn(isDictDimension);
    carbonDataWriterVo.setCarbonDataDirectoryPath(model.getCarbonDataDirectoryPath());
    carbonDataWriterVo.setColCardinality(model.getColCardinality());
    carbonDataWriterVo.setSegmentProperties(model.getSegmentProperties());
    carbonDataWriterVo.setTableBlocksize(model.getBlockSizeInMB());
    carbonDataWriterVo.setBucketNumber(bucketNumber);
    carbonDataWriterVo.setTaskExtension(taskExtension);
    carbonDataWriterVo.setSchemaUpdatedTimeStamp(model.getSchemaUpdatedTimeStamp());
    return carbonDataWriterVo;
  }

  private boolean[] isComplexTypes() {
    int noDictionaryCount = model.getNoDictionaryCount();
    int noOfColumn = colGrpModel.getNoOfColumnStore() + noDictionaryCount + getComplexColumnCount();
    int allColsCount = getColsCount(noOfColumn);
    boolean[] isComplexType = new boolean[allColsCount];

    List<Boolean> complexTypesList = new ArrayList<Boolean>(allColsCount);
    for (int i = 0; i < noOfColumn; i++) {
      GenericDataType complexDataType = model.getComplexIndexMap().get(i - noDictionaryCount);
      if (complexDataType != null) {
        int count = complexDataType.getColsCount();
        for (int j = 0; j < count; j++) {
          complexTypesList.add(true);
        }
      } else {
        complexTypesList.add(false);
      }
    }
    for (int i = 0; i < allColsCount; i++) {
      isComplexType[i] = complexTypesList.get(i);
    }

    return isComplexType;
  }

  /**
   * This method will reset the block processing count
   */
  private void resetBlockletProcessingCount() {
    blockletProcessingCount.set(0);
  }

  /**
   * This class will hold the holder objects and manage producer and consumer for reading
   * and writing the blocklet data
   */
  private final class BlockletDataHolder {
    /**
     * array of blocklet data holder objects
     */
    private NodeHolder[] nodeHolders;
    /**
     * flag to check whether the producer has completed processing for holder
     * object which is required to be picked form an index
     */
    private AtomicBoolean available;
    /**
     * index from which data node holder object needs to be picked for writing
     */
    private int currentIndex;

    private BlockletDataHolder() {
      nodeHolders = new NodeHolder[numberOfCores];
      available = new AtomicBoolean(false);
    }

    /**
     * @return a node holder object
     * @throws InterruptedException if consumer thread is interrupted
     */
    public synchronized NodeHolder get() throws InterruptedException {
      NodeHolder nodeHolder = nodeHolders[currentIndex];
      // if node holder is null means producer thread processing the data which has to
      // be inserted at this current index has not completed yet
      if (null == nodeHolder && !processingComplete) {
        available.set(false);
      }
      while (!available.get()) {
        wait();
      }
      nodeHolder = nodeHolders[currentIndex];
      nodeHolders[currentIndex] = null;
      currentIndex++;
      // reset current index when it reaches length of node holder array
      if (currentIndex >= nodeHolders.length) {
        currentIndex = 0;
      }
      return nodeHolder;
    }

    /**
     * @param nodeHolder
     * @param index
     */
    public synchronized void put(NodeHolder nodeHolder, int index) {
      nodeHolders[index] = nodeHolder;
      // notify the consumer thread when index at which object is to be inserted
      // becomes equal to current index from where data has to be picked for writing
      if (index == currentIndex) {
        available.set(true);
        notifyAll();
      }
    }
  }

  /**
   * Producer which will process data equivalent to 1 blocklet size
   */
  private final class Producer implements Callable<Void> {

    private BlockletDataHolder blockletDataHolder;
    private List<CarbonRow> dataRows;
    private int sequenceNumber;
    private boolean isWriteAll;

    private Producer(BlockletDataHolder blockletDataHolder, List<CarbonRow> dataRows,
        int sequenceNumber, boolean isWriteAll) {
      this.blockletDataHolder = blockletDataHolder;
      this.dataRows = dataRows;
      this.sequenceNumber = sequenceNumber;
      this.isWriteAll = isWriteAll;
    }

    /**
     * Computes a result, or throws an exception if unable to do so.
     *
     * @return computed result
     * @throws Exception if unable to compute a result
     */
    @Override public Void call() throws Exception {
      try {
        NodeHolder nodeHolder = processDataRows(dataRows);
        nodeHolder.setWriteAll(isWriteAll);
        // insert the object in array according to sequence number
        int indexInNodeHolderArray = (sequenceNumber - 1) % numberOfCores;
        blockletDataHolder.put(nodeHolder, indexInNodeHolderArray);
        return null;
      } catch (Throwable throwable) {
        LOGGER.error(throwable, "Error in producer");
        consumerExecutorService.shutdownNow();
        resetBlockletProcessingCount();
        throw new CarbonDataWriterException(throwable.getMessage(), throwable);
      }
    }
  }

  /**
   * Consumer class will get one blocklet data at a time and submit for writing
   */
  private final class Consumer implements Callable<Void> {

    private BlockletDataHolder blockletDataHolder;

    private Consumer(BlockletDataHolder blockletDataHolder) {
      this.blockletDataHolder = blockletDataHolder;
    }

    /**
     * Computes a result, or throws an exception if unable to do so.
     *
     * @return computed result
     * @throws Exception if unable to compute a result
     */
    @Override public Void call() throws Exception {
      while (!processingComplete || blockletProcessingCount.get() > 0) {
        NodeHolder nodeHolder = null;
        try {
          nodeHolder = blockletDataHolder.get();
          if (null != nodeHolder) {
            dataWriter.writeBlockletData(nodeHolder);
          }
          blockletProcessingCount.decrementAndGet();
        } catch (Throwable throwable) {
          if (!processingComplete || blockletProcessingCount.get() > 0) {
            producerExecutorService.shutdownNow();
            resetBlockletProcessingCount();
            LOGGER.error(throwable, "Problem while writing the carbon data file");
            throw new CarbonDataWriterException(throwable.getMessage());
          }
        } finally {
          semaphore.release();
        }
      }
      return null;
    }
  }

  private final class BlockSortThread implements Callable<IndexStorage> {
    private int index;

    private byte[][] data;
    private boolean isSortRequired;
    private boolean isCompressionReq;
    private boolean isUseInvertedIndex;

    private boolean isNoDictionary;

    private BlockSortThread(int index, byte[][] data, boolean isSortRequired,
        boolean isUseInvertedIndex) {
      this.index = index;
      this.data = data;
      isCompressionReq = aggKeyBlock[this.index];
      this.isSortRequired = isSortRequired;
      this.isUseInvertedIndex = isUseInvertedIndex;
    }

    public BlockSortThread(byte[][] data, boolean compression, boolean isNoDictionary,
        boolean isSortRequired, boolean isUseInvertedIndex) {
      this.data = data;
      this.isCompressionReq = compression;
      this.isNoDictionary = isNoDictionary;
      this.isSortRequired = isSortRequired;
      this.isUseInvertedIndex = isUseInvertedIndex;
    }

    @Override public IndexStorage call() throws Exception {
      if (index == 1) {
        int dd = 1 + 1;
      }
      if (isUseInvertedIndex) {
        if (version == ColumnarFormatVersion.V3) {
          return new BlockIndexerStorageForShort(this.data, isCompressionReq, isNoDictionary,
              isSortRequired);
        } else {
          return new BlockIndexerStorageForInt(this.data, isCompressionReq, isNoDictionary,
              isSortRequired);
        }
      } else {
        if (version == ColumnarFormatVersion.V3) {
          return new BlockIndexerStorageForNoInvertedIndexForShort(this.data,isNoDictionary);
        } else {
          return new BlockIndexerStorageForNoInvertedIndex(this.data);
        }
      }

    }

  }

  public class Codec {
    private WriterCompressModel compressionModel;
    private byte[][] encodedMeasureArray;
    private DataType[] measureType;

    Codec(DataType[] measureType) {
      this.measureType = measureType;
    }

    public WriterCompressModel getCompressionModel() {
      return compressionModel;
    }

    public byte[][] getEncodedMeasure() {
      return encodedMeasureArray;
    }

    public Codec encodeAndCompressMeasures(TablePage tablePage) {
      // TODO: following conversion is required only because compress model requires them,
      // remove then after the compress framework is refactoried
      FixLengthColumnPage[] measurePage = tablePage.getMeasurePage();
      int measureCount = measurePage.length;
      Object[] min = new Object[measurePage.length];
      Object[] max = new Object[measurePage.length];
      Object[] uniqueValue = new Object[measurePage.length];
      int[] decimal = new int[measurePage.length];
      for (int i = 0; i < measurePage.length; i++) {
        min[i] = measurePage[i].getStatistics().getMin();
        max[i] = measurePage[i].getStatistics().getMax();
        uniqueValue[i] = measurePage[i].getStatistics().getUniqueValue();
        decimal[i] = measurePage[i].getStatistics().getDecimal();
      }
      // encode and compress measure column page
      compressionModel =
          ValueCompressionUtil.getWriterCompressModel(max, min, decimal, uniqueValue, measureType,
              new byte[measureCount]);
      encodedMeasureArray = encodeMeasure(compressionModel, measurePage);
      return this;
    }

    // this method first invokes encoding routine to encode the data chunk,
    // followed by invoking compression routine for preparing the data chunk for writing.
    private byte[][] encodeMeasure(WriterCompressModel compressionModel,
        FixLengthColumnPage[] columnPages) {

      CarbonWriteDataHolder[] holders = new CarbonWriteDataHolder[columnPages.length];
      for (int i = 0; i < holders.length; i++) {
        holders[i] = new CarbonWriteDataHolder();
        switch (columnPages[i].getDataType()) {
          case SHORT:
          case INT:
          case LONG:
            holders[i].setWritableLongPage(columnPages[i].getLongPage());
            break;
          case DOUBLE:
            holders[i].setWritableDoublePage(columnPages[i].getDoublePage());
            break;
          case DECIMAL:
            holders[i].setWritableDecimalPage(columnPages[i].getDecimalPage());
            break;
          default:
            throw new RuntimeException("Unsupported data type: " + columnPages[i].getDataType());
        }
      }

      DataType[] dataType = compressionModel.getDataType();
      ValueCompressionHolder[] values =
          new ValueCompressionHolder[compressionModel.getValueCompressionHolder().length];
      byte[][] returnValue = new byte[values.length][];
      for (int i = 0; i < compressionModel.getValueCompressionHolder().length; i++) {
        values[i] = compressionModel.getValueCompressionHolder()[i];
        if (dataType[i] != DataType.DECIMAL) {
          values[i].setValue(
              ValueCompressionUtil.getValueCompressor(compressionModel.getCompressionFinders()[i])
                  .getCompressedValues(compressionModel.getCompressionFinders()[i], holders[i],
                      compressionModel.getMaxValue()[i],
                      compressionModel.getMantissa()[i]));
        } else {
          values[i].setValue(holders[i].getWritableByteArrayValues());
        }
        values[i].compress();
        returnValue[i] = values[i].getCompressedData();
      }

      return returnValue;
    }

    /**
     * Encode and compress each column page. The work is done using a thread pool.
     */
    private IndexStorage[] encodeAndCompressDimensions(TablePage tablePage) {
      int noDictionaryCount = tablePage.getNoDictDimensionPage().length;
      int complexColCount = tablePage.getComplexDimensionPage().length;

      // thread pool size to be used for encoding dimension
      // each thread will sort the column page data and compress it
      int thread_pool_size = Integer.parseInt(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.NUM_CORES_BLOCK_SORT,
              CarbonCommonConstants.NUM_CORES_BLOCK_SORT_DEFAULT_VAL));
      ExecutorService executorService = Executors.newFixedThreadPool(thread_pool_size);
      Callable<IndexStorage> callable;
      List<Future<IndexStorage>> submit = new ArrayList<Future<IndexStorage>>(
          model.getPrimitiveDimLens().length + noDictionaryCount + complexColCount);
      int i = 0;
      int dictionaryColumnCount = -1;
      int noDictionaryColumnCount = -1;
      int colGrpId = -1;
      boolean isSortColumn = false;
      SegmentProperties segmentProperties = model.getSegmentProperties();
      for (i = 0; i < isDictDimension.length; i++) {
        isSortColumn = i < segmentProperties.getNumberOfSortColumns();
        if (isDictDimension[i]) {
          dictionaryColumnCount++;
          if (colGrpModel.isColumnar(dictionaryColumnCount)) {
            // dictionary dimension
            callable =
                new BlockSortThread(
                    tablePage.getKeyColumnPage().getKeyVector(dictionaryColumnCount),
                    true,
                    false,
                    isSortColumn,
                    isUseInvertedIndex[i] & isSortColumn);

          } else {
            // column group
            callable = new ColGroupBlockStorage(
                segmentProperties,
                ++colGrpId,
                tablePage.getKeyColumnPage().getKeyVector(dictionaryColumnCount));
          }
        } else {
          // no dictionary dimension
          callable =
              new BlockSortThread(
                  tablePage.getNoDictDimensionPage()[++noDictionaryColumnCount].getByteArrayPage(),
                  false,
                  true,
                  isSortColumn,
                  isUseInvertedIndex[i] & isSortColumn);
        }
        // start a thread to sort the page data
        submit.add(executorService.submit(callable));
      }

      // complex type column
      for (int index = 0; index < getComplexColumnCount(); index++) {
        Iterator<byte[][]> iterator = tablePage.getComplexDimensionPage()[index].iterator();
        while (iterator.hasNext()) {
          byte[][] data = iterator.next();
          callable =
              new BlockSortThread(
                  i++,
                  data,
                  false,
                  true);
          submit.add(executorService.submit(callable));
        }
      }
      executorService.shutdown();
      try {
        executorService.awaitTermination(1, TimeUnit.DAYS);
      } catch (InterruptedException e) {
        LOGGER.error(e, e.getMessage());
      }
      IndexStorage[] dimColumns = new IndexStorage[
          colGrpModel.getNoOfColumnStore() + noDictionaryCount + getExpandedComplexColsCount()];
      try {
        for (int k = 0; k < dimColumns.length; k++) {
          dimColumns[k] = submit.get(k).get();
        }
      } catch (Exception e) {
        LOGGER.error(e, e.getMessage());
      }
      return dimColumns;
    }
  }
}
