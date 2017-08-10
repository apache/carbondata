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
import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.carbondata.core.datastore.columnar.ColumnGroupModel;
import org.apache.carbondata.core.datastore.exception.CarbonDataWriterException;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.keygenerator.columnar.ColumnarSplitter;
import org.apache.carbondata.core.keygenerator.columnar.impl.MultiDimKeyVarLengthEquiSplitGenerator;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.processing.datatypes.GenericDataType;
import org.apache.carbondata.processing.newflow.sort.SortScopeOptions;
import org.apache.carbondata.processing.store.file.FileManager;
import org.apache.carbondata.processing.store.file.IFileManagerComposite;
import org.apache.carbondata.processing.store.writer.CarbonDataWriterVo;
import org.apache.carbondata.processing.store.writer.CarbonFactDataWriter;

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
  private int pageSize;
  /**
   * keyBlockHolder
   */
  private CarbonKeyBlockHolder[] keyBlockHolder;

  // This variable is true if it is dictionary dimension and its cardinality is lower than
  // property of CarbonCommonConstants.HIGH_CARDINALITY_VALUE
  // It decides whether it will do RLE encoding on data page for this dimension
  private boolean[] rleEncodingForDictDimension;
  private boolean[] isNoDictionary;
  private long processedDataCount;
  private ExecutorService producerExecutorService;
  private List<Future<Void>> producerExecutorServiceTaskList;
  private ExecutorService consumerExecutorService;
  private List<Future<Void>> consumerExecutorServiceTaskList;
  private List<CarbonRow> dataRows;
  private ColumnGroupModel colGrpModel;
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
  private TablePageList tablePageList;
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
    this.rleEncodingForDictDimension = new boolean[numDimColumns];
    this.isNoDictionary = new boolean[numDimColumns];

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
      int[] dimLens = model.getSegmentProperties().getDimColumnsCardinality();
      for (int i = 0; i < model.getTableSpec().getNumSimpleDimensions(); i++) {
        if (model.getSegmentProperties().getDimensions().get(i).isGlobalDictionaryEncoding()) {
          this.rleEncodingForDictDimension[i] = true;
        }
      }

      if (model.getDimensionCount() < dimLens.length) {
        int allColsCount = getColsCount(model.getDimensionCount());
        List<Boolean> rleWithComplex = new ArrayList<Boolean>(allColsCount);
        for (int i = 0; i < model.getDimensionCount(); i++) {
          GenericDataType complexDataType = model.getComplexIndexMap().get(i);
          if (complexDataType != null) {
            complexDataType.fillAggKeyBlock(rleWithComplex, this.rleEncodingForDictDimension);
          } else {
            rleWithComplex.add(this.rleEncodingForDictDimension[i]);
          }
        }
        this.rleEncodingForDictDimension = new boolean[allColsCount];
        for (int i = 0; i < allColsCount; i++) {
          this.rleEncodingForDictDimension[i] = rleWithComplex.get(i);
        }
      }
    }
    this.version = CarbonProperties.getInstance().getFormatVersion();
    String noInvertedIdxCol = "";
    for (CarbonDimension cd : model.getSegmentProperties().getDimensions()) {
      if (!cd.isUseInvertedIndex()) {
        noInvertedIdxCol += (cd.getColName() + ",");
      }
    }

    LOGGER.info("Columns considered as NoInverted Index are " + noInvertedIdxCol);
  }

  private void initParameters(CarbonFactDataHandlerModel model) {
    SortScopeOptions.SortScope sortScope = model.getSortScope();
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

    if (sortScope != null && sortScope.equals(SortScopeOptions.SortScope.GLOBAL_SORT)) {
      numberOfCores = 1;
    }

    blockletProcessingCount = new AtomicInteger(0);
    producerExecutorService = Executors.newFixedThreadPool(numberOfCores);
    producerExecutorServiceTaskList =
        new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    LOGGER.info("Initializing writer executors");
    consumerExecutorService = Executors.newFixedThreadPool(1);
    consumerExecutorServiceTaskList = new ArrayList<>(1);
    semaphore = new Semaphore(numberOfCores);
    tablePageList = new TablePageList();

    // Start the consumer which will take each blocklet/page in order and write to a file
    Consumer consumer = new Consumer(tablePageList);
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
    // todo: the fileManager seems to be useless, remove it later
    fileManager.setName(new File(model.getStoreLocation()[0]).getName());
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
    if (this.entryCount == this.pageSize) {
      try {
        semaphore.acquire();

        producerExecutorServiceTaskList.add(
            producerExecutorService.submit(
                new Producer(tablePageList, dataRows, ++writerTaskSequenceCounter, false)
            )
        );
        blockletProcessingCount.incrementAndGet();
        // set the entry count to zero
        processedDataCount += entryCount;
        LOGGER.info("Total Number Of records added to store: " + processedDataCount);
        dataRows = new ArrayList<>(this.pageSize);
        this.entryCount = 0;
      } catch (InterruptedException e) {
        LOGGER.error(e, e.getMessage());
        throw new CarbonDataWriterException(e.getMessage(), e);
      }
    }
  }

  /**
   * generate the EncodedTablePage from the input rows (one page in case of V3 format)
   */
  private TablePage processDataRows(List<CarbonRow> dataRows)
      throws CarbonDataWriterException, KeyGenException, MemoryException, IOException {
    if (dataRows.size() == 0) {
      return new TablePage(model, 0);
    }
    TablePage tablePage = new TablePage(model, dataRows.size());
    int rowId = 0;

    // convert row to columnar data
    for (CarbonRow row : dataRows) {
      tablePage.addRow(rowId++, row);
    }

    tablePage.encode();

    LOGGER.info("Number Of records processed: " + dataRows.size());
    return tablePage;
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
          .submit(new Producer(tablePageList, dataRows, ++writerTaskSequenceCounter, true)));
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
    return model.getExpandedComplexColsCount();
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
      this.dataWriter.writeFooterToFile();
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
    this.pageSize = Integer.parseInt(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.BLOCKLET_SIZE,
            CarbonCommonConstants.BLOCKLET_SIZE_DEFAULT_VAL));
    if (version == ColumnarFormatVersion.V3) {
      this.pageSize = Integer.parseInt(CarbonProperties.getInstance()
          .getProperty(CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE,
              CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_DEFAULT));
    }
    LOGGER.info("Number of rows per column blocklet " + pageSize);
    dataRows = new ArrayList<>(this.pageSize);
    int dimSet =
        Integer.parseInt(CarbonCommonConstants.DIMENSION_SPLIT_VALUE_IN_COLUMNAR_DEFAULTVALUE);
    // if at least one dimension is present then initialize column splitter otherwise null
    int noOfColStore = colGrpModel.getNoOfColumnStore();
    int[] keyBlockSize = new int[noOfColStore + getExpandedComplexColsCount()];

    if (model.getDimLens().length > 0) {
      //Using Variable length variable split generator
      //This will help in splitting mdkey to columns. variable split is required because all
      // columns which are part of
      //row store will be in single column store
      //e.g if {0,1,2,3,4,5} is dimension and {0,1,2) is row store dimension
      //than below splitter will return column as {0,1,2}{3}{4}{5}
      ColumnarSplitter columnarSplitter = model.getSegmentProperties().getFixedLengthKeySplitter();
      System.arraycopy(columnarSplitter.getBlockKeySize(), 0, keyBlockSize, 0, noOfColStore);
      this.keyBlockHolder =
          new CarbonKeyBlockHolder[columnarSplitter.getBlockKeySize().length];
    } else {
      this.keyBlockHolder = new CarbonKeyBlockHolder[0];
    }

    for (int i = 0; i < keyBlockHolder.length; i++) {
      this.keyBlockHolder[i] = new CarbonKeyBlockHolder(pageSize);
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
    this.dataWriter = getFactDataWriter();
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
   * @return data writer instance
   */
  private CarbonFactDataWriter<?> getFactDataWriter() {
    return CarbonDataWriterFactory.getInstance()
        .getFactDataWriter(version, getDataWriterVo());
  }

  /**
   * Below method will be used to get the writer vo
   *
   * @return data writer vo object
   */
  private CarbonDataWriterVo getDataWriterVo() {
    CarbonDataWriterVo carbonDataWriterVo = new CarbonDataWriterVo();
    carbonDataWriterVo.setStoreLocation(model.getStoreLocation());
    carbonDataWriterVo.setMeasureCount(model.getMeasureCount());
    carbonDataWriterVo.setTableName(model.getTableName());
    carbonDataWriterVo.setFileManager(fileManager);
    carbonDataWriterVo.setRleEncodingForDictDim(rleEncodingForDictDimension);
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
    carbonDataWriterVo.setBucketNumber(model.getBucketId());
    carbonDataWriterVo.setTaskExtension(model.getTaskExtension());
    carbonDataWriterVo.setSchemaUpdatedTimeStamp(model.getSchemaUpdatedTimeStamp());
    carbonDataWriterVo.setListener(model.getDataMapWriterlistener());
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
   * This class will hold the table page data
   */
  private final class TablePageList {
    /**
     * array of table page added by Producer and get by Consumer
     */
    private TablePage[] tablePages;
    /**
     * flag to check whether the producer has completed processing for holder
     * object which is required to be picked form an index
     */
    private AtomicBoolean available;
    /**
     * index from which data node holder object needs to be picked for writing
     */
    private int currentIndex;

    private TablePageList() {
      tablePages = new TablePage[numberOfCores];
      available = new AtomicBoolean(false);
    }

    /**
     * @return a node holder object
     * @throws InterruptedException if consumer thread is interrupted
     */
    public synchronized TablePage get() throws InterruptedException {
      TablePage tablePage = tablePages[currentIndex];
      // if node holder is null means producer thread processing the data which has to
      // be inserted at this current index has not completed yet
      if (null == tablePage && !processingComplete) {
        available.set(false);
      }
      while (!available.get()) {
        wait();
      }
      tablePage = tablePages[currentIndex];
      tablePages[currentIndex] = null;
      currentIndex++;
      // reset current index when it reaches length of node holder array
      if (currentIndex >= tablePages.length) {
        currentIndex = 0;
      }
      return tablePage;
    }

    /**
     * @param encodedTablePage
     * @param index
     */
    public synchronized void put(TablePage tablePage, int index) {
      tablePages[index] = tablePage;
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

    private TablePageList tablePageList;
    private List<CarbonRow> dataRows;
    private int pageId;
    private boolean isLastPage;

    private Producer(TablePageList tablePageList, List<CarbonRow> dataRows,
        int pageId, boolean isLastPage) {
      this.tablePageList = tablePageList;
      this.dataRows = dataRows;
      this.pageId = pageId;
      this.isLastPage = isLastPage;
    }

    /**
     * Computes a result, or throws an exception if unable to do so.
     *
     * @return computed result
     * @throws Exception if unable to compute a result
     */
    @Override public Void call() throws Exception {
      try {
        TablePage tablePage = processDataRows(dataRows);
        tablePage.setIsLastPage(isLastPage);
        // insert the object in array according to sequence number
        int indexInNodeHolderArray = (pageId - 1) % numberOfCores;
        tablePageList.put(tablePage, indexInNodeHolderArray);
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

    private TablePageList tablePageList;

    private Consumer(TablePageList tablePageList) {
      this.tablePageList = tablePageList;
    }

    /**
     * Computes a result, or throws an exception if unable to do so.
     *
     * @return computed result
     * @throws Exception if unable to compute a result
     */
    @Override public Void call() throws Exception {
      while (!processingComplete || blockletProcessingCount.get() > 0) {
        TablePage tablePage = null;
        try {
          tablePage = tablePageList.get();
          if (null != tablePage) {
            dataWriter.writeTablePage(tablePage);
            tablePage.freeMemory();
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
}
