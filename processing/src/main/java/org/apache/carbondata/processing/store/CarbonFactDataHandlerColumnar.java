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
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonThreadFactory;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.processing.datatypes.GenericDataType;
import org.apache.carbondata.processing.loading.sort.SortScopeOptions;
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
   * total number of entries in blocklet
   */
  private int entryCount;

  /**
   * blocklet size (for V1 and V2) or page size (for V3). A Producer thread will start to process
   * once this size of input is reached
   */
  private int pageSize;

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
   * current data format version
   */
  private ColumnarFormatVersion version;

  /**
   * CarbonFactDataHandler constructor
   */
  public CarbonFactDataHandlerColumnar(CarbonFactDataHandlerModel model) {
    this.model = model;
    initParameters(model);
    this.version = CarbonProperties.getInstance().getFormatVersion();
    StringBuffer noInvertedIdxCol = new StringBuffer();
    for (CarbonDimension cd : model.getSegmentProperties().getDimensions()) {
      if (!cd.isUseInvertedIndex()) {
        noInvertedIdxCol.append(cd.getColName()).append(",");
      }
    }

    LOGGER.info("Columns considered as NoInverted Index are " + noInvertedIdxCol.toString());
  }

  private void initParameters(CarbonFactDataHandlerModel model) {
    SortScopeOptions.SortScope sortScope = model.getSortScope();
    this.colGrpModel = model.getSegmentProperties().getColumnGroupModel();

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
      numberOfCores = CarbonProperties.getInstance().getNumberOfCores();
    }

    if (sortScope != null && sortScope.equals(SortScopeOptions.SortScope.GLOBAL_SORT)) {
      numberOfCores = 1;
    }
    // Overriding it to the task specified cores.
    if (model.getWritingCoresCount() > 0) {
      numberOfCores = model.getWritingCoresCount();
    }

    blockletProcessingCount = new AtomicInteger(0);
    producerExecutorService = Executors.newFixedThreadPool(numberOfCores,
        new CarbonThreadFactory("ProducerPool:" + model.getTableName()
            + ", range: " + model.getBucketId()));
    producerExecutorServiceTaskList =
        new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    LOGGER.info("Initializing writer executors");
    consumerExecutorService = Executors
        .newFixedThreadPool(1, new CarbonThreadFactory("ConsumerPool:" + model.getTableName()
            + ", range: " + model.getBucketId()));
    consumerExecutorServiceTaskList = new ArrayList<>(1);
    semaphore = new Semaphore(numberOfCores);
    tablePageList = new TablePageList();

    // Start the consumer which will take each blocklet/page in order and write to a file
    Consumer consumer = new Consumer(tablePageList);
    consumerExecutorServiceTaskList.add(consumerExecutorService.submit(consumer));
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
    if (null == dataWriter) {
      return;
    }
    if (producerExecutorService.isShutdown()) {
      return;
    }
    LOGGER.info("Started Finish Operation");
    try {
      semaphore.acquire();
      producerExecutorServiceTaskList.add(producerExecutorService
          .submit(new Producer(tablePageList, dataRows, ++writerTaskSequenceCounter, true)));
      blockletProcessingCount.incrementAndGet();
      processedDataCount += entryCount;
      LOGGER.info("Total Number Of records added to store: " + processedDataCount);
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

  // return the number of complex column after complex columns are expanded
  private int getExpandedComplexColsCount() {
    return model.getExpandedComplexColsCount();
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
      this.pageSize = CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_DEFAULT;
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
    }

    // agg type
    List<Integer> otherMeasureIndexList =
        new ArrayList<Integer>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    List<Integer> customMeasureIndexList =
        new ArrayList<Integer>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    DataType[] type = model.getMeasureDataType();
    for (int j = 0; j < type.length; j++) {
      if (type[j] != DataTypes.BYTE && !DataTypes.isDecimal(type[j])) {
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
    // initialize the channel;
    this.dataWriter.initializeWriter();
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
  private CarbonFactDataWriter getFactDataWriter() {
    return CarbonDataWriterFactory.getInstance().getFactDataWriter(version, model);
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
     * @param tablePage
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
        dataRows = null;
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
