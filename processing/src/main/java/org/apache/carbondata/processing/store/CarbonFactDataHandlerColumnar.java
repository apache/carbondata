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

package org.apache.carbondata.processing.store;


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
import org.apache.carbondata.processing.store.writer.NodeHolder;
import org.apache.carbondata.processing.store.writer.exception.CarbonDataWriterException;

/**
 * Fact data handler class to handle the fact data
 */
public class CarbonFactDataHandlerColumnar extends AbstractCarbonFactHandler {

  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonFactDataHandlerColumnar.class.getName());

  /**
   * total number of entries in blocklet
   */
  private int entryCount;
  private long processedDataCount;
  private ExecutorService producerExecutorService;
  private List<Future<Void>> producerExecutorServiceTaskList;
  private ExecutorService consumerExecutorService;
  private List<Future<Void>> consumerExecutorServiceTaskList;
  private List<Object[]> dataRows;

  /**
   * data file attributes which will used for file construction
   */
  private CarbonDataFileAttributes carbonDataFileAttributes;
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
   * a private class which will take each blocklet in order and write to a file
   */
  private Consumer consumer;
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
   * CarbonFactDataHandler constructor
   */
  public CarbonFactDataHandlerColumnar(CarbonFactDataHandlerModel carbonFactDataHandlerModel) {
    super(carbonFactDataHandlerModel);
    initParameters(carbonFactDataHandlerModel);
  }

  private void initParameters(CarbonFactDataHandlerModel carbonFactDataHandlerModel) {

    dataRows = new ArrayList<Object[]>(this.blockletSize);
    blockletProcessingCount = new AtomicInteger(0);
    producerExecutorService = Executors.newFixedThreadPool(numberOfCores);
    producerExecutorServiceTaskList =
        new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    LOGGER.info("Initializing writer executors");
    consumerExecutorService = Executors.newFixedThreadPool(1);
    consumerExecutorServiceTaskList = new ArrayList<>(1);
    semaphore = new Semaphore(numberOfCores);
    blockletDataHolder = new BlockletDataHolder();
    consumer = new Consumer(blockletDataHolder);
    consumerExecutorServiceTaskList.add(consumerExecutorService.submit(consumer));
  }

  /**
   * below method will be used to add row to store
   *
   * @param row
   * @throws CarbonDataWriterException
   */
  public void addDataToStore(Object[] row) throws CarbonDataWriterException {
    dataRows.add(row);
    this.entryCount++;
    // if entry count reaches to leaf node size then we are ready to
    // write
    // this to leaf node file and update the intermediate files
    if (this.entryCount == this.blockletSize) {
      try {
        semaphore.acquire();
        producerExecutorServiceTaskList.add(producerExecutorService
            .submit(new Producer(blockletDataHolder, dataRows, ++writerTaskSequenceCounter)));
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

  /**
   * below method will be used to finish the data handler
   *
   * @throws CarbonDataWriterException
   */
  public void finish() throws CarbonDataWriterException {
    // still some data is present in stores if entryCount is more
    // than 0
    if (this.entryCount > 0) {
      producerExecutorServiceTaskList.add(producerExecutorService
          .submit(new Producer(blockletDataHolder, dataRows, ++writerTaskSequenceCounter)));
      blockletProcessingCount.incrementAndGet();
      processedDataCount += entryCount;
    }
    closeWriterExecutionService(producerExecutorService);
    processWriteTaskSubmitList(producerExecutorServiceTaskList);
    processingComplete = true;
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
      } catch (InterruptedException e) {
        LOGGER.error(e, e.getMessage());
        throw new CarbonDataWriterException(e.getMessage(), e);
      } catch (ExecutionException e) {
        LOGGER.error(e, e.getMessage());
        throw new CarbonDataWriterException(e.getMessage(), e);
      }
    }
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
   * This method will reset the block processing count
   */
  private void resetBlockletProcessingCount() {
    blockletProcessingCount.set(0);
  }

  /**
   * Producer which will process data equivalent to 1 blocklet size
   */
  private final class Producer implements Callable<Void> {

    private BlockletDataHolder blockletDataHolder;
    private List<Object[]> dataRows;
    private int sequenceNumber;

    private Producer(BlockletDataHolder blockletDataHolder, List<Object[]> dataRows,
        int sequenceNumber) {
      this.blockletDataHolder = blockletDataHolder;
      this.dataRows = dataRows;
      this.sequenceNumber = sequenceNumber;
    }

    /**
     * Computes a result, or throws an exception if unable to do so.
     *
     * @return computed result
     * @throws Exception if unable to compute a result
     */
    @Override public Void call() throws Exception {
      try {
        NodeHolder nodeHolder;
        if (useKettle) {
          nodeHolder = processDataRows(dataRows);
        } else {
          nodeHolder = processDataRowsWithOutKettle(dataRows);
        }
        // insert the object in array according to sequence number
        int indexInNodeHolderArray = (sequenceNumber - 1) % numberOfCores;
        blockletDataHolder.put(nodeHolder, indexInNodeHolderArray);
        return null;
      } catch (Throwable throwable) {
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
            throw new CarbonDataWriterException(throwable.getMessage(), throwable);
          }
        } finally {
          semaphore.release();
        }
      }
      return null;
    }
  }
}
