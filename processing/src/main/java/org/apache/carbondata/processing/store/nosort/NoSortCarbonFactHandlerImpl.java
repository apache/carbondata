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

package org.apache.carbondata.processing.store.nosort;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.processing.store.*;
import org.apache.carbondata.processing.store.writer.NodeHolder;
import org.apache.carbondata.processing.store.writer.exception.CarbonDataWriterException;

/**
 * Fact data handler class to handle the fact data
 */
public class NoSortCarbonFactHandlerImpl extends AbstractCarbonFactHandler {

  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(NoSortCarbonFactHandlerImpl.class.getName());
  /**
   * total number of entries in blocklet
   */
  private int entryCount;
  private long processedDataCount;
  private ExecutorService executorService;
  private Future<Void> future;
  private List<Object[]> prefetchDataRows;

  /**
   * CarbonFactDataHandler constructor
   */
  public NoSortCarbonFactHandlerImpl(CarbonFactDataHandlerModel carbonFactDataHandlerModel) {
    super(carbonFactDataHandlerModel);

    executorService = Executors.newSingleThreadExecutor();
    prefetchDataRows = new ArrayList<Object[]>(this.blockletSize);

  }


  /**
   * below method will be used to add row to store
   *
   * @param row
   * @throws CarbonDataWriterException
   */
  public void addDataToStore(Object[] row) throws CarbonDataWriterException {
    prefetchDataRows.add(row);
    this.entryCount++;
    // if entry count reaches to leaf node size then we are ready to
    // write
    // this to leaf node file and update the intermediate files
    if (this.entryCount == this.blockletSize) {
      if (future != null) {
        try {
          future.get();
        } catch (Exception e) {
          LOGGER.error(e, e.getMessage());
          throw new CarbonDataWriterException(e.getMessage());
        }
      }
      future = executorService.submit(new BlockletWriteThread(prefetchDataRows));
      prefetchDataRows = new ArrayList<>(this.blockletSize);

      // set the entry count to zero
      this.entryCount = 0;
    }
  }

  /**
   * below method will be used to finish the data handler
   *
   * @throws CarbonDataWriterException
   */
  public void finish() throws CarbonDataWriterException {
    if (future != null) {
      try {
        future.get();
      } catch (Exception e) {
        LOGGER.error(e, e.getMessage());
        throw new CarbonDataWriterException(e.getMessage());
      }
    }
    if (this.entryCount > 0) {
      writeBlocklet(prefetchDataRows);
    }
  }

  /**
   * below method will be used to close the handler
   */
  public void closeHandler() throws CarbonDataWriterException {
    if (null != this.dataWriter) {
      // wait until all blocklets have been finished writing
      this.dataWriter.writeBlockletInfoToFile();
      LOGGER.info("All blocklets have been finished writing");
      // close all the open stream for both the files
      this.dataWriter.closeWriter();
    }
    this.dataWriter = null;
    this.keyBlockHolder = null;
  }

  private void writeBlocklet(List<Object[]> dataRowsStore) {
    processedDataCount += entryCount;
    long t1 = System.currentTimeMillis();
    NodeHolder nodeHolder = processDataRowsWithOutKettle(dataRowsStore);
    long t2 = System.currentTimeMillis();
    LOGGER.info("[Blocklet level]1.Generate NodeHolder use time: " + (t2 - t1) + "ms");
    dataWriter.writeBlockletData(nodeHolder);
    LOGGER.info("[Blocklet level]2.Write Blocklet use time: " + (System.currentTimeMillis() - t2)
        + "ms");
    LOGGER.info("[Blocklet level]Total Number Of records added to store: " + processedDataCount);
  }

  private final class BlockletWriteThread implements Callable<Void> {
    private List<Object[]> dataRowsStore;

    public BlockletWriteThread(List<Object[]> dataRowsStore) {
      this.dataRowsStore = dataRowsStore;
    }

    @Override
    public Void call() throws Exception {
      writeBlocklet(this.dataRowsStore);
      return null;
    }
  }
}
