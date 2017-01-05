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

package org.apache.carbondata.core.reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.*;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastorage.store.impl.FileFactory;
import org.apache.carbondata.core.update.DeleteDeltaBlockDetails;
import org.apache.carbondata.core.update.DeleteDeltaBlockletDetails;
import org.apache.carbondata.core.util.CarbonProperties;

import org.apache.commons.lang.ArrayUtils;


/**
 * This class perform the functionality of reading multiple delete delta files
 */
public class CarbonDeleteFilesDataReader {

  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonDeleteFilesDataReader.class.getName());

  /**
   * thread pool size to be used for reading delete delta files
   */
  protected int thread_pool_size;

  /**
   * Constructor
   */
  public CarbonDeleteFilesDataReader() {
    initThreadPoolSize();
  }

  /**
   * This method will initialize the thread pool size to be used for creating the
   * max number of threads for a job
   */
  private void initThreadPoolSize() {
    try {
      thread_pool_size = Integer.parseInt(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.NUM_CORES_LOADING,
              CarbonCommonConstants.NUM_CORES_DEFAULT_VAL));
    } catch (NumberFormatException e) {
      thread_pool_size = Integer.parseInt(CarbonCommonConstants.NUM_CORES_DEFAULT_VAL);
    }
  }

  /**
   * Returns all deleted records from all specified delta files
   *
   * @param deltaFiles
   * @return
   * @throws Exception
   */
  public int[] getDeleteDataFromAllFiles(List<String> deltaFiles, String blockletId)
      throws Exception {

    List<Future<DeleteDeltaBlockDetails>> taskSubmitList = new ArrayList<>();
    ExecutorService executorService = Executors.newFixedThreadPool(thread_pool_size);
    for (final String deltaFile : deltaFiles) {
      taskSubmitList.add(executorService.submit(new Callable<DeleteDeltaBlockDetails>() {
        @Override public DeleteDeltaBlockDetails call() throws IOException {
          CarbonDeleteDeltaFileReaderImpl deltaFileReader =
              new CarbonDeleteDeltaFileReaderImpl(deltaFile, FileFactory.getFileType(deltaFile));
          return deltaFileReader.readJson();
        }
      }));
    }
    try {
      executorService.shutdown();
      executorService.awaitTermination(30, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      LOGGER.error("Error while reading the delete delta files : " + e.getMessage());
    }

    Set<Integer> result = new TreeSet<Integer>();
    for (int i = 0; i < taskSubmitList.size(); i++) {
      try {
        List<DeleteDeltaBlockletDetails> blockletDetails =
            taskSubmitList.get(i).get().getBlockletDetails();
        result.addAll(
            blockletDetails.get(blockletDetails.indexOf(new DeleteDeltaBlockletDetails(blockletId)))
                .getDeletedRows());
      } catch (Throwable e) {
        LOGGER.error(e.getMessage());
        throw new Exception(e.getMessage());
      }
    }
    return ArrayUtils.toPrimitive(result.toArray(new Integer[result.size()]));

  }

  public DeleteDeltaBlockDetails getCompactedDeleteDeltaFileFromBlock(List<String> deltaFiles,
      String blockName) throws Exception {
    // get the data.
    List<Future<DeleteDeltaBlockDetails>> taskSubmitList = new ArrayList<>(deltaFiles.size());
    ExecutorService executorService = Executors.newFixedThreadPool(thread_pool_size);
    for (final String deltaFile : deltaFiles) {
      taskSubmitList.add(executorService.submit(new Callable<DeleteDeltaBlockDetails>() {
        @Override public DeleteDeltaBlockDetails call() throws IOException {
          CarbonDeleteDeltaFileReaderImpl deltaFileReader =
              new CarbonDeleteDeltaFileReaderImpl(deltaFile, FileFactory.getFileType(deltaFile));
          return deltaFileReader.readJson();
        }
      }));
    }
    try {
      executorService.shutdown();
      executorService.awaitTermination(30, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      LOGGER.error("Error while reading the delete delta files : " + e.getMessage());
    }

    // Get a new DeleteDeltaBlockDetails as result set where all the data will me merged
    // based on each Blocklet.
    DeleteDeltaBlockDetails deleteDeltaResultSet = new DeleteDeltaBlockDetails(blockName);

    for (int i = 0; i < taskSubmitList.size(); i++) {
      try {
        List<DeleteDeltaBlockletDetails> blockletDetails =
            taskSubmitList.get(i).get().getBlockletDetails();
        for(DeleteDeltaBlockletDetails blocklet : blockletDetails ) {
          deleteDeltaResultSet.addBlockletDetails(blocklet);
        }
      } catch (Throwable e) {
        LOGGER.error(e.getMessage());
        throw new Exception(e.getMessage());
      }
    }
    return deleteDeltaResultSet;
  }
}


