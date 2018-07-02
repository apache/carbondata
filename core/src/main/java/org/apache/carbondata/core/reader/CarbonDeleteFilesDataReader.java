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

package org.apache.carbondata.core.reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.mutate.DeleteDeltaBlockDetails;
import org.apache.carbondata.core.mutate.DeleteDeltaBlockletDetails;
import org.apache.carbondata.core.mutate.DeleteDeltaVo;
import org.apache.carbondata.core.util.CarbonProperties;

import org.apache.log4j.Logger;

/**
 * This class perform the functionality of reading multiple delete delta files
 */
public class CarbonDeleteFilesDataReader {

  /**
   * LOGGER
   */
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(CarbonDeleteFilesDataReader.class.getName());

  /**
   * thread pool size to be used for reading delete delta files
   */
  protected int thread_pool_size;

  public CarbonDeleteFilesDataReader() {
    initThreadPoolSize();
  }

  public CarbonDeleteFilesDataReader(int thread_pool_size) {
    this.thread_pool_size = thread_pool_size;
  }

  /**
   * This method will initialize the thread pool size to be used for creating the
   * max number of threads for a job
   */
  private void initThreadPoolSize() {
    thread_pool_size = CarbonProperties.getInstance().getNumberOfLoadingCores();
  }

  /**
   * Returns all deleted records from all specified delta files
   *
   * @param deltaFiles
   * @return
   * @throws Exception
   */
  public Map<Integer, Integer[]> getDeleteDataFromAllFiles(List<String> deltaFiles,
      String blockletId) throws Exception {

    List<Future<DeleteDeltaBlockDetails>> taskSubmitList = new ArrayList<>();
    ExecutorService executorService = Executors.newFixedThreadPool(thread_pool_size);
    for (final String deltaFile : deltaFiles) {
      taskSubmitList.add(executorService.submit(new DeleteDeltaFileReaderCallable(deltaFile)));
    }
    try {
      executorService.shutdown();
      executorService.awaitTermination(30, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      LOGGER.error("Error while reading the delete delta files : " + e.getMessage());
    }

    Map<Integer, Integer[]> pageIdDeleteRowsMap =
        new HashMap<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    for (int i = 0; i < taskSubmitList.size(); i++) {
      try {
        List<DeleteDeltaBlockletDetails> blockletDetails =
            taskSubmitList.get(i).get().getBlockletDetails();
        for (DeleteDeltaBlockletDetails eachBlockletDetails : blockletDetails) {
          Integer pageId = eachBlockletDetails.getPageId();
          Set<Integer> rows = blockletDetails
              .get(blockletDetails.indexOf(new DeleteDeltaBlockletDetails(blockletId, pageId)))
              .getDeletedRows();
          pageIdDeleteRowsMap.put(pageId, rows.toArray(new Integer[rows.size()]));
        }

      } catch (Throwable e) {
        LOGGER.error(e.getMessage());
        throw new Exception(e.getMessage());
      }
    }
    return pageIdDeleteRowsMap;
  }

  /**
   * Below method will be used to read the delete delta files
   * and get the map of blockletid and page id mapping to deleted
   * rows
   *
   * @param deltaFiles delete delta files array
   * @return map of blockletid_pageid to deleted rows
   */
  public Map<String, DeleteDeltaVo> getDeletedRowsDataVo(String[] deltaFiles) {
    List<Future<DeleteDeltaBlockDetails>> taskSubmitList = new ArrayList<>();
    ExecutorService executorService = Executors.newFixedThreadPool(thread_pool_size);
    for (final String deltaFile : deltaFiles) {
      taskSubmitList.add(executorService.submit(new DeleteDeltaFileReaderCallable(deltaFile)));
    }
    try {
      executorService.shutdown();
      executorService.awaitTermination(30, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      LOGGER.error("Error while reading the delete delta files : " + e.getMessage());
    }
    Map<String, DeleteDeltaVo> pageIdToBlockLetVo = new HashMap<>();
    List<DeleteDeltaBlockletDetails> blockletDetails = null;
    for (int i = 0; i < taskSubmitList.size(); i++) {
      try {
        blockletDetails = taskSubmitList.get(i).get().getBlockletDetails();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
      for (DeleteDeltaBlockletDetails blockletDetail : blockletDetails) {
        DeleteDeltaVo deleteDeltaVo = pageIdToBlockLetVo.get(blockletDetail.getBlockletKey());
        if (null == deleteDeltaVo) {
          deleteDeltaVo = new DeleteDeltaVo();
          pageIdToBlockLetVo.put(blockletDetail.getBlockletKey(), deleteDeltaVo);
        }
        deleteDeltaVo.insertData(blockletDetail.getDeletedRows());
      }
    }
    return pageIdToBlockLetVo;
  }

  /**
   * returns delete delta file details for the specified block name
   * @param deltaFiles
   * @param blockName
   * @return DeleteDeltaBlockDetails
   * @throws Exception
   */
  public DeleteDeltaBlockDetails getCompactedDeleteDeltaFileFromBlock(List<String> deltaFiles,
      String blockName) throws Exception {
    // get the data.
    List<Future<DeleteDeltaBlockDetails>> taskSubmitList = new ArrayList<>(deltaFiles.size());
    ExecutorService executorService = Executors.newFixedThreadPool(thread_pool_size);
    for (final String deltaFile : deltaFiles) {
      taskSubmitList.add(executorService.submit(new DeleteDeltaFileReaderCallable(deltaFile)));
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
        for (DeleteDeltaBlockletDetails blocklet : blockletDetails) {
          deleteDeltaResultSet.addBlockletDetails(blocklet);
        }
      } catch (Throwable e) {
        LOGGER.error(e.getMessage());
        throw new Exception(e.getMessage());
      }
    }
    return deleteDeltaResultSet;
  }
  private static class DeleteDeltaFileReaderCallable implements Callable<DeleteDeltaBlockDetails> {
    private String deltaFile;
    DeleteDeltaFileReaderCallable(String deltaFile) {
      this.deltaFile = deltaFile;
    }
    @Override public DeleteDeltaBlockDetails call() throws IOException {
      CarbonDeleteDeltaFileReaderImpl deltaFileReader =
          new CarbonDeleteDeltaFileReaderImpl(deltaFile, FileFactory.getFileType(deltaFile));
      return deltaFileReader.readJson();
    }
  }
}


