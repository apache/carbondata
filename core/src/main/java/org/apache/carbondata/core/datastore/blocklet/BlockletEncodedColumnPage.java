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
package org.apache.carbondata.core.datastore.blocklet;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.page.ActualDataBasedFallbackEncoder;
import org.apache.carbondata.core.datastore.page.DecoderBasedFallbackEncoder;
import org.apache.carbondata.core.datastore.page.FallbackEncodedColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.EncodedColumnPage;
import org.apache.carbondata.core.localdictionary.PageLevelDictionary;
import org.apache.carbondata.core.localdictionary.generator.LocalDictionaryGenerator;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.format.LocalDictionaryChunk;

/**
 * Maintains the list of encoded page of a column in a blocklet
 * and encoded dictionary values only if column is encoded using local
 * dictionary
 * Handle the fallback if all the pages in blocklet are not
 * encoded with local dictionary
 */
public class BlockletEncodedColumnPage {

  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(BlockletEncodedColumnPage.class.getName());

  /**
   * list of encoded page of a column in a blocklet
   */
  private List<EncodedColumnPage> encodedColumnPageList;

  /**
   * fallback executor service
   */
  private ExecutorService fallbackExecutorService;

  /**
   * to check whether pages are local dictionary encoded or not
   */
  private boolean isLocalDictEncoded;

  /**
   * page level dictionary only when column is encoded with local dictionary
   */
  private PageLevelDictionary pageLevelDictionary;

  /**
   * fallback future task queue;
   */
  private ArrayDeque<Future<FallbackEncodedColumnPage>> fallbackFutureQueue;

  private String columnName;

  /**
   * is decoder based fallback enabled
   */
  private boolean isDecoderBasedFallBackEnabled;

  /**
   * Local dictionary generator for column
   */
  private LocalDictionaryGenerator localDictionaryGenerator;

  BlockletEncodedColumnPage(ExecutorService fallbackExecutorService,
      boolean isDecoderBasedFallBackEnabled, LocalDictionaryGenerator localDictionaryGenerator) {
    this.fallbackExecutorService = fallbackExecutorService;
    this.fallbackFutureQueue = new ArrayDeque<>();
    this.isDecoderBasedFallBackEnabled = isDecoderBasedFallBackEnabled;
    this.localDictionaryGenerator = localDictionaryGenerator;
  }

  /**
   * Below method will be used to add column page of a column
   *
   * @param encodedColumnPage
   * encoded column page
   */
  void addEncodedColumnPage(EncodedColumnPage encodedColumnPage) {
    if (null == encodedColumnPageList) {
      this.encodedColumnPageList = new ArrayList<>();
      // if dimension page is local dictionary enabled and encoded with local dictionary
      if (encodedColumnPage.isLocalDictGeneratedPage()) {
        this.isLocalDictEncoded = true;
        // get first page dictionary
        this.pageLevelDictionary = encodedColumnPage.getPageDictionary();
      }
      this.encodedColumnPageList.add(encodedColumnPage);
      this.columnName = encodedColumnPage.getActualPage().getColumnSpec().getFieldName();
      return;
    }
    // when first page was encoded without dictionary and next page encoded with dictionary
    // in a blocklet
    if (!isLocalDictEncoded && encodedColumnPage.isLocalDictGeneratedPage()) {
      LOGGER.info(
          "Local dictionary Fallback is initiated for column: " + this.columnName + " for page:"
              + encodedColumnPageList.size());
      initiateFallBack(encodedColumnPage, encodedColumnPageList.size());
      // fill null so once page is decoded again fill the re-encoded page again
      this.encodedColumnPageList.add(null);
    }
    // if local dictionary is false or column is encoded with local dictionary then
    // add a page
    else if (!isLocalDictEncoded || encodedColumnPage.isLocalDictGeneratedPage()) {
      // merge page level dictionary values
      if (null != this.pageLevelDictionary) {
        pageLevelDictionary.mergerDictionaryValues(encodedColumnPage.getPageDictionary());
      }
      this.encodedColumnPageList.add(encodedColumnPage);
    }
    // if all the older pages were encoded with dictionary and new pages are without dictionary
    else {
      isLocalDictEncoded = false;
      pageLevelDictionary = null;
      LOGGER.info("Local dictionary Fallback is initiated for column: " + this.columnName
          + " for pages: 1 to " + encodedColumnPageList.size());
      // submit all the older pages encoded with dictionary for fallback
      for (int pageIndex = 0; pageIndex < encodedColumnPageList.size(); pageIndex++) {
        if (encodedColumnPageList.get(pageIndex).getActualPage().isLocalDictGeneratedPage()) {
          initiateFallBack(encodedColumnPageList.get(pageIndex), pageIndex);
        }
      }
      //add to page list
      this.encodedColumnPageList.add(encodedColumnPage);
    }
  }

  /**
   * Return the list of encoded page list for a column in a blocklet
   *
   * @return list of encoded page list
   */
  public List<EncodedColumnPage> getEncodedColumnPageList() {
    // if fallback queue is null then for some pages fallback was initiated
    if (null != this.fallbackFutureQueue) {
      try {
        // check if queue is not empty
        while (!fallbackFutureQueue.isEmpty()) {
          // get the head element of queue
          FallbackEncodedColumnPage fallbackEncodedColumnPage = fallbackFutureQueue.poll().get();
          // add the encoded column page to list
          encodedColumnPageList.set(fallbackEncodedColumnPage.getPageIndex(),
              fallbackEncodedColumnPage.getEncodedColumnPage());
        }
      } catch (ExecutionException | InterruptedException e) {
        throw new RuntimeException("Problem while encoding the blocklet data during fallback", e);
      }
      // setting to null as all the fallback encoded page has been added to list
      fallbackFutureQueue = null;
    }
    // in case of dictionary encoded column page memory will be freed only after
    // all the pages are added in a blocklet, as fallback can happen anytime so old pages memory
    // cannot be freed, so after encoding is done we can free the page memory
    if (null != pageLevelDictionary) {
      // clear the memory footprint for local dictionary encoded pages
      for (EncodedColumnPage columnPage : encodedColumnPageList) {
        columnPage.freeMemory();
      }
    }
    return encodedColumnPageList;
  }

  /**
   * Below method will be used to get the encoded dictionary
   * values for local dictionary generated columns
   *
   * @return encoded dictionary values if column is local dictionary generated
   */
  public LocalDictionaryChunk getEncodedDictionary() {
    if (null != pageLevelDictionary) {
      try {
        return pageLevelDictionary.getLocalDictionaryChunkForBlocklet();
      } catch (IOException | MemoryException e) {
        throw new RuntimeException(e);
      }
    }
    return null;
  }

  /**
   * This method initiates the fallback for local dictionary encoded column page
   * @param encodedColumnPage
   * @param pageIndex
   */
  private void initiateFallBack(EncodedColumnPage encodedColumnPage, int pageIndex) {
    if (isDecoderBasedFallBackEnabled) {
      fallbackFutureQueue.add(fallbackExecutorService.submit(
          new DecoderBasedFallbackEncoder(encodedColumnPage, pageIndex, localDictionaryGenerator)));
    } else {
      fallbackFutureQueue.add(fallbackExecutorService.submit(
          new ActualDataBasedFallbackEncoder(encodedColumnPage, pageIndex)));
    }
  }
}
