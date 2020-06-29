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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.carbondata.core.datastore.page.EncodedTablePage;
import org.apache.carbondata.core.localdictionary.generator.LocalDictionaryGenerator;

/**
 * Holds the blocklet level data and metadata to be written in carbondata file
 * For dimension pages it will check if all the pages are not encoded with dictionary
 * then it will encode those pages for that column again
 */
public class EncodedBlocklet {

  /**
   * number of rows in a blocklet
   */
  private int blockletSize;

  /**
   * maintains encoded dimension data for each column
   */
  private List<BlockletEncodedColumnPage> encodedDimensionColumnPages;

  /**
   * maintains encoded measure data for each column
   */
  private List<BlockletEncodedColumnPage> encodedMeasureColumnPages;

  /**
   * fallback executor service, will used to re-encode column pages
   */
  private ExecutorService executorService;

  /**
   * number of pages in a blocklet
   */
  private int numberOfPages;

  /**
   * row count in each page
   */
  private List<Short> rowCountInPage;

  /**
   * is decoder based fallback is enabled or not
   */
  private boolean isDecoderBasedFallBackEnabled;

  /**
   * local dictionary generator map of all local dictionary columns
   */
  private Map<String, LocalDictionaryGenerator> localDictionaryGeneratorMap;

  public EncodedBlocklet(ExecutorService executorService, boolean isDecoderBasedFallBackEnabled,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2889
      Map<String, LocalDictionaryGenerator> localDictionaryGeneratorMap) {
    this.executorService = executorService;
    this.isDecoderBasedFallBackEnabled = isDecoderBasedFallBackEnabled;
    this.localDictionaryGeneratorMap = localDictionaryGeneratorMap;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3001
    this.rowCountInPage = new ArrayList<>();
  }

  /**
   * Below method will be used to add page metadata details
   *
   * @param encodedTablePage
   * encoded table page
   */
  private void addPageMetadata(EncodedTablePage encodedTablePage) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3001
    if (null == rowCountInPage) {
      rowCountInPage = new ArrayList<>();
    }
    // update details
    blockletSize += encodedTablePage.getPageSize();
    this.numberOfPages++;
    rowCountInPage.add((short)encodedTablePage.getPageSize());
  }

  /**
   * Below method will be used to add measure column pages
   *
   * @param encodedTablePage
   * encoded table page
   */
  private void addEncodedMeasurePage(EncodedTablePage encodedTablePage) {
    // for first page create new list
    if (null == encodedMeasureColumnPages) {
      encodedMeasureColumnPages = new ArrayList<>();
      // adding measure pages
      for (int i = 0; i < encodedTablePage.getNumMeasures(); i++) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2889
        BlockletEncodedColumnPage blockletEncodedColumnPage =
            new BlockletEncodedColumnPage(null, false, null);
        blockletEncodedColumnPage.addEncodedColumnPage(encodedTablePage.getMeasure(i));
        encodedMeasureColumnPages.add(blockletEncodedColumnPage);
      }
    } else {
      for (int i = 0; i < encodedTablePage.getNumMeasures(); i++) {
        encodedMeasureColumnPages.get(i).addEncodedColumnPage(encodedTablePage.getMeasure(i));
      }
    }
  }

  /**
   * Below method will be used to add dimension column pages
   *
   * @param encodedTablePage
   * encoded table page
   */
  private void addEncodedDimensionPage(EncodedTablePage encodedTablePage) {
    // for first page create new list
    if (null == encodedDimensionColumnPages) {
      encodedDimensionColumnPages = new ArrayList<>();
      // adding measure pages
      for (int i = 0; i < encodedTablePage.getNumDimensions(); i++) {
        BlockletEncodedColumnPage blockletEncodedColumnPage =
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2889
            new BlockletEncodedColumnPage(executorService, isDecoderBasedFallBackEnabled,
                localDictionaryGeneratorMap.get(
                    encodedTablePage.getDimension(i).getActualPage().getColumnSpec()
                        .getFieldName()));
        blockletEncodedColumnPage.addEncodedColumnPage(encodedTablePage.getDimension(i));
        encodedDimensionColumnPages.add(blockletEncodedColumnPage);
      }
    } else {
      for (int i = 0; i < encodedTablePage.getNumDimensions(); i++) {
        encodedDimensionColumnPages.get(i).addEncodedColumnPage(encodedTablePage.getDimension(i));
      }
    }
  }

  /**
   * Use to add table pages
   *
   * @param encodedTablePage
   * encoded table page
   */
  public void addEncodedTablePage(EncodedTablePage encodedTablePage) {
    addPageMetadata(encodedTablePage);
    addEncodedDimensionPage(encodedTablePage);
    addEncodedMeasurePage(encodedTablePage);
  }

  public int getBlockletSize() {
    return blockletSize;
  }

  public List<BlockletEncodedColumnPage> getEncodedDimensionColumnPages() {
    return encodedDimensionColumnPages;
  }

  public List<BlockletEncodedColumnPage> getEncodedMeasureColumnPages() {
    return encodedMeasureColumnPages;
  }

  public int getNumberOfDimension() {
    return encodedDimensionColumnPages.size();
  }

  public int getNumberOfMeasure() {
    return encodedMeasureColumnPages.size();
  }

  public int getNumberOfPages() {
    return this.numberOfPages;
  }

  public List<Short> getRowCountInPage() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3001
    return rowCountInPage;
  }

  public void setRowCountInPage(List<Short> rowCountInPage) {
    this.rowCountInPage = rowCountInPage;
  }

  public void clear() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3787
    if (null != encodedDimensionColumnPages) {
      for (BlockletEncodedColumnPage blockletEncodedColumnPage : encodedDimensionColumnPages) {
        blockletEncodedColumnPage.cleanBuffer();
      }
    }
    if (null != encodedMeasureColumnPages) {
      for (BlockletEncodedColumnPage blockletEncodedColumnPage : encodedMeasureColumnPages) {
        blockletEncodedColumnPage.cleanBuffer();
      }
    }
    this.numberOfPages = 0;
    this.encodedDimensionColumnPages = null;
    this.blockletSize = 0;
    this.encodedMeasureColumnPages = null;
    this.rowCountInPage = null;
  }
}
