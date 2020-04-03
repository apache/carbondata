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

package org.apache.carbondata.processing.datamap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.index.IndexMeta;
import org.apache.carbondata.core.index.IndexStoreManager;
import org.apache.carbondata.core.index.Segment;
import org.apache.carbondata.core.index.TableIndex;
import org.apache.carbondata.core.index.dev.IndexFactory;
import org.apache.carbondata.core.index.dev.IndexWriter;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.processing.store.TablePage;

import org.apache.log4j.Logger;

/**
 * It is for writing Index for one table
 */
public class IndexWriterListener {

  private static final Logger LOG = LogServiceFactory.getLogService(
      IndexWriterListener.class.getCanonicalName());

  // list indexed column -> list of data map writer
  private Map<List<CarbonColumn>, List<IndexWriter>> registry = new ConcurrentHashMap<>();
  // table for this listener
  private CarbonTableIdentifier tblIdentifier;

  public CarbonTableIdentifier getTblIdentifier() {
    return tblIdentifier;
  }

  /**
   * register all datamap writer for specified table and segment
   */
  public void registerAllWriter(CarbonTable carbonTable, String segmentId,
      String taskNo, SegmentProperties segmentProperties) {
    // clear cache in executor side
    IndexStoreManager.getInstance().clearIndex(carbonTable.getTableId());
    List<TableIndex> tableIndices;
    try {
      tableIndices = IndexStoreManager.getInstance().getAllIndexes(carbonTable);
    } catch (IOException e) {
      LOG.error("Error while retrieving datamaps", e);
      throw new RuntimeException(e);
    }
    tblIdentifier = carbonTable.getCarbonTableIdentifier();
    for (TableIndex tableIndex : tableIndices) {
      // register it only if it is not lazy datamap, for lazy datamap, user
      // will rebuild the datamap manually
      if (!tableIndex.getIndexSchema().isLazy()) {
        IndexFactory factory = tableIndex.getIndexFactory();
        register(factory, segmentId, taskNo, segmentProperties);
      }
    }
  }

  /**
   * Register a IndexWriter
   */
  private void register(IndexFactory factory, String segmentId,
      String taskNo, SegmentProperties segmentProperties) {
    assert (factory != null);
    assert (segmentId != null);
    IndexMeta meta = factory.getMeta();
    if (meta == null) {
      // if data map does not have meta, no need to register
      return;
    }
    List<CarbonColumn> columns = factory.getMeta().getIndexedColumns();
    List<IndexWriter> writers = registry.get(columns);
    IndexWriter writer = null;
    try {
      writer = factory.createWriter(new Segment(segmentId), taskNo, segmentProperties);
    } catch (IOException e) {
      LOG.error("Failed to create IndexWriter: " + e.getMessage(), e);
      throw new IndexWriterException(e);
    }
    if (writers != null) {
      writers.add(writer);
    } else {
      writers = new ArrayList<>();
      writers.add(writer);
      registry.put(columns, writers);
    }
    LOG.info("IndexWriter " + writer + " added");
  }

  public void onBlockStart(String blockId) throws IOException {
    for (List<IndexWriter> writers : registry.values()) {
      for (IndexWriter writer : writers) {
        writer.onBlockStart(blockId);
      }
    }
  }

  public void onBlockEnd(String blockId) throws IOException {
    for (List<IndexWriter> writers : registry.values()) {
      for (IndexWriter writer : writers) {
        writer.onBlockEnd(blockId);
      }
    }
  }

  public void onBlockletStart(int blockletId) throws IOException {
    for (List<IndexWriter> writers : registry.values()) {
      for (IndexWriter writer : writers) {
        writer.onBlockletStart(blockletId);
      }
    }
  }

  public void onBlockletEnd(int blockletId) throws IOException {
    for (List<IndexWriter> writers : registry.values()) {
      for (IndexWriter writer : writers) {
        writer.onBlockletEnd(blockletId);
      }
    }
  }

  /**
   * Pick corresponding column pages and add to all registered datamap
   *
   * @param pageId     sequence number of page, start from 0
   * @param tablePage  page data
   */
  public void onPageAdded(int blockletId, int pageId, TablePage tablePage) throws IOException {
    Set<Map.Entry<List<CarbonColumn>, List<IndexWriter>>> entries = registry.entrySet();
    for (Map.Entry<List<CarbonColumn>, List<IndexWriter>> entry : entries) {
      List<CarbonColumn> indexedColumns = entry.getKey();
      ColumnPage[] pages = new ColumnPage[indexedColumns.size()];
      for (int i = 0; i < indexedColumns.size(); i++) {
        pages[i] = tablePage.getColumnPage(indexedColumns.get(i).getColName());
      }
      List<IndexWriter> writers = entry.getValue();
      int pageSize = pages[0].getPageSize();

      for (IndexWriter writer : writers) {
        writer.onPageAdded(blockletId, pageId, pageSize, pages);
      }
    }
  }

  /**
   * Finish all datamap writers
   */
  public void finish() throws IOException {
    for (List<IndexWriter> writers : registry.values()) {
      for (IndexWriter writer : writers) {
        writer.finish();
      }
    }
    registry.clear();
  }

}
