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

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datamap.DataMapMeta;
import org.apache.carbondata.core.datamap.DataMapStoreManager;
import org.apache.carbondata.core.datamap.TableDataMap;
import org.apache.carbondata.core.datamap.dev.AbstractDataMapWriter;
import org.apache.carbondata.core.datamap.dev.IndexDataMapFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.processing.store.TablePage;

/**
 * It is for writing IndexDataMap for one table
 */
public class DataMapWriterListener {

  private static final LogService LOG = LogServiceFactory.getLogService(
      DataMapWriterListener.class.getCanonicalName());

  // list indexed column name -> list of data map writer
  private Map<List<String>, List<AbstractDataMapWriter>> registry = new ConcurrentHashMap<>();

  /**
   * register all datamap writer for specified table and segment
   */
  public void registerAllWriter(CarbonTable carbonTable, String segmentId,
      String dataWritePath) {
    List<TableDataMap> tableDataMaps = DataMapStoreManager.getInstance().getAllDataMap(carbonTable);
    if (tableDataMaps != null) {
      for (TableDataMap tableDataMap : tableDataMaps) {
        IndexDataMapFactory factory = tableDataMap.getIndexDataMapFactory();
        register(factory, segmentId, dataWritePath);
      }
    }
  }

  /**
   * Register a AbstractDataMapWriter
   */
  private void register(IndexDataMapFactory factory, String segmentId, String dataWritePath) {
    assert (factory != null);
    assert (segmentId != null);
    DataMapMeta meta = factory.getMeta();
    if (meta == null) {
      // if data map does not have meta, no need to register
      return;
    }
    List<String> columns = factory.getMeta().getIndexedColumns();
    List<AbstractDataMapWriter> writers = registry.get(columns);
    AbstractDataMapWriter writer = factory.createWriter(segmentId, dataWritePath);
    if (writers != null) {
      writers.add(writer);
    } else {
      writers = new ArrayList<>();
      writers.add(writer);
      registry.put(columns, writers);
    }
    LOG.info("AbstractDataMapWriter " + writer + " added");
  }

  public void onBlockStart(String blockId, String blockPath) {
    for (List<AbstractDataMapWriter> writers : registry.values()) {
      for (AbstractDataMapWriter writer : writers) {
        writer.onBlockStart(blockId);
      }
    }
  }

  public void onBlockEnd(String blockId) {
    for (List<AbstractDataMapWriter> writers : registry.values()) {
      for (AbstractDataMapWriter writer : writers) {
        writer.onBlockEnd(blockId);
      }
    }
  }

  public void onBlockletStart(int blockletId) {
    for (List<AbstractDataMapWriter> writers : registry.values()) {
      for (AbstractDataMapWriter writer : writers) {
        writer.onBlockletStart(blockletId);
      }
    }
  }

  public void onBlockletEnd(int blockletId) {
    for (List<AbstractDataMapWriter> writers : registry.values()) {
      for (AbstractDataMapWriter writer : writers) {
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
  public void onPageAdded(int blockletId, int pageId, TablePage tablePage) {
    Set<Map.Entry<List<String>, List<AbstractDataMapWriter>>> entries = registry.entrySet();
    for (Map.Entry<List<String>, List<AbstractDataMapWriter>> entry : entries) {
      List<String> indexedColumns = entry.getKey();
      ColumnPage[] pages = new ColumnPage[indexedColumns.size()];
      for (int i = 0; i < indexedColumns.size(); i++) {
        pages[i] = tablePage.getColumnPage(indexedColumns.get(i));
      }
      List<AbstractDataMapWriter> writers = entry.getValue();
      for (AbstractDataMapWriter writer : writers) {
        writer.onPageAdded(blockletId, pageId, pages);
      }
    }
  }

  /**
   * Finish all datamap writers
   */
  public void finish() throws IOException {
    for (List<AbstractDataMapWriter> writers : registry.values()) {
      for (AbstractDataMapWriter writer : writers) {
        writer.finish();
      }
    }
  }

}
