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
package org.apache.carbondata.core.indexstore.blockletindex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.events.ChangeEvent;
import org.apache.carbondata.core.indexstore.AbstractTableDataMap;
import org.apache.carbondata.core.indexstore.DataMap;
import org.apache.carbondata.core.indexstore.DataMapDistributable;
import org.apache.carbondata.core.indexstore.DataMapWriter;
import org.apache.carbondata.core.indexstore.TableBlockIndexUniqueIdentifier;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;

/**
 * Table map for blocklet
 */
public class BlockletTableMap extends AbstractTableDataMap {

  private String dataMapName;

  private AbsoluteTableIdentifier identifier;

  private Map<String, List<TableBlockIndexUniqueIdentifier>> segmentMap = new HashMap<>();

  private Cache<TableBlockIndexUniqueIdentifier, DataMap> cache;

  @Override public void init(AbsoluteTableIdentifier identifier, String dataMapName) {
    this.identifier = identifier;
    this.dataMapName = dataMapName;
    cache = CacheProvider.getInstance()
        .createCache(CacheType.DRIVER_BLOCKLET_DATAMAP, identifier.getStorePath());
  }

  @Override public DataMapWriter getMetaDataWriter() {
    return null;
  }

  @Override
  public DataMapWriter getDataMapWriter(AbsoluteTableIdentifier identifier, String segmentId) {
    return null;
  }

  @Override protected List<DataMap> getDataMaps(String segmentId) {
    List<TableBlockIndexUniqueIdentifier> tableBlockIndexUniqueIdentifiers =
        segmentMap.get(segmentId);
    if (tableBlockIndexUniqueIdentifiers == null) {
      tableBlockIndexUniqueIdentifiers = new ArrayList<>();
      String path = identifier.getTablePath() + "/Fact/Part0/Segment_" + segmentId;
      FileFactory.FileType fileType = FileFactory.getFileType(path);
      CarbonFile carbonFile = FileFactory.getCarbonFile(path, fileType);
      CarbonFile[] listFiles = carbonFile.listFiles(new CarbonFileFilter() {
        @Override public boolean accept(CarbonFile file) {
          return file.getName().endsWith(".carbonindex");
        }
      });
      for (int i = 0; i < listFiles.length; i++) {
        tableBlockIndexUniqueIdentifiers.add(
            new TableBlockIndexUniqueIdentifier(identifier, segmentId, listFiles[i].getName()));
      }
    }

    try {
      return cache.getAll(tableBlockIndexUniqueIdentifiers);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override public List<DataMapDistributable> toDistributable(List<String> segmentIds) {
    return null;
  }

  @Override protected DataMap getDataMap(DataMapDistributable distributable) {
    return null;
  }

  @Override public boolean isFiltersSupported(FilterResolverIntf filterExp) {
    return true;
  }

  @Override public void clear(List<String> segmentIds) {
    for (String segmentId : segmentIds) {
      List<TableBlockIndexUniqueIdentifier> blockIndexes = segmentMap.remove(segmentId);
      if (blockIndexes != null) {
        for (TableBlockIndexUniqueIdentifier blockIndex : blockIndexes) {
          DataMap dataMap = cache.getIfPresent(blockIndex);
          dataMap.clear();
          cache.invalidate(blockIndex);
        }
      }
    }
  }

  @Override public void fireEvent(ChangeEvent event) {

  }
}
