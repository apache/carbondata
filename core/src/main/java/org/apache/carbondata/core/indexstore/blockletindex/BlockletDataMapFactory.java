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
import org.apache.carbondata.core.datamap.DataMapDistributable;
import org.apache.carbondata.core.datamap.DataMapMeta;
import org.apache.carbondata.core.datamap.dev.DataMap;
import org.apache.carbondata.core.datamap.dev.DataMapFactory;
import org.apache.carbondata.core.datamap.dev.DataMapWriter;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.events.ChangeEvent;
import org.apache.carbondata.core.indexstore.TableBlockIndexUniqueIdentifier;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

/**
 * Table map for blocklet
 */
public class BlockletDataMapFactory implements DataMapFactory {

  private AbsoluteTableIdentifier identifier;

  // segmentId -> list of index file
  private Map<String, List<TableBlockIndexUniqueIdentifier>> segmentMap = new HashMap<>();

  private Cache<TableBlockIndexUniqueIdentifier, DataMap> cache;

  @Override
  public void init(AbsoluteTableIdentifier identifier, String dataMapName) {
    this.identifier = identifier;
    cache = CacheProvider.getInstance()
        .createCache(CacheType.DRIVER_BLOCKLET_DATAMAP, identifier.getStorePath());
  }

  @Override
  public DataMapWriter createWriter(String segmentId) {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public List<DataMap> getDataMaps(String segmentId) throws IOException {
    List<TableBlockIndexUniqueIdentifier> tableBlockIndexUniqueIdentifiers =
        segmentMap.get(segmentId);
    if (tableBlockIndexUniqueIdentifiers == null) {
      tableBlockIndexUniqueIdentifiers = new ArrayList<>();
      CarbonFile[] listFiles = getCarbonIndexFiles(segmentId);
      for (int i = 0; i < listFiles.length; i++) {
        tableBlockIndexUniqueIdentifiers.add(
            new TableBlockIndexUniqueIdentifier(identifier, segmentId, listFiles[i].getName()));
      }
      segmentMap.put(segmentId, tableBlockIndexUniqueIdentifiers);
    }

    return cache.getAll(tableBlockIndexUniqueIdentifiers);
  }

  private CarbonFile[] getCarbonIndexFiles(String segmentId) {
    String path = identifier.getTablePath() + "/Fact/Part0/Segment_" + segmentId;
    CarbonFile carbonFile = FileFactory.getCarbonFile(path);
    return carbonFile.listFiles(new CarbonFileFilter() {
      @Override public boolean accept(CarbonFile file) {
        return file.getName().endsWith(".carbonindex");
      }
    });
  }

  @Override
  public List<DataMapDistributable> toDistributable(String segmentId) {
    CarbonFile[] carbonIndexFiles = getCarbonIndexFiles(segmentId);
    List<DataMapDistributable> distributables = new ArrayList<>();
    for (int i = 0; i < carbonIndexFiles.length; i++) {
      Path path = new Path(carbonIndexFiles[i].getPath());
      try {
        FileSystem fs = path.getFileSystem(FileFactory.getConfiguration());
        RemoteIterator<LocatedFileStatus> iter = fs.listLocatedStatus(path);
        LocatedFileStatus fileStatus = iter.next();
        String[] location = fileStatus.getBlockLocations()[0].getHosts();
        BlockletDataMapDistributable distributable =
            new BlockletDataMapDistributable(path.getName());
        distributable.setLocations(location);
        distributables.add(distributable);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return distributables;
  }

  @Override
  public void clear(String segmentId) {
    List<TableBlockIndexUniqueIdentifier> blockIndexes = segmentMap.remove(segmentId);
    if (blockIndexes != null) {
      for (TableBlockIndexUniqueIdentifier blockIndex : blockIndexes) {
        DataMap dataMap = cache.getIfPresent(blockIndex);
        if (dataMap != null) {
          cache.invalidate(blockIndex);
          dataMap.clear();
        }
      }
    }
  }

  @Override
  public void clear() {
    for (String segmentId: segmentMap.keySet().toArray(new String[segmentMap.size()])) {
      clear(segmentId);
    }
  }

  @Override
  public DataMap getDataMap(DataMapDistributable distributable) {
    BlockletDataMapDistributable mapDistributable = (BlockletDataMapDistributable) distributable;
    TableBlockIndexUniqueIdentifier uniqueIdentifier =
        new TableBlockIndexUniqueIdentifier(identifier, distributable.getSegmentId(),
            mapDistributable.getFilePath());
    DataMap dataMap;
    try {
      dataMap = cache.get(uniqueIdentifier);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return dataMap;
  }

  @Override
  public void fireEvent(ChangeEvent event) {

  }

  @Override
  public DataMapMeta getMeta() {
    // TODO: pass SORT_COLUMNS into this class
    return null;
  }
}
