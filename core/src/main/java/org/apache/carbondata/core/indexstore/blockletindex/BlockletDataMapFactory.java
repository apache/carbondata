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
import java.util.Set;

import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.DataMapDistributable;
import org.apache.carbondata.core.datamap.DataMapMeta;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.dev.CacheableDataMap;
import org.apache.carbondata.core.datamap.dev.DataMap;
import org.apache.carbondata.core.datamap.dev.DataMapFactory;
import org.apache.carbondata.core.datamap.dev.DataMapWriter;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.indexstore.Blocklet;
import org.apache.carbondata.core.indexstore.BlockletDataMapIndexWrapper;
import org.apache.carbondata.core.indexstore.BlockletDetailsFetcher;
import org.apache.carbondata.core.indexstore.ExtendedBlocklet;
import org.apache.carbondata.core.indexstore.TableBlockIndexUniqueIdentifier;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.util.BlockletDataMapUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.events.Event;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

/**
 * Table map for blocklet
 */
public class BlockletDataMapFactory implements DataMapFactory, BlockletDetailsFetcher,
    CacheableDataMap {

  private AbsoluteTableIdentifier identifier;

  // segmentId -> list of index file
  private Map<String, Set<TableBlockIndexUniqueIdentifier>> segmentMap = new HashMap<>();

  private Cache<TableBlockIndexUniqueIdentifier, BlockletDataMapIndexWrapper> cache;

  @Override
  public void init(AbsoluteTableIdentifier identifier, String dataMapName) {
    this.identifier = identifier;
    cache = CacheProvider.getInstance()
        .createCache(CacheType.DRIVER_BLOCKLET_DATAMAP);
  }

  @Override
  public DataMapWriter createWriter(Segment segment) {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public List<DataMap> getDataMaps(Segment segment) throws IOException {
    Set<TableBlockIndexUniqueIdentifier> identifiers = getTableBlockIndexUniqueIdentifiers(segment);
    List<TableBlockIndexUniqueIdentifier> tableBlockIndexUniqueIdentifiers =
        new ArrayList<>(identifiers.size());
    List<DataMap> dataMaps = new ArrayList<>();
    tableBlockIndexUniqueIdentifiers.addAll(identifiers);
    List<BlockletDataMapIndexWrapper> dataMapsList = cache.getAll(tableBlockIndexUniqueIdentifiers);
    for (BlockletDataMapIndexWrapper wrapper : dataMapsList) {
      dataMaps.addAll(wrapper.getDataMaps());
    }
    return dataMaps;
  }

  @Override public void cache(TableBlockIndexUniqueIdentifier tableBlockIndexUniqueIdentifier,
      BlockletDataMapIndexWrapper wrapper) throws IOException, MemoryException {
    cache.put(tableBlockIndexUniqueIdentifier, wrapper);
  }

  @Override
  public List<DataMapDistributable> getAllUncachedDistributables(
      List<DataMapDistributable> distributables) throws IOException {
    List<DataMapDistributable> distributablesToBeLoaded = new ArrayList<>(distributables.size());
    for (DataMapDistributable distributable : distributables) {
      Segment segment = distributable.getSegment();
      Set<TableBlockIndexUniqueIdentifier> tableBlockIndexUniqueIdentifiers =
          getTableBlockIndexUniqueIdentifiers(segment);
      // filter out the tableBlockIndexUniqueIdentifiers based on distributable
      TableBlockIndexUniqueIdentifier validIdentifier = BlockletDataMapUtil
          .filterIdentifiersBasedOnDistributable(tableBlockIndexUniqueIdentifiers,
              (BlockletDataMapDistributable) distributable);
      if (null == cache.getIfPresent(validIdentifier)) {
        ((BlockletDataMapDistributable) distributable)
            .setTableBlockIndexUniqueIdentifier(validIdentifier);
        distributablesToBeLoaded.add(distributable);
      }
    }
    return distributablesToBeLoaded;
  }

  private Set<TableBlockIndexUniqueIdentifier> getTableBlockIndexUniqueIdentifiers(
      Segment segment) throws IOException {
    Set<TableBlockIndexUniqueIdentifier> tableBlockIndexUniqueIdentifiers =
        segmentMap.get(segment.getSegmentNo());
    if (tableBlockIndexUniqueIdentifiers == null) {
      tableBlockIndexUniqueIdentifiers =
          BlockletDataMapUtil.getTableBlockUniqueIdentifiers(segment, identifier.getTablePath());
      segmentMap.put(segment.getSegmentNo(), tableBlockIndexUniqueIdentifiers);
    }
    return tableBlockIndexUniqueIdentifiers;
  }

  /**
   * Get the blocklet detail information based on blockletid, blockid and segmentid. This method is
   * exclusively for BlockletDataMapFactory as detail information is only available in this default
   * datamap.
   */
  @Override
  public List<ExtendedBlocklet> getExtendedBlocklets(List<Blocklet> blocklets, Segment segment)
      throws IOException {
    List<ExtendedBlocklet> detailedBlocklets = new ArrayList<>();
    // If it is already detailed blocklet then type cast and return same
    if (blocklets.size() > 0 && blocklets.get(0) instanceof ExtendedBlocklet) {
      for (Blocklet blocklet : blocklets) {
        detailedBlocklets.add((ExtendedBlocklet) blocklet);
      }
      return detailedBlocklets;
    }
    Set<TableBlockIndexUniqueIdentifier> identifiers =
        getTableBlockIndexUniqueIdentifiers(segment);
    // Retrieve each blocklets detail information from blocklet datamap
    for (Blocklet blocklet : blocklets) {
      detailedBlocklets.addAll(getExtendedBlocklet(identifiers, blocklet));
    }
    return detailedBlocklets;
  }

  private List<ExtendedBlocklet> getExtendedBlocklet(
      Set<TableBlockIndexUniqueIdentifier> identifiers, Blocklet blocklet) throws IOException {
    List<ExtendedBlocklet> blocklets = new ArrayList<>();
    String carbonIndexFileName = CarbonTablePath.getCarbonIndexFileName(blocklet.getPath());
    try {
      for (TableBlockIndexUniqueIdentifier identifier : identifiers) {
        if (identifier.getIndexFilePath().equals(carbonIndexFileName)) {
          BlockletDataMapIndexWrapper wrapper = cache.get(identifier);
          List<DataMap> dataMaps = wrapper.getDataMaps();
          for (DataMap dataMap : dataMaps) {
            BlockletDataMap blockletDataMap = (BlockletDataMap) dataMap;
            blocklets
                .add(blockletDataMap.getDetailedBlocklet(blocklet.getBlockletId()));
          }
        }
      }
      return blocklets;
    } catch (Exception e) {
      throw new IOException("Blocklet with blockid " + blocklet.getPath() + " not found ");
    }
  }

  @Override
  public List<DataMapDistributable> toDistributable(Segment segment) {
    List<DataMapDistributable> distributables = new ArrayList<>();
    try {
      Set<TableBlockIndexUniqueIdentifier> tableBlockIndexUniqueIdentifiers =
          getTableBlockIndexUniqueIdentifiers(segment);
      CarbonFile[] carbonIndexFiles = new CarbonFile[tableBlockIndexUniqueIdentifiers.size()];
      int identifierCounter = 0;
      for (TableBlockIndexUniqueIdentifier tableBlockIndexUniqueIdentifier :
          tableBlockIndexUniqueIdentifiers) {
        String indexFilePath = tableBlockIndexUniqueIdentifier.getIndexFilePath();
        String fileName = tableBlockIndexUniqueIdentifier.getIndexFileName();
        carbonIndexFiles[identifierCounter++] = FileFactory
            .getCarbonFile(indexFilePath + CarbonCommonConstants.FILE_SEPARATOR + fileName);
      }
      for (int i = 0; i < carbonIndexFiles.length; i++) {
        Path path = new Path(carbonIndexFiles[i].getPath());
        FileSystem fs = path.getFileSystem(FileFactory.getConfiguration());
        RemoteIterator<LocatedFileStatus> iter = fs.listLocatedStatus(path);
        LocatedFileStatus fileStatus = iter.next();
        String[] location = fileStatus.getBlockLocations()[0].getHosts();
        BlockletDataMapDistributable distributable =
            new BlockletDataMapDistributable(path.toString());
        distributable.setLocations(location);
        distributables.add(distributable);

      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return distributables;
  }

  @Override
  public void fireEvent(Event event) {

  }

  @Override
  public void clear(Segment segment) {
    Set<TableBlockIndexUniqueIdentifier> blockIndexes = segmentMap.remove(segment.getSegmentNo());
    if (blockIndexes != null) {
      for (TableBlockIndexUniqueIdentifier blockIndex : blockIndexes) {
        BlockletDataMapIndexWrapper wrapper = cache.getIfPresent(blockIndex);
        if (wrapper != null) {
          List<DataMap> dataMaps = wrapper.getDataMaps();
          cache.invalidate(blockIndex);
          for (DataMap dataMap : dataMaps) {
            dataMap.clear();
          }
        }
      }
    }
  }

  @Override
  public void clear() {
    for (String segmentId : segmentMap.keySet().toArray(new String[segmentMap.size()])) {
      clear(new Segment(segmentId, null));
    }
  }

  @Override
  public List<DataMap> getDataMaps(DataMapDistributable distributable) throws IOException {
    BlockletDataMapDistributable mapDistributable = (BlockletDataMapDistributable) distributable;
    List<TableBlockIndexUniqueIdentifier> identifiers = new ArrayList<>();
    Path indexPath = new Path(mapDistributable.getFilePath());
    String segmentNo = mapDistributable.getSegment().getSegmentNo();
    if (indexPath.getName().endsWith(CarbonTablePath.INDEX_FILE_EXT)) {
      String parent = indexPath.getParent().toString();
      identifiers
          .add(new TableBlockIndexUniqueIdentifier(parent, indexPath.getName(), null, segmentNo));
    } else if (indexPath.getName().endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT)) {
      CarbonFile carbonFile = FileFactory.getCarbonFile(indexPath.toString());
      String parentPath = carbonFile.getParentFile().getAbsolutePath();
      identifiers.add(new TableBlockIndexUniqueIdentifier(parentPath, carbonFile.getName(),
          carbonFile.getName(), segmentNo));
    }
    List<DataMap> dataMaps = new ArrayList<>();
    try {
      List<BlockletDataMapIndexWrapper> blockletDataMapIndexWrappers = cache.getAll(identifiers);
      for (BlockletDataMapIndexWrapper wrapper : blockletDataMapIndexWrappers) {
        dataMaps.addAll(wrapper.getDataMaps());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return dataMaps;
  }

  @Override
  public DataMapMeta getMeta() {
    // TODO: pass SORT_COLUMNS into this class
    return null;
  }

  public Map<String, Set<TableBlockIndexUniqueIdentifier>> getSegmentMap() {
    return segmentMap;
  }

}
