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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.datamap.DataMapDistributable;
import org.apache.carbondata.core.datamap.DataMapMeta;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.dev.CacheableDataMap;
import org.apache.carbondata.core.datamap.dev.DataMap;
import org.apache.carbondata.core.datamap.dev.DataMapBuilder;
import org.apache.carbondata.core.datamap.dev.DataMapWriter;
import org.apache.carbondata.core.datamap.dev.cgdatamap.CoarseGrainDataMap;
import org.apache.carbondata.core.datamap.dev.cgdatamap.CoarseGrainDataMapFactory;
import org.apache.carbondata.core.datamap.dev.expr.DataMapDistributableWrapper;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.features.TableOperation;
import org.apache.carbondata.core.indexstore.Blocklet;
import org.apache.carbondata.core.indexstore.BlockletDataMapIndexWrapper;
import org.apache.carbondata.core.indexstore.BlockletDetailsFetcher;
import org.apache.carbondata.core.indexstore.ExtendedBlocklet;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.indexstore.SegmentPropertiesFetcher;
import org.apache.carbondata.core.indexstore.TableBlockIndexUniqueIdentifier;
import org.apache.carbondata.core.indexstore.TableBlockIndexUniqueIdentifierWrapper;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.util.BlockletDataMapUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.events.Event;

import org.apache.hadoop.fs.Path;

/**
 * Table map for blocklet
 */
public class BlockletDataMapFactory extends CoarseGrainDataMapFactory
    implements BlockletDetailsFetcher, SegmentPropertiesFetcher, CacheableDataMap {

  private static final String NAME = "clustered.btree.blocklet";
  /**
   * variable for cache level BLOCKLET
   */
  public static final String CACHE_LEVEL_BLOCKLET = "BLOCKLET";

  public static final DataMapSchema DATA_MAP_SCHEMA =
      new DataMapSchema(NAME, BlockletDataMapFactory.class.getName());

  private AbsoluteTableIdentifier identifier;

  // segmentId -> list of index file
  private Map<String, Set<TableBlockIndexUniqueIdentifier>> segmentMap = new ConcurrentHashMap<>();

  private Cache<TableBlockIndexUniqueIdentifierWrapper, BlockletDataMapIndexWrapper> cache;

  public BlockletDataMapFactory(CarbonTable carbonTable, DataMapSchema dataMapSchema) {
    super(carbonTable, dataMapSchema);
    this.identifier = carbonTable.getAbsoluteTableIdentifier();
    cache = CacheProvider.getInstance()
        .createCache(CacheType.DRIVER_BLOCKLET_DATAMAP);
  }

  /**
   * create dataMap based on cache level
   *
   * @param carbonTable
   * @return
   */
  public static DataMap createDataMap(CarbonTable carbonTable) {
    boolean cacheLevelBlock = BlockletDataMapUtil.isCacheLevelBlock(carbonTable);
    if (cacheLevelBlock) {
      // case1: when CACHE_LEVEL = BLOCK
      return new BlockDataMap();
    } else {
      // case2: when CACHE_LEVEL = BLOCKLET
      return new BlockletDataMap();
    }
  }

  @Override
  public DataMapWriter createWriter(Segment segment, String shardName,
      SegmentProperties segmentProperties) throws IOException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public DataMapBuilder createBuilder(Segment segment, String shardName,
      SegmentProperties segmentProperties) throws IOException {
    throw new UnsupportedOperationException("not implemented");
  }

  /**
   * Get the datamap for all segments
   */
  public Map<Segment, List<CoarseGrainDataMap>> getDataMaps(List<Segment> segments)
      throws IOException {
    List<TableBlockIndexUniqueIdentifierWrapper> tableBlockIndexUniqueIdentifierWrappers =
        new ArrayList<>();
    Map<Segment, List<CoarseGrainDataMap>> dataMaps = new HashMap<>();
    Map<String, Segment> segmentMap = new HashMap<>();
    for (Segment segment : segments) {
      segmentMap.put(segment.getSegmentNo(), segment);
      Set<TableBlockIndexUniqueIdentifier> identifiers =
          getTableBlockIndexUniqueIdentifiers(segment);

      for (TableBlockIndexUniqueIdentifier tableBlockIndexUniqueIdentifier : identifiers) {
        tableBlockIndexUniqueIdentifierWrappers.add(
            new TableBlockIndexUniqueIdentifierWrapper(tableBlockIndexUniqueIdentifier,
                this.getCarbonTable()));
      }
    }
    List<BlockletDataMapIndexWrapper> blockletDataMapIndexWrappers =
        cache.getAll(tableBlockIndexUniqueIdentifierWrappers);
    for (BlockletDataMapIndexWrapper wrapper : blockletDataMapIndexWrappers) {
      Segment segment = segmentMap.get(wrapper.getSegmentId());
      List<CoarseGrainDataMap> datamapList = dataMaps.get(segment);
      if (null == datamapList) {
        datamapList = new ArrayList<CoarseGrainDataMap>();
      }
      datamapList.addAll(wrapper.getDataMaps());
      dataMaps.put(segment, datamapList);
    }
    return dataMaps;
  }

  @Override public List<CoarseGrainDataMap> getDataMaps(Segment segment) throws IOException {
    List<CoarseGrainDataMap> dataMaps = new ArrayList<>();
    Set<TableBlockIndexUniqueIdentifier> identifiers =
        getTableBlockIndexUniqueIdentifiers(segment);
    List<TableBlockIndexUniqueIdentifierWrapper> tableBlockIndexUniqueIdentifierWrappers =
        new ArrayList<>(identifiers.size());
    for (TableBlockIndexUniqueIdentifier tableBlockIndexUniqueIdentifier : identifiers) {
      tableBlockIndexUniqueIdentifierWrappers.add(
          new TableBlockIndexUniqueIdentifierWrapper(tableBlockIndexUniqueIdentifier,
              this.getCarbonTable()));
    }
    List<BlockletDataMapIndexWrapper> blockletDataMapIndexWrappers =
        cache.getAll(tableBlockIndexUniqueIdentifierWrappers);
    for (BlockletDataMapIndexWrapper wrapper : blockletDataMapIndexWrappers) {
      dataMaps.addAll(wrapper.getDataMaps());
    }
    return dataMaps;
  }

  public Set<TableBlockIndexUniqueIdentifier> getTableBlockIndexUniqueIdentifiers(Segment segment)
      throws IOException {
    Set<TableBlockIndexUniqueIdentifier> tableBlockIndexUniqueIdentifiers =
        segmentMap.get(segment.getSegmentNo());
    if (tableBlockIndexUniqueIdentifiers == null) {
      tableBlockIndexUniqueIdentifiers =
          BlockletDataMapUtil.getTableBlockUniqueIdentifiers(segment);
      if (tableBlockIndexUniqueIdentifiers.size() > 0) {
        segmentMap.put(segment.getSegmentNo(), tableBlockIndexUniqueIdentifiers);
      }
    }
    return tableBlockIndexUniqueIdentifiers;
  }

  /**
   * Get the blocklet detail information based on blockletid, blockid and segmentId. This method is
   * exclusively for BlockletDataMapFactory as detail information is only available in this
   * default datamap.
   */
  @Override
  public List<ExtendedBlocklet> getExtendedBlocklets(List<Blocklet> blocklets, Segment segment)
      throws IOException {
    List<ExtendedBlocklet> detailedBlocklets = new ArrayList<>(blocklets.size() + 1);
    // If it is already detailed blocklet then type cast and return same
    if (blocklets.size() > 0 && blocklets.get(0) instanceof ExtendedBlocklet) {
      for (Blocklet blocklet : blocklets) {
        detailedBlocklets.add((ExtendedBlocklet) blocklet);
      }
      return detailedBlocklets;
    }
    Set<TableBlockIndexUniqueIdentifier> identifiers =
        getTableBlockIndexUniqueIdentifiers(segment);
    Set<TableBlockIndexUniqueIdentifierWrapper> tableBlockIndexUniqueIdentifierWrappers =
        new HashSet<>(identifiers.size());
    for (TableBlockIndexUniqueIdentifier tableBlockIndexUniqueIdentifier : identifiers) {
      tableBlockIndexUniqueIdentifierWrappers.add(
          new TableBlockIndexUniqueIdentifierWrapper(tableBlockIndexUniqueIdentifier,
              this.getCarbonTable()));
    }
    // Retrieve each blocklets detail information from blocklet datamap
    for (Blocklet blocklet : blocklets) {
      detailedBlocklets.add(getExtendedBlocklet(tableBlockIndexUniqueIdentifierWrappers, blocklet));
    }
    return detailedBlocklets;
  }

  @Override
  public ExtendedBlocklet getExtendedBlocklet(Blocklet blocklet, Segment segment)
      throws IOException {
    if (blocklet instanceof ExtendedBlocklet) {
      return (ExtendedBlocklet) blocklet;
    }
    Set<TableBlockIndexUniqueIdentifier> identifiers =
        getTableBlockIndexUniqueIdentifiers(segment);

    Set<TableBlockIndexUniqueIdentifierWrapper> tableBlockIndexUniqueIdentifierWrappers =
        new HashSet<>(identifiers.size());
    for (TableBlockIndexUniqueIdentifier tableBlockIndexUniqueIdentifier : identifiers) {
      tableBlockIndexUniqueIdentifierWrappers.add(
          new TableBlockIndexUniqueIdentifierWrapper(tableBlockIndexUniqueIdentifier,
              this.getCarbonTable()));
    }
    return getExtendedBlocklet(tableBlockIndexUniqueIdentifierWrappers, blocklet);
  }

  private ExtendedBlocklet getExtendedBlocklet(
      Set<TableBlockIndexUniqueIdentifierWrapper> identifiersWrapper, Blocklet blocklet)
      throws IOException {
    for (TableBlockIndexUniqueIdentifierWrapper identifierWrapper : identifiersWrapper) {
      BlockletDataMapIndexWrapper wrapper = cache.get(identifierWrapper);
      List<BlockDataMap> dataMaps = wrapper.getDataMaps();
      for (DataMap dataMap : dataMaps) {
        if (((BlockDataMap) dataMap)
            .getTableTaskInfo(BlockletDataMapRowIndexes.SUMMARY_INDEX_FILE_NAME)
            .startsWith(blocklet.getFilePath())) {
          return ((BlockDataMap) dataMap).getDetailedBlocklet(blocklet.getBlockletId());
        }
      }
    }
    throw new IOException("Blocklet not found: " + blocklet.toString());
  }


  @Override
  public List<DataMapDistributable> toDistributable(Segment segment) {
    List<DataMapDistributable> distributables = new ArrayList<>();
    try {
      BlockletDataMapDistributable distributable = new BlockletDataMapDistributable();
      distributable.setSegment(segment);
      distributable.setDataMapSchema(DATA_MAP_SCHEMA);
      distributable.setSegmentPath(CarbonTablePath.getSegmentPath(identifier.getTablePath(),
          segment.getSegmentNo()));
      distributables.add(new DataMapDistributableWrapper(UUID.randomUUID().toString(),
          distributable).getDistributable());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return distributables;
  }

  @Override
  public void fireEvent(Event event) {

  }

  @Override
  public void clear(String segment) {
    Set<TableBlockIndexUniqueIdentifier> blockIndexes = segmentMap.remove(segment);
    if (blockIndexes != null) {
      for (TableBlockIndexUniqueIdentifier blockIndex : blockIndexes) {
        TableBlockIndexUniqueIdentifierWrapper blockIndexWrapper =
            new TableBlockIndexUniqueIdentifierWrapper(blockIndex, this.getCarbonTable());
        BlockletDataMapIndexWrapper wrapper = cache.getIfPresent(blockIndexWrapper);
        if (null != wrapper) {
          List<BlockDataMap> dataMaps = wrapper.getDataMaps();
          for (DataMap dataMap : dataMaps) {
            if (dataMap != null) {
              cache.invalidate(blockIndexWrapper);
              dataMap.clear();
            }
          }
        }
      }
    }
  }

  @Override
  public synchronized void clear() {
    if (segmentMap.size() > 0) {
      for (String segmentId : segmentMap.keySet().toArray(new String[segmentMap.size()])) {
        clear(segmentId);
      }
    }
  }

  @Override
  public String getCacheSize() {
    long sum = 0L;
    int numOfIndexFiles = 0;
    for (Map.Entry<String, Set<TableBlockIndexUniqueIdentifier>> entry : segmentMap.entrySet()) {
      for (TableBlockIndexUniqueIdentifier tableBlockIndexUniqueIdentifier : entry.getValue()) {
        BlockletDataMapIndexWrapper blockletDataMapIndexWrapper = cache.getIfPresent(
            new TableBlockIndexUniqueIdentifierWrapper(tableBlockIndexUniqueIdentifier,
                getCarbonTable()));
        if (blockletDataMapIndexWrapper != null) {
          sum += blockletDataMapIndexWrapper.getMemorySize();
          numOfIndexFiles++;
        }
      }
    }
    return numOfIndexFiles + ":" + sum;
  }

  @Override public List<CoarseGrainDataMap> getDataMaps(DataMapDistributable distributable)
      throws IOException {
    BlockletDataMapDistributable mapDistributable = (BlockletDataMapDistributable) distributable;
    List<TableBlockIndexUniqueIdentifierWrapper> identifiersWrapper;
    String segmentNo = mapDistributable.getSegment().getSegmentNo();
    if (mapDistributable.getSegmentPath() != null) {
      identifiersWrapper = getTableBlockIndexUniqueIdentifier(distributable);
    } else {
      identifiersWrapper =
          getTableBlockIndexUniqueIdentifier(mapDistributable.getFilePath(), segmentNo);
    }
    List<CoarseGrainDataMap> dataMaps = new ArrayList<>();
    try {
      List<BlockletDataMapIndexWrapper> wrappers = cache.getAll(identifiersWrapper);
      for (BlockletDataMapIndexWrapper wrapper : wrappers) {
        dataMaps.addAll(wrapper.getDataMaps());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return dataMaps;
  }

  private List<TableBlockIndexUniqueIdentifierWrapper> getTableBlockIndexUniqueIdentifier(
      DataMapDistributable distributable) throws IOException {
    List<TableBlockIndexUniqueIdentifierWrapper> identifiersWrapper = new ArrayList<>();
    Set<TableBlockIndexUniqueIdentifier> tableBlockIndexUniqueIdentifiers =
        segmentMap.get(distributable.getSegment().getSegmentNo());
    if (tableBlockIndexUniqueIdentifiers == null) {
      tableBlockIndexUniqueIdentifiers = new HashSet<>();
      Set<String> indexFiles = distributable.getSegment().getCommittedIndexFile().keySet();
      for (String indexFile : indexFiles) {
        CarbonFile carbonFile = FileFactory.getCarbonFile(indexFile);
        String indexFileName;
        String mergeIndexName;
        if (indexFile.endsWith(CarbonTablePath.INDEX_FILE_EXT)) {
          indexFileName = carbonFile.getName();
          mergeIndexName = null;
        } else {
          indexFileName = carbonFile.getName();
          mergeIndexName = carbonFile.getName();
        }
        String parentPath = carbonFile.getParentFile().getAbsolutePath();
        TableBlockIndexUniqueIdentifier tableBlockIndexUniqueIdentifier =
            new TableBlockIndexUniqueIdentifier(parentPath, indexFileName, mergeIndexName,
                distributable.getSegment().getSegmentNo());
        identifiersWrapper.add(
            new TableBlockIndexUniqueIdentifierWrapper(tableBlockIndexUniqueIdentifier,
                this.getCarbonTable()));
        tableBlockIndexUniqueIdentifiers.add(tableBlockIndexUniqueIdentifier);
      }
      segmentMap.put(distributable.getSegment().getSegmentNo(), tableBlockIndexUniqueIdentifiers);
    } else {
      for (TableBlockIndexUniqueIdentifier tableBlockIndexUniqueIdentifier :
          tableBlockIndexUniqueIdentifiers) {
        identifiersWrapper.add(
            new TableBlockIndexUniqueIdentifierWrapper(tableBlockIndexUniqueIdentifier,
                getCarbonTable()));
      }
    }
    return identifiersWrapper;
  }

  private List<TableBlockIndexUniqueIdentifierWrapper> getTableBlockIndexUniqueIdentifier(
      String indexFilePath, String segmentId) throws IOException {
    Path indexPath = new Path(indexFilePath);
    List<TableBlockIndexUniqueIdentifierWrapper> identifiersWrapper = new ArrayList<>();
    if (indexPath.getName().endsWith(CarbonTablePath.INDEX_FILE_EXT)) {
      String parent = indexPath.getParent().toString();
      identifiersWrapper.add(new TableBlockIndexUniqueIdentifierWrapper(
          new TableBlockIndexUniqueIdentifier(parent, indexPath.getName(), null, segmentId),
          this.getCarbonTable()));
    } else if (indexPath.getName().endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT)) {
      SegmentIndexFileStore fileStore = new SegmentIndexFileStore();
      CarbonFile carbonFile = FileFactory.getCarbonFile(indexPath.toString());
      String parentPath = carbonFile.getParentFile().getAbsolutePath();
      List<String> indexFiles = fileStore.getIndexFilesFromMergeFile(carbonFile.getAbsolutePath());
      for (String indexFile : indexFiles) {
        identifiersWrapper.add(new TableBlockIndexUniqueIdentifierWrapper(
            new TableBlockIndexUniqueIdentifier(parentPath, indexFile, carbonFile.getName(),
                segmentId), this.getCarbonTable()));
      }
    }
    return identifiersWrapper;
  }

  @Override
  public DataMapMeta getMeta() {
    // TODO: pass SORT_COLUMNS into this class
    return null;
  }

  @Override
  public void deleteDatamapData(Segment segment) throws IOException {

  }

  @Override public void deleteDatamapData() {

  }

  @Override public SegmentProperties getSegmentProperties(Segment segment) throws IOException {
    List<CoarseGrainDataMap> dataMaps = getDataMaps(segment);
    assert (dataMaps.size() > 0);
    CoarseGrainDataMap coarseGrainDataMap = dataMaps.get(0);
    assert (coarseGrainDataMap instanceof BlockDataMap);
    BlockDataMap dataMap = (BlockDataMap) coarseGrainDataMap;
    return dataMap.getSegmentProperties();
  }

  @Override public SegmentProperties getSegmentPropertiesFromDataMap(DataMap coarseGrainDataMap)
      throws IOException {
    assert (coarseGrainDataMap instanceof BlockDataMap);
    BlockDataMap dataMap = (BlockDataMap) coarseGrainDataMap;
    return dataMap.getSegmentProperties();
  }

  @Override public List<Blocklet> getAllBlocklets(Segment segment, List<PartitionSpec> partitions)
      throws IOException {
    List<Blocklet> blocklets = new ArrayList<>();
    List<CoarseGrainDataMap> dataMaps = getDataMaps(segment);
    for (CoarseGrainDataMap dataMap : dataMaps) {
      blocklets.addAll(
          dataMap.prune((FilterResolverIntf) null, getSegmentProperties(segment), partitions));
    }
    return blocklets;
  }

  @Override public boolean willBecomeStale(TableOperation operation) {
    return false;
  }

  @Override
  public void cache(TableBlockIndexUniqueIdentifierWrapper tableBlockIndexUniqueIdentifierWrapper,
      BlockletDataMapIndexWrapper blockletDataMapIndexWrapper) throws IOException, MemoryException {
    cache.put(tableBlockIndexUniqueIdentifierWrapper, blockletDataMapIndexWrapper);
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
      if (null == cache.getIfPresent(
          new TableBlockIndexUniqueIdentifierWrapper(validIdentifier, this.getCarbonTable()))) {
        ((BlockletDataMapDistributable) distributable)
            .setTableBlockIndexUniqueIdentifier(validIdentifier);
        distributablesToBeLoaded.add(distributable);
      }
    }
    return distributablesToBeLoaded;
  }
}
