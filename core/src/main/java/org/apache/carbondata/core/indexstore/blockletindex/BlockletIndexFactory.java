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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.datamap.IndexInputSplit;
import org.apache.carbondata.core.datamap.IndexMeta;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.dev.CacheableIndex;
import org.apache.carbondata.core.datamap.dev.Index;
import org.apache.carbondata.core.datamap.dev.IndexBuilder;
import org.apache.carbondata.core.datamap.dev.IndexWriter;
import org.apache.carbondata.core.datamap.dev.cgdatamap.CoarseGrainIndex;
import org.apache.carbondata.core.datamap.dev.cgdatamap.CoarseGrainIndexFactory;
import org.apache.carbondata.core.datamap.dev.expr.IndexExprWrapper;
import org.apache.carbondata.core.datamap.dev.expr.IndexInputSplitWrapper;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.features.TableOperation;
import org.apache.carbondata.core.indexstore.Blocklet;
import org.apache.carbondata.core.indexstore.BlockletDetailsFetcher;
import org.apache.carbondata.core.indexstore.BlockletIndexWrapper;
import org.apache.carbondata.core.indexstore.ExtendedBlocklet;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.indexstore.SegmentPropertiesFetcher;
import org.apache.carbondata.core.indexstore.TableBlockIndexUniqueIdentifier;
import org.apache.carbondata.core.indexstore.TableBlockIndexUniqueIdentifierWrapper;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.util.BlockletIndexUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.events.Event;

/**
 * Index for blocklet
 */
public class BlockletIndexFactory extends CoarseGrainIndexFactory
    implements BlockletDetailsFetcher, SegmentPropertiesFetcher, CacheableIndex {

  private static final String NAME = "clustered.btree.blocklet";
  /**
   * variable for cache level BLOCKLET
   */
  public static final String CACHE_LEVEL_BLOCKLET = "BLOCKLET";

  public static final DataMapSchema DATA_MAP_SCHEMA =
      new DataMapSchema(NAME, BlockletIndexFactory.class.getName());

  private AbsoluteTableIdentifier identifier;

  // segmentId -> list of index file
  private Map<String, Set<TableBlockIndexUniqueIdentifier>> segmentMap = new ConcurrentHashMap<>();

  private Cache<TableBlockIndexUniqueIdentifierWrapper, BlockletIndexWrapper> cache;

  public BlockletIndexFactory(CarbonTable carbonTable, DataMapSchema dataMapSchema) {
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
  public static Index createDataMap(CarbonTable carbonTable) {
    boolean cacheLevelBlock = BlockletIndexUtil.isCacheLevelBlock(carbonTable);
    if (cacheLevelBlock) {
      // case1: when CACHE_LEVEL = BLOCK
      return new BlockIndex();
    } else {
      // case2: when CACHE_LEVEL = BLOCKLET
      return new BlockletIndex();
    }
  }

  @Override
  public IndexWriter createWriter(Segment segment, String shardName,
      SegmentProperties segmentProperties) {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public IndexBuilder createBuilder(Segment segment, String shardName,
      SegmentProperties segmentProperties) {
    throw new UnsupportedOperationException("not implemented");
  }

  /**
   * Get the datamap for all segments
   */
  public Map<Segment, List<CoarseGrainIndex>> getIndexes(List<Segment> segments)
      throws IOException {
    return getIndexes(segments, null);
  }

  /**
   * Get the datamap for all segments
   */
  public Map<Segment, List<CoarseGrainIndex>> getIndexes(List<Segment> segments,
      List<PartitionSpec> partitionsToPrune) throws IOException {
    List<TableBlockIndexUniqueIdentifierWrapper> tableBlockIndexUniqueIdentifierWrappers =
        new ArrayList<>();
    Map<Segment, List<CoarseGrainIndex>> dataMaps = new HashMap<>();
    Map<String, Segment> segmentMap = new HashMap<>();
    for (Segment segment : segments) {
      segmentMap.put(segment.getSegmentNo(), segment);
      Set<TableBlockIndexUniqueIdentifier> identifiers =
          getTableBlockIndexUniqueIdentifiers(segment);
      // get tableBlockIndexUniqueIdentifierWrappers from segment file info
      getTableBlockUniqueIdentifierWrappers(partitionsToPrune,
          tableBlockIndexUniqueIdentifierWrappers, identifiers);
    }
    List<BlockletIndexWrapper> blockletIndexWrappers =
        cache.getAll(tableBlockIndexUniqueIdentifierWrappers);
    for (BlockletIndexWrapper wrapper : blockletIndexWrappers) {
      Segment segment = segmentMap.get(wrapper.getSegmentId());
      List<CoarseGrainIndex> datamapList = dataMaps.get(segment);
      if (null == datamapList) {
        datamapList = new ArrayList<CoarseGrainIndex>();
      }
      datamapList.addAll(wrapper.getDataMaps());
      dataMaps.put(segment, datamapList);
    }
    return dataMaps;
  }

  /**
   * get tableBlockUniqueIdentifierWrappers from segment info. If partitionsToPrune is defined,
   * then get tableBlockUniqueIdentifierWrappers for the matched partitions.
   */
  private void getTableBlockUniqueIdentifierWrappers(List<PartitionSpec> partitionsToPrune,
      List<TableBlockIndexUniqueIdentifierWrapper> tableBlockIndexUniqueIdentifierWrappers,
      Set<TableBlockIndexUniqueIdentifier> identifiers) {
    for (TableBlockIndexUniqueIdentifier tableBlockIndexUniqueIdentifier : identifiers) {
      if (null != partitionsToPrune) {
        // add only tableBlockUniqueIdentifier that matches the partition
        // get the indexFile Parent path and compare with the PartitionPath, if matches, then add
        // the corresponding tableBlockIndexUniqueIdentifier for pruning
        for (PartitionSpec partitionSpec : partitionsToPrune) {
          if (partitionSpec.getLocation().toString()
              .equalsIgnoreCase(tableBlockIndexUniqueIdentifier.getIndexFilePath())) {
            tableBlockIndexUniqueIdentifierWrappers.add(
                new TableBlockIndexUniqueIdentifierWrapper(tableBlockIndexUniqueIdentifier,
                    this.getCarbonTable()));
          }
        }
      } else {
        tableBlockIndexUniqueIdentifierWrappers.add(
            new TableBlockIndexUniqueIdentifierWrapper(tableBlockIndexUniqueIdentifier,
                this.getCarbonTable()));
      }
    }
  }

  @Override
  public List<CoarseGrainIndex> getIndexes(Segment segment) throws IOException {
    return getIndexes(segment, null);
  }

  @Override
  public List<CoarseGrainIndex> getIndexes(Segment segment,
      List<PartitionSpec> partitionsToPrune) throws IOException {
    List<CoarseGrainIndex> dataMaps = new ArrayList<>();
    Set<TableBlockIndexUniqueIdentifier> identifiers =
        getTableBlockIndexUniqueIdentifiers(segment);
    List<TableBlockIndexUniqueIdentifierWrapper> tableBlockIndexUniqueIdentifierWrappers =
        new ArrayList<>(identifiers.size());
    getTableBlockUniqueIdentifierWrappers(partitionsToPrune,
        tableBlockIndexUniqueIdentifierWrappers, identifiers);
    List<BlockletIndexWrapper> blockletIndexWrappers =
        cache.getAll(tableBlockIndexUniqueIdentifierWrappers);
    for (BlockletIndexWrapper wrapper : blockletIndexWrappers) {
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
          BlockletIndexUtil.getTableBlockUniqueIdentifiers(segment);
      if (tableBlockIndexUniqueIdentifiers.size() > 0) {
        segmentMap.put(segment.getSegmentNo(), tableBlockIndexUniqueIdentifiers);
      }
    }
    return tableBlockIndexUniqueIdentifiers;
  }

  /**
   * Get the blocklet detail information based on blockletid, blockid and segmentId. This method is
   * exclusively for BlockletIndexFactory as detail information is only available in this
   * default datamap.
   */
  @Override
  public List<ExtendedBlocklet> getExtendedBlocklets(List<Blocklet> blocklets, Segment segment)
      throws IOException {
    List<ExtendedBlocklet> detailedBlocklets = new ArrayList<>(blocklets.size() + 1);
    // if the blocklets is empty, return the empty detailed blocklets list directly.
    if (blocklets.size() == 0) {
      return detailedBlocklets;
    }
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
      BlockletIndexWrapper wrapper = cache.get(identifierWrapper);
      List<BlockIndex> dataMaps = wrapper.getDataMaps();
      for (Index index : dataMaps) {
        if (((BlockIndex) index)
            .getTableTaskInfo(BlockletIndexRowIndexes.SUMMARY_INDEX_FILE_NAME)
            .startsWith(blocklet.getFilePath())) {
          return ((BlockIndex) index).getDetailedBlocklet(blocklet.getBlockletId());
        }
      }
    }
    throw new IOException("Blocklet not found: " + blocklet.toString());
  }

  @Override
  public List<IndexInputSplit> toDistributable(Segment segment) {
    List<IndexInputSplit> distributables = new ArrayList<>();
    try {
      BlockletIndexInputSplit distributable = new BlockletIndexInputSplit();
      distributable.setSegment(segment);
      distributable.setDataMapSchema(DATA_MAP_SCHEMA);
      distributable.setSegmentPath(CarbonTablePath.getSegmentPath(identifier.getTablePath(),
          segment.getSegmentNo()));
      distributables.add(new IndexInputSplitWrapper(UUID.randomUUID().toString(),
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
        BlockletIndexWrapper wrapper = cache.getIfPresent(blockIndexWrapper);
        if (null != wrapper) {
          List<BlockIndex> dataMaps = wrapper.getDataMaps();
          for (Index index : dataMaps) {
            if (index != null) {
              cache.invalidate(blockIndexWrapper);
              index.clear();
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
        BlockletIndexWrapper blockletIndexWrapper = cache.getIfPresent(
            new TableBlockIndexUniqueIdentifierWrapper(tableBlockIndexUniqueIdentifier,
                getCarbonTable()));
        if (blockletIndexWrapper != null) {
          sum += blockletIndexWrapper.getMemorySize();
          numOfIndexFiles++;
        }
      }
    }
    return numOfIndexFiles + ":" + sum;
  }

  @Override
  public List<CoarseGrainIndex> getIndexes(IndexInputSplit distributable)
      throws IOException {
    BlockletIndexInputSplit mapDistributable = (BlockletIndexInputSplit) distributable;
    List<TableBlockIndexUniqueIdentifierWrapper> identifiersWrapper;
    String segmentNo = mapDistributable.getSegment().getSegmentNo();
    if (mapDistributable.getSegmentPath() != null) {
      identifiersWrapper = getTableBlockIndexUniqueIdentifier(distributable);
    } else {
      identifiersWrapper =
          getTableBlockIndexUniqueIdentifier(mapDistributable.getFilePath(), segmentNo);
    }
    List<CoarseGrainIndex> dataMaps = new ArrayList<>();
    try {
      List<BlockletIndexWrapper> wrappers = cache.getAll(identifiersWrapper);
      for (BlockletIndexWrapper wrapper : wrappers) {
        dataMaps.addAll(wrapper.getDataMaps());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return dataMaps;
  }

  private List<TableBlockIndexUniqueIdentifierWrapper> getTableBlockIndexUniqueIdentifier(
      IndexInputSplit distributable) throws IOException {
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
    List<TableBlockIndexUniqueIdentifierWrapper> identifiersWrapper = new ArrayList<>();
    String parent = indexFilePath.substring(0, indexFilePath.lastIndexOf("/"));
    String name =
        indexFilePath.substring(indexFilePath.lastIndexOf("/") + 1, indexFilePath.length());
    if (indexFilePath.endsWith(CarbonTablePath.INDEX_FILE_EXT)) {
      identifiersWrapper.add(new TableBlockIndexUniqueIdentifierWrapper(
          new TableBlockIndexUniqueIdentifier(parent, name, null, segmentId),
          this.getCarbonTable()));
    } else if (indexFilePath.endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT)) {
      SegmentIndexFileStore fileStore = new SegmentIndexFileStore();
      List<String> indexFiles = fileStore.getIndexFilesFromMergeFile(indexFilePath);
      for (String indexFile : indexFiles) {
        identifiersWrapper.add(new TableBlockIndexUniqueIdentifierWrapper(
            new TableBlockIndexUniqueIdentifier(parent, indexFile, name,
                segmentId), this.getCarbonTable()));
      }
    }
    return identifiersWrapper;
  }

  @Override
  public IndexMeta getMeta() {
    // TODO: pass SORT_COLUMNS into this class
    return null;
  }

  @Override
  public void deleteIndexData(Segment segment) {

  }

  @Override
  public void deleteIndexData() {

  }

  @Override
  public SegmentProperties getSegmentProperties(Segment segment) throws IOException {
    return getSegmentProperties(segment, null);
  }

  @Override
  public SegmentProperties getSegmentProperties(Segment segment, List<PartitionSpec> partitions)
      throws IOException {
    List<CoarseGrainIndex> dataMaps = getIndexes(segment, partitions);
    assert (dataMaps.size() > 0);
    CoarseGrainIndex coarseGrainIndex = dataMaps.get(0);
    assert (coarseGrainIndex instanceof BlockIndex);
    BlockIndex dataMap = (BlockIndex) coarseGrainIndex;
    return dataMap.getSegmentProperties();
  }

  @Override
  public SegmentProperties getSegmentPropertiesFromDataMap(Index coarseGrainIndex) {
    assert (coarseGrainIndex instanceof BlockIndex);
    BlockIndex dataMap = (BlockIndex) coarseGrainIndex;
    return dataMap.getSegmentProperties();
  }

  @Override
  public List<Blocklet> getAllBlocklets(Segment segment, List<PartitionSpec> partitions)
      throws IOException {
    List<Blocklet> blocklets = new ArrayList<>();
    List<CoarseGrainIndex> dataMaps = getIndexes(segment, partitions);
    for (CoarseGrainIndex dataMap : dataMaps) {
      blocklets.addAll(dataMap
          .prune((FilterResolverIntf) null, getSegmentProperties(segment, partitions), partitions,
              null, this.getCarbonTable()));
    }
    return blocklets;
  }

  @Override
  public boolean willBecomeStale(TableOperation operation) {
    return false;
  }

  @Override
  public void cache(TableBlockIndexUniqueIdentifierWrapper tableBlockIndexUniqueIdentifierWrapper,
      BlockletIndexWrapper blockletIndexWrapper) throws IOException {
    cache.put(tableBlockIndexUniqueIdentifierWrapper, blockletIndexWrapper);
  }

  @Override
  public List<IndexInputSplit> getAllUncachedDistributables(
      List<IndexInputSplit> distributables) throws IOException {
    List<IndexInputSplit> distributablesToBeLoaded = new ArrayList<>(distributables.size());
    for (IndexInputSplit distributable : distributables) {
      Segment segment = distributable.getSegment();
      Set<TableBlockIndexUniqueIdentifier> tableBlockIndexUniqueIdentifiers =
          getTableBlockIndexUniqueIdentifiers(segment);
      // filter out the tableBlockIndexUniqueIdentifiers based on distributable
      TableBlockIndexUniqueIdentifier validIdentifier = BlockletIndexUtil
          .filterIdentifiersBasedOnDistributable(tableBlockIndexUniqueIdentifiers,
              (BlockletIndexInputSplit) distributable);
      if (null == cache.getIfPresent(
          new TableBlockIndexUniqueIdentifierWrapper(validIdentifier, this.getCarbonTable()))) {
        ((BlockletIndexInputSplit) distributable)
            .setTableBlockIndexUniqueIdentifier(validIdentifier);
        distributablesToBeLoaded.add(distributable);
      }
    }
    return distributablesToBeLoaded;
  }

  private Set<TableBlockIndexUniqueIdentifier> getTableSegmentUniqueIdentifiers(Segment segment)
      throws IOException {
    Set<TableBlockIndexUniqueIdentifier> tableBlockIndexUniqueIdentifiers =
        segmentMap.get(segment.getSegmentNo());
    if (tableBlockIndexUniqueIdentifiers == null) {
      tableBlockIndexUniqueIdentifiers = BlockletIndexUtil.getSegmentUniqueIdentifiers(segment);
    }
    return tableBlockIndexUniqueIdentifiers;
  }

  public void updateSegmentDataMap(
      Map<String, Set<TableBlockIndexUniqueIdentifier>> indexUniqueIdentifiers) {
    for (Map.Entry<String, Set<TableBlockIndexUniqueIdentifier>> identifier : indexUniqueIdentifiers
        .entrySet()) {
      segmentMap.put(identifier.getKey(), identifier.getValue());
    }
  }

  @Override
  public List<IndexInputSplit> getAllUncachedDistributables(List<Segment> validSegments,
      IndexExprWrapper indexExprWrapper) throws IOException {
    List<IndexInputSplit> distributablesToBeLoaded = new ArrayList<>();
    for (Segment segment : validSegments) {
      IndexInputSplitWrapper indexInputSplitWrappers =
          indexExprWrapper.toDistributableSegment(segment);
      Set<TableBlockIndexUniqueIdentifier> tableBlockIndexUniqueIdentifiers =
          getTableSegmentUniqueIdentifiers(segment);
      for (TableBlockIndexUniqueIdentifier identifier : tableBlockIndexUniqueIdentifiers) {
        BlockletIndexWrapper blockletIndexWrapper = cache.getIfPresent(
            new TableBlockIndexUniqueIdentifierWrapper(identifier, this.getCarbonTable()));
        if (identifier.getIndexFilePath() == null || blockletIndexWrapper == null) {
          ((BlockletIndexInputSplit) indexInputSplitWrappers.getDistributable())
              .setTableBlockIndexUniqueIdentifier(identifier);
          distributablesToBeLoaded.add(indexInputSplitWrappers.getDistributable());
        }
      }
    }
    return distributablesToBeLoaded;
  }

  @Override
  public IndexInputSplitWrapper toDistributableSegment(Segment segment,
      DataMapSchema schema, AbsoluteTableIdentifier identifier, String uniqueId) {
    try {
      BlockletIndexInputSplit distributable = new BlockletIndexInputSplit();
      distributable.setDataMapSchema(schema);
      distributable.setSegment(segment);
      distributable.setSegmentPath(
          CarbonTablePath.getSegmentPath(identifier.getTablePath(), segment.getSegmentNo()));
      distributable.setTablePath(identifier.getTablePath());
      return new IndexInputSplitWrapper(uniqueId, distributable);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
