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
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.DataMapDistributable;
import org.apache.carbondata.core.datamap.DataMapMeta;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.dev.CacheableDataMap;
import org.apache.carbondata.core.datamap.dev.DataMap;
import org.apache.carbondata.core.datamap.dev.DataMapBuilder;
import org.apache.carbondata.core.datamap.dev.DataMapWriter;
import org.apache.carbondata.core.datamap.dev.cgdatamap.CoarseGrainDataMap;
import org.apache.carbondata.core.datamap.dev.cgdatamap.CoarseGrainDataMapFactory;
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
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.util.BlockletDataMapUtil;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.events.Event;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

/**
 * Table map for blocklet
 */
public class BlockletDataMapFactory extends CoarseGrainDataMapFactory
    implements BlockletDetailsFetcher, SegmentPropertiesFetcher, CacheableDataMap {

  private static final Log LOG = LogFactory.getLog(BlockletDataMapFactory.class);
  private static final String NAME = "clustered.btree.blocklet";

  public static final DataMapSchema DATA_MAP_SCHEMA =
      new DataMapSchema(NAME, BlockletDataMapFactory.class.getName());

  private AbsoluteTableIdentifier identifier;

  // segmentId -> list of index file
  private Map<String, Set<TableBlockIndexUniqueIdentifier>> segmentMap = new ConcurrentHashMap<>();

  private Cache<TableBlockIndexUniqueIdentifier, BlockletDataMapIndexWrapper> cache;

  public BlockletDataMapFactory(CarbonTable carbonTable, DataMapSchema dataMapSchema) {
    super(carbonTable, dataMapSchema);
    this.identifier = carbonTable.getAbsoluteTableIdentifier();
    cache = CacheProvider.getInstance()
        .createCache(CacheType.DRIVER_BLOCKLET_DATAMAP);
  }

  @Override
  public DataMapWriter createWriter(Segment segment, String shardName) {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public DataMapBuilder createBuilder(Segment segment, String shardName) {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override public List<CoarseGrainDataMap> getDataMaps(Segment segment) throws IOException {
    List<CoarseGrainDataMap> dataMaps = new ArrayList<>();
    Set<TableBlockIndexUniqueIdentifier> identifiers =
        getTableBlockIndexUniqueIdentifiers(segment);
    List<TableBlockIndexUniqueIdentifier> tableBlockIndexUniqueIdentifiers =
        new ArrayList<>(identifiers.size());
    tableBlockIndexUniqueIdentifiers.addAll(identifiers);
    List<BlockletDataMapIndexWrapper> blockletDataMapIndexWrappers =
        cache.getAll(tableBlockIndexUniqueIdentifiers);
    for (BlockletDataMapIndexWrapper wrapper : blockletDataMapIndexWrappers) {
      dataMaps.addAll(wrapper.getDataMaps());
    }
    return dataMaps;
  }

  private Set<TableBlockIndexUniqueIdentifier> getTableBlockIndexUniqueIdentifiers(Segment segment)
      throws IOException {
    Set<TableBlockIndexUniqueIdentifier> tableBlockIndexUniqueIdentifiers =
        segmentMap.get(segment.getSegmentNo());
    if (tableBlockIndexUniqueIdentifiers == null) {
      CarbonTable carbonTable = this.getCarbonTable();
      if (!carbonTable.getTableInfo().isTransactionalTable()) {
        // For NonTransactional table, compare the schema of all index files with inferred schema.
        // If there is a mismatch throw exception. As all files must be of same schema.
        validateSchemaForNewTranscationalTableFiles(segment, carbonTable);
      }
      tableBlockIndexUniqueIdentifiers =
          BlockletDataMapUtil.getTableBlockUniqueIdentifiers(segment);
      segmentMap.put(segment.getSegmentNo(), tableBlockIndexUniqueIdentifiers);
    }
    return tableBlockIndexUniqueIdentifiers;
  }

  private void validateSchemaForNewTranscationalTableFiles(Segment segment, CarbonTable carbonTable)
      throws IOException {
    SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
    Map<String, String> indexFiles = segment.getCommittedIndexFile();
    for (Map.Entry<String, String> indexFileEntry : indexFiles.entrySet()) {
      Path indexFile = new Path(indexFileEntry.getKey());
      org.apache.carbondata.format.TableInfo tableInfo = CarbonUtil.inferSchemaFromIndexFile(
          indexFile.toString(), carbonTable.getTableName());
      TableInfo wrapperTableInfo = schemaConverter.fromExternalToWrapperTableInfo(
          tableInfo, identifier.getDatabaseName(),
          identifier.getTableName(),
          identifier.getTablePath());
      List<ColumnSchema> indexFileColumnList =
          wrapperTableInfo.getFactTable().getListOfColumns();
      List<ColumnSchema> tableColumnList =
          carbonTable.getTableInfo().getFactTable().getListOfColumns();
      if (!isSameColumnSchemaList(indexFileColumnList, tableColumnList)) {
        LOG.error("Schema of " + indexFile.getName()
            + " doesn't match with the table's schema");
        throw new IOException("All the files doesn't have same schema. "
            + "Unsupported operation on nonTransactional table. Check logs.");
      }
    }
  }

  private boolean isSameColumnSchemaList(List<ColumnSchema> indexFileColumnList,
      List<ColumnSchema> tableColumnList) {
    if (indexFileColumnList.size() != tableColumnList.size()) {
      LOG.error("Index file's column size is " + indexFileColumnList.size()
          + " but table's column size is " + tableColumnList.size());
      return false;
    }
    for (int i = 0; i < tableColumnList.size(); i++) {
      if (!indexFileColumnList.get(i).equalsWithStrictCheck(tableColumnList.get(i))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Get the blocklet detail information based on blockletid, blockid and segmentid. This method is
   * exclusively for BlockletDataMapFactory as detail information is only available in this
   * default datamap.
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
      detailedBlocklets.add(getExtendedBlocklet(identifiers, blocklet));
    }
    return detailedBlocklets;
  }

  @Override
  public ExtendedBlocklet getExtendedBlocklet(Blocklet blocklet, Segment segment)
      throws IOException {
    if (blocklet instanceof ExtendedBlocklet) {
      return (ExtendedBlocklet) blocklet;
    }
    Set<TableBlockIndexUniqueIdentifier> identifiers = getTableBlockIndexUniqueIdentifiers(segment);
    return getExtendedBlocklet(identifiers, blocklet);
  }

  private ExtendedBlocklet getExtendedBlocklet(Set<TableBlockIndexUniqueIdentifier> identifiers,
      Blocklet blocklet) throws IOException {
    for (TableBlockIndexUniqueIdentifier identifier : identifiers) {
      BlockletDataMapIndexWrapper wrapper = cache.get(identifier);
      List<BlockletDataMap> dataMaps = wrapper.getDataMaps();
      for (DataMap dataMap : dataMaps) {
        if (((BlockletDataMap) dataMap).getIndexFileName().startsWith(blocklet.getFilePath())) {
          return ((BlockletDataMap) dataMap).getDetailedBlocklet(blocklet.getBlockletId());
        }
      }
    }
    throw new IOException("Blocklet with blockid " + blocklet.getBlockletId() + " not found ");
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
        if (null != wrapper) {
          List<BlockletDataMap> dataMaps = wrapper.getDataMaps();
          for (DataMap dataMap : dataMaps) {
            if (dataMap != null) {
              cache.invalidate(blockIndex);
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
        clear(new Segment(segmentId, null, null));
      }
    }
  }

  @Override
  public List<CoarseGrainDataMap> getDataMaps(DataMapDistributable distributable)
      throws IOException {
    BlockletDataMapDistributable mapDistributable = (BlockletDataMapDistributable) distributable;
    List<TableBlockIndexUniqueIdentifier> identifiers = new ArrayList<>();
    Path indexPath = new Path(mapDistributable.getFilePath());
    String segmentNo = mapDistributable.getSegment().getSegmentNo();
    if (indexPath.getName().endsWith(CarbonTablePath.INDEX_FILE_EXT)) {
      String parent = indexPath.getParent().toString();
      identifiers
          .add(new TableBlockIndexUniqueIdentifier(parent, indexPath.getName(), null, segmentNo));
    } else if (indexPath.getName().endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT)) {
      SegmentIndexFileStore fileStore = new SegmentIndexFileStore();
      CarbonFile carbonFile = FileFactory.getCarbonFile(indexPath.toString());
      String parentPath = carbonFile.getParentFile().getAbsolutePath();
      List<String> indexFiles = fileStore.getIndexFilesFromMergeFile(carbonFile.getAbsolutePath());
      for (String indexFile : indexFiles) {
        identifiers.add(
            new TableBlockIndexUniqueIdentifier(parentPath, indexFile, carbonFile.getName(),
                segmentNo));
      }
    }
    List<CoarseGrainDataMap> dataMaps = new ArrayList<>();
    try {
      List<BlockletDataMapIndexWrapper> wrappers = cache.getAll(identifiers);
      for (BlockletDataMapIndexWrapper wrapper : wrappers) {
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

  @Override public void deleteDatamapData() {

  }

  @Override public SegmentProperties getSegmentProperties(Segment segment) throws IOException {
    List<CoarseGrainDataMap> dataMaps = getDataMaps(segment);
    assert (dataMaps.size() > 0);
    CoarseGrainDataMap coarseGrainDataMap = dataMaps.get(0);
    assert (coarseGrainDataMap instanceof BlockletDataMap);
    BlockletDataMap dataMap = (BlockletDataMap) coarseGrainDataMap;
    return dataMap.getSegmentProperties();
  }

  @Override public List<Blocklet> getAllBlocklets(Segment segment, List<PartitionSpec> partitions)
      throws IOException {
    List<Blocklet> blocklets = new ArrayList<>();
    List<CoarseGrainDataMap> dataMaps = getDataMaps(segment);
    for (CoarseGrainDataMap dataMap : dataMaps) {
      blocklets.addAll(
          dataMap.prune(null, getSegmentProperties(segment), partitions));
    }
    return blocklets;
  }

  @Override public boolean willBecomeStale(TableOperation operation) {
    return false;
  }

  @Override public void cache(TableBlockIndexUniqueIdentifier tableBlockIndexUniqueIdentifier,
      BlockletDataMapIndexWrapper blockletDataMapIndexWrapper) throws IOException, MemoryException {
    cache.put(tableBlockIndexUniqueIdentifier, blockletDataMapIndexWrapper);
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
}
