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
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.indexstore.Blocklet;
import org.apache.carbondata.core.indexstore.BlockletDetailsFetcher;
import org.apache.carbondata.core.indexstore.ExtendedBlocklet;
import org.apache.carbondata.core.indexstore.SegmentPropertiesFetcher;
import org.apache.carbondata.core.indexstore.TableBlockIndexUniqueIdentifier;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.util.DataFileFooterConverter;
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
    SegmentPropertiesFetcher {

  private AbsoluteTableIdentifier identifier;

  // segmentId -> list of index file
  private Map<String, List<TableBlockIndexUniqueIdentifier>> segmentMap = new HashMap<>();

  // segmentId -> SegmentProperties.
  private Map<String, SegmentProperties> segmentPropertiesMap = new HashMap<>();

  private Cache<TableBlockIndexUniqueIdentifier, DataMap> cache;

  @Override
  public void init(AbsoluteTableIdentifier identifier, String dataMapName) {
    this.identifier = identifier;
    cache = CacheProvider.getInstance()
        .createCache(CacheType.DRIVER_BLOCKLET_DATAMAP);
  }

  @Override
  public DataMapWriter createWriter(String segmentId) {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public List<DataMap> getDataMaps(String segmentId) throws IOException {
    List<TableBlockIndexUniqueIdentifier> tableBlockIndexUniqueIdentifiers =
        getTableBlockIndexUniqueIdentifiers(segmentId);
    return cache.getAll(tableBlockIndexUniqueIdentifiers);
  }

  private List<TableBlockIndexUniqueIdentifier> getTableBlockIndexUniqueIdentifiers(
      String segmentId) throws IOException {
    List<TableBlockIndexUniqueIdentifier> tableBlockIndexUniqueIdentifiers =
        segmentMap.get(segmentId);
    if (tableBlockIndexUniqueIdentifiers == null) {
      tableBlockIndexUniqueIdentifiers = new ArrayList<>();
      String path = CarbonTablePath.getSegmentPath(identifier.getTablePath(), segmentId);
      List<String> indexFiles = new SegmentIndexFileStore().getIndexFilesFromSegment(path);
      for (int i = 0; i < indexFiles.size(); i++) {
        tableBlockIndexUniqueIdentifiers.add(
            new TableBlockIndexUniqueIdentifier(identifier, segmentId, indexFiles.get(i)));
      }
      segmentMap.put(segmentId, tableBlockIndexUniqueIdentifiers);
    }
    return tableBlockIndexUniqueIdentifiers;
  }

  /**
   * Get the blocklet detail information based on blockletid, blockid and segmentid. This method is
   * exclusively for BlockletDataMapFactory as detail information is only available in this default
   * datamap.
   */
  @Override
  public List<ExtendedBlocklet> getExtendedBlocklets(List<Blocklet> blocklets, String segmentId)
      throws IOException {
    List<ExtendedBlocklet> detailedBlocklets = new ArrayList<>();
    // If it is already detailed blocklet then type cast and return same
    if (blocklets.size() > 0 && blocklets.get(0) instanceof ExtendedBlocklet) {
      for (Blocklet blocklet : blocklets) {
        detailedBlocklets.add((ExtendedBlocklet) blocklet);
      }
      return detailedBlocklets;
    }
    List<TableBlockIndexUniqueIdentifier> identifiers =
        getTableBlockIndexUniqueIdentifiers(segmentId);
    // Retrieve each blocklets detail information from blocklet datamap
    for (Blocklet blocklet : blocklets) {
      detailedBlocklets.add(getExtendedBlocklet(identifiers, blocklet));
    }
    return detailedBlocklets;
  }

  @Override
  public ExtendedBlocklet getExtendedBlocklet(Blocklet blocklet, String segmentId)
      throws IOException {
    if (blocklet instanceof ExtendedBlocklet) {
      return (ExtendedBlocklet) blocklet;
    }
    List<TableBlockIndexUniqueIdentifier> identifiers =
        getTableBlockIndexUniqueIdentifiers(segmentId);
    return getExtendedBlocklet(identifiers, blocklet);
  }

  private ExtendedBlocklet getExtendedBlocklet(List<TableBlockIndexUniqueIdentifier> identifiers,
      Blocklet blocklet) throws IOException {
    String carbonIndexFileName = CarbonTablePath.getCarbonIndexFileName(blocklet.getPath());
    for (TableBlockIndexUniqueIdentifier identifier : identifiers) {
      if (identifier.getCarbonIndexFileName().equals(carbonIndexFileName)) {
        DataMap dataMap = cache.get(identifier);
        return ((BlockletDataMap) dataMap).getDetailedBlocklet(blocklet.getBlockletId());
      }
    }
    throw new IOException("Blocklet with blockid " + blocklet.getPath() + " not found ");
  }


  @Override
  public List<DataMapDistributable> toDistributable(String segmentId) {
    CarbonFile[] carbonIndexFiles = SegmentIndexFileStore.getCarbonIndexFiles(segmentId);
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

  @Override public void fireEvent(Event event) {

  }

  @Override
  public void clear(String segmentId) {
    segmentPropertiesMap.remove(segmentId);
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
    for (String segmentId : segmentMap.keySet().toArray(new String[segmentMap.size()])) {
      clear(segmentId);
    }
  }

  @Override
  public List<DataMap> getDataMaps(DataMapDistributable distributable) throws IOException {
    BlockletDataMapDistributable mapDistributable = (BlockletDataMapDistributable) distributable;
    List<TableBlockIndexUniqueIdentifier> identifiers = new ArrayList<>();
    if (mapDistributable.getFilePath().endsWith(CarbonTablePath.INDEX_FILE_EXT)) {
      identifiers.add(new TableBlockIndexUniqueIdentifier(identifier, distributable.getSegmentId(),
          mapDistributable.getFilePath()));
    } else if (mapDistributable.getFilePath().endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT)) {
      SegmentIndexFileStore fileStore = new SegmentIndexFileStore();
      List<String> indexFiles = fileStore.getIndexFilesFromMergeFile(
          CarbonTablePath.getSegmentPath(identifier.getTablePath(), mapDistributable.getSegmentId())
              + "/" + mapDistributable.getFilePath());
      for (String indexFile : indexFiles) {
        identifiers.add(
            new TableBlockIndexUniqueIdentifier(identifier, distributable.getSegmentId(),
                indexFile));
      }
    }
    List<DataMap> dataMaps;
    try {
      dataMaps = cache.getAll(identifiers);
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

  @Override public SegmentProperties getSegmentProperties(String segmentId) throws IOException {
    SegmentProperties segmentProperties = segmentPropertiesMap.get(segmentId);
    if (segmentProperties == null) {
      int[] columnCardinality;
      List<TableBlockIndexUniqueIdentifier> tableBlockIndexUniqueIdentifiers =
          getTableBlockIndexUniqueIdentifiers(segmentId);
      DataFileFooterConverter fileFooterConverter = new DataFileFooterConverter();
      List<DataFileFooter> indexInfo =
          fileFooterConverter.getIndexInfo(tableBlockIndexUniqueIdentifiers.get(0).getFilePath());
      for (DataFileFooter fileFooter : indexInfo) {
        List<ColumnSchema> columnInTable = fileFooter.getColumnInTable();
        if (segmentProperties == null) {
          columnCardinality = fileFooter.getSegmentInfo().getColumnCardinality();
          segmentProperties = new SegmentProperties(columnInTable, columnCardinality);
        }
      }
      segmentPropertiesMap.put(segmentId, segmentProperties);
    }
    return segmentProperties;
  }
}
