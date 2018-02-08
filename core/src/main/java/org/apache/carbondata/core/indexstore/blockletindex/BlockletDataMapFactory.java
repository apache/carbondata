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
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.dev.AbstractDataMapWriter;
import org.apache.carbondata.core.datamap.dev.DataMap;
import org.apache.carbondata.core.datamap.dev.cgdatamap.AbstractCoarseGrainDataMap;
import org.apache.carbondata.core.datamap.dev.cgdatamap.AbstractCoarseGrainDataMapFactory;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.indexstore.Blocklet;
import org.apache.carbondata.core.indexstore.BlockletDetailsFetcher;
import org.apache.carbondata.core.indexstore.ExtendedBlocklet;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.indexstore.SegmentPropertiesFetcher;
import org.apache.carbondata.core.indexstore.TableBlockIndexUniqueIdentifier;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.events.Event;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

/**
 * Table map for blocklet
 */
public class BlockletDataMapFactory extends AbstractCoarseGrainDataMapFactory
    implements BlockletDetailsFetcher, SegmentPropertiesFetcher {

  private AbsoluteTableIdentifier identifier;

  // segmentId -> list of index file
  private Map<String, List<TableBlockIndexUniqueIdentifier>> segmentMap = new HashMap<>();

  private Cache<TableBlockIndexUniqueIdentifier, AbstractCoarseGrainDataMap> cache;

  @Override
  public void init(AbsoluteTableIdentifier identifier, String dataMapName) {
    this.identifier = identifier;
    cache = CacheProvider.getInstance()
        .createCache(CacheType.DRIVER_BLOCKLET_DATAMAP);
  }

  @Override
  public AbstractDataMapWriter createWriter(Segment segment, String writeDirectoryPath) {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public List<AbstractCoarseGrainDataMap> getDataMaps(Segment segment) throws IOException {
    List<TableBlockIndexUniqueIdentifier> tableBlockIndexUniqueIdentifiers =
        getTableBlockIndexUniqueIdentifiers(segment);
    return cache.getAll(tableBlockIndexUniqueIdentifiers);
  }

  private List<TableBlockIndexUniqueIdentifier> getTableBlockIndexUniqueIdentifiers(
      Segment segment) throws IOException {
    List<TableBlockIndexUniqueIdentifier> tableBlockIndexUniqueIdentifiers =
        segmentMap.get(segment.getSegmentNo());
    if (tableBlockIndexUniqueIdentifiers == null) {
      tableBlockIndexUniqueIdentifiers = new ArrayList<>();
      Map<String, String> indexFiles;
      if (segment.getSegmentFileName() == null) {
        String path =
            CarbonTablePath.getSegmentPath(identifier.getTablePath(), segment.getSegmentNo());
        indexFiles = new SegmentIndexFileStore().getIndexFilesFromSegment(path);
      } else {
        SegmentFileStore fileStore =
            new SegmentFileStore(identifier.getTablePath(), segment.getSegmentFileName());
        indexFiles = fileStore.getIndexFiles();
      }
      for (Map.Entry<String, String> indexFileEntry: indexFiles.entrySet()) {
        Path indexFile = new Path(indexFileEntry.getKey());
        tableBlockIndexUniqueIdentifiers.add(
            new TableBlockIndexUniqueIdentifier(indexFile.getParent().toString(),
                indexFile.getName(), indexFileEntry.getValue(), segment.getSegmentNo()));
      }
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
    List<TableBlockIndexUniqueIdentifier> identifiers =
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
    List<TableBlockIndexUniqueIdentifier> identifiers =
        getTableBlockIndexUniqueIdentifiers(segment);
    return getExtendedBlocklet(identifiers, blocklet);
  }

  private ExtendedBlocklet getExtendedBlocklet(List<TableBlockIndexUniqueIdentifier> identifiers,
      Blocklet blocklet) throws IOException {
    String carbonIndexFileName = CarbonTablePath.getCarbonIndexFileName(blocklet.getBlockId());
    for (TableBlockIndexUniqueIdentifier identifier : identifiers) {
      if (identifier.getIndexFilePath().equals(carbonIndexFileName)) {
        DataMap dataMap = cache.get(identifier);
        return ((BlockletDataMap) dataMap).getDetailedBlocklet(blocklet.getBlockletId());
      }
    }
    throw new IOException("Blocklet with blockid " + blocklet.getBlockletId() + " not found ");
  }


  @Override
  public List<DataMapDistributable> toDistributable(Segment segment) {
    List<DataMapDistributable> distributables = new ArrayList<>();
    try {
      CarbonFile[] carbonIndexFiles;
      if (segment.getSegmentFileName() == null) {
        carbonIndexFiles = SegmentIndexFileStore.getCarbonIndexFiles(
            CarbonTablePath.getSegmentPath(identifier.getTablePath(), segment.getSegmentNo()));
      } else {
        SegmentFileStore fileStore =
            new SegmentFileStore(identifier.getTablePath(), segment.getSegmentFileName());
        Map<String, String> indexFiles = fileStore.getIndexFiles();
        carbonIndexFiles = new CarbonFile[indexFiles.size()];
        int i = 0;
        for (String indexFile : indexFiles.keySet()) {
          carbonIndexFiles[i++] = FileFactory.getCarbonFile(indexFile);
        }
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
    List<TableBlockIndexUniqueIdentifier> blockIndexes = segmentMap.remove(segment.getSegmentNo());
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
      clear(new Segment(segmentId, null));
    }
  }

  @Override
  public List<AbstractCoarseGrainDataMap> getDataMaps(DataMapDistributable distributable)
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
    List<AbstractCoarseGrainDataMap> dataMaps;
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

  @Override public SegmentProperties getSegmentProperties(Segment segment) throws IOException {
    List<AbstractCoarseGrainDataMap> dataMaps = getDataMaps(segment);
    assert (dataMaps.size() > 0);
    AbstractCoarseGrainDataMap coarseGrainDataMap = dataMaps.get(0);
    assert (coarseGrainDataMap instanceof BlockletDataMap);
    BlockletDataMap dataMap = (BlockletDataMap) coarseGrainDataMap;
    return dataMap.getSegmentProperties();
  }

  @Override public List<Blocklet> getAllBlocklets(Segment segment, List<PartitionSpec> partitions)
      throws IOException {
    List<Blocklet> blocklets = new ArrayList<>();
    List<AbstractCoarseGrainDataMap> dataMaps = getDataMaps(segment);
    for (AbstractCoarseGrainDataMap dataMap : dataMaps) {
      blocklets.addAll(dataMap.prune(null, getSegmentProperties(segment), partitions));
    }
    return blocklets;
  }
}
