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

package org.apache.carbondata.datamap.bloom;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.exceptions.sql.MalformedDataMapCommandException;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.constants.CarbonV3DataFormatConstants;
import org.apache.carbondata.core.datamap.DataMapDistributable;
import org.apache.carbondata.core.datamap.DataMapLevel;
import org.apache.carbondata.core.datamap.DataMapMeta;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.dev.DataMapBuilder;
import org.apache.carbondata.core.datamap.dev.DataMapFactory;
import org.apache.carbondata.core.datamap.dev.DataMapWriter;
import org.apache.carbondata.core.datamap.dev.cgdatamap.CoarseGrainDataMap;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.features.TableOperation;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.events.Event;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

/**
 * This class is for Bloom Filter for blocklet level
 */
@InterfaceAudience.Internal
public class BloomCoarseGrainDataMapFactory extends DataMapFactory<CoarseGrainDataMap> {
  private static final Logger LOGGER = LogServiceFactory.getLogService(
      BloomCoarseGrainDataMapFactory.class.getName());
  /**
   * property for size of bloom filter
   */
  private static final String BLOOM_SIZE = "bloom_size";
  /**
   * default size for bloom filter, cardinality of the column.
   */
  private static final int DEFAULT_BLOOM_FILTER_SIZE =
      CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_DEFAULT * 20;
  /**
   * property for fpp(false-positive-probability) of bloom filter
   */
  private static final String BLOOM_FPP = "bloom_fpp";
  /**
   * default value for fpp of bloom filter is 0.001%
   */
  private static final double DEFAULT_BLOOM_FILTER_FPP = 0.00001d;

  /**
   * property for compressing bloom while saving to disk.
   */
  private static final String COMPRESS_BLOOM = "bloom_compress";
  /**
   * Default value of compressing bloom while save to disk.
   */
  private static final boolean DEFAULT_BLOOM_COMPRESS = true;

  private DataMapMeta dataMapMeta;
  private String dataMapName;
  private int bloomFilterSize;
  private double bloomFilterFpp;
  private boolean bloomCompress;
  private Cache<BloomCacheKeyValue.CacheKey, BloomCacheKeyValue.CacheValue> cache;
  // segmentId -> list of index file
  private Map<String, Set<String>> segmentMap = new ConcurrentHashMap<>();

  public BloomCoarseGrainDataMapFactory(CarbonTable carbonTable, DataMapSchema dataMapSchema)
      throws MalformedDataMapCommandException {
    super(carbonTable, dataMapSchema);
    Objects.requireNonNull(carbonTable);
    Objects.requireNonNull(dataMapSchema);

    this.dataMapName = dataMapSchema.getDataMapName();

    List<CarbonColumn> indexedColumns = carbonTable.getIndexedColumns(dataMapSchema);
    this.bloomFilterSize = validateAndGetBloomFilterSize(dataMapSchema);
    this.bloomFilterFpp = validateAndGetBloomFilterFpp(dataMapSchema);
    this.bloomCompress = validateAndGetBloomCompress(dataMapSchema);
    List<ExpressionType> optimizedOperations = new ArrayList<ExpressionType>();
    // todo: support more optimize operations
    optimizedOperations.add(ExpressionType.EQUALS);
    optimizedOperations.add(ExpressionType.IN);
    this.dataMapMeta = new DataMapMeta(this.dataMapName, indexedColumns, optimizedOperations);
    LOGGER.info(String.format("DataMap %s works for %s with bloom size %d",
        this.dataMapName, this.dataMapMeta, this.bloomFilterSize));
    try {
      this.cache = CacheProvider.getInstance()
          .createCache(new CacheType("bloom_cache"), BloomDataMapCache.class.getName());
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      throw new MalformedDataMapCommandException(e.getMessage());
    }
  }

  /**
   * validate Lucene DataMap BLOOM_SIZE
   * 1. BLOOM_SIZE property is optional, 32000 * 20 will be the default size.
   * 2. BLOOM_SIZE should be an integer that greater than 0
   */
  private int validateAndGetBloomFilterSize(DataMapSchema dmSchema)
      throws MalformedDataMapCommandException {
    String bloomFilterSizeStr = dmSchema.getProperties().get(BLOOM_SIZE);
    if (StringUtils.isBlank(bloomFilterSizeStr)) {
      LOGGER.warn(
          String.format("Bloom filter size is not configured for datamap %s, use default value %d",
              dataMapName, DEFAULT_BLOOM_FILTER_SIZE));
      return DEFAULT_BLOOM_FILTER_SIZE;
    }
    int bloomFilterSize;
    try {
      bloomFilterSize = Integer.parseInt(bloomFilterSizeStr);
    } catch (NumberFormatException e) {
      throw new MalformedDataMapCommandException(
          String.format("Invalid value of bloom filter size '%s', it should be an integer",
              bloomFilterSizeStr));
    }
    // todo: reconsider the boundaries of bloom filter size
    if (bloomFilterSize <= 0) {
      throw new MalformedDataMapCommandException(
          String.format("Invalid value of bloom filter size '%s', it should be greater than 0",
              bloomFilterSizeStr));
    }
    return bloomFilterSize;
  }

  /**
   * validate bloom DataMap BLOOM_FPP
   * 1. BLOOM_FPP property is optional, 0.00001 will be the default value.
   * 2. BLOOM_FPP should be (0, 1)
   */
  private double validateAndGetBloomFilterFpp(DataMapSchema dmSchema)
      throws MalformedDataMapCommandException {
    String bloomFilterFppStr = dmSchema.getProperties().get(BLOOM_FPP);
    if (StringUtils.isBlank(bloomFilterFppStr)) {
      LOGGER.warn(
          String.format("Bloom filter FPP is not configured for datamap %s, use default value %f",
              dataMapName, DEFAULT_BLOOM_FILTER_FPP));
      return DEFAULT_BLOOM_FILTER_FPP;
    }
    double bloomFilterFpp;
    try {
      bloomFilterFpp = Double.parseDouble(bloomFilterFppStr);
    } catch (NumberFormatException e) {
      throw new MalformedDataMapCommandException(
          String.format("Invalid value of bloom filter fpp '%s', it should be an numeric",
              bloomFilterFppStr));
    }
    if (bloomFilterFpp < 0 || bloomFilterFpp - 1 >= 0) {
      throw new MalformedDataMapCommandException(
          String.format("Invalid value of bloom filter fpp '%s', it should be in range 0~1",
              bloomFilterFppStr));
    }
    return bloomFilterFpp;
  }

  /**
   * validate bloom DataMap COMPRESS_BLOOM
   * Default value is true
   */
  private boolean validateAndGetBloomCompress(DataMapSchema dmSchema) {
    String bloomCompress = dmSchema.getProperties().get(COMPRESS_BLOOM);
    if (StringUtils.isBlank(bloomCompress)) {
      LOGGER.warn(
          String.format("Bloom compress is not configured for datamap %s, use default value %b",
              dataMapName, DEFAULT_BLOOM_COMPRESS));
      return DEFAULT_BLOOM_COMPRESS;
    }
    return Boolean.parseBoolean(bloomCompress);
  }

  @Override
  public DataMapWriter createWriter(Segment segment, String shardName,
      SegmentProperties segmentProperties) throws IOException {
    LOGGER.info(
        String.format("Data of BloomCoarseGranDataMap %s for table %s will be written to %s",
            this.dataMapName, getCarbonTable().getTableName() , shardName));
    return new BloomDataMapWriter(getCarbonTable().getTablePath(), this.dataMapName,
        this.dataMapMeta.getIndexedColumns(), segment, shardName, segmentProperties,
        this.bloomFilterSize, this.bloomFilterFpp, bloomCompress);
  }

  @Override
  public DataMapBuilder createBuilder(Segment segment, String shardName,
      SegmentProperties segmentProperties) throws IOException {
    return new BloomDataMapBuilder(getCarbonTable().getTablePath(), this.dataMapName,
        this.dataMapMeta.getIndexedColumns(), segment, shardName, segmentProperties,
        this.bloomFilterSize, this.bloomFilterFpp, bloomCompress);
  }

  /**
   * returns all shard directories of bloom index files for query
   * if bloom index files are merged we should get only one shard path
   */
  public static Set<String> getAllShardPaths(String tablePath, String segmentId,
      String dataMapName) {
    String dataMapStorePath = CarbonTablePath.getDataMapStorePath(
        tablePath, segmentId, dataMapName);
    CarbonFile[] carbonFiles = FileFactory.getCarbonFile(dataMapStorePath).listFiles();
    Set<String> shardPaths = new HashSet<>();
    boolean mergeShardInprogress = false;
    CarbonFile mergeShardFile = null;
    for (CarbonFile carbonFile : carbonFiles) {
      if (carbonFile.getName().equals(BloomIndexFileStore.MERGE_BLOOM_INDEX_SHARD_NAME)) {
        mergeShardFile = carbonFile;
      } else if (carbonFile.getName().equals(BloomIndexFileStore.MERGE_INPROGRESS_FILE)) {
        mergeShardInprogress = true;
      } else if (carbonFile.isDirectory()) {
        shardPaths.add(FileFactory.getPath(carbonFile.getAbsolutePath()).toString());
      }
    }
    if (mergeShardFile != null && !mergeShardInprogress) {
      // should only get one shard path if mergeShard is generated successfully
      shardPaths.clear();
      shardPaths.add(FileFactory.getPath(mergeShardFile.getAbsolutePath()).toString());
    }
    return shardPaths;
  }

  @Override
  public List<CoarseGrainDataMap> getDataMaps(Segment segment) throws IOException {
    List<CoarseGrainDataMap> dataMaps = new ArrayList<>();
    try {
      Set<String> shardPaths = segmentMap.get(segment.getSegmentNo());
      if (shardPaths == null) {
        shardPaths =
            getAllShardPaths(getCarbonTable().getTablePath(), segment.getSegmentNo(), dataMapName);
        segmentMap.put(segment.getSegmentNo(), shardPaths);
      }
      Set<String> filteredShards = segment.getFilteredIndexShardNames();
      for (String shard : shardPaths) {
        if (shard.endsWith(BloomIndexFileStore.MERGE_BLOOM_INDEX_SHARD_NAME) ||
            filteredShards.contains(new File(shard).getName())) {
          // Filter out the tasks which are filtered through Main datamap.
          // for merge shard, shard pruning delay to be done before pruning blocklet
          BloomCoarseGrainDataMap bloomDM = new BloomCoarseGrainDataMap();
          bloomDM.init(new BloomDataMapModel(shard, cache, segment.getConfiguration()));
          bloomDM.initIndexColumnConverters(getCarbonTable(), dataMapMeta.getIndexedColumns());
          bloomDM.setFilteredShard(filteredShards);
          dataMaps.add(bloomDM);
        }
      }
    } catch (Exception e) {
      throw new IOException("Error occurs while init Bloom DataMap", e);
    }
    return dataMaps;
  }

  @Override
  public List<CoarseGrainDataMap> getDataMaps(Segment segment, List<PartitionSpec> partitionSpecs)
      throws IOException {
    return getDataMaps(segment);
  }

  @Override
  public List<CoarseGrainDataMap> getDataMaps(DataMapDistributable distributable) {
    List<CoarseGrainDataMap> dataMaps = new ArrayList<>();
    String indexPath = ((BloomDataMapDistributable) distributable).getIndexPath();
    Set<String> filteredShards = ((BloomDataMapDistributable) distributable).getFilteredShards();
    BloomCoarseGrainDataMap bloomDM = new BloomCoarseGrainDataMap();
    bloomDM.init(new BloomDataMapModel(indexPath, cache, FileFactory.getConfiguration()));
    bloomDM.initIndexColumnConverters(getCarbonTable(), dataMapMeta.getIndexedColumns());
    bloomDM.setFilteredShard(filteredShards);
    dataMaps.add(bloomDM);
    return dataMaps;
  }

  @Override
  public List<DataMapDistributable> toDistributable(Segment segment) {
    List<DataMapDistributable> dataMapDistributableList = new ArrayList<>();
    Set<String> shardPaths = segmentMap.get(segment.getSegmentNo());
    if (shardPaths == null) {
      shardPaths =
          getAllShardPaths(getCarbonTable().getTablePath(), segment.getSegmentNo(), dataMapName);
      segmentMap.put(segment.getSegmentNo(), shardPaths);
    }
    Set<String> filteredShards = segment.getFilteredIndexShardNames();
    for (String shardPath : shardPaths) {
      // Filter out the tasks which are filtered through Main datamap.
      // for merge shard, shard pruning delay to be done before pruning blocklet
      if (shardPath.endsWith(BloomIndexFileStore.MERGE_BLOOM_INDEX_SHARD_NAME) ||
          filteredShards.contains(new File(shardPath).getName())) {
        DataMapDistributable bloomDataMapDistributable =
            new BloomDataMapDistributable(shardPath, filteredShards);
        bloomDataMapDistributable.setSegment(segment);
        bloomDataMapDistributable.setDataMapSchema(getDataMapSchema());
        dataMapDistributableList.add(bloomDataMapDistributable);
      }
    }
    return dataMapDistributableList;
  }

  @Override
  public void fireEvent(Event event) {

  }

  @Override
  public void clear(String segment) {
    Set<String> shards = segmentMap.remove(segment);
    if (shards != null) {
      for (String shard : shards) {
        for (CarbonColumn carbonColumn : dataMapMeta.getIndexedColumns()) {
          cache.invalidate(new BloomCacheKeyValue.CacheKey(shard, carbonColumn.getColName()));
        }
      }
    }
  }

  @Override
  public synchronized void clear() {
    if (segmentMap.size() > 0) {
      List<String> segments = new ArrayList<>(segmentMap.keySet());
      for (String segmentId : segments) {
        clear(segmentId);
      }
    }
  }

  @Override
  public void deleteDatamapData(Segment segment) throws IOException {
    deleteSegmentDatamapData(segment.getSegmentNo());
  }

  @Override
  public void deleteSegmentDatamapData(String segmentId) throws IOException {
    try {
      String datamapPath = CarbonTablePath
          .getDataMapStorePath(getCarbonTable().getTablePath(), segmentId, dataMapName);
      if (FileFactory.isFileExist(datamapPath)) {
        CarbonFile file = FileFactory.getCarbonFile(datamapPath);
        CarbonUtil.deleteFoldersAndFilesSilent(file);
      }
      clear(segmentId);
    } catch (InterruptedException ex) {
      throw new IOException("Failed to delete datamap for segment_" + segmentId);
    }
  }

  @Override
  public void deleteDatamapData() {
    SegmentStatusManager ssm =
        new SegmentStatusManager(getCarbonTable().getAbsoluteTableIdentifier());
    try {
      List<Segment> validSegments =
          ssm.getValidAndInvalidSegments(getCarbonTable().isChildTableForMV()).getValidSegments();
      for (Segment segment : validSegments) {
        deleteDatamapData(segment);
      }
    } catch (IOException e) {
      LOGGER.error("drop datamap failed, failed to delete datamap directory");
    }
  }

  @Override
  public boolean willBecomeStale(TableOperation operation) {
    switch (operation) {
      case ALTER_RENAME:
        return false;
      case ALTER_DROP:
        return true;
      case ALTER_ADD_COLUMN:
        return false;
      case ALTER_CHANGE_DATATYPE:
        return true;
      case ALTER_COLUMN_RENAME:
        return true;
      case STREAMING:
        return false;
      case DELETE:
        return true;
      case UPDATE:
        return true;
      case PARTITION:
        return true;
      default:
        return false;
    }
  }

  @Override
  public boolean isOperationBlocked(TableOperation operation, Object... targets) {
    switch (operation) {
      case ALTER_DROP: {
        // alter table drop columns
        // will be blocked if the columns in bloomfilter datamap
        List<String> columnsToDrop = (List<String>) targets[0];
        List<String> indexedColumnNames = dataMapMeta.getIndexedColumnNames();
        for (String indexedcolumn : indexedColumnNames) {
          for (String column : columnsToDrop) {
            if (column.equalsIgnoreCase(indexedcolumn)) {
              return true;
            }
          }
        }
        return false;
      }
      case ALTER_CHANGE_DATATYPE:
      case ALTER_COLUMN_RENAME: {
        // alter table change one column datatype, or rename
        // will be blocked if the column in bloomfilter datamap
        String columnToChangeDatatype = (String) targets[0];
        List<String> indexedColumnNames = dataMapMeta.getIndexedColumnNames();
        for (String indexedcolumn : indexedColumnNames) {
          if (indexedcolumn.equalsIgnoreCase(columnToChangeDatatype)) {
            return true;
          }
        }
        return false;
      }
      default:
        return false;
    }
  }

  @Override
  public DataMapMeta getMeta() {
    return this.dataMapMeta;
  }

  @Override
  public DataMapLevel getDataMapLevel() {
    return DataMapLevel.CG;
  }

  @Override
  public String getCacheSize() {
    long sum = 0L;
    for (Map.Entry<String, Set<String>> entry : segmentMap.entrySet()) {
      for (String shardName : entry.getValue()) {
        for (CarbonColumn carbonColumn : dataMapMeta.getIndexedColumns()) {
          BloomCacheKeyValue.CacheValue cacheValue = cache
              .getIfPresent(new BloomCacheKeyValue.CacheKey(shardName, carbonColumn.getColName()));
          if (cacheValue != null) {
            sum += cacheValue.getMemorySize();
          }
        }
      }
    } return 0 + ":" + sum;
  }
}
