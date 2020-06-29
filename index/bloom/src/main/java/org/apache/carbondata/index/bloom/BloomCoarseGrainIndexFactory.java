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

package org.apache.carbondata.index.bloom;

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
import org.apache.carbondata.common.exceptions.sql.MalformedIndexCommandException;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.constants.CarbonV3DataFormatConstants;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.features.TableOperation;
import org.apache.carbondata.core.index.IndexInputSplit;
import org.apache.carbondata.core.index.IndexLevel;
import org.apache.carbondata.core.index.IndexMeta;
import org.apache.carbondata.core.index.Segment;
import org.apache.carbondata.core.index.dev.IndexBuilder;
import org.apache.carbondata.core.index.dev.IndexFactory;
import org.apache.carbondata.core.index.dev.IndexWriter;
import org.apache.carbondata.core.index.dev.cgindex.CoarseGrainIndex;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.IndexSchema;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.events.Event;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * This class is for Bloom Filter for blocklet level
 */
@InterfaceAudience.Internal
public class BloomCoarseGrainIndexFactory extends IndexFactory<CoarseGrainIndex> {
  private static final Logger LOGGER = LogServiceFactory.getLogService(
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704
      BloomCoarseGrainIndexFactory.class.getName());
  /**
   * property for size of bloom filter
   */
  private static final String BLOOM_SIZE = "bloom_size";
  /**
   * default size for bloom filter, cardinality of the column.
   */
  private static final int DEFAULT_BLOOM_FILTER_SIZE =
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2790
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

  private IndexMeta indexMeta;
  private String indexName;
  private int bloomFilterSize;
  private double bloomFilterFpp;
  private boolean bloomCompress;
  private Cache<BloomCacheKeyValue.CacheKey, BloomCacheKeyValue.CacheValue> cache;
  // segmentId -> list of index file
  private Map<String, Set<String>> segmentMap = new ConcurrentHashMap<>();

  public BloomCoarseGrainIndexFactory(CarbonTable carbonTable, IndexSchema indexSchema)
      throws MalformedIndexCommandException {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
    super(carbonTable, indexSchema);
    Objects.requireNonNull(carbonTable);
    Objects.requireNonNull(indexSchema);

    this.indexName = indexSchema.getIndexName();

    List<CarbonColumn> indexedColumns =
        carbonTable.getIndexedColumns(indexSchema.getIndexColumns());
    this.bloomFilterSize = validateAndGetBloomFilterSize(indexSchema);
    this.bloomFilterFpp = validateAndGetBloomFilterFpp(indexSchema);
    this.bloomCompress = validateAndGetBloomCompress(indexSchema);
    List<ExpressionType> optimizedOperations = new ArrayList<ExpressionType>();
    // todo: support more optimize operations
    optimizedOperations.add(ExpressionType.EQUALS);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2655
    optimizedOperations.add(ExpressionType.IN);
    this.indexMeta = new IndexMeta(this.indexName, indexedColumns, optimizedOperations);
    LOGGER.info(String.format("Index %s works for %s with bloom size %d",
        this.indexName, this.indexMeta, this.bloomFilterSize));
    try {
      this.cache = CacheProvider.getInstance()
          .createCache(new CacheType("bloom_cache"), BloomIndexCache.class.getName());
    } catch (Exception e) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3107
      LOGGER.error(e.getMessage(), e);
      throw new MalformedIndexCommandException(e.getMessage());
    }
  }

  /**
   * validate Lucene Index BLOOM_SIZE
   * 1. BLOOM_SIZE property is optional, 32000 * 20 will be the default size.
   * 2. BLOOM_SIZE should be an integer that greater than 0
   */
  private int validateAndGetBloomFilterSize(IndexSchema dmSchema)
      throws MalformedIndexCommandException {
    String bloomFilterSizeStr = dmSchema.getProperties().get(BLOOM_SIZE);
    if (StringUtils.isBlank(bloomFilterSizeStr)) {
      LOGGER.warn(
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
          String.format("Bloom filter size is not configured for index %s, use default value %d",
              indexName, DEFAULT_BLOOM_FILTER_SIZE));
      return DEFAULT_BLOOM_FILTER_SIZE;
    }
    int bloomFilterSize;
    try {
      bloomFilterSize = Integer.parseInt(bloomFilterSizeStr);
    } catch (NumberFormatException e) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704
      throw new MalformedIndexCommandException(
          String.format("Invalid value of bloom filter size '%s', it should be an integer",
              bloomFilterSizeStr));
    }
    // todo: reconsider the boundaries of bloom filter size
    if (bloomFilterSize <= 0) {
      throw new MalformedIndexCommandException(
          String.format("Invalid value of bloom filter size '%s', it should be greater than 0",
              bloomFilterSizeStr));
    }
    return bloomFilterSize;
  }

  /**
   * validate bloom Index BLOOM_FPP
   * 1. BLOOM_FPP property is optional, 0.00001 will be the default value.
   * 2. BLOOM_FPP should be (0, 1)
   */
  private double validateAndGetBloomFilterFpp(IndexSchema dmSchema)
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704
      throws MalformedIndexCommandException {
    String bloomFilterFppStr = dmSchema.getProperties().get(BLOOM_FPP);
    if (StringUtils.isBlank(bloomFilterFppStr)) {
      LOGGER.warn(
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
          String.format("Bloom filter FPP is not configured for index %s, use default value %f",
              indexName, DEFAULT_BLOOM_FILTER_FPP));
      return DEFAULT_BLOOM_FILTER_FPP;
    }
    double bloomFilterFpp;
    try {
      bloomFilterFpp = Double.parseDouble(bloomFilterFppStr);
    } catch (NumberFormatException e) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704
      throw new MalformedIndexCommandException(
          String.format("Invalid value of bloom filter fpp '%s', it should be an numeric",
              bloomFilterFppStr));
    }
    if (bloomFilterFpp < 0 || bloomFilterFpp - 1 >= 0) {
      throw new MalformedIndexCommandException(
          String.format("Invalid value of bloom filter fpp '%s', it should be in range 0~1",
              bloomFilterFppStr));
    }
    return bloomFilterFpp;
  }

  /**
   * validate bloom Index COMPRESS_BLOOM
   * Default value is true
   */
  private boolean validateAndGetBloomCompress(IndexSchema dmSchema) {
    String bloomCompress = dmSchema.getProperties().get(COMPRESS_BLOOM);
    if (StringUtils.isBlank(bloomCompress)) {
      LOGGER.warn(
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
          String.format("Bloom compress is not configured for index %s, use default value %b",
              indexName, DEFAULT_BLOOM_COMPRESS));
      return DEFAULT_BLOOM_COMPRESS;
    }
    return Boolean.parseBoolean(bloomCompress);
  }

  @Override
  public IndexWriter createWriter(Segment segment, String shardName,
      SegmentProperties segmentProperties) throws IOException {
    LOGGER.info(
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
        String.format("Data of BloomCoarseGrainIndex %s for table %s will be written to %s",
            this.indexName, getCarbonTable().getTableName() , shardName));
    return new BloomIndexWriter(getCarbonTable().getTablePath(), this.indexName,
        this.indexMeta.getIndexedColumns(), segment, shardName, this.bloomFilterSize,
        this.bloomFilterFpp, bloomCompress);
  }

  @Override
  public IndexBuilder createBuilder(Segment segment, String shardName,
      SegmentProperties segmentProperties) throws IOException {
    return new BloomIndexBuilder(getCarbonTable().getTablePath(), this.indexName,
        this.indexMeta.getIndexedColumns(), segment, shardName, this.bloomFilterSize,
        this.bloomFilterFpp, bloomCompress);
  }

  /**
   * returns all shard directories of bloom index files for query
   * if bloom index files are merged we should get only one shard path
   */
  public static Set<String> getAllShardPaths(String tablePath, String segmentId, String indexName) {
    String indexStorePath = CarbonTablePath.getIndexesStorePath(
        tablePath, segmentId, indexName);
    CarbonFile[] carbonFiles = FileFactory.getCarbonFile(indexStorePath).listFiles();
    Set<String> shardPaths = new HashSet<>();
    boolean mergeShardInProgress = false;
    CarbonFile mergeShardFile = null;
    for (CarbonFile carbonFile : carbonFiles) {
      if (carbonFile.getName().equals(BloomIndexFileStore.MERGE_BLOOM_INDEX_SHARD_NAME)) {
        mergeShardFile = carbonFile;
      } else if (carbonFile.getName().equals(BloomIndexFileStore.MERGE_INPROGRESS_FILE)) {
        mergeShardInProgress = true;
      } else if (carbonFile.isDirectory()) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2980
        shardPaths.add(FileFactory.getPath(carbonFile.getAbsolutePath()).toString());
      }
    }
    if (mergeShardFile != null && !mergeShardInProgress) {
      // should only get one shard path if mergeShard is generated successfully
      shardPaths.clear();
      shardPaths.add(FileFactory.getPath(mergeShardFile.getAbsolutePath()).toString());
    }
    return shardPaths;
  }

  @Override
  public List<CoarseGrainIndex> getIndexes(Segment segment) throws IOException {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
    List<CoarseGrainIndex> indexes = new ArrayList<>();
    try {
      Set<String> shardPaths = segmentMap.get(segment.getSegmentNo());
      if (shardPaths == null) {
        shardPaths =
            getAllShardPaths(getCarbonTable().getTablePath(), segment.getSegmentNo(), indexName);
        segmentMap.put(segment.getSegmentNo(), shardPaths);
      }
      Set<String> filteredShards = segment.getFilteredIndexShardNames();
      for (String shard : shardPaths) {
        if (shard.endsWith(BloomIndexFileStore.MERGE_BLOOM_INDEX_SHARD_NAME) ||
            filteredShards.contains(new File(shard).getName())) {
          // Filter out the tasks which are filtered through Main index.
          // for merge shard, shard pruning delay to be done before pruning blocklet
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704
          BloomCoarseGrainIndex bloomDM = new BloomCoarseGrainIndex();
          bloomDM.init(new BloomIndexModel(shard, cache, segment.getConfiguration()));
          bloomDM.initIndexColumnConverters(getCarbonTable(), indexMeta.getIndexedColumns());
          bloomDM.setFilteredShard(filteredShards);
          indexes.add(bloomDM);
        }
      }
    } catch (Exception e) {
      throw new IOException("Error occurs while init Bloom Index", e);
    }
    return indexes;
  }

  @Override
  public List<CoarseGrainIndex> getIndexes(Segment segment, Set<Path> partitionLocations)
      throws IOException {
    return getIndexes(segment);
  }

  @Override
  public List<CoarseGrainIndex> getIndexes(IndexInputSplit distributable) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
    List<CoarseGrainIndex> indexes = new ArrayList<>();
    String indexPath = ((BloomIndexInputSplit) distributable).getIndexPath();
    Set<String> filteredShards = ((BloomIndexInputSplit) distributable).getFilteredShards();
    BloomCoarseGrainIndex bloomDM = new BloomCoarseGrainIndex();
    bloomDM.init(new BloomIndexModel(indexPath, cache, FileFactory.getConfiguration()));
    bloomDM.initIndexColumnConverters(getCarbonTable(), indexMeta.getIndexedColumns());
    bloomDM.setFilteredShard(filteredShards);
    indexes.add(bloomDM);
    return indexes;
  }

  @Override
  public List<IndexInputSplit> toDistributable(Segment segment) {
    List<IndexInputSplit> indexInputSplitList = new ArrayList<>();
    Set<String> shardPaths = segmentMap.get(segment.getSegmentNo());
    if (shardPaths == null) {
      shardPaths =
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
          getAllShardPaths(getCarbonTable().getTablePath(), segment.getSegmentNo(), indexName);
      segmentMap.put(segment.getSegmentNo(), shardPaths);
    }
    Set<String> filteredShards = segment.getFilteredIndexShardNames();
    for (String shardPath : shardPaths) {
      // Filter out the tasks which are filtered through Main index.
      // for merge shard, shard pruning delay to be done before pruning blocklet
      if (shardPath.endsWith(BloomIndexFileStore.MERGE_BLOOM_INDEX_SHARD_NAME) ||
          filteredShards.contains(new File(shardPath).getName())) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704
        IndexInputSplit bloomIndexInputSplit =
            new BloomIndexInputSplit(shardPath, filteredShards);
        bloomIndexInputSplit.setSegment(segment);
        bloomIndexInputSplit.setIndexSchema(getIndexSchema());
        indexInputSplitList.add(bloomIndexInputSplit);
      }
    }
    return indexInputSplitList;
  }

  @Override
  public void fireEvent(Event event) {

  }

  @Override
  public void clear(String segment) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3337
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3306
    Set<String> shards = segmentMap.remove(segment);
    if (shards != null) {
      for (String shard : shards) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704
        for (CarbonColumn carbonColumn : indexMeta.getIndexedColumns()) {
          cache.invalidate(new BloomCacheKeyValue.CacheKey(shard, carbonColumn.getColName()));
        }
      }
    }
  }

  @Override
  public synchronized void clear() {
    if (segmentMap.size() > 0) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2702
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2496
      List<String> segments = new ArrayList<>(segmentMap.keySet());
      for (String segmentId : segments) {
        clear(segmentId);
      }
    }
  }

  @Override
  public void deleteIndexData(Segment segment) throws IOException {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704
    deleteSegmentIndexData(segment.getSegmentNo());
  }

  @Override
  public void deleteSegmentIndexData(String segmentId) throws IOException {
    try {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
      String indexPath = CarbonTablePath
          .getIndexesStorePath(getCarbonTable().getTablePath(), segmentId, indexName);
      if (FileFactory.isFileExist(indexPath)) {
        CarbonFile file = FileFactory.getCarbonFile(indexPath);
        CarbonUtil.deleteFoldersAndFilesSilent(file);
      }
      clear(segmentId);
    } catch (InterruptedException ex) {
      throw new IOException("Failed to delete index for segment_" + segmentId);
    }
  }

  @Override
  public void deleteIndexData() {
    SegmentStatusManager ssm =
        new SegmentStatusManager(getCarbonTable().getAbsoluteTableIdentifier());
    try {
      List<Segment> validSegments =
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704
          ssm.getValidAndInvalidSegments(getCarbonTable().isMV()).getValidSegments();
      for (Segment segment : validSegments) {
        deleteIndexData(segment);
      }
    } catch (IOException e) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
      LOGGER.error("drop index failed, failed to delete index directory");
    }
  }

  @Override
  public boolean willBecomeStale(TableOperation operation) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2693
    switch (operation) {
      case ALTER_RENAME:
        return false;
      case ALTER_DROP:
        return true;
      case ALTER_ADD_COLUMN:
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2727
        return false;
      case ALTER_CHANGE_DATATYPE:
        return true;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3149
      case ALTER_COLUMN_RENAME:
        return true;
      case STREAMING:
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2823
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2698
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2700
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2732
    switch (operation) {
      case ALTER_DROP: {
        // alter table drop columns
        // will be blocked if the columns in bloomfilter index
        List<String> columnsToDrop = (List<String>) targets[0];
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704
        List<String> indexedColumnNames = indexMeta.getIndexedColumnNames();
        for (String indexedcolumn : indexedColumnNames) {
          for (String column : columnsToDrop) {
            if (column.equalsIgnoreCase(indexedcolumn)) {
              return true;
            }
          }
        }
        return false;
      }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3149
      case ALTER_CHANGE_DATATYPE:
      case ALTER_COLUMN_RENAME: {
        // alter table change one column datatype, or rename
        // will be blocked if the column in bloomfilter index
        String columnToChangeDatatype = (String) targets[0];
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704
        List<String> indexedColumnNames = indexMeta.getIndexedColumnNames();
        for (String indexedcolumn : indexedColumnNames) {
          if (indexedcolumn.equalsIgnoreCase(columnToChangeDatatype)) {
            return true;
          }
        }
        return false;
      }
      default:
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2347
        return false;
    }
  }

  @Override
  public IndexMeta getMeta() {
    return this.indexMeta;
  }

  @Override
  public IndexLevel getIndexLevel() {
    return IndexLevel.CG;
  }

  @Override
  public String getCacheSize() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3398
    long sum = 0L;
    for (Map.Entry<String, Set<String>> entry : segmentMap.entrySet()) {
      for (String shardName : entry.getValue()) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704
        for (CarbonColumn carbonColumn : indexMeta.getIndexedColumns()) {
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
