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

package org.apache.carbondata.core.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.index.dev.BlockletSerializer;
import org.apache.carbondata.core.index.dev.Index;
import org.apache.carbondata.core.index.dev.IndexFactory;
import org.apache.carbondata.core.index.dev.cgindex.CoarseGrainIndex;
import org.apache.carbondata.core.index.dev.expr.IndexInputSplitWrapper;
import org.apache.carbondata.core.index.dev.fgindex.FineGrainBlocklet;
import org.apache.carbondata.core.indexstore.Blocklet;
import org.apache.carbondata.core.indexstore.BlockletDetailsFetcher;
import org.apache.carbondata.core.indexstore.ExtendedBlocklet;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.indexstore.SegmentPropertiesFetcher;
import org.apache.carbondata.core.indexstore.blockletindex.BlockIndex;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.IndexSchema;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.FilterUtil;
import org.apache.carbondata.core.scan.filter.executer.FilterExecutor;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.events.Event;
import org.apache.carbondata.events.OperationContext;
import org.apache.carbondata.events.OperationEventListener;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * Index at the table level, user can add any number of Index for one table, by
 * {@code
 *   CREATE INDEX index ON table
 *   USING 'IndexProvider'
 * }
 * Depends on the filter condition it can prune the data (blocklet or row level).
 */
@InterfaceAudience.Internal
public final class TableIndex extends OperationEventListener {

  private CarbonTable table;

  private AbsoluteTableIdentifier identifier;

  private IndexSchema indexSchema;

  private IndexFactory indexFactory;

  private BlockletDetailsFetcher blockletDetailsFetcher;

  private SegmentPropertiesFetcher segmentPropertiesFetcher;

  private static final Logger LOG =
      LogServiceFactory.getLogService(TableIndex.class.getName());

  /**
   * It is called to initialize and load the required table index metadata.
   */
  TableIndex(CarbonTable table, IndexSchema indexSchema,
      IndexFactory indexFactory, BlockletDetailsFetcher blockletDetailsFetcher,
      SegmentPropertiesFetcher segmentPropertiesFetcher) {
    this.identifier = table.getAbsoluteTableIdentifier();
    this.table = table;
    this.indexSchema = indexSchema;
    this.indexFactory = indexFactory;
    this.blockletDetailsFetcher = blockletDetailsFetcher;
    this.segmentPropertiesFetcher = segmentPropertiesFetcher;
  }

  public BlockletDetailsFetcher getBlockletDetailsFetcher() {
    return blockletDetailsFetcher;
  }

  public CarbonTable getTable() {
    return table;
  }

  /**
   * Pass the valid segments and prune the index using filter expression
   *
   * @param allSegments
   * @param filter
   * @return
   */
  public List<ExtendedBlocklet> prune(List<Segment> allSegments, final IndexFilter filter,
      final List<PartitionSpec> partitions) throws IOException {
    final List<ExtendedBlocklet> blocklets = new ArrayList<>();
    List<Segment> segments = getCarbonSegments(allSegments);
    final Map<Segment, List<Index>> indexes;
    boolean isFilterPresent = filter != null && !filter.isEmpty();
    Set<Path> partitionLocations = getPartitionLocations(partitions);
    if (table.isHivePartitionTable() && isFilterPresent && !partitionLocations.isEmpty()) {
      indexes = indexFactory.getIndexes(segments, partitionLocations, filter);
    } else {
      indexes = indexFactory.getIndexes(segments, filter);
    }

    if (indexes.isEmpty()) {
      return blocklets;
    }
    // for non-filter queries
    // for filter queries
    int totalFiles = 0;
    int indexesCount = 0;
    // In case if filter is present, then update the segments with index's segment list
    // based on segment or partition pruning
    if (isFilterPresent) {
      segments = new ArrayList<>(indexes.keySet());
    }
    for (Segment segment : segments) {
      for (Index index : indexes.get(segment)) {
        totalFiles += index.getNumberOfEntries();
        indexesCount++;
      }
    }
    int numOfThreadsForPruning = CarbonProperties.getNumOfThreadsForPruning();
    int carbonDriverPruningMultiThreadEnableFilesCount =
        CarbonProperties.getDriverPruningMultiThreadEnableFilesCount();
    // when the query is without filter, as we need to return all the blocklets,
    // so no need of multi-thread pruning
    if (numOfThreadsForPruning == 1 || indexesCount < numOfThreadsForPruning || totalFiles
            < carbonDriverPruningMultiThreadEnableFilesCount || !isFilterPresent) {
      // use multi-thread, only if the files are more than 0.1 million.
      // As 0.1 million files block pruning can take only 1 second.
      // Doing multi-thread for smaller values is not recommended as
      // driver should have minimum threads opened to support multiple concurrent queries.
      if (!isFilterPresent) {
        // if filter is not passed, then return all the blocklets.
        return pruneWithoutFilter(segments, partitionLocations, blocklets);
      }
      return pruneWithFilter(segments, filter, partitionLocations, blocklets, indexes);
    }
    // handle by multi-thread
    return pruneMultiThread(segments, filter, blocklets, indexes, totalFiles);
  }

  private List<Segment> getCarbonSegments(List<Segment> allSegments) {
    List<Segment> segments = new ArrayList<>();
    for (Segment segment : allSegments) {
      if (segment.isCarbonSegment()) {
        segments.add(segment);
      }
    }
    return segments;
  }

  private List<ExtendedBlocklet> pruneWithoutFilter(List<Segment> segments,
      Set<Path> partitionLocations, List<ExtendedBlocklet> blocklets) throws IOException {
    for (Segment segment : segments) {
      List<Blocklet> allBlocklets =
          blockletDetailsFetcher.getAllBlocklets(segment, partitionLocations);
      blocklets.addAll(
          addSegmentId(blockletDetailsFetcher.getExtendedBlocklets(allBlocklets, segment),
              segment));
    }
    return blocklets;
  }

  private Set<Path> getPartitionLocations(List<PartitionSpec> partitionSpecs) {
    Set<Path> partitionsLocations = new HashSet<>();
    if (null != partitionSpecs) {
      for (PartitionSpec partitionSpec : partitionSpecs) {
        partitionsLocations.add(partitionSpec.getLocation());
      }
    }
    return partitionsLocations;
  }

  private List<ExtendedBlocklet> pruneWithFilter(List<Segment> segments, IndexFilter filter,
      Set<Path> partitionLocations, List<ExtendedBlocklet> blocklets,
      Map<Segment, List<Index>> indexes) throws IOException {
    Set<String> missingSISegments = filter.getMissingSISegments();
    for (Segment segment : segments) {
      List<Index> segmentIndices = indexes.get(segment);
      if (segment == null ||
          segmentIndices == null || segmentIndices.isEmpty()) {
        continue;
      }
      boolean isExternalOrMissingSISegment = segment.isExternalSegment() ||
          (missingSISegments != null && missingSISegments.contains(segment.getSegmentNo()));
      List<Blocklet> pruneBlocklets = new ArrayList<>();
      SegmentProperties segmentProperties;
      if (segmentIndices.get(0) instanceof BlockIndex) {
        segmentProperties =
            segmentPropertiesFetcher.getSegmentPropertiesFromIndex(segmentIndices.get(0));
      } else {
        segmentProperties =
            segmentPropertiesFetcher.getSegmentProperties(segment, partitionLocations);
      }
      if (filter.isResolvedOnSegment(segmentProperties)) {
        FilterExecutor filterExecutor;
        if (!isExternalOrMissingSISegment) {
          filterExecutor = FilterUtil
              .getFilterExecutorTree(filter.getResolver(), segmentProperties, null,
                  table.getMinMaxCacheColumns(segmentProperties), false);
        } else {
          filterExecutor = FilterUtil
              .getFilterExecutorTree(filter.getExternalSegmentResolver(), segmentProperties, null,
                  table.getMinMaxCacheColumns(segmentProperties), false);
        }
        for (Index index : segmentIndices) {
          if (!isExternalOrMissingSISegment) {
            pruneBlocklets.addAll(index
                .prune(filter.getResolver(), segmentProperties, filterExecutor, this.table));
          } else {
            pruneBlocklets.addAll(index
                .prune(filter.getExternalSegmentResolver(), segmentProperties, filterExecutor,
                    this.table));
          }
        }
      } else {
        FilterExecutor filterExecutor;
        Expression expression = filter.getExpression();
        if (!isExternalOrMissingSISegment) {
          filterExecutor = FilterUtil.getFilterExecutorTree(
              new IndexFilter(segmentProperties, table, expression).getResolver(),
              segmentProperties, null, table.getMinMaxCacheColumns(segmentProperties), false);
        } else {
          filterExecutor = FilterUtil.getFilterExecutorTree(
              new IndexFilter(segmentProperties, table, expression).getExternalSegmentResolver(),
              segmentProperties, null, table.getMinMaxCacheColumns(segmentProperties), false);
        }
        for (Index index : segmentIndices) {
          if (!isExternalOrMissingSISegment) {
            pruneBlocklets.addAll(index.prune(
                filter.getExpression(), segmentProperties, table, filterExecutor));
          } else {
            pruneBlocklets.addAll(index.prune(
                filter.getExternalSegmentFilter(), segmentProperties, table, filterExecutor));
          }
        }
      }
      blocklets.addAll(
          addSegmentId(blockletDetailsFetcher.getExtendedBlocklets(pruneBlocklets, segment),
              segment));
    }
    return blocklets;
  }

  private List<ExtendedBlocklet> pruneMultiThread(List<Segment> segments,
      final IndexFilter filter, List<ExtendedBlocklet> blocklets,
      final Map<Segment, List<Index>> indexes, int totalFiles) {
    /*
     *********************************************************************************
     * Below is the example of how this part of code works.
     * consider a scenario of having 5 segments, 10 indexes in each segment,
     * and each index has one record. So total 50 records.
     *
     * indexes in each segment looks like below.
     * s0 [0-9], s1 [0-9], s2 [0-9], s3[0-9], s4[0-9]
     *
     * If number of threads are 4. so filesPerEachThread = 50/4 = 12 files per each thread.
     *
     * SegmentIndexGroup look like below: [SegmentId, fromIndex, toIndex]
     * In each segment only those indexes are processed between fromIndex and toIndex.
     *
     * Final result will be: (4 list created as numOfThreadsForPruning is 4)
     * Thread1 list: s0 [0-9], s1 [0-1]  : 12 files
     * Thread2 list: s1 [2-9], s2 [0-3]  : 12 files
     * Thread3 list: s2 [4-9], s3 [0-5]  : 12 files
     * Thread4 list: s3 [6-9], s4 [0-9]  : 14 files
     * so each thread will process almost equal number of records.
     *
     *********************************************************************************
     */

    int numOfThreadsForPruning = CarbonProperties.getNumOfThreadsForPruning();
    int filesPerEachThread = totalFiles / numOfThreadsForPruning;
    int prev;
    int filesCount = 0;
    int processedFileCount = 0;
    List<List<SegmentIndexGroup>> indexListForEachThread =
        new ArrayList<>(numOfThreadsForPruning);
    List<SegmentIndexGroup> segmentIndexGroupList = new ArrayList<>();
    Set<String> missingSISegments = filter.getMissingSISegments();
    for (Segment segment : segments) {
      List<Index> eachSegmentIndexList = indexes.get(segment);
      prev = 0;
      for (int i = 0; i < eachSegmentIndexList.size(); i++) {
        Index index = eachSegmentIndexList.get(i);
        filesCount += index.getNumberOfEntries();
        if (filesCount >= filesPerEachThread) {
          if (indexListForEachThread.size() != numOfThreadsForPruning - 1) {
            // not the last segmentList
            segmentIndexGroupList.add(new SegmentIndexGroup(segment, prev, i));
            // save the last value to process in next thread
            prev = i + 1;
            indexListForEachThread.add(segmentIndexGroupList);
            segmentIndexGroupList = new ArrayList<>();
            processedFileCount += filesCount;
            filesCount = 0;
          } else {
            // add remaining in the end
            processedFileCount += filesCount;
            filesCount = 0;
          }
        }
      }
      if (prev == 0 || prev != eachSegmentIndexList.size()) {
        // if prev == 0. Add a segment's all indexes
        // eachSegmentIndexList.size() != prev, adding the last remaining indexes of this segment
        segmentIndexGroupList
            .add(new SegmentIndexGroup(segment, prev, eachSegmentIndexList.size() - 1));
      }
    }
    // adding the last segmentList data
    indexListForEachThread.add(segmentIndexGroupList);
    processedFileCount += filesCount;
    if (processedFileCount != totalFiles) {
      // this should not happen
      throw new RuntimeException(" not all the files processed ");
    }
    if (indexListForEachThread.size() < numOfThreadsForPruning) {
      // If the total indexes fitted in lesser number of threads than numOfThreadsForPruning.
      // Launch only that many threads where indexes are fitted while grouping.
      LOG.info("indexes is distributed in " + indexListForEachThread.size() + " threads");
      numOfThreadsForPruning = indexListForEachThread.size();
    }
    LOG.info(
        "Number of threads selected for multi-thread block pruning is " + numOfThreadsForPruning
            + ". total files: " + totalFiles + ". total segments: " + segments.size());
    List<Future<Void>> results = new ArrayList<>(numOfThreadsForPruning);
    final Map<Segment, List<ExtendedBlocklet>> prunedBlockletMap =
        new ConcurrentHashMap<>(segments.size());
    final ExecutorService executorService = Executors.newFixedThreadPool(numOfThreadsForPruning);
    final String threadName = Thread.currentThread().getName();
    for (int i = 0; i < numOfThreadsForPruning; i++) {
      final List<SegmentIndexGroup> segmentIndexGroups = indexListForEachThread.get(i);
      results.add(executorService.submit(new Callable<Void>() {
        @Override
        public Void call() throws IOException {
          Thread.currentThread().setName(threadName);
          for (SegmentIndexGroup segmentIndexGroup : segmentIndexGroups) {
            List<ExtendedBlocklet> pruneBlocklets = new ArrayList<>();
            List<Index> indexList = indexes.get(segmentIndexGroup.getSegment());
            SegmentProperties segmentProperties =
                segmentPropertiesFetcher.getSegmentPropertiesFromIndex(indexList.get(0));
            Segment segment = segmentIndexGroup.getSegment();
            boolean isExternalOrMissingSISegment = segment.getSegmentPath() != null ||
                (missingSISegments != null && missingSISegments.contains(segment.getSegmentNo()));
            if (filter.isResolvedOnSegment(segmentProperties)) {
              FilterExecutor filterExecutor;
              if (!isExternalOrMissingSISegment) {
                filterExecutor = FilterUtil
                    .getFilterExecutorTree(filter.getResolver(), segmentProperties, null,
                        table.getMinMaxCacheColumns(segmentProperties), false);
              } else {
                filterExecutor = FilterUtil
                    .getFilterExecutorTree(filter.getExternalSegmentResolver(), segmentProperties,
                        null, table.getMinMaxCacheColumns(segmentProperties), false);
              }
              for (int i = segmentIndexGroup.getFromIndex();
                   i <= segmentIndexGroup.getToIndex(); i++) {
                List<Blocklet> dmPruneBlocklets;
                if (!isExternalOrMissingSISegment) {
                  dmPruneBlocklets = indexList.get(i)
                      .prune(filter.getResolver(), segmentProperties, filterExecutor, table);
                } else {
                  dmPruneBlocklets = indexList.get(i)
                      .prune(filter.getExternalSegmentResolver(), segmentProperties, filterExecutor,
                          table);
                }
                pruneBlocklets.addAll(addSegmentId(
                    blockletDetailsFetcher.getExtendedBlocklets(dmPruneBlocklets, segment),
                    segment));
              }
            } else {
              Expression filterExpression = filter.getNewCopyOfExpression();
              FilterExecutor filterExecutor;
              if (!isExternalOrMissingSISegment) {
                filterExecutor = FilterUtil.getFilterExecutorTree(
                    new IndexFilter(segmentProperties, table, filterExpression).getResolver(),
                    segmentProperties, null, table.getMinMaxCacheColumns(segmentProperties), false);
              } else {
                filterExecutor = FilterUtil.getFilterExecutorTree(
                    new IndexFilter(segmentProperties, table, filterExpression)
                        .getExternalSegmentResolver(), segmentProperties, null,
                    table.getMinMaxCacheColumns(segmentProperties), false);
              }
              for (int i = segmentIndexGroup.getFromIndex();
                   i <= segmentIndexGroup.getToIndex(); i++) {
                List<Blocklet> dmPruneBlocklets;
                if (!isExternalOrMissingSISegment) {
                  dmPruneBlocklets = indexList.get(i)
                      .prune(filterExpression, segmentProperties, table, filterExecutor);
                } else {
                  dmPruneBlocklets = indexList.get(i)
                      .prune(filter.getExternalSegmentFilter(), segmentProperties, table,
                          filterExecutor);
                }
                pruneBlocklets.addAll(addSegmentId(
                    blockletDetailsFetcher.getExtendedBlocklets(dmPruneBlocklets, segment),
                    segment));
              }
            }
            synchronized (prunedBlockletMap) {
              List<ExtendedBlocklet> pruneBlockletsExisting =
                  prunedBlockletMap.get(segmentIndexGroup.getSegment());
              if (pruneBlockletsExisting != null) {
                pruneBlockletsExisting.addAll(pruneBlocklets);
              } else {
                prunedBlockletMap.put(segmentIndexGroup.getSegment(), pruneBlocklets);
              }
            }
          }
          return null;
        }
      }));
    }
    executorService.shutdown();
    try {
      executorService.awaitTermination(2, TimeUnit.HOURS);
    } catch (InterruptedException e) {
      LOG.error("Error in pruning index in multi-thread: " + e.getMessage());
    }
    // check for error
    for (Future<Void> result : results) {
      try {
        result.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    }
    for (Map.Entry<Segment, List<ExtendedBlocklet>> entry : prunedBlockletMap.entrySet()) {
      blocklets.addAll(entry.getValue());
    }
    return blocklets;
  }

  private List<ExtendedBlocklet> addSegmentId(List<ExtendedBlocklet> pruneBlocklets,
      Segment segment) {
    for (ExtendedBlocklet blocklet : pruneBlocklets) {
      blocklet.setSegment(segment);
    }
    return pruneBlocklets;
  }

  /**
   * This is used for making the index distributable.
   * It takes the valid segments and returns all the indexes as distributable objects so that
   * it can be distributed across machines.
   *
   * @return
   */
  public List<IndexInputSplit> toDistributable(List<Segment> allSegments) {
    List<IndexInputSplit> distributableList = new ArrayList<>();
    List<Segment> segments = getCarbonSegments(allSegments);
    for (Segment segment : segments) {
      distributableList.addAll(indexFactory.toDistributable(segment));
    }
    return distributableList;
  }

  public IndexInputSplitWrapper toDistributableSegment(Segment segment, String uniqueId) {
    return indexFactory.toDistributableSegment(segment, indexSchema, identifier, uniqueId);
  }

  /**
   * This method returns all the indexes corresponding to the distributable object
   *
   * @param distributable
   * @return
   * @throws IOException
   */
  public List<Index> getTableIndexes(IndexInputSplit distributable) throws IOException {
    return indexFactory.getIndexes(distributable);
  }

  /**
   * This method is used from any machine after it is distributed. It takes the distributable object
   * to prune the filters.
   *
   * @param distributable
   * @param filterExp
   * @return
   */
  public List<ExtendedBlocklet> prune(List<Index> indices, IndexInputSplit distributable,
      FilterResolverIntf filterExp, List<PartitionSpec> partitions) throws IOException {
    List<ExtendedBlocklet> detailedBlocklets = new ArrayList<>();
    List<Blocklet> blocklets = new ArrayList<>();
    Set<Path> partitionsToPrune = getPartitionLocations(partitions);
    SegmentProperties segmentProperties = segmentPropertiesFetcher
        .getSegmentProperties(distributable.getSegment(), partitionsToPrune);
    FilterExecutor filterExecutor = FilterUtil
        .getFilterExecutorTree(filterExp, segmentProperties,
            null, table.getMinMaxCacheColumns(segmentProperties),
            false);
    for (Index index : indices) {
      blocklets.addAll(index.prune(filterExp, segmentProperties, filterExecutor, table));
    }
    BlockletSerializer serializer = new BlockletSerializer();
    String writePath =
        identifier.getTablePath() + CarbonCommonConstants.FILE_SEPARATOR + indexSchema
            .getIndexName();
    if (indexFactory.getIndexLevel() == IndexLevel.FG) {
      FileFactory.mkdirs(writePath);
    }
    for (Blocklet blocklet : blocklets) {
      ExtendedBlocklet detailedBlocklet = blockletDetailsFetcher
          .getExtendedBlocklet(blocklet, distributable.getSegment());
      if (indexFactory.getIndexLevel() == IndexLevel.FG) {
        String blockletWritePath =
            writePath + CarbonCommonConstants.FILE_SEPARATOR + System.nanoTime();
        detailedBlocklet.setIndexWriterPath(blockletWritePath);
        serializer.serializeBlocklet((FineGrainBlocklet) blocklet, blockletWritePath);
      }
      detailedBlocklet.setSegment(distributable.getSegment());
      detailedBlocklets.add(detailedBlocklet);
    }
    return detailedBlocklets;
  }

  /**
   * Clear only the indexes of the segments
   * @param segmentIds list of segmentIds to be cleared from cache.
   */
  public void clear(List<String> segmentIds) {
    for (String segment: segmentIds) {
      indexFactory.clear(segment);
    }
  }

  /**
   * Clears all index
   */
  public void clear() {
    if (null != indexFactory) {
      indexFactory.clear();
    }
  }

  /**
   * delete only the index of the segments
   */
  public void deleteIndexData(List<Segment> allSegments) throws IOException {
    List<Segment> segments = getCarbonSegments(allSegments);
    for (Segment segment: segments) {
      indexFactory.deleteIndexData(segment);
    }
  }

  /**
   * delete index data if any
   */
  public void deleteIndexData() {
    indexFactory.deleteIndexData();
  }

  /**
   * delete index data for a segment if any
   */
  public void deleteSegmentIndexData(String segmentNo) throws IOException {
    indexFactory.deleteSegmentIndexData(segmentNo);
  }

  public IndexSchema getIndexSchema() {
    return indexSchema;
  }

  public IndexFactory getIndexFactory() {
    return indexFactory;
  }

  @Override
  public void onEvent(Event event, OperationContext opContext) {
    indexFactory.fireEvent(event);
  }

  /**
   * Prune the index of the given segments and return the Map of blocklet path and row count
   *
   * @param allSegments
   * @param partitions
   * @return
   * @throws IOException
   */
  public Map<String, Long> getBlockRowCount(List<Segment> allSegments,
      final List<PartitionSpec> partitions, TableIndex defaultIndex)
      throws IOException {
    List<Segment> segments = getCarbonSegments(allSegments);
    Map<String, Long> blockletToRowCountMap = new HashMap<>();
    for (Segment segment : segments) {
      List<CoarseGrainIndex> indexes = defaultIndex.getIndexFactory().getIndexes(segment);
      for (CoarseGrainIndex index : indexes) {
        if (null != partitions && !partitions.isEmpty()) {
          // if it has partitioned index but there is no partitioned information stored, it means
          // partitions are dropped so return empty list.
          if (index.validatePartitionInfo(partitions)) {
            return new HashMap<>();
          }
        }
        index.getRowCountForEachBlock(segment, partitions, blockletToRowCountMap);
      }
    }
    return blockletToRowCountMap;
  }

  /**
   * Get the mapping of blocklet path and row count for all blocks. This method skips the
   * validation of partition info for countStar job with index server enabled.
   */
  public Map<String, Long> getBlockRowCount(TableIndex defaultIndex, List<Segment> allSegments,
      final List<PartitionSpec> partitions) throws IOException {
    List<Segment> segments = getCarbonSegments(allSegments);
    Map<String, Long> blockletToRowCountMap = new HashMap<>();
    for (Segment segment : segments) {
      List<CoarseGrainIndex> indexes = defaultIndex.getIndexFactory().getIndexes(segment);
      for (CoarseGrainIndex index : indexes) {
        index.getRowCountForEachBlock(segment, partitions, blockletToRowCountMap);
      }
    }
    return blockletToRowCountMap;
  }

  /**
   * Prune the index of the given segments and return the Map of blocklet path and row count
   *
   * @param allSegments
   * @param partitions
   * @return
   * @throws IOException
   */
  public long getRowCount(List<Segment> allSegments, final List<PartitionSpec> partitions,
      TableIndex defaultIndex) throws IOException {
    List<Segment> segments = getCarbonSegments(allSegments);
    long totalRowCount = 0L;
    for (Segment segment : segments) {
      List<CoarseGrainIndex> indexes = defaultIndex.getIndexFactory().getIndexes(segment);
      for (CoarseGrainIndex index : indexes) {
        totalRowCount += index.getRowCount(segment, partitions);
      }
    }
    return totalRowCount;
  }

  /**
   * Method to prune the segments based on task min/max values
   *
   */
  public List<Segment> pruneSegments(List<Segment> segments, FilterResolverIntf filterExp)
      throws IOException {
    List<Segment> prunedSegments = new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    for (Segment segment : segments) {
      List<Index> indices = indexFactory.getIndexes(segment);
      for (Index index : indices) {
        if (index.isScanRequired(filterExp)) {
          // If any one task in a given segment contains the data that means the segment need to
          // be scanned and we need to validate further indexes in the same segment
          prunedSegments.add(segment);
          break;
        }
      }
    }
    return prunedSegments;
  }

}
