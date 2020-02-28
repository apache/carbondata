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
import java.util.List;
import java.util.Map;
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
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.index.dev.BlockletSerializer;
import org.apache.carbondata.core.index.dev.Index;
import org.apache.carbondata.core.index.dev.IndexFactory;
import org.apache.carbondata.core.index.dev.cgindex.CoarseGrainIndex;
import org.apache.carbondata.core.index.dev.expr.IndexDistributableWrapper;
import org.apache.carbondata.core.index.dev.fgindex.FineGrainBlocklet;
import org.apache.carbondata.core.indexstore.Blocklet;
import org.apache.carbondata.core.indexstore.BlockletDetailsFetcher;
import org.apache.carbondata.core.indexstore.ExtendedBlocklet;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.indexstore.SegmentPropertiesFetcher;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.events.Event;
import org.apache.carbondata.events.OperationContext;
import org.apache.carbondata.events.OperationEventListener;

import org.apache.log4j.Logger;

/**
 * Index at the table level, user can add any number of Index for one table, by
 * {@code
 *   CREATE INDEX dm ON TABLE table
 *   USING 'IndexProvider'
 * }
 * Depends on the filter condition it can prune the data (blocklet or row level).
 */
@InterfaceAudience.Internal
public final class TableIndex extends OperationEventListener {

  private CarbonTable table;

  private AbsoluteTableIdentifier identifier;

  private DataMapSchema dataMapSchema;

  private IndexFactory indexFactory;

  private BlockletDetailsFetcher blockletDetailsFetcher;

  private SegmentPropertiesFetcher segmentPropertiesFetcher;

  private static final Logger LOG =
      LogServiceFactory.getLogService(TableIndex.class.getName());

  /**
   * It is called to initialize and load the required table index metadata.
   */
  public TableIndex(CarbonTable table, DataMapSchema dataMapSchema, IndexFactory indexFactory,
      BlockletDetailsFetcher blockletDetailsFetcher,
      SegmentPropertiesFetcher segmentPropertiesFetcher) {
    this.identifier = table.getAbsoluteTableIdentifier();
    this.table = table;
    this.dataMapSchema = dataMapSchema;
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
   * Pass the valid segments and prune the segments using filter expression
   *
   * @param allSegments
   * @param filter
   * @return
   */
  public List<ExtendedBlocklet> prune(List<Segment> allSegments, final IndexFilter filter,
      final List<PartitionSpec> partitions) throws IOException {
    final List<ExtendedBlocklet> blocklets = new ArrayList<>();
    List<Segment> segments = getCarbonSegments(allSegments);
    final Map<Segment, List<Index>> indexMap;
    if (filter == null || filter.isEmpty() || partitions == null || partitions.isEmpty()) {
      indexMap = indexFactory.getIndexes(segments);
    } else {
      indexMap = indexFactory.getIndexes(segments, partitions);
    }
    // for non-filter queries
    // for filter queries
    int totalFiles = 0;
    int indexCount = 0;
    // In case if filter has matched partitions, then update the segments with index's
    // segment list, as getIndexes will return segments that matches the partition.
    if (null != partitions && !partitions.isEmpty()) {
      segments = new ArrayList<>(indexMap.keySet());
    }
    for (Segment segment : segments) {
      for (Index index : indexMap.get(segment)) {
        totalFiles += index.getNumberOfEntries();
        indexCount++;
      }
    }
    int numOfThreadsForPruning = CarbonProperties.getNumOfThreadsForPruning();
    if (numOfThreadsForPruning == 1 || indexCount < numOfThreadsForPruning || totalFiles
        < CarbonCommonConstants.CARBON_DRIVER_PRUNING_MULTI_THREAD_ENABLE_FILES_COUNT) {
      // use multi-thread, only if the files are more than 0.1 million.
      // As 0.1 million files block pruning can take only 1 second.
      // Doing multi-thread for smaller values is not recommended as
      // driver should have minimum threads opened to support multiple concurrent queries.
      if (filter == null || filter.isEmpty()) {
        // if filter is not passed, then return all the blocklets.
        return pruneWithoutFilter(segments, partitions, blocklets);
      }
      return pruneWithFilter(segments, filter, partitions, blocklets, indexMap);
    }
    // handle by multi-thread
    List<ExtendedBlocklet> extendedBlocklets = pruneMultiThread(
        segments, filter, partitions, blocklets, indexMap, totalFiles);
    return extendedBlocklets;
  }

  private List<Segment> getCarbonSegments(List<Segment> allsegments) {
    List<Segment> segments = new ArrayList<>();
    for (Segment segment : allsegments) {
      if (segment.isCarbonSegment()) {
        segments.add(segment);
      }
    }
    return segments;
  }

  private List<ExtendedBlocklet> pruneWithoutFilter(List<Segment> segments,
      List<PartitionSpec> partitions, List<ExtendedBlocklet> blocklets) throws IOException {
    for (Segment segment : segments) {
      List<Blocklet> allBlocklets = blockletDetailsFetcher.getAllBlocklets(segment, partitions);
      blocklets.addAll(
          addSegmentId(blockletDetailsFetcher.getExtendedBlocklets(allBlocklets, segment),
              segment));
    }
    return blocklets;
  }

  private List<ExtendedBlocklet> pruneWithFilter(List<Segment> segments, IndexFilter filter,
      List<PartitionSpec> partitions, List<ExtendedBlocklet> blocklets,
      Map<Segment, List<Index>> indexMap) throws IOException {
    for (Segment segment : segments) {
      List<Blocklet> pruneBlocklets = new ArrayList<>();
      SegmentProperties segmentProperties =
          segmentPropertiesFetcher.getSegmentProperties(segment, partitions);
      if (filter.isResolvedOnSegment(segmentProperties)) {
        for (Index index : indexMap.get(segment)) {
          pruneBlocklets.addAll(
              index.prune(filter.getResolver(), segmentProperties, partitions));
        }
      } else {
        for (Index index : indexMap.get(segment)) {
          pruneBlocklets.addAll(
              index.prune(filter.getExpression(), segmentProperties, partitions, table));
        }
      }
      blocklets.addAll(
          addSegmentId(blockletDetailsFetcher.getExtendedBlocklets(pruneBlocklets, segment),
              segment));
    }
    return blocklets;
  }

  private List<ExtendedBlocklet> pruneMultiThread(List<Segment> segments,
      final IndexFilter filter, final List<PartitionSpec> partitions,
      List<ExtendedBlocklet> blocklets, final Map<Segment, List<Index>> indexes,
      int totalFiles) {
    /*
     *********************************************************************************
     * Below is the example of how this part of code works.
     * consider a scenario of having 5 segments, 10 indexes in each segment,
     * and each index has one record. So total 50 records.
     *
     * Index in each segment looks like below.
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
      LOG.info("Index is distributed in " + indexListForEachThread.size() + " threads");
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
                segmentPropertiesFetcher.getSegmentPropertiesFromDataMap(indexList.get(0));
            Segment segment = segmentIndexGroup.getSegment();
            if (filter.isResolvedOnSegment(segmentProperties)) {
              for (int i = segmentIndexGroup.getFromIndex();
                   i <= segmentIndexGroup.getToIndex(); i++) {
                List<Blocklet> dmPruneBlocklets = indexList.get(i).prune(
                    filter.getResolver(), segmentProperties, partitions);
                pruneBlocklets.addAll(addSegmentId(
                    blockletDetailsFetcher.getExtendedBlocklets(dmPruneBlocklets, segment),
                    segment));
              }
            } else {
              for (int i = segmentIndexGroup.getFromIndex();
                   i <= segmentIndexGroup.getToIndex(); i++) {
                List<Blocklet> dmPruneBlocklets = indexList.get(i).prune(
                    filter.getNewCopyOfExpression(), segmentProperties, partitions, table);
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
  public List<IndexDistributable> toDistributable(List<Segment> allsegments) {
    List<IndexDistributable> distributables = new ArrayList<>();
    List<Segment> segments = getCarbonSegments(allsegments);
    for (Segment segment : segments) {
      distributables.addAll(indexFactory.toDistributable(segment));
    }
    return distributables;
  }

  public IndexDistributableWrapper toDistributableSegment(Segment segment, String uniqueId) {
    return indexFactory.toDistributableSegment(segment, dataMapSchema, identifier, uniqueId);
  }

  /**
   * This method returns all the indexes corresponding to the distributable object
   *
   * @param distributable
   * @return
   * @throws IOException
   */
  public List<Index> getTableIndexes(IndexDistributable distributable) throws IOException {
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
  public List<ExtendedBlocklet> prune(List<Index> indices, IndexDistributable distributable,
      FilterResolverIntf filterExp, List<PartitionSpec> partitions) throws IOException {
    List<ExtendedBlocklet> detailedBlocklets = new ArrayList<>();
    List<Blocklet> blocklets = new ArrayList<>();
    for (Index index : indices) {
      blocklets.addAll(index.prune(filterExp,
          segmentPropertiesFetcher.getSegmentProperties(distributable.getSegment(), partitions),
          partitions));
    }
    BlockletSerializer serializer = new BlockletSerializer();
    String writePath =
        identifier.getTablePath() + CarbonCommonConstants.FILE_SEPARATOR + dataMapSchema
            .getDataMapName();
    if (indexFactory.getIndexLevel() == IndexLevel.FG) {
      FileFactory.mkdirs(writePath);
    }
    for (Blocklet blocklet : blocklets) {
      ExtendedBlocklet detailedBlocklet = blockletDetailsFetcher
          .getExtendedBlocklet(blocklet, distributable.getSegment());
      if (indexFactory.getIndexLevel() == IndexLevel.FG) {
        String blockletwritePath =
            writePath + CarbonCommonConstants.FILE_SEPARATOR + System.nanoTime();
        detailedBlocklet.setDataMapWriterPath(blockletwritePath);
        serializer.serializeBlocklet((FineGrainBlocklet) blocklet, blockletwritePath);
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
   * Clears all indexes
   */
  public void clear() {
    if (null != indexFactory) {
      indexFactory.clear();
    }
  }

  /**
   * delete only the indexes of the segments
   */
  public void deleteIndexData(List<Segment> allsegments) throws IOException {
    List<Segment> segments = getCarbonSegments(allsegments);
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

  public DataMapSchema getDataMapSchema() {
    return dataMapSchema;
  }

  public IndexFactory getIndexFactory() {
    return indexFactory;
  }

  @Override
  public void onEvent(Event event, OperationContext opContext) {
    indexFactory.fireEvent(event);
  }

  /**
   * Prune the given segments and return the Map of blocklet path and row count
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
        index.getRowCountForEachBlock(segment, partitions, blockletToRowCountMap);
      }
    }
    return blockletToRowCountMap;
  }

  /**
   * Prune the given segments by index and return the Map of blocklet path and row count
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
      List<Index> indexes = indexFactory.getIndexes(segment);
      for (Index index : indexes) {
        if (index.isScanRequired(filterExp)) {
          // If any one task in a given segment contains the data that means the segment need to
          // be scanned and we need to validate further data maps in the same segment
          prunedSegments.add(segment);
          break;
        }
      }
    }
    return prunedSegments;
  }

}
