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
package org.apache.carbondata.core.datamap;

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
import org.apache.carbondata.core.datamap.dev.BlockletSerializer;
import org.apache.carbondata.core.datamap.dev.DataMap;
import org.apache.carbondata.core.datamap.dev.DataMapFactory;
import org.apache.carbondata.core.datamap.dev.cgdatamap.CoarseGrainDataMap;
import org.apache.carbondata.core.datamap.dev.fgdatamap.FineGrainBlocklet;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.impl.FileFactory;
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
 * Index at the table level, user can add any number of DataMap for one table, by
 * {@code
 *   CREATE DATAMAP dm ON TABLE table
 *   USING 'class name of DataMapFactory implementation'
 * }
 * Depends on the filter condition it can prune the data (blocklet or row level).
 */
@InterfaceAudience.Internal
public final class TableDataMap extends OperationEventListener {

  private CarbonTable table;

  private AbsoluteTableIdentifier identifier;

  private DataMapSchema dataMapSchema;

  private DataMapFactory dataMapFactory;

  private BlockletDetailsFetcher blockletDetailsFetcher;

  private SegmentPropertiesFetcher segmentPropertiesFetcher;

  private static final Logger LOG =
      LogServiceFactory.getLogService(TableDataMap.class.getName());

  /**
   * It is called to initialize and load the required table datamap metadata.
   */
  TableDataMap(CarbonTable table, DataMapSchema dataMapSchema,
      DataMapFactory dataMapFactory, BlockletDetailsFetcher blockletDetailsFetcher,
      SegmentPropertiesFetcher segmentPropertiesFetcher) {
    this.identifier = table.getAbsoluteTableIdentifier();
    this.table = table;
    this.dataMapSchema = dataMapSchema;
    this.dataMapFactory = dataMapFactory;
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
   * Pass the valid segments and prune the datamap using filter expression
   *
   * @param segments
   * @param filter
   * @return
   */
  public List<ExtendedBlocklet> prune(List<Segment> segments, final DataMapFilter filter,
      final List<PartitionSpec> partitions) throws IOException {
    final List<ExtendedBlocklet> blocklets = new ArrayList<>();
    final Map<Segment, List<DataMap>> dataMaps = dataMapFactory.getDataMaps(segments);
    // for non-filter queries
    // for filter queries
    int totalFiles = 0;
    int datamapsCount = 0;
    for (Segment segment : segments) {
      for (DataMap dataMap: dataMaps.get(segment)) {
        totalFiles += dataMap.getNumberOfEntries();
        datamapsCount++;
      }
    }
    int numOfThreadsForPruning = CarbonProperties.getNumOfThreadsForPruning();
    if (numOfThreadsForPruning == 1 || datamapsCount < numOfThreadsForPruning || totalFiles
        < CarbonCommonConstants.CARBON_DRIVER_PRUNING_MULTI_THREAD_ENABLE_FILES_COUNT) {
      // use multi-thread, only if the files are more than 0.1 million.
      // As 0.1 million files block pruning can take only 1 second.
      // Doing multi-thread for smaller values is not recommended as
      // driver should have minimum threads opened to support multiple concurrent queries.
      if (filter.isEmpty()) {
        // if filter is not passed, then return all the blocklets.
        return pruneWithoutFilter(segments, partitions, blocklets);
      }
      return pruneWithFilter(segments, filter, partitions, blocklets, dataMaps);
    }
    // handle by multi-thread
    List<ExtendedBlocklet> extendedBlocklets = pruneMultiThread(
        segments, filter, partitions, blocklets, dataMaps, totalFiles);
    return extendedBlocklets;
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

  private List<ExtendedBlocklet> pruneWithFilter(List<Segment> segments, DataMapFilter filter,
      List<PartitionSpec> partitions, List<ExtendedBlocklet> blocklets,
      Map<Segment, List<DataMap>> dataMaps) throws IOException {
    for (Segment segment : segments) {
      List<Blocklet> pruneBlocklets = new ArrayList<>();
      SegmentProperties segmentProperties = segmentPropertiesFetcher.getSegmentProperties(segment);
      if (filter.isResolvedOnSegment(segmentProperties)) {
        for (DataMap dataMap : dataMaps.get(segment)) {
          pruneBlocklets.addAll(
              dataMap.prune(filter.getResolver(), segmentProperties, partitions));
        }
      } else {
        for (DataMap dataMap : dataMaps.get(segment)) {
          pruneBlocklets.addAll(
              dataMap.prune(filter.getExpression(), segmentProperties, partitions, table));
        }
      }
      blocklets.addAll(
          addSegmentId(blockletDetailsFetcher.getExtendedBlocklets(pruneBlocklets, segment),
              segment));
    }
    return blocklets;
  }

  private List<ExtendedBlocklet> pruneMultiThread(List<Segment> segments,
      final DataMapFilter filter, final List<PartitionSpec> partitions,
      List<ExtendedBlocklet> blocklets, final Map<Segment, List<DataMap>> dataMaps,
      int totalFiles) {
    /*
     *********************************************************************************
     * Below is the example of how this part of code works.
     * consider a scenario of having 5 segments, 10 datamaps in each segment,
     * and each datamap has one record. So total 50 records.
     *
     * Datamaps in each segment looks like below.
     * s0 [0-9], s1 [0-9], s2 [0-9], s3[0-9], s4[0-9]
     *
     * If number of threads are 4. so filesPerEachThread = 50/4 = 12 files per each thread.
     *
     * SegmentDataMapGroup look like below: [SegmentId, fromIndex, toIndex]
     * In each segment only those datamaps are processed between fromIndex and toIndex.
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
    LOG.info(
        "Number of threads selected for multi-thread block pruning is " + numOfThreadsForPruning
            + ". total files: " + totalFiles + ". total segments: " + segments.size());
    int filesPerEachThread = totalFiles / numOfThreadsForPruning;
    int prev;
    int filesCount = 0;
    int processedFileCount = 0;
    List<List<SegmentDataMapGroup>> datamapListForEachThread =
        new ArrayList<>(numOfThreadsForPruning);
    List<SegmentDataMapGroup> segmentDataMapGroupList = new ArrayList<>();
    for (Segment segment : segments) {
      List<DataMap> eachSegmentDataMapList = dataMaps.get(segment);
      prev = 0;
      for (int i = 0; i < eachSegmentDataMapList.size(); i++) {
        DataMap dataMap = eachSegmentDataMapList.get(i);
        filesCount += dataMap.getNumberOfEntries();
        if (filesCount >= filesPerEachThread) {
          if (datamapListForEachThread.size() != numOfThreadsForPruning - 1) {
            // not the last segmentList
            segmentDataMapGroupList.add(new SegmentDataMapGroup(segment, prev, i));
            // save the last value to process in next thread
            prev = i + 1;
            datamapListForEachThread.add(segmentDataMapGroupList);
            segmentDataMapGroupList = new ArrayList<>();
            processedFileCount += filesCount;
            filesCount = 0;
          } else {
            // add remaining in the end
            processedFileCount += filesCount;
            filesCount = 0;
          }
        }
      }
      if (prev == 0 || prev != eachSegmentDataMapList.size()) {
        // if prev == 0. Add a segment's all datamaps
        // eachSegmentDataMapList.size() != prev, adding the last remaining datamaps of this segment
        segmentDataMapGroupList
            .add(new SegmentDataMapGroup(segment, prev, eachSegmentDataMapList.size() - 1));
      }
    }
    // adding the last segmentList data
    datamapListForEachThread.add(segmentDataMapGroupList);
    processedFileCount += filesCount;
    if (processedFileCount != totalFiles) {
      // this should not happen
      throw new RuntimeException(" not all the files processed ");
    }
    List<Future<Void>> results = new ArrayList<>(numOfThreadsForPruning);
    final Map<Segment, List<ExtendedBlocklet>> prunedBlockletMap =
        new ConcurrentHashMap<>(segments.size());
    final ExecutorService executorService = Executors.newFixedThreadPool(numOfThreadsForPruning);
    final String threadName = Thread.currentThread().getName();
    for (int i = 0; i < numOfThreadsForPruning; i++) {
      final List<SegmentDataMapGroup> segmentDataMapGroups = datamapListForEachThread.get(i);
      results.add(executorService.submit(new Callable<Void>() {
        @Override public Void call() throws IOException {
          Thread.currentThread().setName(threadName);
          for (SegmentDataMapGroup segmentDataMapGroup : segmentDataMapGroups) {
            List<ExtendedBlocklet> pruneBlocklets = new ArrayList<>();
            List<DataMap> dataMapList = dataMaps.get(segmentDataMapGroup.getSegment());
            SegmentProperties segmentProperties =
                segmentPropertiesFetcher.getSegmentPropertiesFromDataMap(dataMapList.get(0));
            Segment segment = segmentDataMapGroup.getSegment();
            if (filter.isResolvedOnSegment(segmentProperties)) {
              for (int i = segmentDataMapGroup.getFromIndex();
                   i <= segmentDataMapGroup.getToIndex(); i++) {
                List<Blocklet> dmPruneBlocklets = dataMapList.get(i).prune(
                    filter.getResolver(), segmentProperties, partitions);
                pruneBlocklets.addAll(addSegmentId(
                    blockletDetailsFetcher.getExtendedBlocklets(dmPruneBlocklets, segment),
                    segment));
              }
            } else {
              for (int i = segmentDataMapGroup.getFromIndex();
                   i <= segmentDataMapGroup.getToIndex(); i++) {
                List<Blocklet> dmPruneBlocklets = dataMapList.get(i).prune(
                    filter.getExpression(), segmentProperties, partitions, table);
                pruneBlocklets.addAll(addSegmentId(
                    blockletDetailsFetcher.getExtendedBlocklets(dmPruneBlocklets, segment),
                    segment));
              }
            }
            synchronized (prunedBlockletMap) {
              List<ExtendedBlocklet> pruneBlockletsExisting =
                  prunedBlockletMap.get(segmentDataMapGroup.getSegment());
              if (pruneBlockletsExisting != null) {
                pruneBlockletsExisting.addAll(pruneBlocklets);
              } else {
                prunedBlockletMap.put(segmentDataMapGroup.getSegment(), pruneBlocklets);
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
      LOG.error("Error in pruning datamap in multi-thread: " + e.getMessage());
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
   * This is used for making the datamap distributable.
   * It takes the valid segments and returns all the datamaps as distributable objects so that
   * it can be distributed across machines.
   *
   * @return
   */
  public List<DataMapDistributable> toDistributable(List<Segment> segments) throws IOException {
    List<DataMapDistributable> distributables = new ArrayList<>();
    for (Segment segment : segments) {
      distributables.addAll(dataMapFactory.toDistributable(segment));
    }
    return distributables;
  }

  /**
   * This method returns all the datamaps corresponding to the distributable object
   *
   * @param distributable
   * @return
   * @throws IOException
   */
  public List<DataMap> getTableDataMaps(DataMapDistributable distributable) throws IOException {
    return dataMapFactory.getDataMaps(distributable);
  }

  /**
   * This method is used from any machine after it is distributed. It takes the distributable object
   * to prune the filters.
   *
   * @param distributable
   * @param filterExp
   * @return
   */
  public List<ExtendedBlocklet> prune(List<DataMap> dataMaps, DataMapDistributable distributable,
      FilterResolverIntf filterExp, List<PartitionSpec> partitions) throws IOException {
    List<ExtendedBlocklet> detailedBlocklets = new ArrayList<>();
    List<Blocklet> blocklets = new ArrayList<>();
    for (DataMap dataMap : dataMaps) {
      blocklets.addAll(dataMap.prune(filterExp,
          segmentPropertiesFetcher.getSegmentProperties(distributable.getSegment()),
          partitions));
    }
    BlockletSerializer serializer = new BlockletSerializer();
    String writePath =
        identifier.getTablePath() + CarbonCommonConstants.FILE_SEPARATOR + dataMapSchema
            .getDataMapName();
    if (dataMapFactory.getDataMapLevel() == DataMapLevel.FG) {
      FileFactory.mkdirs(writePath, FileFactory.getFileType(writePath));
    }
    for (Blocklet blocklet : blocklets) {
      ExtendedBlocklet detailedBlocklet = blockletDetailsFetcher
          .getExtendedBlocklet(blocklet, distributable.getSegment());
      if (dataMapFactory.getDataMapLevel() == DataMapLevel.FG) {
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
   * Clear only the datamaps of the segments
   * @param segmentIds list of segmentIds to be cleared from cache.
   */
  public void clear(List<String> segmentIds) {
    for (String segment: segmentIds) {
      dataMapFactory.clear(segment);
    }
  }

  /**
   * Clears all datamap
   */
  public void clear() {
    if (null != dataMapFactory) {
      dataMapFactory.clear();
    }
  }

  /**
   * delete only the datamaps of the segments
   */
  public void deleteDatamapData(List<Segment> segments) throws IOException {
    for (Segment segment: segments) {
      dataMapFactory.deleteDatamapData(segment);
    }
  }
  /**
   * delete datamap data if any
   */
  public void deleteDatamapData() {
    dataMapFactory.deleteDatamapData();
  }

  /**
   * delete datamap data for a segment if any
   */
  public void deleteSegmentDatamapData(String segmentNo) throws IOException {
    dataMapFactory.deleteSegmentDatamapData(segmentNo);
  }

  public DataMapSchema getDataMapSchema() {
    return dataMapSchema;
  }

  public DataMapFactory getDataMapFactory() {
    return dataMapFactory;
  }

  @Override public void onEvent(Event event, OperationContext opContext) throws Exception {
    dataMapFactory.fireEvent(event);
  }

  /**
   * Prune the datamap of the given segments and return the Map of blocklet path and row count
   *
   * @param segments
   * @param partitions
   * @return
   * @throws IOException
   */
  public Map<String, Long> getBlockRowCount(List<Segment> segments,
      final List<PartitionSpec> partitions, TableDataMap defaultDataMap)
      throws IOException {
    Map<String, Long> blockletToRowCountMap = new HashMap<>();
    for (Segment segment : segments) {
      List<CoarseGrainDataMap> dataMaps = defaultDataMap.getDataMapFactory().getDataMaps(segment);
      for (CoarseGrainDataMap dataMap : dataMaps) {
        dataMap.getRowCountForEachBlock(segment, partitions, blockletToRowCountMap);
      }
    }
    return blockletToRowCountMap;
  }

  /**
   * Prune the datamap of the given segments and return the Map of blocklet path and row count
   *
   * @param segments
   * @param partitions
   * @return
   * @throws IOException
   */
  public long getRowCount(List<Segment> segments, final List<PartitionSpec> partitions,
      TableDataMap defaultDataMap) throws IOException {
    long totalRowCount = 0L;
    for (Segment segment : segments) {
      List<CoarseGrainDataMap> dataMaps = defaultDataMap.getDataMapFactory().getDataMaps(segment);
      for (CoarseGrainDataMap dataMap : dataMaps) {
        totalRowCount += dataMap.getRowCount(segment, partitions);
      }
    }
    return totalRowCount;
  }

}
