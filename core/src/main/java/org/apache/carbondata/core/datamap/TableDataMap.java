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
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.dev.BlockletSerializer;
import org.apache.carbondata.core.datamap.dev.DataMap;
import org.apache.carbondata.core.datamap.dev.DataMapFactory;
import org.apache.carbondata.core.datamap.dev.fgdatamap.FineGrainBlocklet;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.indexstore.Blocklet;
import org.apache.carbondata.core.indexstore.BlockletDetailsFetcher;
import org.apache.carbondata.core.indexstore.ExtendedBlocklet;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.indexstore.SegmentPropertiesFetcher;
import org.apache.carbondata.core.indexstore.blockletindex.BlockDataMap;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.events.Event;
import org.apache.carbondata.events.OperationContext;
import org.apache.carbondata.events.OperationEventListener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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

  private AbsoluteTableIdentifier identifier;

  private DataMapSchema dataMapSchema;

  private DataMapFactory dataMapFactory;

  private BlockletDetailsFetcher blockletDetailsFetcher;

  private SegmentPropertiesFetcher segmentPropertiesFetcher;

  private static final Log LOG = LogFactory.getLog(TableDataMap.class);

  /**
   * It is called to initialize and load the required table datamap metadata.
   */
  TableDataMap(AbsoluteTableIdentifier identifier, DataMapSchema dataMapSchema,
      DataMapFactory dataMapFactory, BlockletDetailsFetcher blockletDetailsFetcher,
      SegmentPropertiesFetcher segmentPropertiesFetcher) {
    this.identifier = identifier;
    this.dataMapSchema = dataMapSchema;
    this.dataMapFactory = dataMapFactory;
    this.blockletDetailsFetcher = blockletDetailsFetcher;
    this.segmentPropertiesFetcher = segmentPropertiesFetcher;
  }

  public BlockletDetailsFetcher getBlockletDetailsFetcher() {
    return blockletDetailsFetcher;
  }


  /**
   * Pass the valid segments and prune the datamap using filter expression
   *
   * @param segments
   * @param filterExp
   * @return
   */
  public List<ExtendedBlocklet> prune(List<Segment> segments, Expression filterExp,
      List<PartitionSpec> partitions) throws IOException {
    List<ExtendedBlocklet> blocklets = new ArrayList<>();
    SegmentProperties segmentProperties;
    Map<Segment, List<DataMap>> dataMaps = dataMapFactory.getDataMaps(segments);
    for (Segment segment : segments) {
      List<Blocklet> pruneBlocklets = new ArrayList<>();
      // if filter is not passed then return all the blocklets
      if (filterExp == null) {
        pruneBlocklets = blockletDetailsFetcher.getAllBlocklets(segment, partitions);
      } else {
        segmentProperties = segmentPropertiesFetcher.getSegmentProperties(segment);
        for (DataMap dataMap : dataMaps.get(segment)) {
          pruneBlocklets
              .addAll(dataMap.prune(filterExp, segmentProperties, partitions, identifier));
        }
      }
      blocklets.addAll(addSegmentId(
          blockletDetailsFetcher.getExtendedBlocklets(pruneBlocklets, segment),
          segment.toString()));
    }
    return blocklets;
  }

  /**
   * Pass the valid segments and prune the datamap using filter expression
   *
   * @param segments
   * @param filterExp
   * @return
   */
  public List<ExtendedBlocklet> prune(List<Segment> segments, final FilterResolverIntf filterExp,
      final List<PartitionSpec> partitions) throws IOException {
    final List<ExtendedBlocklet> blocklets = new ArrayList<>();
    final Map<Segment, List<DataMap>> dataMaps = dataMapFactory.getDataMaps(segments);
    // for non-filter queries
    if (filterExp == null) {
      // if filter is not passed, then return all the blocklets.
      return pruneWithoutFilter(segments, partitions, blocklets);
    }
    // for filter queries
    int totalFiles = 0;
    int datamapsCount = 0;
    int filesCountPerDatamap;
    boolean isBlockDataMapType = true;
    for (Segment segment : segments) {
      for (DataMap dataMap : dataMaps.get(segment)) {
        if (!(dataMap instanceof BlockDataMap)) {
          isBlockDataMapType = false;
          break;
        }
        filesCountPerDatamap = ((BlockDataMap) dataMap).getTotalBlocks();
        // old legacy store can give 0, so consider one datamap as 1 record.
        totalFiles += (filesCountPerDatamap == 0) ? 1 : filesCountPerDatamap;
        datamapsCount++;
      }
      if (!isBlockDataMapType) {
        // totalFiles fill be 0 for non-BlockDataMap Type. ex: lucene, bloom datamap. use old flow.
        break;
      }
    }
    int numOfThreadsForPruning = getNumOfThreadsForPruning();
    if (numOfThreadsForPruning == 1 || datamapsCount < numOfThreadsForPruning || totalFiles
        < CarbonCommonConstants.CARBON_DRIVER_PRUNING_MULTI_THREAD_ENABLE_FILES_COUNT) {
      // use multi-thread, only if the files are more than 0.1 million.
      // As 0.1 million files block pruning can take only 1 second.
      // Doing multi-thread for smaller values is not recommended as
      // driver should have minimum threads opened to support multiple concurrent queries.
      return pruneWithFilter(segments, filterExp, partitions, blocklets, dataMaps);
    }
    // handle by multi-thread
    return pruneWithFilterMultiThread(segments, filterExp, partitions, blocklets, dataMaps,
        totalFiles);
  }

  private List<ExtendedBlocklet> pruneWithoutFilter(List<Segment> segments,
      List<PartitionSpec> partitions, List<ExtendedBlocklet> blocklets) throws IOException {
    for (Segment segment : segments) {
      List<Blocklet> allBlocklets = blockletDetailsFetcher.getAllBlocklets(segment, partitions);
      blocklets.addAll(
          addSegmentId(blockletDetailsFetcher.getExtendedBlocklets(allBlocklets, segment),
              segment.toString()));
    }
    return blocklets;
  }

  private List<ExtendedBlocklet> pruneWithFilter(List<Segment> segments,
      FilterResolverIntf filterExp, List<PartitionSpec> partitions,
      List<ExtendedBlocklet> blocklets, Map<Segment, List<DataMap>> dataMaps) throws IOException {
    for (Segment segment : segments) {
      List<Blocklet> pruneBlocklets = new ArrayList<>();
      SegmentProperties segmentProperties = segmentPropertiesFetcher.getSegmentProperties(segment);
      for (DataMap dataMap : dataMaps.get(segment)) {
        pruneBlocklets.addAll(dataMap.prune(filterExp, segmentProperties, partitions));
      }
      blocklets.addAll(
          addSegmentId(blockletDetailsFetcher.getExtendedBlocklets(pruneBlocklets, segment),
              segment.toString()));
    }
    return blocklets;
  }

  private List<ExtendedBlocklet> pruneWithFilterMultiThread(List<Segment> segments,
      final FilterResolverIntf filterExp, final List<PartitionSpec> partitions,
      List<ExtendedBlocklet> blocklets, final Map<Segment, List<DataMap>> dataMaps,
      int totalFiles) {
    int numOfThreadsForPruning = getNumOfThreadsForPruning();
    LOG.info(
        "Number of threads selected for multi-thread block pruning is " + numOfThreadsForPruning
            + ". total files: " + totalFiles + ". total segments: " + segments.size());
    int filesPerEachThread = totalFiles / numOfThreadsForPruning;
    int prev;
    int filesCount = 0;
    int processedFileCount = 0;
    int filesCountPerDatamap;
    List<List<SegmentDataMapGroup>> segmentList = new ArrayList<>(numOfThreadsForPruning);
    List<SegmentDataMapGroup> segmentDataMapGroupList = new ArrayList<>();
    for (Segment segment : segments) {
      List<DataMap> eachSegmentDataMapList = dataMaps.get(segment);
      prev = 0;
      for (int i = 0; i < eachSegmentDataMapList.size(); i++) {
        DataMap dataMap = eachSegmentDataMapList.get(i);
        filesCountPerDatamap = ((BlockDataMap) dataMap).getTotalBlocks();
        // old legacy store can give 0, so consider one datamap as 1 record.
        filesCount += (filesCountPerDatamap == 0) ? 1 : filesCountPerDatamap;
        if (filesCount >= filesPerEachThread) {
          if (segmentList.size() != numOfThreadsForPruning - 1) {
            // not the last segmentList
            segmentDataMapGroupList.add(new SegmentDataMapGroup(segment, prev, i));
            // save the last value to process in next thread
            prev = i + 1;
            segmentList.add(segmentDataMapGroupList);
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
    segmentList.add(segmentDataMapGroupList);
    processedFileCount += filesCount;
    if (processedFileCount != totalFiles) {
      // this should not happen
      throw new RuntimeException(" not all the files processed ");
    }
    List<Future<Void>> results = new ArrayList<>(numOfThreadsForPruning);
    final Map<Segment, List<Blocklet>> prunedBlockletMap = new ConcurrentHashMap<>(segments.size());
    final ExecutorService executorService = Executors.newFixedThreadPool(numOfThreadsForPruning);
    final String threadName = Thread.currentThread().getName();
    for (int i = 0; i < numOfThreadsForPruning; i++) {
      final List<SegmentDataMapGroup> segmentDataMapGroups = segmentList.get(i);
      results.add(executorService.submit(new Callable<Void>() {
        @Override public Void call() throws IOException {
          Thread.currentThread().setName(threadName);
          for (SegmentDataMapGroup segmentDataMapGroup : segmentDataMapGroups) {
            List<Blocklet> pruneBlocklets = new ArrayList<>();
            List<DataMap> dataMapList = dataMaps.get(segmentDataMapGroup.getSegment());
            for (int i = segmentDataMapGroup.getFromIndex();
                 i <= segmentDataMapGroup.getToIndex(); i++) {
              pruneBlocklets.addAll(dataMapList.get(i).prune(filterExp,
                  segmentPropertiesFetcher.getSegmentProperties(segmentDataMapGroup.getSegment()),
                  partitions));
            }
            synchronized (prunedBlockletMap) {
              List<Blocklet> pruneBlockletsExisting =
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
    for (Map.Entry<Segment, List<Blocklet>> entry : prunedBlockletMap.entrySet()) {
      try {
        blocklets.addAll(addSegmentId(
            blockletDetailsFetcher.getExtendedBlocklets(entry.getValue(), entry.getKey()),
            entry.getKey().toString()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return blocklets;
  }

  private int getNumOfThreadsForPruning() {
    int numOfThreadsForPruning = Integer.parseInt(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_MAX_DRIVER_THREADS_FOR_BLOCK_PRUNING,
            CarbonCommonConstants.CARBON_MAX_DRIVER_THREADS_FOR_BLOCK_PRUNING_DEFAULT));
    if (numOfThreadsForPruning > Integer
        .parseInt(CarbonCommonConstants.CARBON_MAX_DRIVER_THREADS_FOR_BLOCK_PRUNING_DEFAULT)
        || numOfThreadsForPruning < 1) {
      LOG.info("Invalid value for carbon.max.driver.threads.for.block.pruning, value :"
          + numOfThreadsForPruning + " .using the default threads : "
          + CarbonCommonConstants.CARBON_MAX_DRIVER_THREADS_FOR_BLOCK_PRUNING_DEFAULT);
      numOfThreadsForPruning = Integer
          .parseInt(CarbonCommonConstants.CARBON_MAX_DRIVER_THREADS_FOR_BLOCK_PRUNING_DEFAULT);
    }
    return numOfThreadsForPruning;
  }

  private List<ExtendedBlocklet> addSegmentId(List<ExtendedBlocklet> pruneBlocklets,
      String segmentId) {
    for (ExtendedBlocklet blocklet : pruneBlocklets) {
      blocklet.setSegmentId(segmentId);
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
      List<DataMapDistributable> list =
          dataMapFactory.toDistributable(segment);
      for (DataMapDistributable distributable : list) {
        distributable.setDataMapSchema(dataMapSchema);
        distributable.setSegment(segment);
        distributable.setTablePath(identifier.getTablePath());
      }
      distributables.addAll(list);
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
      detailedBlocklet.setSegmentId(distributable.getSegment().toString());
      detailedBlocklets.add(detailedBlocklet);
    }
    return detailedBlocklets;
  }

  /**
   * Clear only the datamaps of the segments
   * @param segments
   */
  public void clear(List<Segment> segments) {
    for (Segment segment: segments) {
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
   * Method to prune the segments based on task min/max values
   *
   * @param segments
   * @param filterExp
   * @return
   * @throws IOException
   */
  public List<Segment> pruneSegments(List<Segment> segments, FilterResolverIntf filterExp)
      throws IOException {
    List<Segment> prunedSegments = new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    for (Segment segment : segments) {
      List<DataMap> dataMaps = dataMapFactory.getDataMaps(segment);
      for (DataMap dataMap : dataMaps) {
        if (dataMap.isScanRequired(filterExp)) {
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
