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
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.events.Event;
import org.apache.carbondata.events.OperationContext;
import org.apache.carbondata.events.OperationEventListener;

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
  public List<ExtendedBlocklet> prune(List<Segment> segments, FilterResolverIntf filterExp,
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
          pruneBlocklets.addAll(dataMap.prune(filterExp, segmentProperties, partitions));
        }
      }
      blocklets.addAll(addSegmentId(
          blockletDetailsFetcher.getExtendedBlocklets(pruneBlocklets, segment),
          segment.toString()));
    }
    return blocklets;
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
