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

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.dev.BlockletSerializer;
import org.apache.carbondata.core.datamap.dev.IndexDataMap;
import org.apache.carbondata.core.datamap.dev.IndexDataMapFactory;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.indexstore.Blocklet;
import org.apache.carbondata.core.indexstore.BlockletDetailsFetcher;
import org.apache.carbondata.core.indexstore.ExtendedBlocklet;
import org.apache.carbondata.core.indexstore.FineGrainBlocklet;
import org.apache.carbondata.core.indexstore.SegmentPropertiesFetcher;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.events.Event;
import org.apache.carbondata.events.OperationContext;
import org.apache.carbondata.events.OperationEventListener;

/**
 * IndexDataMap at the table level, user can add any number of datamaps for one table. Depends
 * on the filter condition it can prune the blocklets.
 */
public final class TableDataMap extends OperationEventListener {

  private AbsoluteTableIdentifier identifier;

  private DataMapSchema dataMapSchema;

  private IndexDataMapFactory indexDataMapFactory;

  private BlockletDetailsFetcher blockletDetailsFetcher;

  private SegmentPropertiesFetcher segmentPropertiesFetcher;

  /**
   * It is called to initialize and load the required table datamap metadata.
   */
  public TableDataMap(AbsoluteTableIdentifier identifier, DataMapSchema dataMapSchema,
      IndexDataMapFactory indexDataMapFactory, BlockletDetailsFetcher blockletDetailsFetcher,
      SegmentPropertiesFetcher segmentPropertiesFetcher) {
    this.identifier = identifier;
    this.dataMapSchema = dataMapSchema;
    this.indexDataMapFactory = indexDataMapFactory;
    this.blockletDetailsFetcher = blockletDetailsFetcher;
    this.segmentPropertiesFetcher = segmentPropertiesFetcher;
  }

  /**
   * Pass the valid segments and prune the datamap using filter expression
   *
   * @param segmentIds
   * @param filterExp
   * @return
   */
  public List<ExtendedBlocklet> prune(List<String> segmentIds, FilterResolverIntf filterExp,
      List<String> partitions) throws IOException {
    List<ExtendedBlocklet> blocklets = new ArrayList<>();
    SegmentProperties segmentProperties;
    for (String segmentId : segmentIds) {
      List<Blocklet> pruneBlocklets = new ArrayList<>();
      // if filter is not passed then return all the blocklets
      if (filterExp == null) {
        pruneBlocklets = blockletDetailsFetcher.getAllBlocklets(segmentId, partitions);
      } else {
        List<IndexDataMap> indexDataMaps = indexDataMapFactory.getDataMaps(segmentId);
        segmentProperties = segmentPropertiesFetcher.getSegmentProperties(segmentId);
        for (IndexDataMap indexDataMap : indexDataMaps) {
          pruneBlocklets.addAll(indexDataMap.prune(filterExp, segmentProperties, partitions));
        }
      }
      blocklets.addAll(addSegmentId(blockletDetailsFetcher
          .getExtendedBlocklets(pruneBlocklets, segmentId), segmentId));
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
  public List<DataMapDistributable> toDistributable(List<String> segmentIds) throws IOException {
    List<DataMapDistributable> distributables = new ArrayList<>();
    for (String segmentsId : segmentIds) {
      List<DataMapDistributable> list = indexDataMapFactory.toDistributable(segmentsId);
      for (DataMapDistributable distributable: list) {
        distributable.setDataMapSchema(dataMapSchema);
        distributable.setSegmentId(segmentsId);
        distributable.setTablePath(identifier.getTablePath());
      }
      distributables.addAll(list);
    }
    return distributables;
  }

  /**
   * This method is used from any machine after it is distributed. It takes the distributable object
   * to prune the filters.
   *
   * @param distributable
   * @param filterExp
   * @return
   */
  public List<ExtendedBlocklet> prune(DataMapDistributable distributable,
      FilterResolverIntf filterExp, List<String> partitions) throws IOException {
    List<ExtendedBlocklet> detailedBlocklets = new ArrayList<>();
    List<Blocklet> blocklets = new ArrayList<>();
    List<IndexDataMap> indexDataMaps = indexDataMapFactory.getDataMaps(distributable);
    for (IndexDataMap indexDataMap : indexDataMaps) {
      blocklets.addAll(
          indexDataMap.prune(
              filterExp,
              segmentPropertiesFetcher.getSegmentProperties(distributable.getSegmentId()),
              partitions));
    }
    BlockletSerializer serializer = new BlockletSerializer();
    String writePath =
        identifier.getTablePath() + CarbonCommonConstants.FILE_SEPARATOR + dataMapSchema
            .getDataMapName();
    if (indexDataMapFactory.getDataMapType() == DataMapType.FG) {
      FileFactory.mkdirs(writePath, FileFactory.getFileType(writePath));
    }
    for (Blocklet blocklet : blocklets) {
      ExtendedBlocklet detailedBlocklet =
          blockletDetailsFetcher.getExtendedBlocklet(blocklet, distributable.getSegmentId());
      if (indexDataMapFactory.getDataMapType() == DataMapType.FG) {
        String blockletwritePath =
            writePath + CarbonCommonConstants.FILE_SEPARATOR + System.nanoTime();
        detailedBlocklet.setDataMapWriterPath(blockletwritePath);
        serializer.serializeBlocklet((FineGrainBlocklet) blocklet, blockletwritePath);
      }
      detailedBlocklet.setSegmentId(distributable.getSegmentId());
      detailedBlocklets.add(detailedBlocklet);
    }
    return detailedBlocklets;
  }

  /**
   * Clear only the datamaps of the segments
   * @param segmentIds
   */
  public void clear(List<String> segmentIds) {
    for (String segmentId: segmentIds) {
      indexDataMapFactory.clear(segmentId);
    }
  }

  /**
   * Clears all datamap
   */
  public void clear() {
    indexDataMapFactory.clear();
  }

  public DataMapSchema getDataMapSchema() {
    return dataMapSchema;
  }

  public IndexDataMapFactory getIndexDataMapFactory() {
    return indexDataMapFactory;
  }

  @Override public void onEvent(Event event, OperationContext opContext) throws Exception {
    indexDataMapFactory.fireEvent(event);
  }

  /**
   * Method to prune the segments based on task min/max values
   *
   * @param segmentIds
   * @param filterExp
   * @return
   * @throws IOException
   */
  public List<String> pruneSegments(List<String> segmentIds, FilterResolverIntf filterExp)
      throws IOException {
    List<String> prunedSegments = new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    for (String segmentId : segmentIds) {
      List<IndexDataMap> indexDataMaps = indexDataMapFactory.getDataMaps(segmentId);
      for (IndexDataMap indexDataMap : indexDataMaps) {
        if (indexDataMap.isScanRequired(filterExp)) {
          // If any one task in a given segment contains the data that means the segment need to
          // be scanned and we need to validate further data maps in the same segment
          prunedSegments.add(segmentId);
          break;
        }
      }
    }
    return prunedSegments;
  }
}
