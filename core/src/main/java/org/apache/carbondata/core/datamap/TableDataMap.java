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
import org.apache.carbondata.core.datamap.dev.DataMap;
import org.apache.carbondata.core.datamap.dev.DataMapFactory;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.indexstore.Blocklet;
import org.apache.carbondata.core.indexstore.BlockletDetailsFetcher;
import org.apache.carbondata.core.indexstore.ExtendedBlocklet;
import org.apache.carbondata.core.indexstore.FineGrainBlocklet;
import org.apache.carbondata.core.indexstore.SegmentPropertiesFetcher;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.events.Event;
import org.apache.carbondata.events.OperationContext;
import org.apache.carbondata.events.OperationEventListener;

/**
 * DataMap at the table level, user can add any number of datamaps for one table. Depends
 * on the filter condition it can prune the blocklets.
 */
public final class TableDataMap implements OperationEventListener {

  private AbsoluteTableIdentifier identifier;

  private String dataMapName;

  private DataMapFactory dataMapFactory;

  private BlockletDetailsFetcher blockletDetailsFetcher;

  private SegmentPropertiesFetcher segmentPropertiesFetcher;

  /**
   * It is called to initialize and load the required table datamap metadata.
   */
  public TableDataMap(AbsoluteTableIdentifier identifier, String dataMapName,
      DataMapFactory dataMapFactory, BlockletDetailsFetcher blockletDetailsFetcher,
      SegmentPropertiesFetcher segmentPropertiesFetcher) {
    this.identifier = identifier;
    this.dataMapName = dataMapName;
    this.dataMapFactory = dataMapFactory;
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
  public List<ExtendedBlocklet> prune(List<String> segmentIds, FilterResolverIntf filterExp)
      throws IOException {
    List<ExtendedBlocklet> blocklets = new ArrayList<>();
    SegmentProperties segmentProperties;
    for (String segmentId : segmentIds) {
      List<Blocklet> pruneBlocklets = new ArrayList<>();
      // if filter is not passed then return all the blocklets
      if (filterExp == null) {
        pruneBlocklets = blockletDetailsFetcher.getAllBlocklets(segmentId);
      } else {
        List<DataMap> dataMaps = dataMapFactory.getDataMaps(segmentId);
        segmentProperties = segmentPropertiesFetcher.getSegmentProperties(segmentId);
        for (DataMap dataMap : dataMaps) {
          pruneBlocklets.addAll(dataMap.prune(filterExp, segmentProperties));
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
      List<DataMapDistributable> list = dataMapFactory.toDistributable(segmentsId);
      for (DataMapDistributable distributable: list) {
        distributable.setDataMapName(dataMapName);
        distributable.setSegmentId(segmentsId);
        distributable.setTablePath(identifier.getTablePath());
        distributable.setDataMapFactoryClass(dataMapFactory.getClass().getName());
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
      FilterResolverIntf filterExp) throws IOException {
    List<ExtendedBlocklet> detailedBlocklets = new ArrayList<>();
    List<Blocklet> blocklets = new ArrayList<>();
    List<DataMap> dataMaps = dataMapFactory.getDataMaps(distributable);
    for (DataMap dataMap : dataMaps) {
      blocklets.addAll(dataMap.prune(filterExp,
          segmentPropertiesFetcher.getSegmentProperties(distributable.getSegmentId())));
    }
    BlockletSerializer serializer = new BlockletSerializer();
    String writePath =
        identifier.getTablePath() + CarbonCommonConstants.FILE_SEPARATOR + dataMapName;
    if (dataMapFactory.getDataMapType() == DataMapType.FG) {
      FileFactory.mkdirs(writePath, FileFactory.getFileType(writePath));
    }
    for (Blocklet blocklet : blocklets) {
      ExtendedBlocklet detailedBlocklet =
          blockletDetailsFetcher.getExtendedBlocklet(blocklet, distributable.getSegmentId());
      if (dataMapFactory.getDataMapType() == DataMapType.FG) {
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
      dataMapFactory.clear(segmentId);
    }
  }

  /**
   * Clears all datamap
   */
  public void clear() {
    dataMapFactory.clear();
  }
  /**
   * Get the unique name of datamap
   *
   * @return
   */
  public String getDataMapName() {
    return dataMapName;
  }

  public DataMapFactory getDataMapFactory() {
    return dataMapFactory;
  }

  @Override public void onEvent(Event event, OperationContext opContext) {
    dataMapFactory.fireEvent(event);
  }
}
