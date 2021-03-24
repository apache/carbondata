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

package org.apache.carbondata.index.secondary;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.index.IndexUtil;
import org.apache.carbondata.core.index.dev.IndexModel;
import org.apache.carbondata.core.index.dev.cgindex.CoarseGrainIndex;
import org.apache.carbondata.core.indexstore.Blocklet;
import org.apache.carbondata.core.indexstore.ExtendedBlocklet;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.executer.FilterExecutor;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.index.secondary.SecondaryIndexModel.PositionReferenceInfo;

import org.apache.log4j.Logger;

/**
 * Secondary Index to prune at blocklet level.
 */
public class SecondaryIndex extends CoarseGrainIndex {
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(SecondaryIndex.class.getName());
  private String indexName;
  private String currentSegmentId;
  private Set<String> validSegmentIds;
  private PositionReferenceInfo positionReferenceInfo;
  private List<ExtendedBlocklet> defaultIndexPrunedBlocklet;

  public void setDefaultIndexPrunedBlocklet(List<ExtendedBlocklet> extendedBlockletList) {
    this.defaultIndexPrunedBlocklet = extendedBlockletList;
  }

  @Override
  public void init(IndexModel indexModel) {
    assert (indexModel instanceof SecondaryIndexModel);
    SecondaryIndexModel model = (SecondaryIndexModel) indexModel;
    indexName = model.getIndexName();
    currentSegmentId = model.getCurrentSegmentId();
    validSegmentIds = new HashSet<>(model.getValidSegmentIds());
    positionReferenceInfo = model.getPositionReferenceInfo();
  }

  public void validateSegmentList(String indexPath) {
    LoadMetadataDetails[] loadMetadataDetails = SegmentStatusManager
        .readLoadMetadata(CarbonTablePath.getMetadataPath(indexPath));
    Set<String> validSISegments = new HashSet<>();
    for (LoadMetadataDetails loadMetadataDetail : loadMetadataDetails) {
      if (loadMetadataDetail.getSegmentStatus() == SegmentStatus.SUCCESS
          || loadMetadataDetail.getSegmentStatus() == SegmentStatus.MARKED_FOR_UPDATE
          || loadMetadataDetail.getSegmentStatus() == SegmentStatus.LOAD_PARTIAL_SUCCESS) {
        validSISegments.add(loadMetadataDetail.getLoadName());
      }
    }
    validSegmentIds = Sets.intersection(validSISegments, validSegmentIds);
  }

  private Set<String> getPositionReferences(String databaseName, String indexName,
      Expression expression) {
    /* If the position references are not obtained yet(i.e., prune happening for the first valid
    segment), then get them from the given index table with the given filter from all the valid
    segments at once and store them as map of segmentId to set of position references in that
    particular segment. Upon the subsequent prune for other segments, return the position
    references for the respective segment from the map directly */
    if (!positionReferenceInfo.isFetched()) {
      Object[] rows = IndexUtil.getPositionReferences(String
          .format("select distinct positionReference from %s.%s where insegment('%s') and %s",
              databaseName, indexName, String.join(",", validSegmentIds),
              expression.getStatement()));
      for (Object row : rows) {
        String positionReference = (String) row;
        int blockletPathIndex = positionReference.indexOf("/");
        String blockletPath = positionReference.substring(blockletPathIndex + 1);
        int segEndIndex = blockletPath.lastIndexOf(CarbonCommonConstants.DASH);
        int segStartIndex = blockletPath.lastIndexOf(CarbonCommonConstants.DASH, segEndIndex - 1);
        Set<String> blockletPaths = positionReferenceInfo.getSegmentToPosReferences()
            .computeIfAbsent(blockletPath.substring(segStartIndex + 1, segEndIndex),
                k -> new HashSet<>());
        blockletPaths.add(blockletPath);
      }
      positionReferenceInfo.setFetched(true);
    }
    Set<String> blockletPaths =
        positionReferenceInfo.getSegmentToPosReferences().get(currentSegmentId);
    return blockletPaths != null ? blockletPaths : new HashSet<>();
  }

  @Override
  public List<Blocklet> prune(FilterResolverIntf filterExp, SegmentProperties segmentProperties,
      FilterExecutor filterExecutor, CarbonTable carbonTable) {
    Set<String> blockletPaths = getPositionReferences(carbonTable.getDatabaseName(), indexName,
        filterExp.getFilterExpression());
    List<Blocklet> blocklets = new ArrayList<>();
    if (!this.validSegmentIds.contains(currentSegmentId)) {
      // if current segment is not a valid SI segment then
      // add the list of blocklet pruned by default index.
      blocklets.addAll(defaultIndexPrunedBlocklet);
    } else {
      for (String blockletPath : blockletPaths) {
        blockletPath = blockletPath.substring(blockletPath.indexOf(CarbonCommonConstants.DASH) + 1)
            .replace(CarbonCommonConstants.UNDERSCORE, CarbonTablePath.BATCH_PREFIX);
        int blockletIndex = blockletPath.lastIndexOf("/");
        blocklets.add(new Blocklet(blockletPath.substring(0, blockletIndex),
            blockletPath.substring(blockletIndex + 1)));
      }
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(String
          .format("Secondary Index pruned blocklet count for segment %s is %d ", currentSegmentId,
              blocklets.size()));
    }
    return blocklets;
  }

  @Override
  public boolean isScanRequired(FilterResolverIntf filterExp) {
    return true;
  }

  @Override
  public void clear() {
  }

  @Override
  public void finish() {
  }
}
