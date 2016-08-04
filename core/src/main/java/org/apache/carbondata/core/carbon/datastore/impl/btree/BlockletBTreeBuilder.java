/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.carbondata.core.carbon.datastore.impl.btree;

import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.carbon.datastore.BTreeBuilderInfo;
import org.apache.carbondata.core.carbon.datastore.IndexKey;
import org.apache.carbondata.core.constants.CarbonCommonConstants;

/**
 * Btree based builder which will build the leaf node in a b+ tree format
 */
public class BlockletBTreeBuilder extends AbstractBTreeBuilder {

  /**
   * Attribute for Carbon LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(BlockletBTreeBuilder.class.getName());

  /**
   * Below method will be used to build the segment info bplus tree format
   * Tree will be a read only tree, and it will be build on Bottoms up approach
   * first all the leaf node will be built and then intermediate node
   * in our case one leaf node will have not only one entry it will have group of entries
   */
  @Override public void build(BTreeBuilderInfo segmentBuilderInfos) {
    long totalNumberOfTuple = 0;
    int groupCounter;
    int nInternal = 0;
    BTreeNode curNode = null;
    BTreeNode prevNode = null;
    List<BTreeNode[]> nodeGroups =
        new ArrayList<BTreeNode[]>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    BTreeNode[] currentGroup = null;
    List<List<IndexKey>> interNSKeyList =
        new ArrayList<List<IndexKey>>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    List<IndexKey> leafNSKeyList = null;
    long nodeNumber = 0;
    for (int index = 0;
         index < segmentBuilderInfos.getFooterList().get(0).getBlockletList()
             .size(); index++) {
      // creating a leaf node
      curNode = new BlockletBTreeLeafNode(segmentBuilderInfos, index, nodeNumber++);
      totalNumberOfTuple +=
          segmentBuilderInfos.getFooterList().get(0).getBlockletList().get(index)
              .getNumberOfRows();
      nLeaf++;
      // setting a next node as its a b+tree
      // so all the leaf node will be chained
      // will be stored in linked list
      if (prevNode != null) {
        prevNode.setNextNode(curNode);
      }
      prevNode = curNode;
      // as intermediate node will have more than one leaf
      // in cerating a group
      groupCounter = (nLeaf - 1) % (maxNumberOfEntriesInNonLeafNodes);
      if (groupCounter == 0) {
        // Create new node group if current group is full
        leafNSKeyList = new ArrayList<IndexKey>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        currentGroup = new BTreeNode[maxNumberOfEntriesInNonLeafNodes];
        nodeGroups.add(currentGroup);
        nInternal++;
        interNSKeyList.add(leafNSKeyList);
      }
      if (null != leafNSKeyList) {
        leafNSKeyList.add(convertStartKeyToNodeEntry(
            segmentBuilderInfos.getFooterList().get(0).getBlockletList().get(index)
                .getBlockletIndex().getBtreeIndex().getStartKey()));
      }
      if (null != currentGroup) {
        currentGroup[groupCounter] = curNode;
      }
    }
    if (totalNumberOfTuple == 0) {
      return;
    }
    // adding a intermediate node
    addIntermediateNode(curNode, nodeGroups, currentGroup, interNSKeyList, nInternal);
    LOGGER.info("****************************Total Number Rows In BTREE: " + totalNumberOfTuple);
  }

}
