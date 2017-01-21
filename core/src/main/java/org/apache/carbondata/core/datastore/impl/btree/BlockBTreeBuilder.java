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
package org.apache.carbondata.core.datastore.impl.btree;
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

import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.BTreeBuilderInfo;
import org.apache.carbondata.core.datastore.IndexKey;

/**
 * Below class will be used to build the btree BTree will be built for all the
 * blocks of a segment
 */
public class BlockBTreeBuilder extends AbstractBTreeBuilder {

  /**
   * Attribute for Carbon LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(BlockBTreeBuilder.class.getName());

  /**
   * Below method will be used to build the segment info bplus tree format
   * Tree will be a read only tree, and it will be build on Bottoms up
   * approach first all the leaf node will be built and then intermediate node
   * in our case one leaf node will have not only one entry it will have group
   * of entries
   */
  @Override public void build(BTreeBuilderInfo btreeBuilderInfo) {
    int groupCounter;
    int nInternal = 0;
    BTreeNode curNode = null;
    BTreeNode prevNode = null;
    List<BTreeNode[]> nodeGroups =
        new ArrayList<BTreeNode[]>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    BTreeNode[] currentGroup = null;
    List<List<IndexKey>> interNSKeyList =
        new ArrayList<List<IndexKey>>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    List<IndexKey> leafNSKeyList = null;
    long nodeNumber = 0;
    for (int metadataIndex = 0;
         metadataIndex < btreeBuilderInfo.getFooterList().size(); metadataIndex++) {
      // creating a leaf node
      curNode = new BlockBTreeLeafNode(btreeBuilderInfo, metadataIndex, nodeNumber++);
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
        leafNSKeyList = new ArrayList<IndexKey>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
        currentGroup = new BTreeNode[maxNumberOfEntriesInNonLeafNodes];
        nodeGroups.add(currentGroup);
        nInternal++;
        interNSKeyList.add(leafNSKeyList);
      }
      if (null != leafNSKeyList) {
        leafNSKeyList.add(convertStartKeyToNodeEntry(
            btreeBuilderInfo.getFooterList().get(metadataIndex).getBlockletIndex()
                .getBtreeIndex().getStartKey()));
      }
      if (null != currentGroup) {
        currentGroup[groupCounter] = curNode;
      }
    }
    if (nLeaf == 0) {
      return;
    }
    // adding a intermediate node
    addIntermediateNode(curNode, nodeGroups, currentGroup, interNSKeyList, nInternal);
    LOGGER.info("************************Total Number Rows In BTREE: " + nLeaf);
  }
}
