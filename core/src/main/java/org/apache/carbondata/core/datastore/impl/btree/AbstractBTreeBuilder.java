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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.BtreeBuilder;
import org.apache.carbondata.core.datastore.IndexKey;
import org.apache.carbondata.core.api.CarbonProperties;

/**
 * Abstract Btree based builder
 */
public abstract class AbstractBTreeBuilder implements BtreeBuilder {

  /**
   * default Number of keys per page
   */
  private static final int DEFAULT_NUMBER_OF_ENTRIES_NONLEAF = 32;

  /**
   * Maximum number of entries in intermediate nodes
   */
  protected int maxNumberOfEntriesInNonLeafNodes;

  /**
   * Number of leaf nodes
   */
  protected int nLeaf;

  /**
   * root node of a btree
   */
  protected BTreeNode root;

  public AbstractBTreeBuilder() {
    maxNumberOfEntriesInNonLeafNodes = DEFAULT_NUMBER_OF_ENTRIES_NONLEAF;
  }

  /**
   * Below method is to build the intermediate node of the btree
   *
   * @param curNode              current node
   * @param childNodeGroups      children group which will have all the children for
   *                             particular intermediate node
   * @param currentGroup         current group
   * @param interNSKeyList       list if keys
   * @param numberOfInternalNode number of internal node
   */
  protected void addIntermediateNode(BTreeNode curNode, List<BTreeNode[]> childNodeGroups,
      BTreeNode[] currentGroup, List<List<IndexKey>> interNSKeyList, int numberOfInternalNode) {

    int groupCounter;
    // Build internal nodes level by level. Each upper node can have
    // upperMaxEntry keys and upperMaxEntry+1 children
    int remainder;
    int nHigh = numberOfInternalNode;
    boolean bRootBuilt = false;
    remainder = nLeaf % (maxNumberOfEntriesInNonLeafNodes);
    List<IndexKey> interNSKeys = null;
    while (nHigh > 1 || !bRootBuilt) {
      List<BTreeNode[]> internalNodeGroups =
          new ArrayList<BTreeNode[]>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
      List<List<IndexKey>> interNSKeyTmpList =
          new ArrayList<List<IndexKey>>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
      numberOfInternalNode = 0;
      for (int i = 0; i < nHigh; i++) {
        // Create a new internal node
        curNode = new BTreeNonLeafNode();
        // Allocate a new node group if current node group is full
        groupCounter = i % (maxNumberOfEntriesInNonLeafNodes);
        if (groupCounter == 0) {
          // Create new node group
          currentGroup = new BTreeNonLeafNode[maxNumberOfEntriesInNonLeafNodes];
          internalNodeGroups.add(currentGroup);
          numberOfInternalNode++;
          interNSKeys = new ArrayList<IndexKey>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
          interNSKeyTmpList.add(interNSKeys);
        }

        // Add the new internal node to current group
        if (null != currentGroup) {
          currentGroup[groupCounter] = curNode;
        }
        int nNodes;

        if (i == nHigh - 1 && remainder != 0) {
          nNodes = remainder;
        } else {
          nNodes = maxNumberOfEntriesInNonLeafNodes;
        }
        // Point the internal node to its children node group
        curNode.setChildren(childNodeGroups.get(i));
        // Fill the internal node with keys based on its child nodes
        for (int j = 0; j < nNodes; j++) {
          curNode.setKey(interNSKeyList.get(i).get(j));
          if (j == 0 && null != interNSKeys) {
            interNSKeys.add(interNSKeyList.get(i).get(j));

          }
        }
      }
      // If nHigh is 1, we have the root node
      if (nHigh == 1) {
        bRootBuilt = true;
      }

      remainder = nHigh % (maxNumberOfEntriesInNonLeafNodes);
      nHigh = numberOfInternalNode;
      childNodeGroups = internalNodeGroups;
      interNSKeyList = interNSKeyTmpList;
    }
    root = curNode;
  }

  /**
   * Below method is to convert the start key
   * into fixed and variable length key.
   * data format<lenght><fixed length key><length><variable length key>
   *
   * @param startKey
   * @return Index key
   */
  protected IndexKey convertStartKeyToNodeEntry(byte[] startKey) {
    ByteBuffer buffer = ByteBuffer.wrap(startKey);
    buffer.rewind();
    int dictonaryKeySize = buffer.getInt();
    int nonDictonaryKeySize = buffer.getInt();
    byte[] dictionaryKey = new byte[dictonaryKeySize];
    buffer.get(dictionaryKey);
    byte[] nonDictionaryKey = new byte[nonDictonaryKeySize];
    buffer.get(nonDictionaryKey);
    return new IndexKey(dictionaryKey, nonDictionaryKey);
  }

  /**
   * Below method will be used to get the first data block
   * in Btree case it will be root node
   */
  @Override public BTreeNode get() {
    return root;
  }
}
