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

import org.apache.carbondata.core.datastore.DataRefNode;
import org.apache.carbondata.core.datastore.IndexKey;

/**
 * Interface for btree node
 */
public interface BTreeNode extends DataRefNode {

  /**
   * below method will return the one node indexes
   *
   * @return node entry array
   */
  IndexKey[] getNodeKeys();

  /**
   * to check whether node in a btree is a leaf node or not
   *
   * @return leaf node or not
   */
  boolean isLeafNode();

  /**
   * below method will be used to set the children of intermediate node
   *
   * @param children array
   */
  void setChildren(BTreeNode[] children);

  /**
   * below method will used to set the next node
   *
   * @param nextNode
   */
  void setNextNode(BTreeNode nextNode);

  /**
   * Below method is to get the children based on index
   *
   * @param index children index
   * @return btree node
   */
  BTreeNode getChild(int index);

  /**
   * below method to set the node entry
   *
   * @param key node entry
   */
  void setKey(IndexKey key);
}
