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
package org.apache.carbondata.processing.merger;

import java.util.Comparator;
import java.util.List;

import org.apache.carbondata.core.datastore.block.Distributable;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;

public class NodeMultiBlockRelation implements Comparable<NodeMultiBlockRelation> {

  private final List<Distributable> blocks;
  private final String node;

  /**
   * comparator to sort by data size in descending order. This is used to assign big blocks to
   * bigger nodes first.
   */
  public static final Comparator<NodeMultiBlockRelation> DATA_SIZE_DESC_COMPARATOR =
      new Comparator<NodeMultiBlockRelation>() {
        @Override
        public int compare(NodeMultiBlockRelation o1, NodeMultiBlockRelation o2) {
          long diff = o1.getTotalSizeOfBlocks() - o2.getTotalSizeOfBlocks();
          return diff > 0 ? -1 : (diff < 0 ? 1 : 0);
        }
      };
  /**
   * comparator to sort by data size in ascending order. This is used to assign left over blocks to
   * smaller nodes first.
   */
  public static final Comparator<NodeMultiBlockRelation> DATA_SIZE_ASC_COMPARATOR =
      new Comparator<NodeMultiBlockRelation>() {
        @Override
        public int compare(NodeMultiBlockRelation o1, NodeMultiBlockRelation o2) {
          long diff = o1.getTotalSizeOfBlocks() - o2.getTotalSizeOfBlocks();
          return diff > 0 ? 1 : (diff < 0 ? -1 : 0);
        }
      };
  public NodeMultiBlockRelation(String node, List<Distributable> blocks) {
    this.node = node;
    this.blocks = blocks;

  }

  public List<Distributable> getBlocks() {
    return blocks;
  }

  public String getNode() {
    return node;
  }

  /**
   * get the total size of the blocks
   * @return size in bytes
   */
  public long getTotalSizeOfBlocks() {
    long totalSize = 0;
    if (blocks.get(0) instanceof TableBlockInfo) {
      for (Distributable block : blocks) {
        totalSize += ((TableBlockInfo) block).getBlockLength();
      }
    }
    return totalSize;
  }

  @Override public int compareTo(NodeMultiBlockRelation obj) {
    return this.blocks.size() - obj.getBlocks().size();
  }

  @Override public boolean equals(Object obj) {
    if (!(obj instanceof NodeMultiBlockRelation)) {
      return false;
    }
    NodeMultiBlockRelation o = (NodeMultiBlockRelation) obj;
    return blocks.equals(o.blocks) && node.equals(o.node);
  }

  @Override public int hashCode() {
    return blocks.hashCode() + node.hashCode();
  }
}
