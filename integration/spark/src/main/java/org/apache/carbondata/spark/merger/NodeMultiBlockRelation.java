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
package org.apache.carbondata.spark.merger;

import java.util.List;

import org.apache.carbondata.core.carbon.datastore.block.Distributable;

public class NodeMultiBlockRelation implements Comparable<NodeMultiBlockRelation> {

  private final List<Distributable> blocks;
  private final String node;

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
