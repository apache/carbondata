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

import org.apache.carbondata.core.datastore.block.Distributable;

/**
 * Block to Node mapping
 */
public class NodeBlockRelation<T extends Distributable> implements Comparable<NodeBlockRelation> {

  private final T block;
  private final String node;

  public NodeBlockRelation(T block, String node) {
    this.block = block;
    this.node = node;

  }

  public T getBlock() {
    return block;
  }

  public String getNode() {
    return node;
  }

  @Override public int compareTo(NodeBlockRelation obj) {
    return this.getNode().compareTo(obj.getNode());
  }

  @Override public boolean equals(Object obj) {
    if (!(obj instanceof NodeBlockRelation)) {
      return false;
    }
    NodeBlockRelation o = (NodeBlockRelation) obj;
    return node.equals(o.node);
  }

  @Override public int hashCode() {
    return node.hashCode();
  }
}
