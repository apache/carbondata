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

package org.apache.carbondata.spark.partition.api.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.spark.partition.api.Partition;

/**
 * A sample load balancer to distribute the partitions to the available nodes in a round robin mode.
 */
public class DefaultLoadBalancer {
  private Map<String, List<Partition>> nodeToPartitonMap =
      new HashMap<String, List<Partition>>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

  private Map<Partition, String> partitonToNodeMap =
      new HashMap<Partition, String>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

  public DefaultLoadBalancer(List<String> nodes, List<Partition> partitions) {
    //Per form a round robin allocation
    int nodeCount = nodes.size();

    int partitioner = 0;
    for (Partition partition : partitions) {
      int nodeindex = partitioner % nodeCount;
      String node = nodes.get(nodeindex);

      List<Partition> oldList = nodeToPartitonMap.get(node);
      if (oldList == null) {
        oldList = new ArrayList<Partition>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
        nodeToPartitonMap.put(node, oldList);
      }
      oldList.add(partition);

      partitonToNodeMap.put(partition, node);

      partitioner++;
    }
  }

  public List<Partition> getPartitionsForNode(String node) {
    return nodeToPartitonMap.get(node);
  }

  public String getNodeForPartitions(Partition partition) {
    return partitonToNodeMap.get(partition);
  }
}
