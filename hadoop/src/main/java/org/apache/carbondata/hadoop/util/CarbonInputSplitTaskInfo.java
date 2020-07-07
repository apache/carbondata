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

package org.apache.carbondata.hadoop.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

import org.apache.carbondata.core.datastore.block.Distributable;
import org.apache.carbondata.hadoop.CarbonInputSplit;

public class CarbonInputSplitTaskInfo implements Distributable {

  private final List<CarbonInputSplit> carbonBlockInfoList;

  private final String taskId;

  public String getTaskId() {
    return taskId;
  }

  public List<CarbonInputSplit> getCarbonInputSplitList() {
    return carbonBlockInfoList;
  }

  public CarbonInputSplitTaskInfo(String taskId, List<CarbonInputSplit> carbonSplitListInfo) {
    this.taskId = taskId;
    this.carbonBlockInfoList = carbonSplitListInfo;
  }

  @Override
  public String[] getLocations() {
    Set<String> locations = new HashSet<>();
    for (CarbonInputSplit splitInfo : carbonBlockInfoList) {
      try {
        locations.addAll(Arrays.asList(splitInfo.getLocations()));
      } catch (IOException e) {
        throw new RuntimeException("Fail to get location of split: " + splitInfo, e);
      }
    }
    locations.toArray(new String[locations.size()]);
    List<String> nodes = CarbonInputSplitTaskInfo.maxNoNodes(carbonBlockInfoList);
    return nodes.toArray(new String[nodes.size()]);
  }

  @Override
  public int compareTo(Distributable o) {
    return taskId.compareTo(((CarbonInputSplitTaskInfo) o).getTaskId());
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (!(obj instanceof CarbonInputSplitTaskInfo)) {
      return false;
    }

    CarbonInputSplitTaskInfo that = (CarbonInputSplitTaskInfo)obj;
    return null != taskId ? 0 == taskId.compareTo(that.taskId) : null == that.taskId;
  }

  @Override
  public int hashCode() {
    return null != taskId ? taskId.hashCode() : 0;
  }

  /**
   * Finding which node has the maximum number of blocks for it.
   */
  public static List<String> maxNoNodes(List<CarbonInputSplit> splitList) {
    boolean useIndex = true;
    Integer maxOccurrence = 0;
    String maxNode = null;
    Map<String, Integer> nodeAndOccurrenceMapping = new TreeMap<>();

    // populate the map of node and number of occurrences of that node.
    for (CarbonInputSplit split : splitList) {
      try {
        for (String node : split.getLocations()) {
          nodeAndOccurrenceMapping.putIfAbsent(node, 1);
        }
      } catch (IOException e) {
        throw new RuntimeException("Fail to get location of split: " + split, e);
      }
    }
    Integer previousValueOccurrence = null;

    // check which node is occurred maximum times.
    for (Map.Entry<String, Integer> entry : nodeAndOccurrenceMapping.entrySet()) {
      // finding the maximum node.
      if (entry.getValue() > maxOccurrence) {
        maxOccurrence = entry.getValue();
        maxNode = entry.getKey();
      }
      // first time scenario. initializing the previous value.
      if (null == previousValueOccurrence) {
        previousValueOccurrence = entry.getValue();
      } else {
        // for the case where all the nodes have same number of blocks then
        // we need to return complete list instead of max node.
        if (!Objects.equals(previousValueOccurrence, entry.getValue())) {
          useIndex = false;
        }
      }
    }

    // if all the nodes have equal occurrence then returning the complete key set.
    if (useIndex) {
      return new ArrayList<>(nodeAndOccurrenceMapping.keySet());
    }

    // if any max node is found then returning the max node.
    List<String> node = new ArrayList<>(1);
    node.add(maxNode);
    return node;
  }

}
