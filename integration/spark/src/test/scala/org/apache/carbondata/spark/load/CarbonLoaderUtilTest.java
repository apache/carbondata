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
package org.apache.carbondata.spark.load;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.core.carbon.datastore.block.Distributable;
import org.apache.carbondata.core.carbon.datastore.block.TableBlockInfo;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test class to test block distribution functionality
 */
public class CarbonLoaderUtilTest {
  List<Distributable> blockInfos = null;
  int noOfNodesInput = -1;
  List<String> activeNode = null;
  Map<String, List<Distributable>> expected = null;
  Map<String, List<Distributable>> mapOfNodes = null;

  @Test public void nodeBlockMapping() throws Exception {

    // scenario when the 3 nodes and 3 executors
    initSet1();
    Map<String, List<Distributable>> mapOfNodes =
        CarbonLoaderUtil.nodeBlockMapping(blockInfos, noOfNodesInput, activeNode);
    // node allocation
    Assert.assertTrue("Node Allocation", expected.size() == mapOfNodes.size());
    // block allocation
    boolean isEqual = compareResult(expected, mapOfNodes);
    Assert.assertTrue("Block Allocation", isEqual);

    // 2 node and 3 executors
    initSet2();
    mapOfNodes = CarbonLoaderUtil.nodeBlockMapping(blockInfos, noOfNodesInput, activeNode);
    // node allocation
    Assert.assertTrue("Node Allocation", expected.size() == mapOfNodes.size());
    // block allocation
    isEqual = compareResult(expected, mapOfNodes);
    Assert.assertTrue("Block Allocation", isEqual);

    // 3 data node and 2 executors
    initSet3();
    mapOfNodes = CarbonLoaderUtil.nodeBlockMapping(blockInfos, noOfNodesInput, activeNode);
    // node allocation
    Assert.assertTrue("Node Allocation", expected.size() == mapOfNodes.size());
    // block allocation
    isEqual = compareResult(expected, mapOfNodes);
    Assert.assertTrue("Block Allocation", isEqual);
  }

  /**
   * compares the blocks allocation
   *
   * @param expectedResult
   * @param actualResult
   * @return
   */
  private boolean compareResult(Map<String, List<Distributable>> expectedResult,
      Map<String, List<Distributable>> actualResult) {
    expectedResult = sortByListSize(expectedResult);
    actualResult = sortByListSize(actualResult);
    List<List<Distributable>> expectedList = new LinkedList(expectedResult.entrySet());
    List<List<Distributable>> mapOfNodesList = new LinkedList(actualResult.entrySet());
    boolean isEqual = expectedList.size() == mapOfNodesList.size();
    if (isEqual) {
      for (int i = 0; i < expectedList.size(); i++) {
        int size1 = ((List) ((Map.Entry) (expectedList.get(i))).getValue()).size();
        int size2 = ((List) ((Map.Entry) (mapOfNodesList.get(i))).getValue()).size();
        isEqual = size1 == size2;
        if (!isEqual) {
          break;
        }
      }
    }
    return isEqual;
  }

  /**
   * sort by list size
   *
   * @param map
   * @return
   */
  private static Map<String, List<Distributable>> sortByListSize(
      Map<String, List<Distributable>> map) {
    List<List<Distributable>> list = new LinkedList(map.entrySet());
    Collections.sort(list, new Comparator() {
      public int compare(Object obj1, Object obj2) {
        if (obj1 == null && obj2 == null) {
          return 0;
        } else if (obj1 == null) {
          return 1;
        } else if (obj2 == null) {
          return -1;
        }
        int size1 = ((List) ((Map.Entry) (obj1)).getValue()).size();
        int size2 = ((List) ((Map.Entry) (obj2)).getValue()).size();
        return size2 - size1;
      }
    });

    Map res = new LinkedHashMap();
    for (Iterator it = list.iterator(); it.hasNext(); ) {
      Map.Entry entry = (Map.Entry) it.next();
      res.put(entry.getKey(), entry.getValue());
    }
    return res;
  }

  void initSet1() {
    blockInfos = new ArrayList<>();
    activeNode = new ArrayList<>();
    activeNode.add("node-7");
    activeNode.add("node-9");
    activeNode.add("node-11");
    String[] location = { "node-7", "node-9", "node-11" };
    blockInfos.add(new TableBlockInfo("node", 1, "1", location, 0));
    blockInfos.add(new TableBlockInfo("node", 2, "1", location, 0));
    blockInfos.add(new TableBlockInfo("node", 3, "1", location, 0));
    blockInfos.add(new TableBlockInfo("node", 4, "1", location, 0));
    blockInfos.add(new TableBlockInfo("node", 5, "1", location, 0));
    blockInfos.add(new TableBlockInfo("node", 6, "1", location, 0));
    expected = new HashMap<>();
    expected.put("node-7", blockInfos.subList(0, 2));
    expected.put("node-9", blockInfos.subList(2, 4));
    expected.put("node-11", blockInfos.subList(4, 6));
  }

  void initSet2() {
    blockInfos = new ArrayList<>();
    activeNode = new ArrayList<>();
    activeNode.add("node-7");
    activeNode.add("node-9");
    activeNode.add("node-11");
    String[] location = { "node-7", "node-11" };
    blockInfos.add(new TableBlockInfo("node", 1, "1", location, 0));
    blockInfos.add(new TableBlockInfo("node", 2, "1", location, 0));
    blockInfos.add(new TableBlockInfo("node", 3, "1", location, 0));
    blockInfos.add(new TableBlockInfo("node", 4, "1", location, 0));
    blockInfos.add(new TableBlockInfo("node", 5, "1", location, 0));
    blockInfos.add(new TableBlockInfo("node", 6, "1", location, 0));
    expected = new HashMap<>();
    expected.put("node-7", blockInfos.subList(0, 2));
    expected.put("node-9", blockInfos.subList(2, 4));
    expected.put("node-11", blockInfos.subList(4, 6));
  }

  void initSet3() {
    blockInfos = new ArrayList<>();
    activeNode = new ArrayList<>();
    activeNode.add("node-7");
    activeNode.add("node-11");
    String[] location = { "node-7", "node-9", "node-11" };
    blockInfos.add(new TableBlockInfo("node", 1, "1", location, 0));
    blockInfos.add(new TableBlockInfo("node", 2, "1", location, 0));
    blockInfos.add(new TableBlockInfo("node", 3, "1", location, 0));
    blockInfos.add(new TableBlockInfo("node", 4, "1", location, 0));
    blockInfos.add(new TableBlockInfo("node", 5, "1", location, 0));
    blockInfos.add(new TableBlockInfo("node", 6, "1", location, 0));
    expected = new HashMap<>();
    expected.put("node-7", blockInfos.subList(0, 3));
    expected.put("node-11", blockInfos.subList(3, 6));
  }
}