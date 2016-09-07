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
package org.apache.carbondata.integration.spark.load;

import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.carbondata.spark.load.CarbonLoaderUtil;
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


  /**
   * Test case with 4 blocks and 4 nodes with 3 replication.
   *
   * @throws Exception
   */
  @Test public void nodeBlockMapping() throws Exception {

    Map<TableBlockInfo, List<String>> inputMap = new HashMap<TableBlockInfo, List<String>>(5);

    TableBlockInfo block1 =
        new TableBlockInfo("path1", 123, "1", new String[] { "1", "2", "3" }, 111);
    TableBlockInfo block2 =
        new TableBlockInfo("path2", 123, "2", new String[] { "2", "3", "4" }, 111);
    TableBlockInfo block3 =
        new TableBlockInfo("path3", 123, "3", new String[] { "3", "4", "1" }, 111);
    TableBlockInfo block4 =
        new TableBlockInfo("path4", 123, "4", new String[] { "1", "2", "4" }, 111);

    inputMap.put(block1, Arrays.asList(new String[]{"1","2","3"}));
    inputMap.put(block2, Arrays.asList(new String[]{"2","3","4"}));
    inputMap.put(block3, Arrays.asList(new String[]{"3","4","1"}));
    inputMap.put(block4, Arrays.asList(new String[]{"1","2","4"}));

    List<TableBlockInfo> inputBlocks = new ArrayList(6);
    inputBlocks.add(block1);
    inputBlocks.add(block2);
    inputBlocks.add(block3);
    inputBlocks.add(block4);

    Map<String, List<TableBlockInfo>> outputMap
        = CarbonLoaderUtil.nodeBlockMapping(inputBlocks, 4);

    Assert.assertTrue(calculateBlockDistribution(inputMap, outputMap, 4, 4));

    Assert.assertTrue(calculateBlockLocality(inputMap, outputMap, 4, 4));
  }

  private boolean calculateBlockLocality(Map<TableBlockInfo, List<String>> inputMap,
      Map<String, List<TableBlockInfo>> outputMap, int numberOfBlocks, int numberOfNodes) {

    double notInNodeLocality = 0;
    for (Map.Entry<String, List<TableBlockInfo>> entry : outputMap.entrySet()) {

      List<TableBlockInfo> blockListOfANode = entry.getValue();

      for (TableBlockInfo eachBlock : blockListOfANode) {

        // for each block check the node locality

        List<String> blockLocality = inputMap.get(eachBlock);
        if (!blockLocality.contains(entry.getKey())) {
          notInNodeLocality++;
        }
      }
    }

    System.out.println(
        ((notInNodeLocality / numberOfBlocks) * 100) + " " + "is the node locality mismatch");
    if ((notInNodeLocality / numberOfBlocks) * 100 > 30) {
      return false;
    }
    return true;
  }

  private boolean calculateBlockDistribution(Map<TableBlockInfo, List<String>> inputMap,
      Map<String, List<TableBlockInfo>> outputMap, int numberOfBlocks, int numberOfNodes) {

    int nodesPerBlock = numberOfBlocks / numberOfNodes;

    for (Map.Entry<String, List<TableBlockInfo>> entry : outputMap.entrySet()) {

      if (entry.getValue().size() < nodesPerBlock) {
        return false;
      }
    }
    return true;
  }

  /**
   * Test case with 5 blocks and 3 nodes
   *
   * @throws Exception
   */
  @Test public void nodeBlockMappingTestWith5blocks3nodes() throws Exception {

    Map<TableBlockInfo, List<String>> inputMap = new HashMap<TableBlockInfo, List<String>>(5);

    TableBlockInfo block1 =
        new TableBlockInfo("part-0-0-1462341987000", 123, "1", new String[] { "1", "2", "3" }, 111);
    TableBlockInfo block2 =
        new TableBlockInfo("part-1-0-1462341987000", 123, "2", new String[] { "1", "2", "3" }, 111);
    TableBlockInfo block3 =
        new TableBlockInfo("part-2-0-1462341987000", 123, "3", new String[] { "1", "2", "3" }, 111);
    TableBlockInfo block4 =
        new TableBlockInfo("part-3-0-1462341987000", 123, "4", new String[] { "1", "2", "3" }, 111);
    TableBlockInfo block5 =
        new TableBlockInfo("part-4-0-1462341987000", 123, "5", new String[] { "1", "2", "3" }, 111);

    inputMap.put(block1, Arrays.asList(new String[]{"1","2","3"}));
    inputMap.put(block2, Arrays.asList(new String[]{"1","2","3"}));
    inputMap.put(block3, Arrays.asList(new String[]{"1","2","3"}));
    inputMap.put(block4, Arrays.asList(new String[]{"1","2","3"}));
    inputMap.put(block5, Arrays.asList(new String[]{"1","2","3"}));

    List<TableBlockInfo> inputBlocks = new ArrayList(6);
    inputBlocks.add(block1);
    inputBlocks.add(block2);
    inputBlocks.add(block3);
    inputBlocks.add(block4);
    inputBlocks.add(block5);

    Map<String, List<TableBlockInfo>> outputMap = CarbonLoaderUtil.nodeBlockMapping(inputBlocks, 3);

    Assert.assertTrue(calculateBlockDistribution(inputMap, outputMap, 5, 3));

    Assert.assertTrue(calculateBlockLocality(inputMap, outputMap, 5, 3));

  }

  /**
   * Test case with 6 blocks and 4 nodes where 4 th node doesnt have any local data.
   *
   * @throws Exception
   */
  @Test public void nodeBlockMappingTestWith6Blocks4nodes() throws Exception {

    Map<TableBlockInfo, List<String>> inputMap = new HashMap<TableBlockInfo, List<String>>(5);

    TableBlockInfo block1 =
        new TableBlockInfo("part-0-0-1462341987000", 123, "1", new String[] { "1", "2", "3" }, 111);
    TableBlockInfo block2 =
        new TableBlockInfo("part-1-0-1462341987000", 123, "2", new String[] { "1", "2", "3" }, 111);
    TableBlockInfo block3 =
        new TableBlockInfo("part-2-0-1462341987000", 123, "3", new String[] { "1", "2", "3" }, 111);
    TableBlockInfo block4 =
        new TableBlockInfo("part-3-0-1462341987000", 123, "4", new String[] { "1", "2", "3" }, 111);
    TableBlockInfo block5 =
        new TableBlockInfo("part-4-0-1462341987000", 123, "5", new String[] { "1", "2", "3" }, 111);
    TableBlockInfo block6 =
        new TableBlockInfo("part-5-0-1462341987000", 123, "6", new String[] { "1", "2", "3" }, 111);

    inputMap.put(block1, Arrays.asList(new String[]{"1","2","3"}));
    inputMap.put(block2, Arrays.asList(new String[]{"1","2","3"}));
    inputMap.put(block3, Arrays.asList(new String[]{"1","2","3"}));
    inputMap.put(block4, Arrays.asList(new String[]{"1","2","3"}));
    inputMap.put(block5, Arrays.asList(new String[]{"1","2","3"}));
    inputMap.put(block6, Arrays.asList(new String[]{"1","2","3"}));


    List<TableBlockInfo> inputBlocks = new ArrayList(6);
    inputBlocks.add(block1);
    inputBlocks.add(block2);
    inputBlocks.add(block3);
    inputBlocks.add(block4);
    inputBlocks.add(block5);
    inputBlocks.add(block6);

    Map<String, List<TableBlockInfo>> outputMap = CarbonLoaderUtil.nodeBlockMapping(inputBlocks, 4);

    Assert.assertTrue(calculateBlockDistribution(inputMap, outputMap, 6, 4));

    Assert.assertTrue(calculateBlockLocality(inputMap, outputMap, 6, 4));

  }

  /**
   * Test case with 10 blocks and 4 nodes with 10,60,30 % distribution
   *
   * @throws Exception
   */
  @Test public void nodeBlockMappingTestWith10Blocks4nodes() throws Exception {

    Map<TableBlockInfo, List<String>> inputMap = new HashMap<TableBlockInfo, List<String>>(5);

    TableBlockInfo block1 =
        new TableBlockInfo("part-1-0-1462341987000", 123, "1", new String[] { "2", "4" }, 111);
    TableBlockInfo block2 =
        new TableBlockInfo("part-2-0-1462341987000", 123, "2", new String[] { "2", "4" }, 111);
    TableBlockInfo block3 =
        new TableBlockInfo("part-3-0-1462341987000", 123, "3", new String[] { "2", "4" }, 111);
    TableBlockInfo block4 =
        new TableBlockInfo("part-4-0-1462341987000", 123, "4", new String[] { "2", "4" }, 111);
    TableBlockInfo block5 =
        new TableBlockInfo("part-5-0-1462341987000", 123, "5", new String[] { "2", "4" }, 111);
    TableBlockInfo block6 =
        new TableBlockInfo("part-6-0-1462341987000", 123, "6", new String[] { "2", "4" }, 111);
    TableBlockInfo block7 =
        new TableBlockInfo("part-7-0-1462341987000", 123, "7", new String[] { "3", "4" }, 111);
    TableBlockInfo block8 =
        new TableBlockInfo("part-8-0-1462341987000", 123, "8", new String[] { "3", "4" }, 111);
    TableBlockInfo block9 =
        new TableBlockInfo("part-9-0-1462341987000", 123, "9", new String[] { "3", "4" }, 111);
    TableBlockInfo block10 =
        new TableBlockInfo("part-10-0-1462341987000", 123, "9", new String[] { "1", "4" }, 111);

    inputMap.put(block1, Arrays.asList(new String[]{"2","4"}));
    inputMap.put(block2, Arrays.asList(new String[]{"2","4"}));
    inputMap.put(block3, Arrays.asList(new String[]{"2","4"}));
    inputMap.put(block4, Arrays.asList(new String[]{"2","4"}));
    inputMap.put(block5, Arrays.asList(new String[]{"2","4"}));
    inputMap.put(block6, Arrays.asList(new String[]{"2","4"}));
    inputMap.put(block7, Arrays.asList(new String[]{"3","4"}));
    inputMap.put(block8, Arrays.asList(new String[]{"3","4"}));
    inputMap.put(block9, Arrays.asList(new String[]{"3","4"}));
    inputMap.put(block10, Arrays.asList(new String[]{"1","4"}));

    List<TableBlockInfo> inputBlocks = new ArrayList(6);
    inputBlocks.add(block1);
    inputBlocks.add(block2);
    inputBlocks.add(block3);
    inputBlocks.add(block4);
    inputBlocks.add(block5);
    inputBlocks.add(block6);
    inputBlocks.add(block7);
    inputBlocks.add(block8);
    inputBlocks.add(block9);
    inputBlocks.add(block10);

    Map<String, List<TableBlockInfo>> outputMap = CarbonLoaderUtil.nodeBlockMapping(inputBlocks, 4);

    Assert.assertTrue(calculateBlockDistribution(inputMap, outputMap, 10, 4));

    Assert.assertTrue(calculateBlockLocality(inputMap, outputMap, 10, 4));
  }

}