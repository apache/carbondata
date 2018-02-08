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
package org.apache.carbondata.processing.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.block.Distributable;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

public class CarbonLoaderUtilTest {
  private final static LogService LOGGER
      = LogServiceFactory.getLogService(CarbonLoaderUtilTest.class.getName());

  private List<Distributable> generateBlocks() {
    List<Distributable> blockInfos = new ArrayList<>();
    String filePath = "/fakepath";
    String blockId = "1";

    String[] locations = new String[] { "host2", "host3" };
    ColumnarFormatVersion version = ColumnarFormatVersion.V1;

    TableBlockInfo tableBlockInfo1 = new TableBlockInfo(filePath + "_a", 0,
        blockId, locations, 30 * 1024 * 1024, version, null);
    blockInfos.add(tableBlockInfo1);

    TableBlockInfo tableBlockInfo2 = new TableBlockInfo(filePath + "_b", 0,
        blockId, locations, 40 * 1024 * 1024, version, null);
    blockInfos.add(tableBlockInfo2);

    TableBlockInfo tableBlockInfo3 = new TableBlockInfo(filePath + "_c", 0,
        blockId, locations, 20 * 1024 * 1024, version, null);
    blockInfos.add(tableBlockInfo3);

    TableBlockInfo tableBlockInfo4 = new TableBlockInfo(filePath + "_d", 0,
        blockId, locations, 1, version, null);
    blockInfos.add(tableBlockInfo4);

    TableBlockInfo tableBlockInfo5 = new TableBlockInfo(filePath + "_e", 0,
        blockId, locations, 1, version, null);
    blockInfos.add(tableBlockInfo5);

    TableBlockInfo tableBlockInfo6 = new TableBlockInfo(filePath + "_f", 0,
        blockId, locations, 1, version, null);
    blockInfos.add(tableBlockInfo6);

    TableBlockInfo tableBlockInfo7 = new TableBlockInfo(filePath + "_g", 0,
        blockId, locations, 1, version, null);
    blockInfos.add(tableBlockInfo7);
    return blockInfos;
  }

  private List<String> generateExecutors() {
    List<String> activeNodes = new ArrayList<>();
    activeNodes.add("host1");
    activeNodes.add("host2");
    activeNodes.add("host3");
    return activeNodes;
  }

  @Test
  public void testNodeBlockMappingByDataSize() throws Exception {
    List<Distributable> blockInfos = generateBlocks();
    List<String> activeNodes = generateExecutors();

    // the blocks are assigned by size, so the number of block for each node are different
    Map<String, List<Distributable>> nodeMappingBySize =
        CarbonLoaderUtil.nodeBlockMapping(blockInfos, -1, activeNodes,
            CarbonLoaderUtil.BlockAssignmentStrategy.BLOCK_SIZE_FIRST);
    LOGGER.info(convertMapListAsString(nodeMappingBySize));
    Assert.assertEquals(3, nodeMappingBySize.size());
    for (Map.Entry<String, List<Distributable>> entry : nodeMappingBySize.entrySet()) {
      if (entry.getValue().size() == 1) {
        // only contains the biggest block
        Assert.assertEquals(40 * 1024 * 1024L,
            ((TableBlockInfo) entry.getValue().get(0)).getBlockLength());
      } else {
        Assert.assertTrue(entry.getValue().size() > 1);
      }
    }

    // the blocks are assigned by number, so the number of blocks for each node are nearly the same
    Map<String, List<Distributable>> nodeMappingByNum =
        CarbonLoaderUtil.nodeBlockMapping(blockInfos, -1, activeNodes,
            CarbonLoaderUtil.BlockAssignmentStrategy.BLOCK_NUM_FIRST);
    LOGGER.info(convertMapListAsString(nodeMappingByNum));
    Assert.assertEquals(3, nodeMappingBySize.size());
    for (Map.Entry<String, List<Distributable>> entry : nodeMappingByNum.entrySet()) {
      Assert.assertTrue(entry.getValue().size() == blockInfos.size() / 3
          || entry.getValue().size() == blockInfos.size() / 3 + 1);
    }
  }

  private <K, T> String convertMapListAsString(Map<K, List<T>> mapList) {
    StringBuffer sb = new StringBuffer();
    for (Map.Entry<K, List<T>> entry : mapList.entrySet()) {
      String key = entry.getKey().toString();
      String value = StringUtils.join(entry.getValue(), ", ");
      sb.append(key).append(" -- ").append(value).append(System.lineSeparator());
    }
    return sb.toString();
  }
}