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
package org.apache.carbondata.core.datastore.block;

import org.apache.carbondata.core.metadata.ColumnarFormatVersion;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TableTaskInfoTest {

  static TableTaskInfo tableTaskInfo;
  static List<TableBlockInfo> tableBlockInfoList;

  @BeforeClass public static void setup() {
    tableBlockInfoList = new ArrayList<>(5);

    String[] locations = { "loc1", "loc2", "loc3" };
    tableBlockInfoList.add(0, new TableBlockInfo("filePath", 2, "segmentID", locations, 6, ColumnarFormatVersion.V1, null));

    String[] locs = { "loc4", "loc5" };
    tableBlockInfoList.add(1, new TableBlockInfo("filepath", 2, "segmentId", locs, 6, ColumnarFormatVersion.V1, null));

    tableTaskInfo = new TableTaskInfo("taskId", tableBlockInfoList);
  }

  @Test public void getLocationsTest() {
    String locations[] = { "loc1", "loc2", "loc3", "loc4", "loc5" };
    String res[] = tableTaskInfo.getLocations();
    Assert.assertTrue(Arrays.equals(locations, res));
  }

  @Test public void maxNoNodesTest() {
    List<String> locs = new ArrayList<String>();
    locs.add("loc1");
    locs.add("loc2");
    locs.add("loc3");
    locs.add("loc4");
    locs.add("loc5");

    List<String> res = TableTaskInfo.maxNoNodes(tableBlockInfoList);
    Assert.assertTrue(res.equals(locs));
  }

  @Test public void maxNoNodesTestForElse() {
    List<String> locs = new ArrayList<String>();
    locs.add("loc1");
    locs.add("loc2");
    locs.add("loc3");
    List<TableBlockInfo> tableBlockInfoListTest = new ArrayList<>();

    String[] locations = { "loc1", "loc2", "loc3" };
    tableBlockInfoListTest.add(0, new TableBlockInfo("filePath", 2, "segmentID", locations, 6, ColumnarFormatVersion.V1, null));

    String[] locations1 = { "loc1", "loc2", "loc3" };
    tableBlockInfoListTest.add(1, new TableBlockInfo("filePath", 2, "segmentID", locations1, 6, ColumnarFormatVersion.V1, null));

    List<String> res = TableTaskInfo.maxNoNodes(tableBlockInfoListTest);
    Assert.assertTrue(res.equals(locs));
  }
}
