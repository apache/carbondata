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
package org.apache.carbondata.core.carbon.datastore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.carbon.ColumnarFormatVersion;
import org.apache.carbondata.core.carbon.datastore.block.AbstractIndex;
import org.apache.carbondata.core.carbon.datastore.block.SegmentTaskIndex;
import org.apache.carbondata.core.carbon.datastore.block.SegmentTaskIndexWrapper;
import org.apache.carbondata.core.carbon.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.carbon.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.carbon.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.carbon.metadata.blocklet.SegmentInfo;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.carbon.path.CarbonTablePath;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.CarbonUtilException;

import mockit.Mock;
import mockit.MockUp;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertNull;

public class SegmentTaskIndexStoreTest {

  private static short version = 1;
  private static String locations[] = { "/tmp" };
  private static SegmentTaskIndexStore taskIndexStore;
  private static TableBlockInfo tableBlockInfo;
  private static AbsoluteTableIdentifier absoluteTableIdentifier;

  @BeforeClass public static void setUp() {
    CacheProvider cacheProvider = CacheProvider.getInstance();
    taskIndexStore = (SegmentTaskIndexStore) cacheProvider.
        <TableSegmentUniqueIdentifier, SegmentTaskIndexWrapper>
            createCache(CacheType.DRIVER_BTREE, "");
    tableBlockInfo = new TableBlockInfo("file", 0L, "SG100", locations, 10L,
        ColumnarFormatVersion.valueOf(version));
    absoluteTableIdentifier = new AbsoluteTableIdentifier("/tmp",
        new CarbonTableIdentifier("testdatabase", "testtable", "TB100"));
  }

  private List<DataFileFooter> getDataFileFooters() {
    SegmentInfo segmentInfo = new SegmentInfo();
    DataFileFooter footer = new DataFileFooter();
    ColumnSchema columnSchema = new ColumnSchema();
    BlockletInfo blockletInfo = new BlockletInfo();
    List<DataFileFooter> footerList = new ArrayList<DataFileFooter>();
    List<ColumnSchema> columnSchemaList = new ArrayList<ColumnSchema>();

    columnSchema.setColumnName("employeeName");
    columnSchemaList.add(new ColumnSchema());

    footer.setSegmentInfo(segmentInfo);
    footer.setColumnInTable(columnSchemaList);
    footer.setBlockletList(Arrays.asList(blockletInfo));
    footerList.add(footer);
    return footerList;
  }

  @Test public void loadAndGetTaskIdToSegmentsMap() throws CarbonUtilException {
    new MockUp<CarbonTablePath.DataFileUtil>() {
      @Mock String getTaskNo(String carbonDataFileName) {
        return "100";
      }
    };

    new MockUp<CarbonUtil>() {
      @Mock List<DataFileFooter> readCarbonIndexFile(String taskId,
          String bucketNumber,
          List<TableBlockInfo> tableBlockInfoList,
          AbsoluteTableIdentifier absoluteTableIdentifier) {
        return getDataFileFooters();
      }
    };

    new MockUp<CarbonTablePath>() {
      @Mock public String getCarbonIndexFilePath(final String taskId, final String partitionId,
          final String segmentId, final String bucketNumber) {
        return "/src/test/resources";
      }
    };

    new MockUp<SegmentTaskIndex>() {
      @Mock void buildIndex(List<DataFileFooter> footerList) {
      }
    };
    TableSegmentUniqueIdentifier tableSegmentUniqueIdentifier =
        new TableSegmentUniqueIdentifier(absoluteTableIdentifier, "SG100");

    HashMap<String, List<TableBlockInfo>> segmentToTableBlocksInfos =
        new HashMap<String, List<TableBlockInfo>>() {{
          put("SG100", Arrays.asList(tableBlockInfo));
        }};
    tableSegmentUniqueIdentifier.setSegmentToTableBlocksInfos(segmentToTableBlocksInfos);
    Map<SegmentTaskIndexStore.TaskBucketHolder, AbstractIndex> result =
        taskIndexStore.get(tableSegmentUniqueIdentifier).getTaskIdToTableSegmentMap();

    assertEquals(result.size(), 1);
    assertTrue(result.containsKey(new SegmentTaskIndexStore.TaskBucketHolder("100", "0")));
  }

  @Test public void checkExistenceOfSegmentBTree() {
    TableSegmentUniqueIdentifier tableSegmentUniqueIdentifier =
        new TableSegmentUniqueIdentifier(absoluteTableIdentifier, "SG100");
    SegmentTaskIndexWrapper segmentTaskIndexWrapper =
        taskIndexStore.getIfPresent(tableSegmentUniqueIdentifier);
    Map<SegmentTaskIndexStore.TaskBucketHolder, AbstractIndex> result = segmentTaskIndexWrapper != null ?
        segmentTaskIndexWrapper.getTaskIdToTableSegmentMap() :
        null;
    assertNull(result);
  }

}
