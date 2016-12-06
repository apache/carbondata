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

import org.apache.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.carbon.datastore.block.AbstractIndex;
import org.apache.carbondata.core.carbon.datastore.block.SegmentTaskIndex;
import org.apache.carbondata.core.carbon.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.carbon.datastore.exception.IndexBuilderException;
import org.apache.carbondata.core.carbon.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.carbon.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.carbon.metadata.blocklet.SegmentInfo;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.carbon.path.CarbonTablePath;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.carbon.ColumnarFormatVersion;

import mockit.Mock;
import mockit.MockUp;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class SegmentTaskIndexStoreTest {

  short version=1;
  String locations[] = { "/tmp" };
  SegmentTaskIndexStore taskIndexStore = SegmentTaskIndexStore.getInstance();
  TableBlockInfo tableBlockInfo = new TableBlockInfo("file", 0L, "SG100", locations, 10L, ColumnarFormatVersion.valueOf(version));
  AbsoluteTableIdentifier absoluteTableIdentifier = new AbsoluteTableIdentifier("/tmp",
      new CarbonTableIdentifier("testdatabase", "testtable", "TB100"));

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

  @Test public void loadAndGetTaskIdToSegmentsMap() throws IndexBuilderException {
    new MockUp<CarbonTablePath.DataFileUtil>() {
      @Mock String getTaskNo(String carbonDataFileName) {
        return "100";
      }
    };

    new MockUp<CarbonUtil>() {
      @Mock List<DataFileFooter> readCarbonIndexFile(String taskId,
          List<TableBlockInfo> tableBlockInfoList,
          AbsoluteTableIdentifier absoluteTableIdentifier) {
        return getDataFileFooters();
      }
    };

    new MockUp<SegmentTaskIndex>() {
      @Mock void buildIndex(List<DataFileFooter> footerList) {
      }
    };

    Map<String, AbstractIndex> result =
        taskIndexStore.loadAndGetTaskIdToSegmentsMap(new HashMap<String, List<TableBlockInfo>>() {{
          put("SG100", Arrays.asList(tableBlockInfo));
        }}, absoluteTableIdentifier);

    assertEquals(result.size(), 1);
    assertTrue(result.containsKey(new String("100")));

  }

  @Test public void checkExistenceOfSegmentBTree() {
    Map<String, AbstractIndex> result =
        taskIndexStore.getSegmentBTreeIfExists(absoluteTableIdentifier, "SG100");
    assertEquals(result, null);
  }

}
