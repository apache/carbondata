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
package org.apache.carbondata.core.carbon.datastore.block;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.carbondata.core.carbon.datastore.BTreeBuilderInfo;
import org.apache.carbondata.core.carbon.datastore.impl.btree.BlockletBTreeBuilder;
import org.apache.carbondata.core.carbon.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.carbon.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.carbon.metadata.blocklet.SegmentInfo;
import org.apache.carbondata.core.carbon.metadata.blocklet.index.BlockletIndex;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;

import mockit.Mock;
import mockit.MockUp;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class BlockIndexTest {

  private static SegmentInfo segmentInfo;
  private static DataFileFooter footer;
  private static ColumnSchema columnSchema;
  private static BlockletInfo blockletInfo;
  private static BlockletIndex blockletIndex;
  private static List<DataFileFooter> footerList = new ArrayList<DataFileFooter>();
  private static List<ColumnSchema> columnSchemaList = new ArrayList<ColumnSchema>();

  @BeforeClass public static void setUp() {
    segmentInfo = new SegmentInfo();
    footer = new DataFileFooter();
    columnSchema = new ColumnSchema();
    blockletInfo = new BlockletInfo();
    blockletIndex = new BlockletIndex();
  }

  @Test public void testBuild() {
    segmentInfo = new SegmentInfo();
    new MockUp<BlockletBTreeBuilder>() {
      @Mock public void build(BTreeBuilderInfo segmentBuilderInfos) {
      }
    };
    int expectedValue = 0;
    BlockIndex blockIndex = new BlockIndex();
    columnSchema.setColumnName("employeeName");
    columnSchemaList.add(new ColumnSchema());

    footer.setSegmentInfo(segmentInfo);
    footer.setColumnInTable(columnSchemaList);
    footer.setBlockletList(Arrays.asList(blockletInfo));
    footerList.add(footer);

    blockIndex.buildIndex(footerList);
    assertEquals(footerList.get(0).getNumberOfRows(), expectedValue);

  }

}
