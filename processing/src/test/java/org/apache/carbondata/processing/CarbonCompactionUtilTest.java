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
package org.apache.carbondata.processing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.datastore.block.TaskBlockInfo;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.schema.SchemaEvolution;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.format.AlterOperation;
import org.apache.carbondata.processing.merger.CarbonCompactionUtil;

import mockit.Mock;
import mockit.MockUp;
import org.junit.BeforeClass;
import org.junit.Test;
import static junit.framework.TestCase.*;

public class CarbonCompactionUtilTest {

  private static Map<String, TaskBlockInfo> segmentMapping = new HashMap<>();
  private static Map<String, List<DataFileFooter>> dataFileMetadataSegMapping = new HashMap<>();

  @BeforeClass public static void beforeTest () {
    segmentMapping.put("key1", new TaskBlockInfo());
    DataFileFooter dataFileFooter = new DataFileFooter();
    dataFileFooter.setSchemaUpdatedTimeStamp(5L);
    List<DataFileFooter> dataFileFooters = new ArrayList<>();
    dataFileFooters.add(dataFileFooter);
    dataFileMetadataSegMapping.put("key1", dataFileFooters);
  }

  @Test public void testCheckIfSortingIsRequiredReturnsFalseForRenameOperationAfterCompaction() {
    CarbonTable carbonTable = new MockUp<CarbonTable>(){
      @Mock @SuppressWarnings("unused")
      public Map<Long, List<AlterOperation>> getOperationsMap() {
        Map<Long, List<AlterOperation>> operationsMap = new HashMap<>();
        operationsMap.put(1L, Arrays.asList(AlterOperation.AddColumn));
        operationsMap.put(2L, Arrays.asList(AlterOperation.RemoveColumn));
        operationsMap.put(6L, Arrays.asList(AlterOperation.RenameTable));
        return operationsMap;
      }
    }.getMockInstance();
    assertFalse(CarbonCompactionUtil.checkIfAnyRestructuredBlockExists(segmentMapping, dataFileMetadataSegMapping, carbonTable));
  }

  @Test public void testCheckIfSortingIsRequiredReturnsFalse() {
    CarbonTable carbonTable = new MockUp<CarbonTable>(){
      @Mock @SuppressWarnings("unused")
      public Map<Long, List<AlterOperation>> getOperationsMap() {
        Map<Long, List<AlterOperation>> operationsMap = new HashMap<>();
        operationsMap.put(1L, Arrays.asList(AlterOperation.AddColumn));
        operationsMap.put(2L, Arrays.asList(AlterOperation.RemoveColumn));
        return operationsMap;
      }
    }.getMockInstance();
    assertFalse(CarbonCompactionUtil.checkIfAnyRestructuredBlockExists(segmentMapping, dataFileMetadataSegMapping, carbonTable));
  }

  @Test public void testCheckIfSortingIsRequiredReturnsTrue() {
    CarbonTable carbonTable = new MockUp<CarbonTable>(){
      @Mock @SuppressWarnings("unused")
      public Map<Long, List<AlterOperation>> getOperationsMap() {
        Map<Long, List<AlterOperation>> operationsMap = new HashMap<>();
        operationsMap.put(6L, Arrays.asList(AlterOperation.AddColumn));
        return operationsMap;
      }
    }.getMockInstance();
    assertTrue(CarbonCompactionUtil.checkIfAnyRestructuredBlockExists(segmentMapping, dataFileMetadataSegMapping, carbonTable));
  }

  @Test public void testCheckIfSortingIsRequiredReturnsFalseIfNoValueHasGreaterTime() {
    CarbonTable carbonTable = new MockUp<CarbonTable>(){
      @Mock @SuppressWarnings("unused")
      public Map<Long, List<AlterOperation>> getOperationsMap() {
        Map<Long, List<AlterOperation>> operationsMap = new HashMap<>();
        operationsMap.put(1L, Arrays.asList(AlterOperation.RenameTable));
        operationsMap.put(2L, Arrays.asList(AlterOperation.AddColumn));
        operationsMap.put(5L, Arrays.asList(AlterOperation.AddColumn));
        return operationsMap;
      }
    }.getMockInstance();
    assertFalse(CarbonCompactionUtil.checkIfAnyRestructuredBlockExists(segmentMapping, dataFileMetadataSegMapping, carbonTable));
  }

}
