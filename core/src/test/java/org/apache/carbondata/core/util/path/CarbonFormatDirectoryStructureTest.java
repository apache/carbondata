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

package org.apache.carbondata.core.util.path;

import java.io.IOException;
import java.util.UUID;

import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;

import org.junit.Test;

import static junit.framework.TestCase.assertTrue;

/**
 * Test carbon directory structure
 */
public class CarbonFormatDirectoryStructureTest {

  private final String CARBON_STORE = "/opt/carbonstore";

  /**
   * test table path methods
   */
  @Test public void testTablePathStructure() throws IOException {
    CarbonTableIdentifier tableIdentifier = new CarbonTableIdentifier("d1", "t1", UUID.randomUUID().toString());
    AbsoluteTableIdentifier identifier =
        AbsoluteTableIdentifier.from(CARBON_STORE + "/d1/t1", tableIdentifier);
    assertTrue(identifier.getTablePath().replace("\\", "/").equals(CARBON_STORE + "/d1/t1"));
    assertTrue(CarbonTablePath.getSchemaFilePath(identifier.getTablePath()).replace("\\", "/").equals(CARBON_STORE + "/d1/t1/Metadata/schema"));
    assertTrue(CarbonTablePath.getTableStatusFilePath(identifier.getTablePath()).replace("\\", "/")
        .equals(CARBON_STORE + "/d1/t1/Metadata/tablestatus"));
    assertTrue(CarbonTablePath.getDictionaryFilePath(identifier.getTablePath(), "t1_c1").replace("\\", "/")
        .equals(CARBON_STORE + "/d1/t1/Metadata/t1_c1.dict"));
    assertTrue(CarbonTablePath.getDictionaryMetaFilePath(identifier.getTablePath(), "t1_c1").replace("\\", "/")
        .equals(CARBON_STORE + "/d1/t1/Metadata/t1_c1.dictmeta"));
    assertTrue(CarbonTablePath.getSortIndexFilePath(identifier.getTablePath(),"t1_c1").replace("\\", "/")
        .equals(CARBON_STORE + "/d1/t1/Metadata/t1_c1.sortindex"));
    assertTrue(CarbonTablePath.getCarbonDataFilePath(identifier.getTablePath(), "2", 3, 4L,  0, 0, "999").replace("\\", "/")
        .equals(CARBON_STORE + "/d1/t1/Fact/Part0/Segment_2/part-3-4_batchno0-0-999.carbondata"));
  }

  /**
   * test data file name
   */
  @Test public void testDataFileName() throws IOException {
    assertTrue(CarbonTablePath.DataFileUtil.getPartNo("part-3-4-999.carbondata").equals("3"));
    assertTrue(CarbonTablePath.DataFileUtil.getTaskNo("part-3-4-999.carbondata").equals("4"));
    assertTrue(
        CarbonTablePath.DataFileUtil.getTimeStampFromFileName("part-3-4-999.carbondata").equals("999"));
    assertTrue(CarbonTablePath.DataFileUtil.getPartNo("/opt/apache-carbon/part-3-4-999.carbondata").equals("3"));
    assertTrue(CarbonTablePath.DataFileUtil.getTaskNo("/opt/apache-carbon/part-3-4-999.carbondata").equals("4"));
    assertTrue(
        CarbonTablePath.DataFileUtil.getTimeStampFromFileName("/opt/apache-carbon/part-3-4-999.carbondata").equals("999"));
  }
}
