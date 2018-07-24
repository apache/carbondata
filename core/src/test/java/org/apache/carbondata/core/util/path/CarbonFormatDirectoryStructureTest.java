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
    assertTrue(CarbonTablePath.DataFileUtil.getSegmentNo("part-3-4-0-999.carbondata") == null);
    assertTrue(CarbonTablePath.DataFileUtil.getSegmentNo("part-3-4-0-0-999.carbondata").equals("0"));
  }

  @Test public void testGetShardName() throws IOException {
    assertTrue(CarbonTablePath.getShardName("part-1-2_batchno3-4-5-999.carbondata").equals("2_batchno3-4-5-999"));

    // check compatible for data generated before carbon version 1.4 which does not have segment id in filename
    assertTrue(CarbonTablePath.getShardName("part-1-2_batchno3-4-999.carbondata").equals("2_batchno3-4-999"));

  }
}
