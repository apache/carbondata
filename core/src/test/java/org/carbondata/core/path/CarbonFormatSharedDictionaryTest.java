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

package org.carbondata.core.path;

import java.io.IOException;

import org.carbondata.core.carbon.path.CarbonSharedDictionaryPath;

import org.junit.Test;

import static junit.framework.TestCase.assertTrue;

/**
 * test shared dictionary paths
 */
public class CarbonFormatSharedDictionaryTest {

  private final String CARBON_STORE = "/opt/carbonstore";

  /**
   * test shared dictionary location
   */
  @Test public void testSharedDimentionLocation() throws IOException {
    assertTrue(CarbonSharedDictionaryPath.getDictionaryFilePath(CARBON_STORE, "d1", "shared_c1").replace("\\", "/")
        .equals(CARBON_STORE + "/d1/SharedDictionary/shared_c1.dict"));
    assertTrue(CarbonSharedDictionaryPath.getDictionaryMetaFilePath(CARBON_STORE, "d1", "shared_c1").replace("\\", "/")
        .equals(CARBON_STORE + "/d1/SharedDictionary/shared_c1.dictmeta"));
    assertTrue(CarbonSharedDictionaryPath.getSortIndexFilePath(CARBON_STORE, "d1", "shared_c1").replace("\\", "/")
        .equals(CARBON_STORE + "/d1/SharedDictionary/shared_c1.sortindex"));
  }
}
