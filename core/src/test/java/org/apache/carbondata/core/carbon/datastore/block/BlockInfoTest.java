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

import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class BlockInfoTest {

  static BlockInfo blockInfo;

  @BeforeClass public static void setup() {
    blockInfo = new BlockInfo(new TableBlockInfo("filePath", 6, "segmentId", null, 6));
  }

  @Test public void hashCodeTest() {
    int res = blockInfo.hashCode();
    int expectedResult = -520590451;
    assertEquals(res, expectedResult);
  }

  @Test public void equalsTestwithSameObject() {
    Boolean res = blockInfo.equals(blockInfo);
    assert (res);
  }

  @Test public void equalsTestWithSimilarObject() {
    BlockInfo blockInfoTest =
        new BlockInfo(new TableBlockInfo("filePath", 6, "segmentId", null, 6));
    Boolean res = blockInfo.equals(blockInfoTest);
    assert (res);
  }

  @Test public void equalsTestWithNullObject() {
    Boolean res = blockInfo.equals(null);
    assert (!res);
  }

  @Test public void equalsTestWithStringObject() {
    Boolean res = blockInfo.equals("dummy");
    assert (!res);
  }

  @Test public void equalsTestWithDifferentSegmentId() {
    BlockInfo blockInfoTest =
        new BlockInfo(new TableBlockInfo("filePath", 6, "diffSegmentId", null, 6));
    Boolean res = blockInfo.equals(blockInfoTest);
    assert (!res);
  }

  @Test public void equalsTestWithDifferentOffset() {
    BlockInfo blockInfoTest =
        new BlockInfo(new TableBlockInfo("filePath", 62, "segmentId", null, 6));
    Boolean res = blockInfo.equals(blockInfoTest);
    assert (!res);
  }

  @Test public void equalsTestWithDifferentBlockLength() {
    BlockInfo blockInfoTest =
        new BlockInfo(new TableBlockInfo("filePath", 6, "segmentId", null, 62));
    Boolean res = blockInfo.equals(blockInfoTest);
    assert (!res);
  }

  @Test public void equalsTestWithDiffFilePath() {
    BlockInfo blockInfoTest =
        new BlockInfo(new TableBlockInfo("diffFilePath", 6, "segmentId", null, 62));
    Boolean res = blockInfoTest.equals(blockInfo);
    assert (!res);
  }
}
