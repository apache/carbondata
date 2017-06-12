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
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class BlockInfoTest {

  static BlockInfo blockInfo;

  @BeforeClass public static void setup() {
    blockInfo = new BlockInfo(new TableBlockInfo("/filePath.carbondata", 6, "segmentId", null, 6, ColumnarFormatVersion.V1, null));
  }

  @Test public void hashCodeTest() {
    int res = blockInfo.hashCode();
    int expectedResult = 1694768249;
    assertEquals(expectedResult, res);
  }

  @Test public void equalsTestwithSameObject() {
    Boolean res = blockInfo.equals(blockInfo);
    assert (res);
  }

  @Test public void equalsTestWithSimilarObject() {
    BlockInfo blockInfoTest =
        new BlockInfo(new TableBlockInfo("/filePath.carbondata", 6, "segmentId", null, 6, ColumnarFormatVersion.V1, null));
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
        new BlockInfo(new TableBlockInfo("/filePath.carbondata", 6, "diffSegmentId", null, 6, ColumnarFormatVersion.V1, null));
    Boolean res = blockInfo.equals(blockInfoTest);
    assert (!res);
  }

  @Test public void equalsTestWithDifferentOffset() {
    BlockInfo blockInfoTest =
        new BlockInfo(new TableBlockInfo("/filePath.carbondata", 62, "segmentId", null, 6, ColumnarFormatVersion.V1, null));
    Boolean res = blockInfo.equals(blockInfoTest);
    assert (!res);
  }

  @Test public void equalsTestWithDifferentBlockLength() {
    BlockInfo blockInfoTest =
        new BlockInfo(new TableBlockInfo("/filePath.carbondata", 6, "segmentId", null, 62, ColumnarFormatVersion.V1, null));
    Boolean res = blockInfo.equals(blockInfoTest);
    assert (!res);
  }

  @Test public void equalsTestWithDiffFilePath() {
    BlockInfo blockInfoTest =
        new BlockInfo(new TableBlockInfo("/diffFilePath.carbondata", 6, "segmentId", null, 62, ColumnarFormatVersion.V1, null));
    Boolean res = blockInfoTest.equals(blockInfo);
    assert (!res);
  }
}
