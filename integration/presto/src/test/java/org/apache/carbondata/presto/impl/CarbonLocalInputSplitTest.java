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

package org.apache.carbondata.presto.impl;

import java.util.ArrayList;

import org.apache.carbondata.core.indexstore.BlockletDetailInfo;
import org.apache.carbondata.hadoop.CarbonInputSplit;

import org.junit.Test;
import org.testng.Assert;

public class CarbonLocalInputSplitTest {

  public static CarbonLocalInputSplit localInputSplit;

  @Test public void testConvertSplit() {
    localInputSplit = new CarbonLocalInputSplit("segmentId", "path-to-file-requested", 0, 5,
        new ArrayList<String>(), 5, (short) 3, new String[] { "d1", "d2" },
        "{\n" + "\t\"rowCount\": \"123456789\"\n" + "}");
    localInputSplit.setDetailInfo(new BlockletDetailInfo());
    CarbonInputSplit split = CarbonLocalInputSplit.convertSplit(localInputSplit);

    Assert.assertEquals("-1", split.getBucketId());
    Assert.assertEquals("segmentId", split.getSegmentId());
  }
}
