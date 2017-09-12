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
