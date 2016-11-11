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

import mockit.Mock;
import mockit.MockUp;
import org.apache.carbondata.core.carbon.path.CarbonTablePath;
import org.junit.BeforeClass;
import org.junit.Test;


public class TableBlockInfoTest {

    static TableBlockInfo tableBlockInfo;
    static TableBlockInfo tableBlockInfos;

    @BeforeClass
    public static void setup() {
        tableBlockInfo = new TableBlockInfo("filePath", 4, "segmentId", null, 6);
        tableBlockInfos = new TableBlockInfo("filepath", 6, "5", null, 6, new BlockletInfos(6, 2, 2));
    }

    @Test
    public void equalTestWithSameObject() {
        Boolean res = tableBlockInfo.equals(tableBlockInfo);
        assert (res);
    }

    @Test
    public void equalTestWithSimilarObject() {
        TableBlockInfo tableBlockInfoTest = new TableBlockInfo("filePath", 4, "segmentId", null, 6);
        Boolean res = tableBlockInfo.equals(tableBlockInfoTest);
        assert (res);
    }

    @Test
    public void equalTestWithNullObject() {
        Boolean res = tableBlockInfo.equals(null);
        assert (!res);
    }

    @Test
    public void equalTestWithStringObject() {
        Boolean res = tableBlockInfo.equals("dummyObject");
        assert (!res);
    }

    @Test
    public void equlsTestWithDiffSegmentId() {
        TableBlockInfo tableBlockInfoTest = new TableBlockInfo("filePath", 4, "diffsegmentId", null, 6);
        Boolean res = tableBlockInfo.equals(tableBlockInfoTest);
        assert (!res);
    }

    @Test
    public void equlsTestWithDiffBlockOffset() {
        TableBlockInfo tableBlockInfoTest = new TableBlockInfo("filePath", 6, "segmentId", null, 6);
        Boolean res = tableBlockInfo.equals(tableBlockInfoTest);
        assert (!res);
    }

    @Test
    public void equalsTestWithDiffBlockLength() {
        TableBlockInfo tableBlockInfoTest = new TableBlockInfo("filePath", 4, "segmentId", null, 4);
        Boolean res = tableBlockInfo.equals(tableBlockInfoTest);
        assert (!res);
    }

    @Test
    public void equalsTestWithDiffBlockletNumber() {
        TableBlockInfo tableBlockInfoTest = new TableBlockInfo("filepath", 6, "segmentId", null, 6, new BlockletInfos(6, 3, 2));
        Boolean res = tableBlockInfos.equals(tableBlockInfoTest);
        assert (!res);
    }

    @Test
    public void equalsTestWithDiffFilePath() {
        TableBlockInfo tableBlockInfoTest = new TableBlockInfo("difffilepath", 6, "segmentId", null, 6, new BlockletInfos(6, 3, 2));
        Boolean res = tableBlockInfos.equals(tableBlockInfoTest);
        assert (!res);
    }

    @Test
    public void compareToTestForSegmentId() {
        TableBlockInfo tableBlockInfo = new TableBlockInfo("difffilepath", 6, "5", null, 6, new BlockletInfos(6, 3, 2));
        int res = tableBlockInfos.compareTo(tableBlockInfo);
        assert (res == 2);

        TableBlockInfo tableBlockInfo1 = new TableBlockInfo("difffilepath", 6, "6", null, 6, new BlockletInfos(6, 3, 2));
        int res1 = tableBlockInfos.compareTo(tableBlockInfo1);
        assert (res1 == -1);

        TableBlockInfo tableBlockInfo2 = new TableBlockInfo("difffilepath", 6, "4", null, 6, new BlockletInfos(6, 3, 2));
        int res2 = tableBlockInfos.compareTo(tableBlockInfo2);
        assert (res2 == 1);
    }

    @Test
    public void compareToTestForOffsetAndLength() {
        new MockUp<CarbonTablePath>() {
            @Mock
            boolean isCarbonDataFile(String fileNameWithPath) {
                return true;
            }

        };

        new MockUp<CarbonTablePath.DataFileUtil>() {
            @Mock
            String getTaskNo(String carbonDataFileName) {
                return carbonDataFileName.length() + "";
            }

            @Mock
            String getPartNo(String carbonDataFileName) {
                return "5";
            }

        };

        TableBlockInfo tableBlockInfo = new TableBlockInfo("difffilepaths", 6, "5", null, 3);
        int res = tableBlockInfos.compareTo(tableBlockInfo);
        assert (res == -5);

        TableBlockInfo tableBlockInfo1 = new TableBlockInfo("filepath", 6, "5", null, 3);
        int res1 = tableBlockInfos.compareTo(tableBlockInfo1);
        assert (res1 == 1);

        TableBlockInfo tableBlockInfoTest = new TableBlockInfo("filePath", 6, "5", null, 7, new BlockletInfos(6, 2, 2));
        int res2 = tableBlockInfos.compareTo(tableBlockInfoTest);
        assert (res2 == -1);
    }

    @Test
    public void compareToTestWithStartBlockletNo(){
        TableBlockInfo tableBlockInfo = new TableBlockInfo("filepath", 6, "5", null, 6, new BlockletInfos(6, 3, 2));
        int res = tableBlockInfos.compareTo(tableBlockInfo);
        assert (res == -1);

        TableBlockInfo tableBlockInfo1 = new TableBlockInfo("filepath", 6, "5", null, 6, new BlockletInfos(6, 1, 2));
        int res1 = tableBlockInfos.compareTo(tableBlockInfo1);
        assert (res1 == 1);
    }

    @Test
    public void compareToTest(){
        int res = tableBlockInfos.compareTo(tableBlockInfos);
        assert (res == 0);
    }

    @Test
    public void hashCodeTest(){
        int res = tableBlockInfo.hashCode();
        System.out.println(res);
        assert(res == 1041505621);
    }
}
