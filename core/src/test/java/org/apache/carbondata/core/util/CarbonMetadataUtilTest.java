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

package org.apache.carbondata.core.util;

import mockit.Mock;
import mockit.MockUp;
import org.apache.carbondata.core.carbon.metadata.blocklet.index.BlockletBTreeIndex;
import org.apache.carbondata.core.carbon.metadata.blocklet.index.BlockletIndex;
import org.apache.carbondata.core.carbon.metadata.blocklet.index.BlockletMinMaxIndex;
import org.apache.carbondata.core.carbon.metadata.index.BlockIndexInfo;
import org.apache.carbondata.core.metadata.BlockletInfoColumnar;
import org.apache.carbondata.format.BlockIndex;
import org.apache.carbondata.format.ColumnSchema;
import org.apache.carbondata.format.IndexHeader;
import org.apache.carbondata.format.SegmentInfo;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.*;
import static org.apache.carbondata.core.util.CarbonMetadataUtil.getBlockIndexInfo;
import static org.apache.carbondata.core.util.CarbonMetadataUtil.getIndexHeader;

public class CarbonMetadataUtilTest {

    @Test
    public void testGetIndexHeader(){
        int[] columnCardinality = {1,2,3,4};
        List<ColumnSchema> columnSchemaList = new ArrayList<>();
        SegmentInfo segmentInfo = new SegmentInfo();
        segmentInfo.setNum_cols(0);
        segmentInfo.setColumn_cardinalities(CarbonUtil.convertToIntegerList(columnCardinality));

        IndexHeader indexHeader = new IndexHeader();
        indexHeader.setSegment_info(segmentInfo);
        indexHeader.setTable_columns(columnSchemaList);

        IndexHeader indexheaderResult =getIndexHeader(columnCardinality,columnSchemaList);
        assertEquals(indexHeader,indexheaderResult);
    }

}
