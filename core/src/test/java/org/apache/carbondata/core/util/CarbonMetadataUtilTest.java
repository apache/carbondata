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
