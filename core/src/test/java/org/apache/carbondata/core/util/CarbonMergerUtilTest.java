package org.apache.carbondata.core.util;

import static junit.framework.TestCase.*;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Test;

import static org.apache.carbondata.core.util.CarbonMergerUtil.getCardinalityFromLevelMetadata;

/**
 * Created by knoldus on 26/10/16.
 */
public class CarbonMergerUtilTest {



    @Test
    public void testGetCardinalityFromLevelMetadata() throws Exception{
        final int[] localCardinality = {1,2,3,4,5,6};
        new MockUp<CarbonUtil>() {
            @SuppressWarnings("unused")
            @Mock
            public int[] getCardinalityFromLevelMetadataFile(String levelPath){
                return  localCardinality;
            }
        };
        int[] result= getCardinalityFromLevelMetadata("STORE_PATH","table1");
        assertEquals(result,localCardinality);
    }
}
