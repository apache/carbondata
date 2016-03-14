package com.huawei.unibi.molap.engine.querystats;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;



@RunWith(Suite.class)
@Suite.SuiteClasses({ 
        BinaryQueryStoreTest.class,
        PartitionAccumulatorTest.class
})
public class QueryStatsTestSuite
{

}
