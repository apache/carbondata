package com.huawei.datasight.molap.autoagg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.huawei.datasight.molap.autoagg.model.RequestTest;
import com.huawei.datasight.molap.datastats.util.CommonUtilTest;
import com.huawei.datasight.molap.datastats.util.DataStatsUtilTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({ 
        DataStatsAggregateServiceTest.class,
        QueryStatsAggServiceTest.class,MergeDistinctDataTest.class,CommonUtilTest.class,DataStatsUtilTest.class
        ,RequestTest.class,RestructureTest.class,FactDataHandlerTest.class
})
public class AggregateTestSuite
{

}
