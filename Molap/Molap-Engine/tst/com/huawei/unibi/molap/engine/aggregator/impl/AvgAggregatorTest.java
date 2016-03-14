package com.huawei.unibi.molap.engine.aggregator.impl;
import junit.framework.TestCase;


/**
 * @author S71730
 *
 */
public class AvgAggregatorTest extends TestCase
{
    public void testsetNewValue()
    {
        AvgAggregator agg = new AvgAggregator();
        agg.setNewValue(3L);
    }
}
