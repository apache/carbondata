package com.huawei.unibi.molap.engine.querystats;

import org.junit.Test;

public class PartitionAccumulatorTest
{
    @Test
    public void testAddInPlace()
    {
        PartitionAccumulator pac=new PartitionAccumulator();
        PartitionDetail partDetail1=createPartitionDetail();
        PartitionDetail partDetail2=createPartitionDetail();
        pac.addInPlace(partDetail1, partDetail2);
    }
    @Test
    public void testAddAccumulator()
    {
        PartitionAccumulator pac=new PartitionAccumulator();
        pac.zero(null);
        PartitionDetail partDetail1=createPartitionDetail();
        PartitionDetail partDetail2=createPartitionDetail();
        pac.addAccumulator(partDetail1, partDetail2);
    }
    @Test
    public void testAddAccumulator_nulldata()
    {
        PartitionAccumulator pac=new PartitionAccumulator();
        pac.zero(null);
        PartitionDetail partDetail1=createPartitionDetail();
        PartitionDetail partDetail2=createPartitionDetail();
        pac.addAccumulator(null, partDetail2);
        pac.addAccumulator(partDetail1, null);
    }
    
    
    private PartitionDetail createPartitionDetail()
    {
        PartitionDetail partDetail=new PartitionDetail();
        partDetail.addNumberOfNodesScanned(2332);
        partDetail.addNumberOfRowsScanned(323);
        
        return partDetail;
    }

}
