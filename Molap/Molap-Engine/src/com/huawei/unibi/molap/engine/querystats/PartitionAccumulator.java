package com.huawei.unibi.molap.engine.querystats;

import java.io.Serializable;

import org.apache.spark.AccumulatorParam;

/**
 * Accumulator, in spark, is like global variable. It is registered at driver side and updated
 * at executor side. This class does the merging task i.e all executor value it merges to one.
 * @author A00902717
 *
 */
public class PartitionAccumulator implements AccumulatorParam<PartitionDetail>, Serializable
{

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    @Override
    public PartitionDetail addInPlace(PartitionDetail part1, PartitionDetail part2)
    {

        return mergePartitions(part1, part2);
    }

    @Override
    public PartitionDetail zero(PartitionDetail part)
    {
        return part;
    }

    @Override
    public PartitionDetail addAccumulator(PartitionDetail part1, PartitionDetail part2)
    {
        return mergePartitions(part1, part2);
    }

    private PartitionDetail mergePartitions(PartitionDetail part1, PartitionDetail part2)
    {
        if(null == part1)
        {
            return part2;
        }
        if(null == part2)
        {
            return part1;
        }
        PartitionDetail merged = new PartitionDetail();
        merged.addNumberOfNodesScanned(part1.getNumberOfNodesScanned());
        merged.addNumberOfNodesScanned(part2.getNumberOfNodesScanned());

        merged.addNumberOfRowsScanned(part1.getNoOfRowsScanned());
        merged.addNumberOfRowsScanned(part2.getNoOfRowsScanned());

        return merged;
    }

}
