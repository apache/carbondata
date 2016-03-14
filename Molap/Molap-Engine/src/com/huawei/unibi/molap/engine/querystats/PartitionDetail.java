package com.huawei.unibi.molap.engine.querystats;

import java.io.Serializable;

/**
 * this class will have query detail on each partition
 *
 * @author A00902717
 *
 */
public class PartitionDetail implements Serializable
{

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private String partitionId;

    /**
     * Fact file scanned by this query
     * Commented this part because, it was giving serialization issue. Currently this parameter is not used
     * and hence when required, please add Concurrent collection which is serializable
     */
    //private Set<String> scannedFactFile = new HashSet<String>();

    /**
     * No of nodes scanned for given query
     */
    private long numberOfNodesScanned;

    private long noOfRowsScanned;
    
    public PartitionDetail()
    {
        
    }

    public PartitionDetail(String partitionId)
    {
        this.partitionId = partitionId;
    }

    public String getPartitionId()
    {
        return partitionId;
    }

    public void addNumberOfNodesScanned(long numberOfNodesToScan)
    {
        this.numberOfNodesScanned+= numberOfNodesToScan;

    }

    public long getNumberOfNodesScanned()
    {
        return this.numberOfNodesScanned;
    }

    /**
     * add up no of rows for each leaf node
     * 
     * @param noOfRows
     */
    public void addNumberOfRowsScanned(long noOfRows)
    {
        noOfRowsScanned += noOfRows;

    }

    public long getNoOfRowsScanned()
    {
        return noOfRowsScanned;
    }

}
