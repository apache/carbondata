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
