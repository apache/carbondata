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
import java.util.Arrays;

import org.apache.spark.Accumulator;


/**
 * This class has information about user query.
 * 
 * @author a00902717
 *
 */
public class QueryDetail implements Comparable<QueryDetail>,Serializable
{
 
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * Execution time of query
     */
    private long totalExecutionTime;

    private int frequency=1;

    private String cubeName;

    private String schemaName;

    private long recordSize;

    private String factTableName;

    private String queryId;

    private int[] dimOrdinals;
    
    private long queryStartTime;
    
    private boolean isGroupBy;
    
    private boolean isFilterQuery;
    
    private long noOfRowsScanned;
    
    private long noOfNodesScanned;
    
    private int benefitRatio;
    
    private long weightage;
    
    
    /**
     * if given select query has limit parameter
     */
    private boolean isLimitPassed;
    
    private String metaPath;
    /**
     * this will have each partition detail
     */
    private Accumulator<PartitionDetail> partitionsDetail;

    
    public QueryDetail(String queryId)
    {
        this.queryId = queryId;
    }
    
    public QueryDetail()
    {
        
    }
    
    
    
    public long getNoOfRowsScanned()
    {
        return noOfRowsScanned;
    }

    public void setNoOfRowsScanned(long noOfRowsScanned)
    {
        this.noOfRowsScanned = noOfRowsScanned;
    }

    public Accumulator<PartitionDetail> getPartitionsDetail()
    {
        return partitionsDetail;
    }

    public void setPartitionsDetail(Accumulator<PartitionDetail> partitionsDetail)
    {
        this.partitionsDetail = partitionsDetail;
    }

    public long getNoOfNodesScanned()
    {
        return noOfNodesScanned;
    }

    public void setQueryStartTime(long queryTime)
    {
        this.queryStartTime=queryTime;
    }
    
    public long getQueryStartTime()
    {
        return this.queryStartTime;
    }
    
    public void setDimOrdinals(int[] dimOrdinals)
    {
        this.dimOrdinals=dimOrdinals;
       
    }

    public String getQueryId()
    {
        return queryId;
    }

    public String getFactTableName()
    {
        return factTableName;
    }

    public void setFactTableName(String factTableName)
    {
        this.factTableName = factTableName;
    }

    public String getCubeName()
    {
        return cubeName;
    }

    public void setCubeName(String cubeName)
    {
        this.cubeName = cubeName;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public void setSchemaName(String schemaName)
    {
        this.schemaName = schemaName;
    }

    public long getRecordSize()
    {
        return recordSize;
    }

    public void setRecordSize(long recordSize)
    {
        this.recordSize = recordSize;
    }

    public int[] getDimOrdinals()
    {
       return dimOrdinals;
    }

    public long getTotalExecutionTime()
    {
        return totalExecutionTime;
    }

    public void setTotalExecutionTime(long totalExecutionTime)
    {
        this.totalExecutionTime = totalExecutionTime;
    }
    
    
    public void setNoOfNodesScanned(long noOfNodesScanned)
    {
        this.noOfNodesScanned =noOfNodesScanned;
    }
    
    
  
    public void setNumberOfRowsScanned(long noOfRowsScanned)
    {
       this.noOfRowsScanned=noOfRowsScanned;
        
    }
    

   

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(dimOrdinals);
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if(this == obj)
        {
            return true;
        }   
        if(obj == null)
        {
            return false;
        }   
        if(getClass() != obj.getClass())
        {
            return false;
        }   
        QueryDetail other = (QueryDetail)obj;
        if(!Arrays.equals(dimOrdinals, other.dimOrdinals))
        {
            return false;
        }   
        return true;
    }

    @Override
    public int compareTo(QueryDetail o)
    {
        // sort based on weightage
        int res=Long.valueOf(o.weightage).compareTo(this.weightage);
        if(res!=0)
        {
            return res;
        }
        // sort based on execution time
        res = Long.valueOf(o.totalExecutionTime).compareTo(this.totalExecutionTime);
        if (res != 0)
        {
            return res;
        }
        // sort based on result size
        res = Long.valueOf(o.recordSize).compareTo(this.recordSize);
        if (res != 0)
        {
            return res;
        }
        // sort based on frequency of query
        res = Integer.valueOf(o.frequency).compareTo(frequency);
        return res;
    }

    public void incrementFrequency()
    {
        this.frequency++;

    }
    
    public int getFrequency()
    {
        return this.frequency;
    }

    public boolean isGroupBy()
    {
        return isGroupBy;
    }

    public void setGroupBy(boolean isGroupBy)
    {
        this.isGroupBy = isGroupBy;
    }

   
   public void setBenefitRatio(int benefitRatio)
   {
       this.benefitRatio = benefitRatio;
   }
  
   public int getBenefitRatio()
   {
       return this.benefitRatio;
   }
   
   public void setWeightage(long weightage)
   {
       this.weightage=weightage;
   }

   public boolean isFilterQuery()
   {
       return isFilterQuery;
   }

   public void setFilterQuery(boolean isFilterQuery)
   {
       this.isFilterQuery = isFilterQuery;
   }


   public boolean isLimitPassed()
   {
       return isLimitPassed;
   }

   public void setLimitPassed(boolean isLimitPassed)
   {
       this.isLimitPassed = isLimitPassed;
   }

   public String getMetaPath()
   {
       return metaPath;
   }

   public void setMetaPath(String dataPath)
   {
       this.metaPath = dataPath;
   }
   
   
}
