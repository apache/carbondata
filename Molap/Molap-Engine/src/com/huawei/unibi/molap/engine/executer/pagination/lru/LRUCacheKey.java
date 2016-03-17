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

/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwedLwWEET5JCCp2J65j3EiB2PJ4ohyqaGEDuXyJ
TTt3d0nsrTx7ES8AQGey2f3lzw4JiAMhPCO9Elgg64zoxrOmDrr8C9/Gc9hMkEyK0j2syUWK
gb12a/Kw/S3768wv/RT/uVWNuI2M1jjF2ifp567TAgunJCMdI3rmSuoaPI7oeQ==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.executer.pagination.lru;

import com.huawei.unibi.molap.engine.cache.MolapSegmentHeader;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;
import com.huawei.unibi.molap.metadata.MolapMetadata.Measure;

/**
 * It is the key for LRU cache
 * @author R00900208
 *
 */
public class LRUCacheKey
{

    private String queryId;
    
    
    private long size;
    
    
    private boolean completed;

    
    private FileSizeBasedLRU lru;
    
    /**
     * segmentHeader
     */
    private MolapSegmentHeader segmentHeader;

    
    private String path;
    
    
    private int[] maskedKeyRanges;
    
    
    private int byteCount;
    
    private Measure[] measures;
    
    private String tableName;
    
    private KeyGenerator generator;
    
    /**
     * @return the measures
     */
    public Measure[] getMeasures()
    {
        return measures;
    }


    /**
     * @param measures the measures to set
     */
    public void setMeasures(Measure[] measures)
    {
        this.measures = measures;
    }


    /**
     * @return the queryId
     */
    public String getQueryId()
    {
        return queryId;
    }


    /**
     * @param queryId the queryId to set
     */
    public void setQueryId(String queryId)
    {
        this.queryId = queryId;
    }


    /**
     * @return the size
     */
    public long getSize()
    {
        return size;
    }


    /**
     * @param size the size to set
     */
    public void setSize(long size)
    {
        this.size = size;
    }
    
    /**
     * @param size the size to set
     */
    public synchronized void setIncrementalSize(long size)
    {
        this.size += size;
        lru.put(this,0);
        while(!lru.isSizeInLimits() && lru.getCount() > 1)
        {
            lru.put(this,0);
        }
    }

    /**
     * @param size the size to set
     */
    public synchronized void setDecrementalSize(long size)
    {
        this.size -= size;
        lru.put(this,0);
        while(!lru.isSizeInLimits() && lru.getCount() > 1)
        {
            lru.put(this,0);
        }
    }


    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((queryId == null) ? 0 : queryId.hashCode());
        return result;
    }


    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj)
    {
        if(obj == null ||!(obj instanceof LRUCacheKey))
        {
            return false;
        }
        if(this == obj)
        {
            return true;
        }
        LRUCacheKey other = (LRUCacheKey)obj;
        
        if(queryId == null)
        {
            if(other.queryId != null)
            {
                return false;
            }
        }
        else if(!queryId.equals(other.queryId))
        {
            return false;
        }
        return true;
    }


    /**
     * @return the completed
     */
    public boolean isCompleted()
    {
        return completed;
    }


    /**
     * @param completed the completed to set
     */
    public void setCompleted(boolean completed)
    {
        this.completed = completed;
    }


    /**
     * @param lru the lru to set
     */
    public void setLru(FileSizeBasedLRU lru)
    {
        this.lru = lru;
    }


    /**
     * @return the path
     */
    public String getPath()
    {
        return path;
    }


    /**
     * @param path the path to set
     */
    public void setPath(String path)
    {
        this.path = path;
    }


    /**
     * @return the segmentHeader
     */
    public MolapSegmentHeader getSegmentHeader()
    {
        return segmentHeader;
    }


    /**
     * @param segmentHeader the segmentHeader to set
     */
    public void setSegmentHeader(MolapSegmentHeader segmentHeader)
    {
        this.segmentHeader = segmentHeader;
    }


    /**
     * @return the maskedKeyRanges
     */
    public int[] getMaskedKeyRanges()
    {
        return maskedKeyRanges;
    }


    /**
     * @param maskedKeyRanges the maskedKeyRanges to set
     */
    public void setMaskedKeyRanges(int[] maskedKeyRanges)
    {
        this.maskedKeyRanges = maskedKeyRanges;
    }


    /**
     * @return the byteCount
     */
    public int getByteCount()
    {
        return byteCount;
    }


    /**
     * @param byteCount the byteCount to set
     */
    public void setByteCount(int byteCount)
    {
        this.byteCount = byteCount;
    }


    /**
     * @return the tableName
     */
    public String getTableName()
    {
        return tableName;
    }


    /**
     * @param tableName the tableName to set
     */
    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }


    /**
     * @return the generator
     */
    public KeyGenerator getGenerator()
    {
        return generator;
    }


    /**
     * @param generator the generator to set
     */
    public void setGenerator(KeyGenerator generator)
    {
        this.generator = generator;
    }
    
}
