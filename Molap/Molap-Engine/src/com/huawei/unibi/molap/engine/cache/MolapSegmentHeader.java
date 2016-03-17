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
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwdh/HjOjN0Brs7b7TRorj6S6iAIeaqK90lj7BAM
GSGxBhD+xZAwJhttXwF7/kP1XshrM2dEfvn4L1hnki8I8pg5mgCqvyCgc3LoqhvjFI7EockQ
mPFgx95NoPYzdZ2ttTauvYgbYxeh1oJJVbN7QIugxVW5WdJLO6wbVv8XYOpyXg==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.cache;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * @author R00900208
 *
 */
public class MolapSegmentHeader implements Serializable
{
    
    /**
     * 
     */
    private static final long serialVersionUID = 8170452732144940835L;
    
    /**
     * dimensions
     */
    private int [] dims;
    
    /**
     * cube name
     */
    private String cubeUniqueName;
    
    /**
     * table name
     */
    private String factTableName;
    
    /**
     * predicates
     */
    private List<MolapPredicates> preds;
    
    /**
     * hash code
     */
    private transient int hashcode;
    
    /**
     * start key
     */
    private long[] startKey;
    
    /**
     * end key
     */
    private long[] endKey;
    
    public MolapSegmentHeader(String cubeUniqueName, String factTableName)
    {
        this.cubeUniqueName = cubeUniqueName;
        this.factTableName = factTableName;
    }


    /**
     * @return the dims
     */
    public int[] getDims()
    {
        return dims;
    }

    /**
     * @param dims the dims to set
     */
    public void setDims(int[] dims)
    {
        this.dims = dims;
    }

    /**
     * @return the cubeName
     */
    public String getCubeName()
    {
        return cubeUniqueName;
    }

    /**
     * @param cubeName the cubeName to set
     */
//    public void setCubeName(String cubeName)
//    {
//        this.cubeUniqueName = cubeName;
//    }


    /**
     * @return the factTableName
     */
    public String getFactTableName()
    {
        return factTableName;
    }

    /**
     * @param factTableName the factTableName to set
     */
//    public void setFactTableName(String factTableName)
//    {
//        this.factTableName = factTableName;
//    }

    /**
     * @return the preds
     */
    public List<MolapPredicates> getPreds()
    {
        return preds;
    }

    /**
     * @param preds the preds to set
     */
    public void setPreds(List<MolapPredicates> preds)
    {
        this.preds = preds;
    }
    

    
    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode()
    {
        if(hashcode == 0)
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((cubeUniqueName == null) ? 0 : cubeUniqueName.hashCode());
            result = prime * result + Arrays.hashCode(dims);
            result = prime * result + ((factTableName == null) ? 0 : factTableName.hashCode());
            hashcode = result; 
        }

        return hashcode;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof MolapSegmentHeader)
        {
            if(this == obj)
            {
                return true;
            }

            MolapSegmentHeader other = (MolapSegmentHeader)obj;

            if(!cubeUniqueName.equals(other.cubeUniqueName) || !Arrays.equals(dims, other.dims)
                    || !factTableName.equals(other.factTableName)
                    || preds != null && !preds.equals(other.preds))
            {
                return false;
            }

            return true;
        }

        return false;

    }
    
    /**
     * equalsForSubSet
     * @param other
     * @return
     */
    public boolean equalsForSubSet(MolapSegmentHeader other)
    {

            if(!cubeUniqueName.equals(other.cubeUniqueName) || !isSubSet(other.dims)
                    || !factTableName.equals(other.factTableName)
                    || preds != null && !preds.equals(other.preds))
            {
                return false;
            }

            return true;
    }
    
    
    private boolean isSubSet(int[] otherDims)
    {
        for(int i = 0;i < otherDims.length;i++)
        {
            boolean found = false;
            for(int j = 0;j < dims.length;j++)
            {
                if(otherDims[i] == dims[j])
                {
                    found = true;
                    break;
                }
            }
            if(!found)
            {
                return false;
            }
        }
        return true;
    }
    
    
    /**
     * equalsForSubSet
     * @param other
     * @return
     */
    public boolean equalsForSubSetOfPreds(MolapSegmentHeader other)
    {

            if(!cubeUniqueName.equals(other.cubeUniqueName) || (dims.length>0 && !Arrays.equals(dims, other.dims))
                    || !factTableName.equals(other.factTableName)
                    || !isPredSubSet(preds, other.preds, other.dims))
            {
                return false;
            }

            return true;
    }
    /**
     * equalsForSubSet
     * @param other
     * @return
     */
    public boolean equalsForSubSetOfPredsForSegmentQuery(MolapSegmentHeader other)
    {
      /*  if(!cubeUniqueName.equals(other.cubeUniqueName) || !factTableName.equals(other.factTableName)
                || !isDimensionSubSet(dims, other.dims) || !isPredSubSetForSegmentQuery(preds, other.dims))
        {
            return false;
        }

        return true;*/
        return !(!cubeUniqueName.equals(other.cubeUniqueName) || !factTableName.equals(other.factTableName)
                || !isDimensionSubSet(dims, other.dims) || !isPredSubSetForSegmentQuery(preds, other.dims));
    }
    
    private boolean isDimensionSubSet(int[] dims1, int[] dims2)
    {
        for(int i = 0;i < dims1.length;i++)
        {
           if(Arrays.binarySearch(dims2,dims1[i])<0)
           {
               return false;
           }
        }
        return true;
    }


    private boolean isPredSubSetForSegmentQuery(List<MolapPredicates> preds, int[] dims)
    {
            return isAllPredPresentOnDims(dims,preds);
    }
    
    private boolean isPredSubSet(List<MolapPredicates> preds,List<MolapPredicates> otherPreds,int[] otherDims)
    {
        if(isAllPredPresentOnDims(otherDims, otherPreds))
        {
            if(hasExcludeFilters(preds) || hasExcludeFilters(otherPreds))
            {
                return false;
            }
            if(otherPreds == null || otherPreds.size() == 0)
            {
                return true;
            }
            if(preds== null || preds.size() == 0)
            {
                return false;
            }
            if(preds.size() != otherPreds.size())
            {
                return false;
            }
            for(int i = 0;i < preds.size();i++)
            {
                MolapPredicates molapPredicates = preds.get(i);
                boolean found = false;
                for(int j = 0;j < otherPreds.size();j++)
                {
                    if(otherPreds.get(j).getOrdinal() == molapPredicates.getOrdinal())
                    {
                        found = true;
                        if(!isFilterSubSet(molapPredicates.getInclude(), otherPreds.get(j).getInclude()))
                        {
                            return false;
                        }
                    }
                }
                if(!found)
                {
                    return false;
                }
            }
            
        }
        else
        {
            return false;
        }
        return true;
    }
    
    private boolean hasExcludeFilters(List<MolapPredicates> preds)
    {
        if(preds != null && preds.size() > 0)
        {
            for(MolapPredicates pred : preds)
            {
                if((pred.getExclude() != null && pred.getExclude().length > 0) || (pred.getIncludeOr() != null && pred.getIncludeOr().length > 0))
                {
                    return true;
                }
            }
            
        }
        return false;
    }
    
    private boolean isFilterSubSet(long[] filterIds,long[] otherfilterIds)
    {
        for(int i = 0;i < filterIds.length;i++)
        {
            boolean found = false;
            for(int j = 0;j < otherfilterIds.length;j++)
            {
                if(filterIds[i] == otherfilterIds[j])
                {
                    found = true;
                    break;
                }
            }
            if(!found)
            {
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * Is all predicates are part of rows also
     * @param dims
     * @param preds
     * @return
     */
    public boolean isAllPredPresentOnDims(int[] dims,List<MolapPredicates> preds)
    {
        if(preds == null)
        {
            return true;
        }
        for(int i = 0;i < preds.size();i++)
        {
            boolean found = false;
            int ordinal = preds.get(i).getOrdinal();
            for(int j = 0;j < dims.length;j++)
            {
                if(ordinal == dims[j])
                {
                    found = true;
                    break;
                }
            }
            if(!found)
            {
                return false;
            }
        }
        return true;
    }
    
    
    public boolean equalsWithOutPreds(MolapSegmentHeader header)
    {
        if(!cubeUniqueName.equals(header.cubeUniqueName) || !Arrays.equals(dims, header.dims)
                || !factTableName.equals(header.factTableName))
        {
            return false;
        }

        return true;
    }

    public long[] getStartKey()
    {
        return startKey;
    }

    public void setStartKey(long[] ls)
    {
        this.startKey = ls;
    }

    public long[] getEndKey()
    {
        return endKey;
    }

    public void setEndKey(long[] ls)
    {
        this.endKey = ls;
    }
    
}
