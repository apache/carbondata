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

package com.huawei.unibi.molap.engine.datastorage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.engine.util.MemberSortModel;
import com.huawei.unibi.molap.olap.MolapDef;
import com.huawei.unibi.molap.util.ByteUtil;

public class MemberStore
{

    /**
     * Level name
     */
    private String levelName;

    /**
     * Mondrian rolap level
     */
    private MolapDef.Level rolapLevel;

    /**
     * member Cache
     */
    // private Map<Member, Member> cache = new HashMap<Member, Member>();
    private Member [][] cache;
    
    /**
     * globalCache
     */
    private int[] globalCache;
    
//    private Map<Integer,Integer> globalCacheMap = new HashMap<Integer,Integer>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE); 

    /**
     * Sort index after members are started
     */
    private int[][] sortOrderIndex;

    /**
     * Reverse sort index to retrive the member
     */
    private int[][] sortReverseOrderIndex;
    
    
    private int sortReverseOrderIndexSize;

    // private List<Member> allMembers = new ArrayList<Member>();

    /**
     * Column name ffrom schema
     */
    private String columnName;
    
    /**
     * table
     */
    private String tableName;

    /**
     * Max member surrogate
     */
    private int maxMember;

    /**
     * Min member surrogate
     */
    private int minMember;
    
//    /**
//     * Whether it is number or string
//     */
//    private DataType memberDataType;
    

    
    private int minValue;
    
    private int min;
    
    private DataType memberDataType;
    


    public MemberStore(MolapDef.Level rolapLevel, String tableName)
    {
        this.rolapLevel = rolapLevel;

        if(rolapLevel != null)
        {
            this.columnName = rolapLevel.column;
            
            this.tableName = tableName;
            
            this.levelName = rolapLevel.getName();
            
            String datatype = rolapLevel.type; 
            if(datatype.equals("Numeric") || datatype.equals("Integer") || datatype.equals("BigInt"))
            {//CHECKSTYLE:ON
                memberDataType = DataType.NUMBER;
            }
            else if(datatype.equals("Timestamp"))
            {
                memberDataType=DataType.TIMESTAMP;
            }
            else
            {
                memberDataType=DataType.STRING;
            }
        }
        
        

    }

    /**
     * @return
     */
    public int getCardinality()
    {
        return cache.length+min;
    }

    /**
     * Table for member is nothing but the column name.
     * 
     */
    public String getTableForMember()
    {
        return tableName+'_'+columnName;
    }
    
    /**
     * Col name
     * @return
     */
    public String getColumnName()
    {
        return columnName;
    }

    /**
     * Getter for level
     */
    public MolapDef.Level getRolapLevel()
    {
        return rolapLevel;
    }

    /**
     * @param lName
     */
    public void setLevelName(String lName)
    {
        levelName = lName;
    }

    /**
     * Get level name
     */
    public String getLevelName()
    {
        return levelName;
    }



    /**
     * 
     */
    public void clear()
    {
        cache=null;
        maxMember = 0;
        minValue = 0;
        minMember = 0;
        min = 0;
        sortOrderIndex = null;
        globalCache = null;
        sortReverseOrderIndex = null;
        sortReverseOrderIndexSize = 0;
    }

    /**
     * @return
     */
    public long getMaxValue()
    {
        return maxMember;
    }


    /**
     * @param name
     * @return
     */
    public int getMemberId(String name)
    {
        return getMemberId(name,false);
    }
    
    /**
     * @param name
     * @param isActualCol
     * @return
     */
    public int getMemberId(String name,boolean isActualCol)
    {
        if(name == null)
        {
            return 0;
        }
        if(null==cache)
        {
            return 0;
        }
        byte[] nameChar = name.getBytes();
        int surrogate=0;
        for(int index=0;index<cache.length;index++)
        {
            for(int i = 0;i < cache[index].length;i++)
            {
                if(ByteUtil.UnsafeComparer.INSTANCE.equals(nameChar, cache[index][i].getCharArray()))  
                {
                    return surrogate+min;
                }
                surrogate++;
            }
        }
        return 0;
    }
    
    /**
     * Get the sorted index of a surrogate
     * @param key
     * @return
     */
    public int getSortedIndex(int key)
    {
        if(key >= sortReverseOrderIndexSize || key<minMember)
        {
            return -1;
        }
        int rowIndex=key/MolapCommonConstants.LEVEL_ARRAY_SIZE;
        int columnIndex=(key%MolapCommonConstants.LEVEL_ARRAY_SIZE);
        return sortReverseOrderIndex[rowIndex][columnIndex];
    }




    /**
     * @param id
     * @return
     */
    public Member getMemberByID(int localSurrogate)
    {
        int key = localSurrogate - min;
        // when number of slices are more than one then localSurrogate - min may
        // give value -1 so we have to skip that member store
        if(null != cache && cache.length>0 && localSurrogate <=maxMember && key > -1)
        {
            int rowIndex = key / MolapCommonConstants.LEVEL_ARRAY_SIZE;
            int columnIndex = (key % MolapCommonConstants.LEVEL_ARRAY_SIZE);
            return this.cache[rowIndex][columnIndex];
        }
        return null;
    }

    /**
     * @param minValue2 
     * @param map
     */
    public void addAll(Member[][] memberArray, int minValue, int maxValue,List<int[][]> sortOrderAndReverseOrderIndex)
    {
        if(null==memberArray)
        {
            cache = new Member[0][0];
            return;
        }
        cache = memberArray;
        this.min = minValue;
        maxMember=maxValue;
        minMember = minValue;
        if(null!=cache)
        {
            if(null!=sortOrderAndReverseOrderIndex)
            {
                setSortOrderIndex(sortOrderAndReverseOrderIndex.get(0));
                setSortReverseOrderIndex(sortOrderAndReverseOrderIndex.get(1));
            }
            else
            {
                createSortAndReverseIndex();
            }
        }
    }
    
    private void createSortAndReverseIndex()
    {
        int startKey=min;
        List<MemberSortModel> model = new ArrayList<MemberSortModel>();
        for(int i = 0;i < cache.length;i++)
        {
            for(int j = 0;j < cache[i].length;j++)
            {
                if(memberDataType.equals(DataType.STRING))
                {
                    model.add(new MemberSortModel(startKey, null, cache[i][j].getCharArray(),memberDataType));
                }
                else
                {
                    model.add(new MemberSortModel(startKey,cache[i][j].toString(),null,memberDataType));
                }
                startKey++;
            }
        }
        createSortIndex(model);
    }
    
    /**
     * Create the sort index for the members. It will be useful when sorting
     * using surrogates
     */
    private void createSortIndex(List<MemberSortModel> models)
    {
        Collections.sort(models);
        int []sortOrderIndexTemp = new int[min + models.size()];
        int []sortReverseOrderIndexTemp = new int[maxMember + 1];
        for(int i = 0;i < models.size();i++)
        {
            MemberSortModel memberSortModel = models.get(i);
            sortOrderIndexTemp[i + min] = memberSortModel.getKey();
            sortReverseOrderIndexTemp[memberSortModel.getKey()] = i + min;
        }
        models = null;
        setSortOrderIndex(convertTo2DArray(sortOrderIndexTemp));
        setSortReverseOrderIndex(convertTo2DArray(sortReverseOrderIndexTemp));
    }
    
    private int[][] convertTo2DArray(int[] singleArray)
    {
        int div=singleArray.length/MolapCommonConstants.LEVEL_ARRAY_SIZE;
        int rem=singleArray.length%MolapCommonConstants.LEVEL_ARRAY_SIZE;
        if(rem>0)
        {
            div++;
        }
        int[][]doubleArray= new int[div][];
        
        for(int i = 0;i < div-1;i++)
        {
            doubleArray[i]= new int[MolapCommonConstants.LEVEL_ARRAY_SIZE];
        }
        
        if(rem>0)
        {
            doubleArray[doubleArray.length-1]= new int[rem];
        }
        else
        {
            doubleArray[doubleArray.length-1]= new int[MolapCommonConstants.LEVEL_ARRAY_SIZE];
        }
        int counter=0;
        for(int i = 0;i < doubleArray.length;i++)
        {
            for(int j = 0;j < doubleArray[i].length;j++)
            {
                doubleArray[i][j]=singleArray[counter++];
            }
        }
        return doubleArray;
    }
    
    public void addGlobalKey(int[] globalCache, int minValue)
    {
        this.globalCache=globalCache;
        this.minValue=minValue;
    }
    
    public int getGlobalSurrogateKey(int localSurrogate)
    {
        int key =localSurrogate-minValue;
        if(null!=globalCache && key>-1 && key <globalCache.length)
        {
            return this.globalCache[key];
        }
        return -1;
    }
    
    public Member [][] getAllMembers()
    {
        return cache;
    }
    
    /**
     * Get the member form sorted index.
     * @param index
     * @return
     */
    public Member getActualKeyFromSortedIndex(int index)
    {
        if(index >= sortReverseOrderIndexSize || index < minMember)
        {
            return null;
        }
        int rowIndex = index / MolapCommonConstants.LEVEL_ARRAY_SIZE;
        int columnIndex = index % MolapCommonConstants.LEVEL_ARRAY_SIZE;
        int key = sortOrderIndex[rowIndex][columnIndex] - min;
        rowIndex = key / MolapCommonConstants.LEVEL_ARRAY_SIZE;
        columnIndex = key % MolapCommonConstants.LEVEL_ARRAY_SIZE;
        return cache[rowIndex][columnIndex];
    }
    
    public void setSortOrderIndex(int[][] sortOrderIndex)
    {
        this.sortOrderIndex = sortOrderIndex;
    }

    public void setSortReverseOrderIndex(int[][] sortReverseOrderIndex)
    {
        this.sortReverseOrderIndex = sortReverseOrderIndex;
        for(int i = 0;i < sortReverseOrderIndex.length;i++)
        {
            sortReverseOrderIndexSize+=sortReverseOrderIndex[i].length;
        }
    }
}
