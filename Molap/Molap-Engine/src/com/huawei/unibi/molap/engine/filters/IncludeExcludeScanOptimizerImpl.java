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


package com.huawei.unibi.molap.engine.filters;

import com.huawei.unibi.molap.engine.scanner.optimizer.impl.ScanOptimizerImpl;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;

public class IncludeExcludeScanOptimizerImpl extends ScanOptimizerImpl
{

    public IncludeExcludeScanOptimizerImpl(long[] maxKey, long[][] filters, KeyGenerator keyGenerator)
    {
        super(maxKey, filters, keyGenerator);
    }

//    /**
//     * 
//     */
//    private long[][] excludeFilters;
//    
//    private int lastIndex;
//    
//    private int[][] groups;
//
//    public IncludeExcludeScanOptimizerImpl(long[] maxKey, long[][] includeFilters, long[][] excludeFilters,
//            KeyGenerator keyGenerator,int[][] groups)
//    {
//        super(maxKey, includeFilters, keyGenerator);
//        this.excludeFilters = excludeFilters;
//        this.groups = groups;
//        processGroups();
//        for(int i = excludeFilters.length-1;i >= 0;i--)
//        {
//            if(excludeFilters[i] != null)
//            {
//                lastIndex = i;
//                break;
//            }
//        }
//        
//    }
//
//    @Override
//    protected int checkWithFilter(long[] key)
//    {
//        boolean changed = false;
//        int index = 0;
//        int lastIndexExPred = -1;
//        // iterate all columns and set index to the last modified column
//        // which has filter
//        colIndxloop: for(int colIndex = 0;colIndex < key.length;colIndex++)
//        {
//            long[] includeFil = filters[colIndex];
//            long[] excludeFil = excludeFilters[colIndex];
//
//            if(includeFil != null)
//            {
//                index = colIndex;
//                if(changed)
//                {
//                    // if any of the previous column already has got changed
//                    // then reset to the first filter
//                    // for all succeeding columns which has filter and move
//                    // on to next column.
//                    key[colIndex] = includeFil[0];
//                }
//                else
//                {
//                    for(long aFilter : includeFil)
//                    {
//                        if(key[colIndex] == aFilter)
//                        {
//                            continue colIndxloop;
//                        }
//                        else if(key[colIndex] < aFilter)
//                        {
//                            key[colIndex] = aFilter;
//                            changed = true;
//                            continue colIndxloop;
//                        }
//                    }
//                    // Now current col value exceeded all the filters,
//                    // so move the cursor a step back and try reset the prev
//                    // column val
//
//                    // TODO if -1 is returned then all scanning is finished
//                    // returned colIndex value indicates from which position
//                    // the columns needs to recheck with filter
//                    colIndex = setKey(key, colIndex);
//                    changed = true;
//                }
//            }
//            else if(excludeFil != null)
//            {
//                index = colIndex;
//                setKeyIfChanges(key, changed, colIndex);
//                //Incase of groups and exclude filter, check only for the last value of group, remaining values we can ignore.
//                if(groups != null)
//                {
//                    int[] group = getGroup(key, colIndex);
//                    if(group != null && group.length > 0)
//                    {
//                        if(colIndex != group[group.length-1])
//                        {
//                            continue colIndxloop;
//                        }
//                    }
//                }
//                int idx = 0;
//                for(long filter : excludeFil)
//                {
//                    if(groups != null?isItPresentInGroup(key, colIndex, filter, idx):key[colIndex] == filter)
//                    {
//                        // if the column val is same as filter then increase
//                        // the column val till the value
//                        // is not equal to any of the filter keys
//                        if(colIndex == lastIndex)
//                        {
//                            key[colIndex]++;
//                            changed = true;
//                            lastIndexExPred = -1;
//                        }
//                        else
//                        {
//                            lastIndexExPred = colIndex;
//                        }
//                    }
//                    else if(groups == null && key[colIndex] < filter)
//                    {
//                        continue colIndxloop;
//                    }
//                    idx++;
//                }
//                if(key[colIndex] > maxKey[colIndex])
//                {
//                    colIndex = setKey(key, colIndex);
//                    changed = true;
//                    lastIndexExPred = -1;
//                }
//            }
//            else
//            {
//                setKeyIfChanges(key, changed, colIndex);
//            }
//            // Actually for exclude we should come from right to left and
//            // increment the key. But for include we require right to left. so
//            // this is one of the work around
//            if(colIndex > lastIndex && lastIndexExPred >= 0)
//            {
//                key[lastIndexExPred]++;
//                changed = true;
//                setKeyIfChanges(key, changed, lastIndexExPred+1);
//                index = lastIndexExPred+1;
//            }
//        }
//        setKeyIfNoChanges(key, changed);
//        return index;
//    }
//    
//    /**
//     * In exclude case , we need to check in the complete group other wise there is a chance of jumping out and loose some rows.
//     * DTS2014012703394
//     * @param key
//     * @param colIndex
//     * @param filter
//     * @param filterIndex
//     * @return
//     */
//    private boolean isItPresentInGroup(long[] key,int colIndex,long filter,int filterIndex)
//    {
//      
//        for(int i = 0;i < groups.length;i++)
//        {
//            int [] group = groups[i];
//            
//            int binarySearch = Arrays.binarySearch(group, colIndex);
//            if(binarySearch >= 0)
//            {
//                for(int j = 0;j <binarySearch+1;j++)
//                {
//                    filter = excludeFilters[group[j]][filterIndex];
//                    if(key[group[j]] != filter)
//                    {
//                         return false;
//                    }
//                }
//            }
//        }
//        return true;
//    }
//    
//    private void processGroups()
//    {
//        if(groups != null)
//        {
//            int[][] groupsLocal = new int[groups.length][];
//            
//            for(int i = 0;i < groupsLocal.length;i++)
//            {
//                int index = getIndex(groups[i]);
//                if(index >= 0)
//                {
//                    int val = groups[i][index];
//                    int[] groupLocal = new int[index+1];
//                    for(int j = groupLocal.length-1;j >= 0;j--)
//                    {
//                        groupLocal[j] = val--;
//                    }
//                    groupsLocal[i] = groupLocal;
//                }
//            }
//            groups = groupsLocal;
//        }
//    }
//    
//    private int getIndex(int[] group)
//    {
//        for(int i = group.length-1;i >=0;i--)
//        {
//            if(group[i] != -1)
//            {
//                return i;
//            }
//        }
//        return -1;
//    }
//    
//    /**
//     * In exclude case , we need to check in the complete group other wise there is a chance of jumping out and loose some rows.
//     * DTS2014012703394
//     * @param key
//     * @param colIndex
//     * @param filter
//     * @param filterIndex
//     * @return
//     */
//    private int[] getGroup(long[] key,int colIndex)
//    {
//      
//        for(int i = 0;i < groups.length;i++)
//        {
//            int [] group = groups[i];
//            
//            int binarySearch = Arrays.binarySearch(group, colIndex);
//            if(binarySearch >= 0)
//            {
//                return group;
//            }
//        }
//        return null;
//    }
//
//    /**
//     * 
//     * @param key
//     * @param changed
//     * 
//     */
//    private void setKeyIfNoChanges(long[] key, boolean changed)
//    {
//        if(!changed)
//        {
//            key[key.length - 1]++;
//        }
//    }
//
//    /**
//     * 
//     * @param key
//     * @param changed
//     * @param colIndex
//     * 
//     */
//    private void setKeyIfChanges(long[] key, boolean changed, int colIndex)
//    {
//        if(changed)
//        {
//            // if there is no filter on the current column and if
//            // any of previous column has got changed
//            // then reset this col to initial key
//            key[colIndex] = 0L;
//        }
//    }
//
//    protected int setKey(long[] key, int index)
//    {
//        for(int i = index - 1;i >= 0;i--)
//        {
//            long[] fil = filters[i];
//            long[] excludeFil = excludeFilters[i];
//            if(fil != null)
//            {
//                for(long f : fil)
//                {
//                    if(key[i] < f)
//                    {
//                        key[i] = f;
//                        return i;
//                    }
//                }
//            }
//            else if(excludeFil != null)
//            {
//                while(key[i] < maxKey[i])
//                {
//                    key[i]++;
//                    if(groups != null)
//                    {
//                        boolean found = false;
//                        for(int j = 0;j < excludeFil.length;j++)
//                        {
//                            if(isItPresentInGroup(key, i, excludeFil[j], j))
//                            {
//                                found = true;
//                            }
//                        }
//                        if(!found)
//                        {
//                            return i;
//                        }
//                    }
//                    else
//                    {
//                        // ensure after increasing its not matching
//                        if(Arrays.binarySearch(excludeFil, key[i]) < 0)
//                        {
//                            return i;
//                        }
//                    }
//                }
//            }
//            else
//            {
//                if(key[i] < maxKey[i])
//                {
//                    key[i]++;
//                    return i;
//                }
//            }
//        }
//        return index;
//    }


}

