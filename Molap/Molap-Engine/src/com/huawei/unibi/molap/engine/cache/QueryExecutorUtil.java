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

package com.huawei.unibi.molap.engine.cache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.engine.datastorage.InMemoryCube;
import com.huawei.unibi.molap.engine.filters.metadata.InMemFilterModel;
import com.huawei.unibi.molap.engine.holders.MolapResultHolder;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
import com.huawei.unibi.molap.engine.wrappers.ArrayWrapper;
import com.huawei.unibi.molap.filter.MolapFilterInfo;
import com.huawei.unibi.molap.keygenerator.KeyGenException;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;
import com.huawei.unibi.molap.metadata.MolapMetadata;
import com.huawei.unibi.molap.metadata.MolapMetadata.Cube;
import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;
//import mondrian.rolap.SqlStatement;
import com.huawei.unibi.molap.olap.SqlStatement;
import com.huawei.unibi.molap.vo.HybridStoreModel;

/**
 * Util class
 * @author R00900208
 *
 */
public final class QueryExecutorUtil
{
    
    /**
     * LOGGER
     */
    private static final LogService LOGGER = LogServiceFactory.getLogService(QueryExecutorUtil.class.getName());
     private QueryExecutorUtil()
     {
         
     }
    
    /**
     * 
     * Return the valid range based on the startKey, end key and ranges.
     * For Example : 
     *  Start key : [0, 0, 0, 1, 0, 0]
     *  End Key :   [20, 2, 12, 1, 10, 6]
     *  ranges :    [[4, 1, 5, 1, 5, 2], [7, 1, 9, 1, 9, 1], [11, 1, 1, 1, 3, 4], [14, 1, 5, 1, 6, 4], [17, 1, 9, 1, 8, 5], [20, 2, 1, 5, 10, 2]]
     *  
     *  then it returns 
     *  [[[0, 0, 0, 1, 0, 0], [4, 1, 5, 1, 5, 3]], [[4, 1, 5, 1, 5, 3], 
     *  [7, 1, 9, 1, 9, 2]], [[7, 1, 9, 1, 9, 2], [11, 1, 1, 1, 3, 5]], 
     *  [[11, 1, 1, 1, 3, 5], [14, 1, 5, 1, 6, 5]], [[14, 1, 5, 1, 6, 5],
     *   [17, 1, 9, 1, 8, 6]], [[17, 1, 9, 1, 8, 6], [20, 2, 1, 5, 10, 3]], [[20, 2, 1, 5, 10, 3], [20, 2, 12, 1, 10, 6]]]
     *                
     * 
     * @param slice
     * @param ranges
     * @param startKey
     * @param endKey
     * @return  the ranges based on start key, end key and ranges.
     *
     */
    public static long[][][] getValidRanges(InMemoryCube slice, long[][] ranges, long[] startKey, long[] endKey)
    {
        long[][][] rangeValues = null;
        ArrayWrapper temp = new ArrayWrapper(startKey);
        if(ranges != null && ranges.length > 0)
        {
            // find the upper index and lower index in ranges which lie in
            // between startKey and endKey

            // finding lower index
            int lowerIndex = 0;
            for(;lowerIndex < ranges.length;lowerIndex++)
            {
                if(temp.compareTo(new ArrayWrapper(ranges[lowerIndex])) < 0)
                {
                    break;
                }
            }

            // finding upper index
            int upperindex = (ranges.length - 1);
            temp.initialize(endKey);
            for(;upperindex >= 0;upperindex--)
            {

                if(temp.compareTo(new ArrayWrapper(ranges[upperindex])) > 0)
                {
                    break;
                }
            }

            if(lowerIndex >= ranges.length || upperindex < 0 || upperindex < lowerIndex)
            {
                // consider the range is 4,8,12,16
                // In all the below cases no need to spawn threads

                // case 1 : start key and end key is 17 - 20
                // case 2 : start key and end key is 1 - 3
                // case 3 : start key and end key is 9 and 11 (so upper index
                // will be 1 and lower index will be 2)

                return null;
            }
            /*
             * else if(lowerIndex==upperindex) { //if either end key or start
             * key is same as the index val then no need to split and run as
             * threads //case 4: start key and end key is 12 - 14 (so upper
             * index will be 1 and lower index will be 2) if(temp.compareTo(new
             * ArrayWrapper(ranges[lowerIndex])) == 0 || new
             * ArrayWrapper(startKey) .compareTo(new
             * ArrayWrapper(ranges[lowerIndex])) == 0) { return null; } }
             */

            List<long[][]> rangesList = new ArrayList<long[][]>(10);
            long[][] range = new long[2][];
            range[0] = startKey;
            for(int i = lowerIndex;i <= upperindex;i++)
            {//CHECKSTYLE:OFF    Approval No:Approval-281
                // set upper index and it to the list
                range[1] = ranges[i].clone();//CHECKSTYLE:ON
                rangesList.add(range);
//CHECKSTYLE:OFF    Approval No:Approval-282
                // next lower index will be the current long array with the last
                // index val incremented
                range = new long[2][];
                range[0] = ranges[i].clone();
                range[0][range[0].length - 1]++;
            }//CHECKSTYLE:ON
            range[1] = endKey;
            rangesList.add(range);
            rangeValues = rangesList.toArray(new long[rangesList.size()][][]);
        }
        return rangeValues;
    }
    
    
    /**
     * Get surrogates
     * @param slices
     * @param columnName
     * @param name
     * @return
     */
    public static void getMemberIdByName(List<InMemoryCube> slices, Dimension columnName, String name,List<Long> surrogatesActual)
    {
        List<Long> surrogates = new ArrayList<Long>(10);
        if(null != slices && null != columnName)//Coverity Fix add 
        {
            long surr = 0;
            boolean hasNameColumn = columnName.isHasNameColumn() && !columnName.isActualCol();
            for(InMemoryCube slice : slices)
            {

                    long surrLoc = (long)slice.getMemberCache(columnName.getTableName()+'_'+columnName.getColName() + '_' + columnName.getDimName() + '_' + columnName.getHierName()).getMemberId(name,columnName.isActualCol());
                    if(surrLoc > 0)
                    {
                        surr = surrLoc;
                    }

                }//CHECKSTYLE:ON
            
            
            if (hasNameColumn && surrogates.size() == 0)
            {
                surrogates.add(Long.MAX_VALUE);
                //LOGGER.error(MolapEngineLogEvent.U
                LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, " Member does not exist for name column :" +name);
            }
            if(surr > 0)
            {
                surrogates.add(surr);
            }
            else if(!hasNameColumn)
            {
                LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, " Member does not exist for level " + columnName.getName() +" : "+name);
                surrogates.add(Long.MAX_VALUE);
            }
        }
        surrogatesActual.addAll(surrogates);
    }
    
    /**
     * Get surrogates
     * @param slices
     * @param columnName
     * @param name
     * @return
     */
    public static void getMemberIdByActualName(List<InMemoryCube> slices, Dimension columnName, String name,List<Long> surrogatesActual)
    {
        List<Long> surrogates = new ArrayList<Long>(10);
        if(null != slices && null != columnName)//Coverity Fix add 
        {
            long surr = 0;
            boolean hasNameColumn = columnName.isHasNameColumn() && !columnName.isActualCol();
            for(InMemoryCube slice : slices)
            {
                
                  //CHECKSTYLE:OFF    Approval No:Approval-367
                    long surrLoc = (long)slice.getMemberCache(columnName.getTableName()+'_'+columnName.getColName() + '_' + columnName.getDimName() + '_' + columnName.getHierName()).getMemberId(name,columnName.isActualCol());
                    if(surrLoc > 0)
                    {
                        surr = surrLoc;
                    }

          
            }
            if(surr > 0)
            {
                surrogates.add(surr);
            }
            else if(!hasNameColumn)
            {
                LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, " Member does not exist for level " + columnName.getName() +" : "+name);
                surrogates.add(Long.MAX_VALUE);
            }
        }
        surrogatesActual.addAll(surrogates);
    }
    
    /**
     * To get the max key based on dimensions. i.e. all other dimensions will be
     * set to 0 bits and the required query dimension will be masked with all
     * 1's so that we can mask key and then compare while aggregating
     * 
     * @param queryDimensions
     * @return
     * @throws KeyGenException
     * 
     */
    public static byte[] getMaxKeyBasedOnDimensions(Dimension[] queryDimensions, KeyGenerator generator,
            Dimension[] dimTables) throws KeyGenException
    {
        long[] max = new long[dimTables.length];
        Arrays.fill(max, 0L);
        for(int i = 0;i < queryDimensions.length;i++)
        {
            if(queryDimensions[i].isHighCardinalityDim())
            {
                continue;
            }
            max[queryDimensions[i].getOrdinal()] = Long.MAX_VALUE;
        }
        return generator.generateKey(max);
    }
    
    
    public static void getFiltersFromIterator(int[] ordinals, MolapResultHolder iter,MolapFilterInfo filterInfo,boolean include)
    {
        List<String> list = new ArrayList<String>(10);
        while(iter.isNext())
        {
            StringBuilder builder = new StringBuilder();
            for(int i = 0;i < ordinals.length;i++)
            {
                if (MolapCommonConstants.SQLNULLVALUE.equals(iter.getObject(i + 1)))
                {
                    builder.append('[').append(MolapCommonConstants.MEMBER_DEFAULT_VAL).append(']');
                }
                else
                {
                    builder.append('[').append(iter.getObject(i+1)).append(']');
                }
                
                if(i < ordinals.length -1)
                {
                    builder.append('.');
                }
            }
            list.add(builder.toString());
        }
        if(include)
        {
            filterInfo.addAllIncludedMembers(list);
        }
        else
        {
            filterInfo.addAllExcludedMembers(list);
        }
    }
    



  
    
    
    /**
     * @param constraints
     * @param slices
     * @param entry
     */
   /* private static void resolveAndUpdateContentMatchFilter(Map<Dimension, MolapFilterInfo> constraints,
            List<InMemoryCube> slices, Entry<Dimension, MolapFilterInfo> entry, boolean isIncludeFilterInSameDim)
    {
        ContentMatchFilterInfo matchFilterInfo = (ContentMatchFilterInfo)entry.getValue(); 
        List<String> likeExpressionMembers =null; 
        if(null!=matchFilterInfo)
        {
            List<FilterLikeExpressionIntf> listFilterExpression=matchFilterInfo.getLikeFilterExpression();
            if(listFilterExpression.size()>0 )
            {
                likeExpressionMembers=new ArrayList<String>(20);
                for(FilterLikeExpressionIntf filterExpr:listFilterExpression)
                {
                    List<LikeExpression> expressions=filterExpr.getLikeExpression(); 
                   
                    for(LikeExpression expression:expressions)
                    {
                        likeExpressionMembers.add(expression.getExpressionName()); 
                    }
                    filterExpr.processLikeExpressionFilters(likeExpressionMembers, slices, entry,matchFilterInfo,false,null);
                }
                
            }
        }
        
        
        
        
        
        List<String> includedContentMatchMembers = matchFilterInfo.getIncludedContentMatchMembers();
        boolean hasNameColumn = entry.getKey().isHasNameColumn()&&!entry.getKey().isActualCol();
        Locale locale= Locale.getDefault();
        String incConVar;
      //CHECKSTYLE:OFF    Approval No:V3R8C00_010
        for(String incCon : includedContentMatchMembers)
        {
            incConVar = incCon.toLowerCase(locale);
          //CHECKSTYLE:ON
            for(InMemoryCube slice : slices)
            {
                Iterator<Member> allMembers = slice.getMemberCache(
                        entry.getKey().getTableName() + '_' + entry.getKey().getColName() + '_'
                                + entry.getKey().getDimName() + '_' + entry.getKey().getHierName()).getAllMembers();
                
                while(allMembers.hasNext())
                {
                    Member member = allMembers.next();
                    String memString = hasNameColumn?member.getAttributes()[0].toString():member.toString();
                    String memStringCompare = memString.toLowerCase(locale);
                    if(memString.equals(MolapCommonConstants.MEMBER_DEFAULT_VAL))
                    {
                        memStringCompare = "not available";
                        if(memStringCompare.contains(incConVar))
                        {
                            matchFilterInfo.addIncludedMembers('['+memString+']');
                         
                        }
                    
                    }
                    else if(memStringCompare.contains(incConVar))
                    {
                        matchFilterInfo.addIncludedMembers('['+memString+']');
                    }
                }
            }
        }
        if(includedContentMatchMembers.size() > 0 && matchFilterInfo.getIncludedMembers().size() == 0)
        {
            constraints.put(null, matchFilterInfo);
        }
        if(!isIncludeFilterInSameDim)
        {
            updateDoesNotContainsConstraintsForEmptyInclude(slices, entry, matchFilterInfo, hasNameColumn, locale);
        }
        else
        {
            updateDoesNotContainsConstraintsForNonEmptyInclude(slices, entry, matchFilterInfo, hasNameColumn, locale);
        }
    }*/


    /*private static void updateDoesNotContainsConstraintsForEmptyInclude(List<InMemoryCube> slices,
            Entry<Dimension, MolapFilterInfo> entry, ContentMatchFilterInfo matchFilterInfo, boolean hasNameColumn,
            Locale locale)
    {
        List<String> excludedContentMatchMembers = matchFilterInfo.getExcludedContentMatchMembers();
        String excConVar;
      //CHECKSTYLE:OFF    Approval No:V3R8C00_011
        for(String excCon : excludedContentMatchMembers)
        {
            excConVar = excCon.toLowerCase(locale);
          //CHECKSTYLE:ON
            for(InMemoryCube slice : slices)
            {
                Iterator<Member> allMembers = slice.getMemberCache(
                        entry.getKey().getTableName() + '_' + entry.getKey().getColName() + '_'
                                + entry.getKey().getDimName() + '_' + entry.getKey().getHierName()).getAllMembers();
                
                while(allMembers.hasNext())
                {
                    Member member = allMembers.next();
                    String memString = hasNameColumn?member.getAttributes()[0].toString():member.toString();
                    String memStringCompare = memString.toLowerCase(locale);
                    if(memString.equals(MolapCommonConstants.MEMBER_DEFAULT_VAL))
                    {
                        memStringCompare = "not available";
                        if(memStringCompare.equals(excConVar))
                        {
                            continue;
                        }
//                        if(!excCon.contains(memStringCompare))
//                        {
//                            matchFilterInfo.addIncludedMembers('['+memString+']');
//                         
//                        }
                        else if(!memStringCompare.contains(excConVar))
                        {
                            matchFilterInfo.addIncludedMembers('['+memString+']');
                        }
       
                    
                    }
                    else if(!memStringCompare.contains(excConVar))
                    {
                        if(!matchFilterInfo.getIncludedMembers().contains('['+memString+']'))
                        {
                        matchFilterInfo.addIncludedMembers('['+memString+']');
                        }
                    }
                    else
                    {
                        matchFilterInfo.addExcludedMembers('['+memString+']');  
                    }

                 
                }
            }
        }
        if(matchFilterInfo.getExcludedMembers().size()>0)
        { //CHECKSTYLE:OFF    Approval No:V3R8C00_011
            
            List<String> tempList = new ArrayList<String>();
            for(String excludedFilter:matchFilterInfo.getExcludedMembers()) //CHECKSTYLE:ON
            {
                if(matchFilterInfo.getIncludedMembers().contains(excludedFilter))
                {
                    matchFilterInfo.getIncludedMembers().remove(excludedFilter);
                }
                else
                {
                    tempList.add(excludedFilter);
                }
            }
            Iterator<String> iterator = matchFilterInfo.getExcludedMembers().iterator();
            while(iterator.hasNext())
            {
                if(!tempList.contains(iterator.next()))
                {
                    iterator.remove();
                }
            }
        }
        matchFilterInfo.getExcludedMembers().clear();
    }
    
 /*   private static void updateDoesNotContainsConstraintsForNonEmptyInclude(List<InMemoryCube> slices,
            Entry<Dimension, MolapFilterInfo> entry, ContentMatchFilterInfo matchFilterInfo, boolean hasNameColumn,
            Locale locale)
    {
        List<String> excludedContentMatchMembers = matchFilterInfo.getExcludedContentMatchMembers();
        //CHECKSTYLE:OFF    Approval No:V3R8C00_011
        String excConVar;
        for(String excCon : excludedContentMatchMembers)
        {
            excConVar = excCon.toLowerCase(locale);
         
            for(InMemoryCube slice : slices)
            {
                Iterator<Member> allMembers = slice.getMemberCache(
                        entry.getKey().getTableName() + '_' + entry.getKey().getColName() + '_'
                                + entry.getKey().getDimName() + '_' + entry.getKey().getHierName()).getAllMembers();
                
                while(allMembers.hasNext())
                {
                    Member member = allMembers.next();
                    String memString = hasNameColumn?member.getAttributes()[0].toString():member.toString();
                    String memStringCompare = memString.toLowerCase(Locale.getDefault()); //CHECKSTYLE:ON
                    if(memString.equals(MolapCommonConstants.MEMBER_DEFAULT_VAL))
                    {
                        memStringCompare = "not available";
//                        if(memStringCompare.equals(excConVar))
//                        {
//                            continue;
//                        }
//                        if(!excCon.contains(memStringCompare))
//                        {
//                            matchFilterInfo.addIncludedMembers('['+memString+']');
//                         
//                        }
                        if(memStringCompare.contains(excConVar))
                        {
                            matchFilterInfo.addExcludedMembers('['+memString+']');
                        }
       
                    
                    }
                    else if(memStringCompare.contains(excConVar))
                    {
                        matchFilterInfo.addExcludedMembers('['+memString+']');  
                    }
                 
                }
            }
        }
    }*/
    
    
    /**
     * Get the surrogates for the members
     * getSurrogates
     * @param dimMem
     * @param dimName
     * @return
     */
    public static long[] getSurrogates(List<String> dimMem, Dimension dimName,List<InMemoryCube> slices,boolean sortRequired, boolean isCompoundPredicate)
    {
        if(null == dimMem || dimMem.size() == 0)//Coverity fix added null check
        {
            return null;
        }

        List<Long> surrogates = new ArrayList<Long>(dimMem.size());

        for(String dimension : dimMem)
        {
            getSurrogate(dimName, slices, dimension,surrogates,isCompoundPredicate);
        }
        
        long[] result = convertToLongArraySurrogates(surrogates, dimName);
        
        if(sortRequired)
        {
            // sort the result before sending
            Arrays.sort(result);
        }
        return result;
    }
    
    private static long[] convertToLongArraySurrogates(List<Long> surrogates,Dimension dimName)
    {
        long[] ls = new long[surrogates.size()];
        for(int i = 0;i < surrogates.size();i++)
        {
            long surg = surrogates.get(i);
            if(surg > 0)
            {
                ls[i] = surg;
            }
            else
            {
                LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, " Member does not exist for level " + dimName.getName());
                ls[i] = Long.MAX_VALUE;
            }
        }
        return ls;
    }

    /**
     * Get the surrogate for the member.
     * @param dimName
     * @param slices
     * @param dimension
     * @return
     */
    private static void getSurrogate(Dimension dimName, List<InMemoryCube> slices, String dimension,List<Long> surrogates, boolean isCompoundPredicate)
    {
        if(null == dimension || "#null".equals(dimension))
        {
            dimension = MolapCommonConstants.MEMBER_DEFAULT_VAL;
        }
        //COverity fix add null check
        if(null == dimName)
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, dimension + " Member does not exist for level is null");
            return;
        }
        if(isCompoundPredicate)
        {
            getMemberIdByActualName(slices, dimName, dimension,surrogates);
        }
        else
        {
            getMemberIdByName(slices, dimName, dimension,surrogates);
        }
    }
    
    /**
     * Fills the includePredicates 2D array and creates the offset array
     * 
     * @param pred
     * @param excludePredicates
     * @return
     * @throws IOException
     * @throws KeyGenException
     * 
     */
    public static void computeColIncludePredicateKeys(Map<Dimension, MolapFilterInfo> pred, InMemFilterModel filterModel,
            Dimension[] dimTables, KeyGenerator keyGenerator,List<InMemoryCube> slices) throws IOException, KeyGenException
    {
        //
        List<Integer> dimFilterOffSet = new ArrayList<Integer>(10);
        long[][] includePredicateKeys = new long[dimTables.length][];
        byte[][][] includeFiltersOnDim = new byte[dimTables.length][][];
        filterModel.setMaskedByteRanges(new int[dimTables.length][]);
        for(Map.Entry<MolapMetadata.Dimension, MolapFilterInfo> entry : pred.entrySet())
        {
            //
            Dimension dim = entry.getKey();
            List<String[]> updatedIncludedMembers = getUpdatedConstarints(entry.getValue().getEffectiveIncludedMembers());
            List<String> effectiveIncludedMembers = getEffectiveIncludedMembers(dim, updatedIncludedMembers);
            long[] vals = getSurrogates(effectiveIncludedMembers, dim,slices,true,false);

            if(null != vals && vals.length > 0)
            {
                includePredicateKeys[dim.getOrdinal()] = vals;
                dimFilterOffSet.add(dim.getOrdinal());
                includeFiltersOnDim[dim.getOrdinal()] = getFilterByteArray(vals, dim.getOrdinal(), dimTables,
                        keyGenerator);

                // to set the max Key for this dimension index, so that to
                // compare the actual key only this is compared
                long[] keymax = new long[dimTables.length];
                Arrays.fill(keymax, 0L);
                keymax[dim.getOrdinal()] = Long.MAX_VALUE;
                byte[] maxKey = keyGenerator.generateKey(keymax);
                int[] ranges = getRangesForMaskedByte(new int[]{dim.getOrdinal()}, keyGenerator);
                filterModel.getMaxKey()[dim.getOrdinal()] = getMaskedKey(ranges,maxKey);
                filterModel.getMaskedByteRanges()[dim.getOrdinal()] = ranges;
            }
        }
        //
        filterModel.setColIncludeDimOffset(toIntArray(dimFilterOffSet));
        filterModel.setFilter(includeFiltersOnDim);
        filterModel.setIncludePredicateKeys(includePredicateKeys);
    }


    private static List<String> getEffectiveIncludedMembers(Dimension dim, List<String[]> updatedIncludedMembers)
    {
        List<String> effectiveIncludedMembers = new ArrayList<String>(10);

            for(int i = 0;i < updatedIncludedMembers.size();i++)
            {
                String[] string = updatedIncludedMembers.get(i);
                if(dim.getDataType().equals(SqlStatement.Type.DOUBLE))
                {
                    convertToDouble(effectiveIncludedMembers, string);
                }
                else if(dim.getDataType().equals(SqlStatement.Type.INT))
                {
                    convertToInt(effectiveIncludedMembers, string);
                }
                else
                {
                    effectiveIncludedMembers.add(string[string.length-1]);
                }
            }
        return effectiveIncludedMembers;
    }
    
    
    /**
     * Fills the includePredicates 2D array and creates the offset array
     * 
     * @param pred
     * @param excludePredicates
     * @return
     * @throws IOException
     * @throws KeyGenException
     * 
     */
    public static void computeColIncludeOrPredicateKeys(Map<Dimension, MolapFilterInfo> pred, InMemFilterModel filterModel,
            Dimension[] dimTables, KeyGenerator keyGenerator,List<InMemoryCube> slices) throws IOException, KeyGenException
    {
        //
        List<Integer> dimFilterOffSet = new ArrayList<Integer>(10);
        long[][] includePredicateKeys = new long[dimTables.length][];
        byte[][][] includeFiltersOnDim = new byte[dimTables.length][][];
        filterModel.setMaskedByteRangesIncludeOr(new int[dimTables.length][]);
        for(Map.Entry<MolapMetadata.Dimension, MolapFilterInfo> entry : pred.entrySet())
        {
            //
            Dimension dim = entry.getKey();
            List<String[]> updatedIncludedMembers = getUpdatedConstarints(entry.getValue().getIncludedOrMembers());
            List<String> effectiveIncludedMembers = getEffectiveIncludedMembers(dim, updatedIncludedMembers);
            long[] vals = getSurrogates(effectiveIncludedMembers, dim,slices,true,false);

            if(null != vals)
            {
                includePredicateKeys[dim.getOrdinal()] = vals;
                dimFilterOffSet.add(dim.getOrdinal());
                includeFiltersOnDim[dim.getOrdinal()] = getFilterByteArray(vals, dim.getOrdinal(), dimTables,
                        keyGenerator);

                // to set the max Key for this dimension index, so that to
                // compare the actual key only this is compared
                long[] keymax = new long[dimTables.length];
                Arrays.fill(keymax, 0L);
                keymax[dim.getOrdinal()] = Long.MAX_VALUE;
                byte[] maxKey = keyGenerator.generateKey(keymax);
                int[] ranges = getRangesForMaskedByte(new int[]{dim.getOrdinal()}, keyGenerator);
                filterModel.getMaxKeyIncludeOr()[dim.getOrdinal()] = getMaskedKey(ranges,maxKey);
                filterModel.getMaskedByteRangesIncludeOr()[dim.getOrdinal()] = ranges;
            }
        }
        //
        filterModel.setColIncludeDimOffsetOr(toIntArray(dimFilterOffSet));
        filterModel.setIncludeFilterOr(includeFiltersOnDim);
        filterModel.setIncludePredicateKeysOr(includePredicateKeys);
    }
    
    private static byte[] getMaskedKey(int[] ranges,byte[] key)
    {
        byte[] maskkey = new byte[ranges.length];
//        System.arraycopy(key, 0,maskkey,0, ranges.length);
      //CHECKSTYLE:OFF    Approval No:Approval-368
        for(int i = 0;i < maskkey.length;i++)
        {
            maskkey[i] = key[ranges[i]];
        }
        //CHECKSTYLE:ON
        return maskkey;
    }

    /**
     * @param effectiveIncludedMembers
     * @param string
     */
    private static void convertToDouble(List<String> effectiveIncludedMembers, String[] string)
    {
        //take always last one in this case
        try
        {
            double parseDouble = Double.parseDouble(string[string.length-1]);
            effectiveIncludedMembers.add(Double.toString(parseDouble));
        }
        catch (NumberFormatException e) 
        {
            effectiveIncludedMembers.add(string[string.length-1]);
        }
    }
    
    /**
     * @param effectiveIncludedMembers
     * @param string
     */
    private static void convertToInt(List<String> effectiveIncludedMembers, String[] string)
    {
        //take always last one in this case
        try
        {
            int parseInt = Integer.parseInt(string[string.length-1]);
            effectiveIncludedMembers.add(Integer.toString(parseInt));
        }
        catch (NumberFormatException e) 
        {
            effectiveIncludedMembers.add(string[string.length-1]);
        }
    }
    
        
    /**
     * Remove the braces of each member and return
     * @param effectiveIncludedMembers
     * @return
     */
    private static List<String[]> getUpdatedConstarints(List<String> effectiveIncludedMembers)
    {
        List<String[]> updatedList = new ArrayList<String[]>(10);
        List<String> memList = new ArrayList<String>(10);
        for(String mem : effectiveIncludedMembers)
        {
            String[] mems = parseMembers(mem, memList);
//            for(int i = 0;i < mems.length;i++)
//            {
//                mems[i] = removeBracesOfLastMember(mems[i]);
//            }
            updatedList.add(mems);
        }
        return updatedList;
    }
    
    /**
     * Parse the members like [2000].[1] to 2000,1
     * @param memString
     * @param memList
     * @return
     */
    public static String[] parseMembers(String memString,List<String> memList)
    {
        memList.clear();
        while(memString.charAt(0) == '[')
        {
            int index=-1;
            if(memString.contains("]."))
            {
                 index=memString.indexOf("].");
            }
            else
            {
                index=memString.lastIndexOf(']');
            }
            String mem = memString.substring(1,index).trim();
            memList.add(mem);
            if(index+2 > memString.length())
            {
                break;
            }
            memString = memString.substring(index+2).trim();
        }
        return memList.toArray(new String[memList.size()]);
    }
    
    /**
     * This method will be used to update the filter map in case if filter is present in same heir and multiple levels then we can select last level as a filter
     * @param cube
     * @param constraints
     * @return updated constraints
     */
    public static Map<Dimension, MolapFilterInfo> getUpdatedConstraints(Cube cube,Map<Dimension, MolapFilterInfo> constraints)
    {
        Map<Dimension, MolapFilterInfo> newconstraints = new HashMap<MolapMetadata.Dimension, MolapFilterInfo>(constraints);
        List<Dimension> processedDims=new ArrayList<MolapMetadata.Dimension>(10);
        int removeStartIndex=0;
        for(Entry<Dimension, MolapFilterInfo> entry : constraints.entrySet())
        {
            Dimension key = entry.getKey();
            if(processedDims.contains(key))
            {
                continue;
            }
            List<Dimension> dimList = cube.getHierarchiesMapping(key.getDimName()+'_'+key.getHierName());
            boolean [] dimPresent= new boolean[dimList.size()];
            for(int i = 0;i < dimList.size();i++)
            {
                if(constraints.containsKey(dimList.get(i)))
                {
                    processedDims.add(dimList.get(i));
                    dimPresent[i]=true;
                    removeStartIndex=i;
                }
            }
            for(int i = (removeStartIndex-1);i >=0;i--)
            {
                if(dimPresent[i])
                {
                    newconstraints.remove(dimList.get(i));
                }
            }
        }
        return newconstraints;
    }
    
    /**
     * Fills the excludePredicates 2D array and creates the offset array
     * 
     * @param pred
     * @param filterModel
     * @param dimTables
     * @param keyGenerator
     * @throws IOException
     * @throws KeyGenException
     *
     */
    public static void computeColExcludePredicateKeys(Map<Dimension, MolapFilterInfo> pred, InMemFilterModel filterModel,
            Dimension[] dimTables, KeyGenerator keyGenerator,List<InMemoryCube> slices) throws IOException, KeyGenException
    {
        List<Integer> dimFilterOffSet = new ArrayList<Integer>(10);
        long[][] excludePredicateKeys = new long[dimTables.length][];
        byte[][][] excludeFiltersOnDim = new byte[dimTables.length][][];
        filterModel.setMaskedByteRangesExclude(new int[dimTables.length][]);
        for(Map.Entry<MolapMetadata.Dimension, MolapFilterInfo> entry : pred.entrySet())
        //for(Iterator<Dimension> iterator = pred.keySet().iterator();iterator.hasNext();)
        {
            Dimension dim = entry.getKey();
            List<String[]> updatedExcludedMembers = getUpdatedConstarints(entry.getValue().getEffectiveExcludedMembers());
            List<String> effectiveExcludedMembers = getEffectiveIncludedMembers(dim, updatedExcludedMembers);
            long[] vals = getSurrogates(effectiveExcludedMembers, dim,slices,true,false);

            if(null != vals)
            {
                //
                excludePredicateKeys[dim.getOrdinal()] = vals;
                dimFilterOffSet.add(dim.getOrdinal());
                excludeFiltersOnDim[dim.getOrdinal()] = getFilterByteArray(vals, dim.getOrdinal(), dimTables,
                        keyGenerator);

                // to set the max Key for this dimension index, so that to
                // compare the actual key only this dimension column is compared
                long[] keymax = new long[dimTables.length];
                Arrays.fill(keymax, 0L);
                keymax[dim.getOrdinal()] = Long.MAX_VALUE;
                
                byte[] maxKey = keyGenerator.generateKey(keymax);
                int[] ranges = getRangesForMaskedByte(new int[]{dim.getOrdinal()}, keyGenerator);
                filterModel.getMaxKeyExclude()[dim.getOrdinal()] = getMaskedKey(ranges,maxKey);
                filterModel.getMaskedByteRangesExclude()[dim.getOrdinal()] = ranges;
//                filterModel.getMaxKeyExclude()[dim.getOrdinal()] = keyGenerator.generateKey(keymax);
            }
        }
        //
        filterModel.setColExcludeDimOffset(toIntArray(dimFilterOffSet));
        filterModel.setExcludeFilter(excludeFiltersOnDim);
        filterModel.setExcludePredicateKeys(excludePredicateKeys);
    }
    
    /**
     * 
     * @param filter
     * @param dimensionOrdinal
     * @param dimTables
     * @param keyGenerator
     * @return
     * @throws KeyGenException
     */
    private static byte[][] getFilterByteArray(long[] filter, int dimensionOrdinal, Dimension[] dimTables,
            KeyGenerator keyGenerator) throws KeyGenException
    {
        byte[][] dimFil = new byte[filter.length][];
        for(int j = 0;j < filter.length;j++)
        {
            long sdim = filter[j];
            long[] keys = new long[dimTables.length];
            Arrays.fill(keys, 0L);
            keys[dimensionOrdinal] = sdim;
            dimFil[j] = getMaskedKey(getRangesForMaskedByte(new int[]{dimensionOrdinal},keyGenerator),keyGenerator.generateKey(keys));
        }
        return dimFil;
    }
    
    /**
     * Fills the includePredicates 2D array and creates the offset array
     * 
     * @param pred
     * @param excludePredicates
     * @return
     * @throws IOException
     * @throws KeyGenException
     * 
     */
    public static void computeColIncludePredicateKeys(Map<Dimension, MolapFilterInfo> pred, InMemFilterModel filterModel,
            Dimension[] dimTables, KeyGenerator keyGenerator,int[][] groups,List<InMemoryCube> slices,Cube cube, String factTableName) throws IOException, KeyGenException
    {
        //
        List<Integer> dimFilterOffSet = new ArrayList<Integer>(10);
        byte[][][] includeFiltersOnDim = new byte[dimTables.length][][];
        List<String[]>[] includedMembers = new  List[dimTables.length];
        filterModel.setMaskedByteRanges(new int[dimTables.length][]);
        for(Map.Entry<MolapMetadata.Dimension, MolapFilterInfo> entry : pred.entrySet())
        {
            //
            Dimension dim = entry.getKey();
            List<String[]> effectiveIncludedMembers = getUpdatedConstarints(entry.getValue().getIncludedMembers());

//            for(int i = 0;i < effectiveIncludedMembers.size();i++)
//            {
//                String[] string = effectiveIncludedMembers.get(i);
//                
//                int k = 0;
//                for(int j = string.length-1;j >= 0;j--,k++)
//                {
//                    if(dimTables[dim.getOrdinal()-k].getDataType().equals(SqlStatement.Type.DOUBLE))
//                    {
//                        convertToDouble(string, j);
//                    }
//                    else if(dimTables[dim.getOrdinal()-k].getDataType().equals(SqlStatement.Type.INT))
//                    {
//                        convertToInt(string, j);
//                    }
//                }
//            }

            if(effectiveIncludedMembers.size() > 0)
            {
//                includedMembers[dim.getOrdinal()] = effectiveIncludedMembers;
                if(null!=factTableName)
                {
                    setFilters(effectiveIncludedMembers, includedMembers, dim, groups,dimTables,cube,factTableName);
                }
                else
                {
                    setFilters(effectiveIncludedMembers, includedMembers, dim, groups,dimTables,cube,null);
                }
            }
        }
        Map<Integer, long[]> preds = new HashMap<Integer, long[]>(10);
        for(int i = 0;i < groups.length;i++)
        {
            
      
            for(int j = 0;j < groups[i].length;j++)
            {
                if(groups[i][j] != -1)
                {
                    long[][] vals = new long[dimTables.length][];
                    // to set the max Key for this dimension index, so that to
                    // compare the actual key only this is compared
                    long[] keymax = new long[dimTables.length];
                    Arrays.fill(keymax, 0L);
                    List<Integer> ranges = new ArrayList<Integer>(10);
                    fillFilters(includedMembers[groups[i][j]], vals, j, groups[i], keymax, slices, dimTables,preds,ranges);

                    includeFiltersOnDim[groups[i][j]] = getFilterByteArray(vals, groups[i],j, dimTables,keyGenerator);
                    if(includeFiltersOnDim[groups[i][j]].length > 0)
                    {
//                        filterModel.getMaxKey()[groups[i][j]] = keyGenerator.generateKey(keymax);
                        
                        byte[] maxKey = keyGenerator.generateKey(keymax);
                        int[] rangeArry = getRangesForMaskedByte(convertIntegerListToIntArray(ranges), keyGenerator);
                        filterModel.getMaxKey()[groups[i][j]] = getMaskedKey(rangeArry,maxKey);
                        filterModel.getMaskedByteRanges()[groups[i][j]] = rangeArry; 
                        
                        dimFilterOffSet.add(groups[i][j]);
                    }
                }
            }
        }
        //
        filterModel.setColIncludeDimOffset(toIntArray(dimFilterOffSet));
        filterModel.setFilter(includeFiltersOnDim);
        long[][] includePredicateKeys = new long[dimTables.length][];
        setPredArray(preds, includePredicateKeys);
        processFilters(includePredicateKeys);
        filterModel.setIncludePredicateKeys(includePredicateKeys);
    }

    /**
     * @param string
     * @param j
     */
    private static void convertToDouble(String[] string, int j)
    {
        try
        {
            string[j] = Double.toString(Double.parseDouble(string[j]));
        }
        catch (NumberFormatException e) 
        {
            return;
        }
    }
    
    /**
     * @param string
     * @param j
     */
    private static void convertToInt(String[] string, int j)
    {
        try
        {
            string[j] = Integer.toString(Integer.parseInt(string[j]));
        }
        catch (NumberFormatException e) 
        {
            return;
        }
    }
    
    /**
     * 
     * @param preds
     * @param includePredicateKeys
     */
    private static void setPredArray(Map<Integer, long[]> preds,long[][] includePredicateKeys)
    {
        for(Entry<Integer, long[]> entry : preds.entrySet())
        {
            includePredicateKeys[entry.getKey()] = entry.getValue();
        }
    }
    
    /**
     * 
     * @param includedMember
     * @param vals
     * @param groupIndex
     * @param group
     * @param keymax
     * @param slices
     * @param dimTables
     * @param preds
     */
    private static void fillFilters(List<String[]> includedMember,long[][] vals,int groupIndex,int[] group,long[] keymax,List<InMemoryCube> slices,Dimension[] dimTables,Map<Integer, long[]> preds,List<Integer> ranges)
    {
        if(includedMember == null || includedMember.size() == 0)
        {
            return;
        }
//        Map<Integer,List<String>> hierMembers = new HashMap<Integer,List<String>>();
//        for(String[] mems : includedMember)
//        {
//            for(int i = mems.length-1;i >= 0;i--)
//            {
//                List<String> list = hierMembers.get(i);
//                if(list == null)
//                {
//                    list = new ArrayList<String>();
//                    hierMembers.put(i, list);
//                }
//                list.add(mems[i]);
//            }
//        }
        Map<Integer, long[]> predsLocal = new HashMap<Integer, long[]>(10);
        int size = includedMember.size()>0?includedMember.get(0).length:0;
        for(int k = 0;k < includedMember.size();k++)
        {
            String[] strings = includedMember.get(k);
            long[][] tempMems = new long[strings.length][];
            //TODO : Get the ordinal and decrement it. We will change this logic when we implement keyOrdinal
            int ordinal = group[groupIndex];
            for(int i = size-1;i >= 0;i--,ordinal--)
            {
 
                String member = strings[i];
                List<String> members = new ArrayList<String>(10);
                members.add(member);
//                vals[ordinal] = getSurrogates(members, getDimensionByOrdinal(dimTables, ordinal), slices, false);
                tempMems[i] = getSurrogates(members, getDimensionByOrdinal(dimTables, ordinal), slices, false, false);

                keymax[ordinal] = Long.MAX_VALUE;
                ranges.add(ordinal);
            }
            long[][] mems = extrapolateMembers(tempMems);
            ordinal = group[groupIndex];
            for(int i = size-1;i >= 0;i--,ordinal--)
            {

                checkAndUpdateFilters(vals, preds, ordinal, mems[i],false);
                checkAndUpdateFilters(vals, predsLocal, ordinal, mems[i],true);
            }
        }
    }
    
    
    /**
     * 
     * @param includedMember
     * @param vals
     * @param groupIndex
     * @param group
     * @param keymax
     * @param slices
     * @param dimTables
     * @param preds
     */
    private static void fillFiltersForSubtotal(List<String[]> includedMember,long[][] vals,int groupIndex,int[] group,long[] keymax,List<InMemoryCube> slices,Dimension[] dimTables,Map<Integer, long[]> preds,List<Integer> ranges)
    {
        if(includedMember == null || includedMember.size() == 0)
        {
            return;
        }
        
        Map<Integer, long[]> predsLocal = new HashMap<Integer, long[]>(10);
        int size = includedMember.size()>0?includedMember.get(0).length:0;
        for(int k = 0;k < includedMember.size();k++)
        {
            String[] strings = includedMember.get(k);
            long[][] tempMems = new long[strings.length][];
            //TODO : Get the ordinal and decrement it. We will change this logic when we implement keyOrdinal
            
            int[] groupOrdinals = getGroupOrdinalsEligible(group, groupIndex);
            
//            int ordinal = group[groupIndex];
            int ordinal = 0;
            for(int i = size-1;i >= 0;i--)
            {
                ordinal = groupOrdinals[i];
                
                String member = strings[i];
                List<String> members = new ArrayList<String>(10);
                members.add(member);
//                vals[ordinal] = getSurrogates(members, getDimensionByOrdinal(dimTables, ordinal), slices, false);
                tempMems[i] = getSurrogates(members, getDimensionByOrdinal(dimTables, ordinal), slices, false,true);

                keymax[ordinal] = Long.MAX_VALUE;
                ranges.add(ordinal);
            }
            long[][] mems = extrapolateMembers(tempMems);
            
            for(int i = size-1;i >= 0;i--)
            {
                ordinal = groupOrdinals[i];
            
                checkAndUpdateFilters(vals, preds, ordinal, mems[i],false);
                checkAndUpdateFilters(vals, predsLocal, ordinal, mems[i],true);
            }
        }
    }



    private static int[] getGroupOrdinalsEligible(int[] group, int groupIndex)
    {
        List<Integer> eligibleGroups = new ArrayList<Integer>(10);
        for(int i =0; i <= groupIndex; i++)
        {
            if(group[i] != -1)
            {
                eligibleGroups.add(group[i]);
            }
        }
        
        Integer[] array = eligibleGroups.toArray(new Integer[eligibleGroups.size()]);
        
        int [] result = new int[array.length];
        
        int i = 0;
        for(Integer integer : array)
        {
            result[i++] = integer.intValue();
        }
        
        return result;
    }


    /**
     * @param vals
     * @param preds
     * @param ordinal
     * @param mems
     * @param i
     */
    private static void checkAndUpdateFilters(long[][] vals, Map<Integer, long[]> preds, int ordinal, long[] mems,boolean updatePreds)
    {
        long[] existed = preds.get(ordinal);
        if(existed != null)
        {
            long[] newSurgs = new long[existed.length+mems.length];
            System.arraycopy(existed, 0, newSurgs, 0, existed.length);
            System.arraycopy(mems, 0, newSurgs, existed.length, mems.length);
            if(updatePreds)
            {
                vals[ordinal] = newSurgs;
            }
            preds.put(ordinal, newSurgs);
            
        }
        else
        {
            vals[ordinal] = mems;
            preds.put(ordinal,  vals[ordinal]);
        }
    }
    
    private static long[][] extrapolateMembers(long[][] mems)
    {
        int size = 1;
        for(int i = 0;i < mems.length;i++)
        {
            size *=  mems[i].length;
        }
        
        long[][] temp = new long[mems.length][size];
        
        for(int i = 0;i < mems.length;i++)
        {
            int l =  size/mems[i].length;
            for(int j = 0;j < l;j++)
            {
                System.arraycopy(mems[i], 0, temp[i], mems[i].length*j, mems[i].length);
                
            }
        }
        return temp;
    }
    
    /**
     * Get the dimension level by ordinal
     * @param dimTables
     * @return
     */
    public static Dimension getDimensionByOrdinal(Dimension[] dimTables, int ordinal) 
    {
        for(Dimension dimension : dimTables)
        {
            if(dimension.getOrdinal() == ordinal)
            {
                return dimension;
            }
        }
        return null;
    }
    
    /**
     * Remove the duplicate filters from array
     * @param includePredicateKeys
     */
    private static void processFilters(long[][] includePredicateKeys)
    {
        Set<Long> longs = new HashSet<Long>(10);
        for(int i = 0;i < includePredicateKeys.length;i++)
        {
            long[] ls = includePredicateKeys[i];
            if(ls != null && ls.length > 0)
            {
                longs.clear();
                for(int j = 0;j < ls.length;j++)
                {
                    longs.add(ls[j]);
                }
                
                long [] updated = new long[longs.size()];
                int k = 0;
                for(long val : longs)
                {
                    updated[k] = val;
                    k++;
                }
                Arrays.sort(updated);
                includePredicateKeys[i] = updated;
            }
            
        }
    }
    
    /**
     * Sort filter value
     * @param includePredicateKeys
     */
    private static void processFiltersExclude(long[][] includePredicateKeys)
    {
        for(int i = 0;i < includePredicateKeys.length;i++)
        {
            long[] ls = includePredicateKeys[i];
            if(ls != null && ls.length > 0)
            {
                Arrays.sort(ls);
                includePredicateKeys[i] = ls;
            }
            
        }
    }
    
    
    /**
     * Fills the excludePredicates 2D array and creates the offset array
     * 
     * @param pred
     * @param filterModel
     * @param dimTables
     * @param keyGenerator
     * @throws IOException
     * @throws KeyGenException
     *
     */
    public static void computeColExcludePredicateKeys(Map<Dimension, MolapFilterInfo> pred, InMemFilterModel filterModel,
            Dimension[] dimTables, KeyGenerator keyGenerator,int[][] groups,List<InMemoryCube> slices,Cube cube,String factTableName) throws IOException, KeyGenException
    {
        List<Integer> dimFilterOffSet = new ArrayList<Integer>(10);
        byte[][][] excludeFiltersOnDim = new byte[dimTables.length][][];
        List<String[]>[] excludedMembers = new  List[dimTables.length];
        filterModel.setMaskedByteRangesExclude(new int[dimTables.length][]);
        for(Map.Entry<MolapMetadata.Dimension, MolapFilterInfo> entry : pred.entrySet())
        //for(Iterator<Dimension> iterator = pred.keySet().iterator();iterator.hasNext();)
        {
            Dimension dim = entry.getKey();
            List<String[]>  effectiveExcludedMembers = getUpdatedConstarints(entry.getValue().getExcludedMembers());
         

                
            if(effectiveExcludedMembers.size() > 0)
            {
                setFilters(effectiveExcludedMembers, excludedMembers, dim, groups,dimTables,cube,factTableName);
            }
        }
        Map<Integer, long[]> preds = new HashMap<Integer, long[]>(10);
        for(int i = 0;i < groups.length;i++)
        {
            
      
            for(int j = 0;j < groups[i].length;j++)
            {
                if(groups[i][j] != -1)
                {
                    long[][] vals = new long[dimTables.length][];
                    // to set the max Key for this dimension index, so that to
                    // compare the actual key only this is compared
                    long[] keymax = new long[dimTables.length];
                    Arrays.fill(keymax, 0L);
                    
                    List<Integer> ranges = new ArrayList<Integer>(10); 
                    fillFilters(excludedMembers[groups[i][j]], vals, j, groups[i], keymax, slices, dimTables,preds,ranges);

                    excludeFiltersOnDim[groups[i][j]] = getFilterByteArray(vals, groups[i],j, dimTables,keyGenerator);
                    if(excludeFiltersOnDim[groups[i][j]].length > 0)
                    {
//                        filterModel.getMaxKeyExclude()[groups[i][j]] = keyGenerator.generateKey(keymax);
                        
                        byte[] maxKey = keyGenerator.generateKey(keymax);
                        int[] rangeArray = getRangesForMaskedByte(convertIntegerListToIntArray(ranges), keyGenerator);
                        filterModel.getMaxKeyExclude()[groups[i][j]] = getMaskedKey(rangeArray,maxKey);
                        filterModel.getMaskedByteRangesExclude()[groups[i][j]] = rangeArray;
                        
                        dimFilterOffSet.add(groups[i][j]);
                    }
                }
            }
        }
        //
        filterModel.setColExcludeDimOffset(toIntArray(dimFilterOffSet));
        filterModel.setExcludeFilter(excludeFiltersOnDim);
        long[][] excludePredicateKeys = new long[dimTables.length][];
        setPredArray(preds, excludePredicateKeys);
        processFiltersExclude(excludePredicateKeys);
        filterModel.setExcludePredicateKeys(excludePredicateKeys);
        filterModel.setGroups(groups);
    }
    
    /**
     * Fills the includePredicate 
     * 
     * @param pred
     * @param filterModel
     * @param dimTables
     * @param keyGenerator
     * @throws IOException
     * @throws KeyGenException
     *
     */
    public static void computeColIncludePredicateKeysForSubtotal(Map<Dimension, MolapFilterInfo> pred, InMemFilterModel filterModel,
            Dimension[] dimTables, KeyGenerator keyGenerator,int[][] groups,List<InMemoryCube> slices,Cube cube,String factTableName) throws IOException, KeyGenException
    {
        //
        List<Integer> dimFilterOffSet = new ArrayList<Integer>(10);
        byte[][][] includeFiltersOnDim = new byte[dimTables.length][][];
        List<String[]>[] includedMembers = new  List[dimTables.length];
        filterModel.setMaskedByteRanges(new int[dimTables.length][]);
        for(Map.Entry<MolapMetadata.Dimension, MolapFilterInfo> entry : pred.entrySet())
        {
            //
            Dimension dim = entry.getKey();
            List<String[]> effectiveIncludedMembers = getUpdatedConstarints(entry.getValue().getIncludedMembers());


            if(effectiveIncludedMembers.size() > 0)
            {
                setFiltersForSubtotal(effectiveIncludedMembers, includedMembers, dim,dimTables);
            }
        }
        Map<Integer, long[]> preds = new HashMap<Integer, long[]>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        for(int i = 0;i < groups.length;i++)
        {
            
      
            for(int j = 0;j < groups[i].length;j++)
            {
                if(groups[i][j] != -1)
                {
                    long[][] vals = new long[dimTables.length][];
                    // to set the max Key for this dimension index, so that to
                    // compare the actual key only this is compared
                    long[] keymax = new long[dimTables.length];
                    Arrays.fill(keymax, 0L);
                    List<Integer> ranges = new ArrayList<Integer>(MolapCommonConstants.CONSTANT_SIZE_TEN);
                    fillFiltersForSubtotal(includedMembers[groups[i][j]], vals, j, groups[i], keymax, slices, dimTables,preds,ranges);

                    includeFiltersOnDim[groups[i][j]] = getFilterByteArray(vals, groups[i],j, dimTables,keyGenerator);
                    if(includeFiltersOnDim[groups[i][j]].length > 0)
                    {
//                        filterModel.getMaxKey()[groups[i][j]] = keyGenerator.generateKey(keymax);
                        
                        byte[] maxKey = keyGenerator.generateKey(keymax);
                        int[] rangeArray = getRangesForMaskedByte(convertIntegerListToIntArray(ranges), keyGenerator);
                        filterModel.getMaxKey()[groups[i][j]] = getMaskedKey(rangeArray,maxKey);
                        filterModel.getMaskedByteRanges()[groups[i][j]] = rangeArray;
                        
                        dimFilterOffSet.add(groups[i][j]);
                    }
                }
            }
        }
        //
        filterModel.setColIncludeDimOffset(toIntArray(dimFilterOffSet));
        filterModel.setFilter(includeFiltersOnDim);
        long[][] includePredicateKeys = new long[dimTables.length][];
        setPredArray(preds, includePredicateKeys);
        processFiltersExclude(includePredicateKeys);
        filterModel.setIncludePredicateKeys(includePredicateKeys);
    }
    
    
    private static void setFilters(List<String[]> filter,List<String[]>[] filters,Dimension dim,int[][] groups,Dimension[] dimTables,Cube cube, String factTableName)
    {
        
        Map<Integer, List<String[]>> filterMap = new HashMap<Integer, List<String[]>>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        for(String[] strArry : filter)
        {
            int length = strArry.length-1;

            List<String[]> list = filterMap.get(length);
            if(list == null)
            {
                list = new ArrayList<String[]>(MolapCommonConstants.CONSTANT_SIZE_TEN);
                filterMap.put(length,list);
            }
            list.add(strArry);
        }
        
        int ordinal = dim.getOrdinal();
        List<Dimension> hierarchiesMapping = cube.getHierarchiesMapping(dim.getDimName()+'_'+dim.getHierName());
        for(Entry<Integer, List<String[]>> entry : filterMap.entrySet())
        {
            Dimension dimLocal = hierarchiesMapping.get(entry.getKey());
            int updatedOrdinal=-1;

            if(null==factTableName)
            {
                updatedOrdinal = cube.getDimensionByLevelName(dimLocal.getDimName(), dimLocal.getHierName(), dimLocal.getName()).getOrdinal();
            }
            else
            {
                updatedOrdinal = cube.getDimensionByLevelName(dimLocal.getDimName(), dimLocal.getHierName(), dimLocal.getName(),factTableName).getOrdinal();
            }
         
            filters[updatedOrdinal] = entry.getValue();
            for(int i = 0;i < entry.getValue().size();i++)
            {
                String[] string = entry.getValue().get(i);
                int k = 0;
                for(int j = string.length-1;j >= 0;j--,k++)
                {
                    if(dimTables[updatedOrdinal-k].getDataType().equals(SqlStatement.Type.DOUBLE))
                    {
                        convertToDouble(string, j);
                    } 
                    else if(dimTables[updatedOrdinal-k].getDataType().equals(SqlStatement.Type.INT))
                    {
                        convertToInt(string, j);
                    }
                }

            }
        }
        
        int[] group = null;
        for(int i = 0;i < groups.length;i++)
        {
            for(int j = 0;j < groups[i].length;j++)
            {
                if(groups[i][j] == ordinal)
                {
                    group = groups[i];
                    break;
                }
            }
        }
        
        for(Entry<Integer, List<String[]>> entry : filterMap.entrySet())
        {
            Dimension dimLocal = hierarchiesMapping.get(entry.getKey());
            if(null!=group)
            {
                group[entry.getKey()] = cube.getDimensionByLevelName(dimLocal.getDimName(), dimLocal.getHierName(), dimLocal.getName(),factTableName).getOrdinal();
            }
        }
        
    }
    
    
    private static void setFiltersForSubtotal(List<String[]> filter,List<String[]>[] filters,Dimension dim,Dimension[] dimTables)
    {
        
        Map<Integer, List<String[]>> filterMap = new HashMap<Integer, List<String[]>>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        for(String[] strArry : filter)
        {
            int length = strArry.length-1;

            List<String[]> list = filterMap.get(length);
            if(list == null)
            {
                list = new ArrayList<String[]>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
                filterMap.put(length,list);
            }
            list.add(strArry);
        }
        
        int ordinal = dim.getOrdinal();
        
        for(Entry<Integer, List<String[]>> entry : filterMap.entrySet())
        {
            filters[ordinal] = entry.getValue();
            for(int i = 0;i < entry.getValue().size();i++)
            {
                String[] string = entry.getValue().get(i);
                int k = 0;
                for(int j = string.length-1;j >= 0;j--,k++)
                {
                    if(dimTables[ordinal-k].getDataType().equals(SqlStatement.Type.DOUBLE))
                    {
                        convertToDouble(string, j);
                    } 
                    else if(dimTables[ordinal-k].getDataType().equals(SqlStatement.Type.INT))
                    {
                        convertToInt(string, j);
                    }
                }

            }
        }
    }

    
    /**
     * toIntArray
     * @param list
     * @return
     */
    private static int[] toIntArray(List<Integer> list)
    {
        int[] a = new int[list.size()];

        for(int i = 0;i < a.length;i++)
        {
            a[i] = list.get(i);
        }
        return a;
    }
    
    /**
     * 
     * @param filter
     * @param dimensionOrdinal
     * @param dimTables
     * @param keyGenerator
     * @return
     * @throws KeyGenException
     */
    private static byte[][] getFilterByteArray(long[][] filter, int[] dimensionOrdinal,int groupIndex, Dimension[] dimTables,
            KeyGenerator keyGenerator) throws KeyGenException
    {
        int len = 0;
        
        for(int i = 0;i < filter.length;i++)
        {
            int localLen = filter[i] != null ? filter[i].length : 0;
            if(localLen > len)
            {
                len = localLen;
            }
        }
        
        List<byte[]> dimFil = new ArrayList<byte[]>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        for(int j = 0;j < len;j++)
        {
            int ordinal = dimensionOrdinal[groupIndex];
            long[] keys = new long[dimTables.length];
            List<Integer> range = new ArrayList<Integer>(MolapCommonConstants.CONSTANT_SIZE_TEN);
//            Arrays.fill(keys, 0L);
            for(int i = groupIndex;i >= 0;i--,ordinal--)
            {
                if(filter[ordinal] != null && j < filter[ordinal].length)
                {
                    long sdim = filter[ordinal][j];
                    keys[ordinal] = sdim;
                    range.add(ordinal);
                }
            }
            dimFil.add(getMaskedKey(getRangesForMaskedByte(convertIntegerListToIntArray(range),keyGenerator),keyGenerator.generateKey(keys)));
        }
        Collections.sort(dimFil,keyGenerator);
        return dimFil.toArray(new byte[0][0]);
    }
    
    //TODO SIMIAN
    /**
     * getDimensionGroups
     * @param cube
     * @param constraints
     */
    public static int[][] getDimensionGroups(Cube cube,Map<Dimension, MolapFilterInfo> constraints)
    {
        Map<String, int[]> map = new LinkedHashMap<String, int[]>();
        for(Entry<Dimension, MolapFilterInfo> entry : constraints.entrySet())
        {
            Dimension key = entry.getKey();
            
            List<Dimension> dimList = cube.getHierarchiesMapping(key.getDimName()+'_'+key.getHierName());
            Dimension firstLevel = dimList.get(0);
            String keyStr = firstLevel.getDimName()+'_'+firstLevel.getHierName()+'_'+firstLevel.getName();
            int[] grp = map.get(keyStr);
            if(grp == null)
            {
                grp = new int[dimList.size()];
                Arrays.fill(grp, -1);
                map.put(keyStr, grp);
            }
            int dimIndx = getDimIndex(dimList, key); 
            grp[dimIndx] = key.getOrdinal();
        }
        
        int[][] groups  = new int[map.size()][];
        List<Dimension> dimensions = cube.getDimensions(cube.getFactTableName());
        int i = 0;
        for(Dimension dim : dimensions)
        {
            String keyStr = dim.getDimName()+'_'+dim.getHierName()+'_'+dim.getName();
            int[] grp = map.get(keyStr);
            if(grp != null)
            {
                groups[i] = grp;
                i++;
            }
        }
        return groups;
    }
    
    /**
     * getDimensionGroups
     * @param cube
     * @param constraints
     */
    public static int[][] getDimensionGroupsForDifferentDimAsOneGroup(Cube cube,Map<Dimension, MolapFilterInfo> constraints, String factTableName)
    {
        Map<String, int[]> map = new LinkedHashMap<String, int[]>();
        List<Dimension> dimList = cube.getDimensions(factTableName);
        Dimension firstLevel = dimList.get(0);
        String keyStr = firstLevel.getDimName()+'_'+firstLevel.getHierName()+'_'+firstLevel.getName();
        
        for(Entry<Dimension, MolapFilterInfo> entry : constraints.entrySet())
        {
            Dimension key = entry.getKey();
            
            int[] grp = map.get(keyStr);
            if(grp == null)
            {
                grp = new int[dimList.size()];
                Arrays.fill(grp, -1);
                map.put(keyStr, grp);
            }
            int dimIndex = getDimIndex(dimList, key);
            grp[dimIndex] = key.getOrdinal();
        }
        
        int[][] groups  = new int[map.size()][];
        List<Dimension> dimensions = cube.getDimensions(cube.getFactTableName());
        int i = 0;
        for(Dimension dim : dimensions)
        {
            String localKeyStr = dim.getDimName()+'_'+dim.getHierName()+'_'+dim.getName();
            int[] grp = map.get(localKeyStr);
            if(grp != null)
            {
                groups[i] = grp;
                i++;
            }
        }
        return groups;
    }
    
    private static int getDimIndex(List<Dimension> dimList,Dimension dim)
    {
        int i = 0;
        for(Dimension dimLoc : dimList)
        {
            if(dim.getName().equals(dimLoc.getName()) && dim.getDimName().equals(dimLoc.getDimName()) && dim.getHierName().equals(dimLoc.getHierName()) && dim.getTableName().equals(dimLoc.getTableName()))
            {
                return i;
            }
            i++;
        }
        return -1;
    }
    
    /**
     * Get the surrogates for each predicates for cache
     * @param constraints
     * @param dimTables
     * @param slices
     * @return
     */
    public static Map<Dimension,MolapPredicates> getMolapPredicates(Map<Dimension, MolapFilterInfo> constraints,Dimension[] dimTables,List<InMemoryCube> slices,boolean isActual,boolean isFilterinGroup,Cube cube, String factTableName)
    {
        Map<Dimension,MolapPredicates> preds = new LinkedHashMap<MolapMetadata.Dimension, MolapPredicates>();
        
        for(Entry<Dimension, MolapFilterInfo> entry : constraints.entrySet())
        {
            
            if(entry.getValue() == null || entry.getKey() == null)
            {
                continue;
            }
            
            List<String[]> effectiveIncludedMembers = getUpdatedConstarints(entry.getValue().getIncludedMembers());
            List<String[]> effectiveExcludedMembers = getUpdatedConstarints(entry.getValue().getExcludedMembers());
            List<String[]> effectiveIncludedOrMembers = getUpdatedConstarints(entry.getValue().getIncludedOrMembers());
            
            Map<Integer, List<String>> incPreds = getUpdatePredicateList(entry.getKey(), effectiveIncludedMembers,isFilterinGroup,cube,factTableName);
            Map<Integer, List<String>> exPreds = getUpdatePredicateList(entry.getKey(), effectiveExcludedMembers,isFilterinGroup,cube,factTableName);
            Map<Integer, List<String>> incOrPreds = getUpdatePredicateList(entry.getKey(), effectiveIncludedOrMembers,isFilterinGroup,cube,factTableName);
            
            Dimension[] copyDim = new Dimension[dimTables.length];
                for(int i = 0;i < copyDim.length;i++)
                {
                    copyDim[i] = dimTables[i].getDimCopy();
                    if(constraints.get(dimTables[i]) != null)
                    {
                        copyDim[i].setActualCol(isActualInDimension(constraints, dimTables[i]));
                    }
                    else
                    {
                        copyDim[i].setActualCol(isActual);
                    }
                }
            updateMolapPredMap(copyDim, slices, preds, entry, incPreds,false,false);
            updateMolapPredMap(copyDim, slices, preds, entry, incOrPreds,false,true);
            updateMolapPredMap(copyDim, slices, preds, entry, exPreds,true,false);
            
        }
        
        return preds;
    }

    
    private static boolean isActualInDimension(Map<Dimension, MolapFilterInfo> constraints,Dimension key)
    {
        for(Dimension dimension : constraints.keySet())
        {
            if(key.equals(dimension))
            {
                return dimension.isActualCol();
            }
        }
        return false;
    }
    
    
    /**
     * @param dimTables
     * @param slices
     * @param preds
     * @param entry
     * @param incPreds
     */
    private static void updateMolapPredMap(Dimension[] dimTables, List<InMemoryCube> slices,
            Map<Dimension, MolapPredicates> preds, Entry<Dimension, MolapFilterInfo> entry,
            Map<Integer, List<String>> incPreds,boolean exclude,boolean includeOr)
    {
        for(Entry<Integer, List<String>> entryInc : incPreds.entrySet())
        {
            Dimension dimension = getDimensionByOrdinal(dimTables, entryInc.getKey());
            List<String> value = entryInc.getValue();
            if(dimension.getDataType().equals(SqlStatement.Type.DOUBLE) && value != null)
            {
                for(int i = 0;i < value.size();i++)
                {
                    try
                    {
                        value.set(i, Double.toString(Double.parseDouble(value.get(i))));
                    }
                    catch (NumberFormatException e) 
                    {
                        value.set(i, value.get(i));
                    }
                }
            }
            else if(dimension.getDataType().equals(SqlStatement.Type.INT) && value != null)
            {
                for(int i = 0;i < value.size();i++)
                {
                    try
                    {
                        value.set(i, Integer.toString(Integer.parseInt(value.get(i))));
                    }
                    catch (NumberFormatException e) 
                    {
                        value.set(i, value.get(i));
                    }
                }
            }

            long[] include =getSurrogates(value, dimension,slices,true, false);
            MolapPredicates molapPredicates = preds.get(dimension);
            if(molapPredicates == null)
            {
                molapPredicates = new MolapPredicates();
                if(exclude)
                {
                    molapPredicates.setExclude(include == null ? new long[0] : include);
                }
                else
                {
                    if(includeOr)
                    {
                        molapPredicates.setIncludeOr(include == null ? new long[0] : include);
                    }
                    else
                    {
                        molapPredicates.setInclude(include == null ? new long[0] : include);
                    }
                }
                molapPredicates.setOrdinal(dimension.getOrdinal());
                preds.put(dimension, molapPredicates);
            }
            else
            {
                long[] existed = null;
                if(exclude)
                {
                    existed = molapPredicates.getExclude();
                }
                else
                {
                    if(includeOr)
                    {
                        existed = molapPredicates.getIncludeOr();
                    }
                    else
                    {
                        existed = molapPredicates.getInclude();
                    }
                }
                long[] newSurgs = new long[existed.length+include.length];
                System.arraycopy(existed, 0, newSurgs, 0, existed.length);
                System.arraycopy(include, 0, newSurgs, existed.length, include.length);
                if(exclude)
                {
//                    molapPredicates.setExclude(newSurgs == null ? new long[0] : newSurgs);
                    molapPredicates.setExclude(newSurgs);
                }
                else
                {
                    if(includeOr)
                    {
                        molapPredicates.setIncludeOr(newSurgs);
                    }
                    else
                    {
//                      molapPredicates.setInclude(newSurgs == null ? new long[0] : newSurgs);
                        molapPredicates.setInclude(newSurgs);
                    }
                }
            }
        }
    }

    /**
     * @param entry
     * @param effectiveIncludedMembers
     * @return
     */
    private static Map<Integer, List<String>> getUpdatePredicateList(Dimension dim,
            List<String[]> effectiveIncludedMembers,boolean isFilterinGroup,Cube cube,String factTableName)
    {
        Map<Integer, List<String>> incPreds = new HashMap<Integer, List<String>>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        for(String[] mems : effectiveIncludedMembers)
        {
            int ordinal = dim.getOrdinal();
            if(isFilterinGroup)
            {
                List<Dimension> hierarchiesMapping = cube.getHierarchiesMapping(dim.getDimName()+'_'+dim.getHierName());
                int indexOf = mems.length-1;
                for(int i = mems.length-1;i >=0;i--,indexOf--)
                {
                    Dimension dimLocal = hierarchiesMapping.get(indexOf);
                    ordinal = cube.getDimensionByLevelName(dimLocal.getDimName(), dimLocal.getHierName(), dimLocal.getName(),factTableName).getOrdinal();
                    List<String> list = incPreds.get(ordinal);
                    if(list == null)
                    {
                        list = new ArrayList<String>(MolapCommonConstants.CONSTANT_SIZE_TEN);
                        incPreds.put(ordinal, list);
                    }
                    list.add(mems[i]);
                }
            }
            else
            {
                List<String> list = incPreds.get(ordinal);
                if(list == null)
                {
                    list = new ArrayList<String>(MolapCommonConstants.CONSTANT_SIZE_TEN);
                    incPreds.put(ordinal, list);
                }
                list.add(mems[mems.length-1]);
            }
        }
        return incPreds;
    }
    
    /**
     * Updates the constraints as per the passed dimensions.If passed constraints does not exist in passed dimensions then it will remove
     * @param constraints
     * @param allDims
     * @return
     */
    public static Map<Dimension, MolapFilterInfo> updateConstraintsAsperDimensions(Map<Dimension, MolapFilterInfo> constraints,List<Dimension> allDims)
    {
        Map<Dimension, MolapFilterInfo> updatedCons = new HashMap<MolapMetadata.Dimension, MolapFilterInfo>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        
        for(Entry<Dimension, MolapFilterInfo> entry : constraints.entrySet())
        {
            Dimension key = entry.getKey();
            for(Dimension dimension : allDims)
            {
                if(key.getDimName().equals(dimension.getDimName()) && key.getHierName().equals(dimension.getHierName())
                        && key.getName().equals(dimension.getName()))
                {
                    updatedCons.put(dimension, entry.getValue());
                    break;
                }
            }
        }
        return updatedCons;
    }
    
    /**
     * getDimension
     * @param dimGet
     * @param dims
     * @return
     */
    public static Dimension getDimension(Dimension dimGet, Dimension[] dims)
    {
        for(Dimension dimension : dims)
        {
            if(dimGet.getDimName().equals(dimension.getDimName()) && dimGet.getHierName().equals(dimension.getHierName())
                    && dimGet.getName().equals(dimension.getName()))
            {
                return dimension;
            }
        }
        return dimGet;
    }
    

    /**
     * This method will be used to get whether execution is required or not for older slices
     * It will update the constraints also based on below condition
     * 1. If Filter only on old slice dims then isExecutionRequired true. no need to update the constraints  
     * 2. If Filter only on new dimensions and default value then isExecutionRequired true and remove that filter from constraints
     * 3. If Filter only on new dimension and filter not default value then isExecutionRequired false
     * 4. If Filter only on new dimension and filter default and some other value then isExecutionRequired true and remove that filter
     * 5. If filter is on both old and new then isExecutionRequired true and removed filter for old dimension 
     * 
     * @param newConstraints
     *          newConstraints
     * @param currentDimeTables
     *          currentDimeTables
     * @return boolean 
     *
     */
    public static boolean updateFilterForOlderSlice(Map<Dimension, MolapFilterInfo> newConstraints,
            Dimension[] currentDimeTables,List<InMemoryCube> slices)
    {
        // if newConstraints size is  less than zero then there is not filter added return true 
        if(newConstraints.size()<1)
        {
            return true;
        }
        boolean isExecutionRequired= false;
        List<Entry<Dimension, MolapFilterInfo>> entryList = new ArrayList<Map.Entry<Dimension,MolapFilterInfo>>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        for(Entry<Dimension, MolapFilterInfo>entry: newConstraints.entrySet())
        {
            boolean isFound= false;
            Dimension dim = entry.getKey();
            for(int i = 0;i < currentDimeTables.length;i++)
            {
                if(dim.getColName().equals(currentDimeTables[i].getColName()))
                {
                    isExecutionRequired= true;
                    isFound= true;
                    break;
                    
                }
            }
            if(!isFound)
            {
                entryList.add(entry);
            }
        }
        for(Entry<Dimension, MolapFilterInfo>entry:entryList)
        {
            Dimension dim = entry.getKey();
            MolapFilterInfo value = entry.getValue();
            List<String> effectiveExcludedMembers = value.getEffectiveExcludedMembers();
            List<String> memList = new ArrayList<String>(MolapCommonConstants.CONSTANT_SIZE_TEN);
            if(effectiveExcludedMembers.size()>0)
            {
                for(int j = 0;j < effectiveExcludedMembers.size();j++)
                    {
                        String[] parseMembers = parseMembers(effectiveExcludedMembers.get(j), memList);
                        List<Long> longs = new ArrayList<Long>(MolapCommonConstants.CONSTANT_SIZE_TEN);
                        getSurrogate(dim, slices, parseMembers[parseMembers.length-1],longs,false);
                        Long surrogate = longs.get(0);
                        if((surrogate.longValue() == 1L))
                        {
                            isExecutionRequired = false;
                            break;
                        }
                        else
                        {
                            isExecutionRequired=true;
                        }
                    }
//                }
            }
            List<String> effectiveIncludedMembers = value.getEffectiveIncludedMembers();
            if(effectiveIncludedMembers.size()>0)
            {
                    for(int j = 0;j < effectiveIncludedMembers.size();j++)
                    {
                        String[] parseMembers = parseMembers(effectiveIncludedMembers.get(j), memList);
                        List<Long> longs = new ArrayList<Long>(MolapCommonConstants.CONSTANT_SIZE_TEN);
                        getSurrogate(dim, slices, parseMembers[parseMembers.length-1],longs,false);
                        Long surrogate = longs.get(0);
                        if((surrogate.longValue() != 1L))
                        {
                            isExecutionRequired = false;
                        }
                        else
                        {
                            isExecutionRequired=true;
                            break;
                        }
                    }
//                }
            }
            newConstraints.remove(dim);
        }
        return isExecutionRequired;
    }
    
    /**
     * It checks whether the passed dimension is there in list or not.
     * @param dimension
     * @param dimList
     * @return
     */
    public static boolean contain(Dimension dimension ,  List<Dimension> dimList)
    {
        for(Dimension dim : dimList)
        {
            if(dim.getHierName().equals(dimension.getHierName()) && dim.getDimName().equals(dimension.getDimName()) && dim.getName().equals(dimension.getName()))
            {
                return true;
            }
        }
        return false;
    }
    
    public static boolean containWithSameColumnName(Dimension dimension ,  List<Dimension> dimList)
    {
        for(Dimension dim : dimList)
        {
            if(dim.getHierName().equals(dimension.getHierName()) && dim.getDimName().equals(dimension.getDimName()) && dim.getColName().equals(dimension.getColName()))
            {
                return true;
            }
        }
        return false;
    }
    
    /**
     * It checks whether the passed dimension is there in list or not.
     * @param dimension
     * @param dimList
     * @return
     */
    public static boolean contain(Dimension dimension ,  Dimension[] dimList)
    {
        for(Dimension dim : dimList)
        {
            if(dim.getHierName().equals(dimension.getHierName()) && dim.getDimName().equals(dimension.getDimName()) && dim.getName().equals(dimension.getName()))
            {
                return true;
            }
        }
        return false;
    }
    
    /**
     * It creates the masked key in case of topN query.
     * Analyzer uses the groups to do topN . it means it groups the levels of just before level you asked for topN and apply topN on it.
     * @param queryDimensions
     * @param generator
     * @param topNdimIndex
     * @param actualDims
     * @param topNDim
     * @param considerTopN
     * @return
     * @throws KeyGenException
     */
    public static byte[] getMaskedBytesForTopN(Dimension[] queryDimensions, KeyGenerator generator, int topNdimIndex,
            Dimension[] actualDims,Dimension[] actualDimsRows,Dimension[] actualDimsCols, Dimension topNDim, boolean considerTopN, int[] maskedKeyRanges, List<Integer> ranges)
            throws KeyGenException
    {
        
        //First get the topN index on actual dimensions got from Analyzer.
        int index = getTopNIndex(actualDimsRows, topNDim);
        boolean topNOnCols = false;
        if(index < 0)
        {
            index = getTopNIndex(actualDimsCols, topNDim);
            if(index >= 0)
            {
                topNOnCols = true;
            }
            else
            {
                index = 0;
            }
        }
        
        int topNOrdinal = 0;
        List<Dimension> considerList = new ArrayList<Dimension>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        if(!considerTopN)
        {
            if(index > 0)
            {
                index--;
            }
            else
            {
                return new byte[0];
            }
        }
        if(topNOnCols)
        {
            topNOrdinal = actualDimsCols[index].getOrdinal();
        }
        else
        {
            topNOrdinal = actualDims[index].getOrdinal();
        }
        for(int i = 0;i <= index;i++)
        {
           Dimension dimension = null;
            if(topNOnCols)
            {
                dimension = actualDimsCols[i];
            }
            else
            {
                dimension = actualDims[i];
            }
            for(int j = 0;j < queryDimensions.length;j++)
            {
                if(queryDimensions[j].getHierName().equals(dimension.getHierName()) && queryDimensions[j].getDimName().equals(dimension.getDimName()))
                {
                    if(queryDimensions[j].getHierName().equals(topNDim.getHierName()) && queryDimensions[j].getDimName().equals(topNDim.getDimName()))
                    {
                        if(queryDimensions[j].getOrdinal() > topNOrdinal)
                        {
                            continue;
                        }
                    }
                    if(!QueryExecutorUtil.contain(queryDimensions[j],considerList))
                    {
                        considerList.add(queryDimensions[j]);
                    }
                    
                }
            }
        }
        

       
        Set<Integer> integers = new TreeSet<Integer>();
        
        
        for(int i = 0;i < considerList.size();i++)
        {

            int[] range = generator.getKeyByteOffsets(considerList.get(i).getOrdinal());
            for(int j = range[0];j <= range[1];j++)
            {
                integers.add(j);
            }
        }
        //
        int[] byteIndexs = new int[integers.size()];
        int j = 0;
        Iterator<Integer> iterator = integers.iterator();
        while(iterator.hasNext())
        {
            Integer integer = (Integer)iterator.next();
            byteIndexs[j++] = integer.intValue(); 
        }
        byte[] maskedKey = getMaskedKeyForTopN(generator, maskedKeyRanges, ranges, considerList, byteIndexs);
        return maskedKey;
    }


    /**
     * @param actualDims
     * @param topNDim
     * @return
     */
    public static int getTopNIndex(Dimension[] actualDims, Dimension topNDim)
    {
        int index = -1;
        for(int i = 0;i < actualDims.length;i++)
        {
            if(actualDims[i].getHierName().equals(topNDim.getHierName()) && actualDims[i].getDimName().equals(topNDim.getDimName()) && actualDims[i].getName().equals(topNDim.getName()))
            {
                index = i;
                break;
            }
        }
        return index;
    }


    private static byte[] getMaskedKeyForTopN(KeyGenerator generator, int[] maskedKeyRanges, List<Integer> ranges,
            List<Dimension> considerList, int[] byteIndexs) throws KeyGenException
    {
        long[] key = new long[generator.getDimCount()];
        for(int i = 0;i < considerList.size();i++)
        {
            key[considerList.get(i).getOrdinal()] = Long.MAX_VALUE;
        }
        
        return getMaskedKey(generator, maskedKeyRanges, ranges, byteIndexs, key);
    }

    //TODO SIMIAN
    /**
     * @param generator
     * @param maskedKeyRanges
     * @param ranges
     * @param byteIndexs
     * @param key
     * @return
     * @throws KeyGenException
     */
    private static byte[] getMaskedKey(KeyGenerator generator, int[] maskedKeyRanges, List<Integer> ranges,
            int[] byteIndexs, long[] key) throws KeyGenException
    {
        byte[] mdKey = generator.generateKey(key);
        
        byte[] maskedKey = new byte[byteIndexs.length];
        
        //For better performance.
//         System.arraycopy(mdKey, 0, maskedKey, 0, byteIndexs.length);
      //CHECKSTYLE:OFF    Approval No:Approval-284
         for(int i = 0;i < byteIndexs.length;i++)
         {
             maskedKey[i] = mdKey[byteIndexs[i]];
         }
       //CHECKSTYLE:ON
        for(int i = 0;i < byteIndexs.length;i++)
        {
            for(int k = 0;k < maskedKeyRanges.length;k++)
            {
                if(byteIndexs[i] == maskedKeyRanges[k])
                {
                    ranges.add(k);
                    break;
                }
            }
        }
        
        return maskedKey;
    }
    
 
    /**
     * getMaskedBytesForRollUp.
     * @param dims
     * @param generator
     * @param maskedKeyRanges
     * @param ranges
     * @return
     * @throws KeyGenException
     */
    public static byte[] getMaskedBytesForRollUp(int[] dims,KeyGenerator generator,int[] maskedKeyRanges,List<Integer> ranges) throws KeyGenException
    {
        Set<Integer> integers = new TreeSet<Integer>();
        
        //
        for(int i = 0;i < dims.length;i++)
        {

            int[] range = generator.getKeyByteOffsets(dims[i]);
            for(int j = range[0];j <= range[1];j++)
            {
                integers.add(j);
            }
        }
        //
        int[] byteIndexs = new int[integers.size()];
        int j = 0;
        for(Iterator<Integer> iterator = integers.iterator();iterator.hasNext();)
        {
            Integer integer = (Integer)iterator.next();
            byteIndexs[j++] = integer.intValue();
        }
        
        long[] key = new long[generator.getDimCount()];
        for(int i = 0;i < dims.length;i++)
        {
            key[dims[i]] = Long.MAX_VALUE;
        }
        
        return getMaskedKey(generator, maskedKeyRanges, ranges, byteIndexs, key);
    }
    
    /**
     * This method will return the ranges for the masked Bytes 
     * based on the key Generator.
     * 
     * @param queryDimensions
     * @param generator
     * @return
     *
     */
    public static int[] getRangesForMaskedByte(int[] queryDimensions, KeyGenerator generator)
    {

        return getRanges(queryDimensions, generator);
    }

    //TODO SIMIAN
    /**
     * @param queryDimensions
     * @param generator
     * @return
     */
    private static int[] getRanges(int[] queryDimensions, KeyGenerator generator)
    {
        Set<Integer> integers = new TreeSet<Integer>();
        //
        for(int i = 0;i < queryDimensions.length;i++)
        {

            int[] range = generator.getKeyByteOffsets(queryDimensions[i]);
            for(int j = range[0];j <= range[1];j++)
            {
                integers.add(j);
            }

        }
        //
        int[] byteIndexs = new int[integers.size()];
        int j = 0;
        for(Iterator<Integer> iterator = integers.iterator();iterator.hasNext();)
        {
            Integer integer = (Integer)iterator.next();
            byteIndexs[j++] = integer.intValue();
        }

        return byteIndexs;
    }
    
    
    /**
     * Converts int[] to List
     * @param queryDimOrdinals
     * @return
     */
    public static List<Integer> tolist(int[] queryDimOrdinals)
    {
        List<Integer> list = new ArrayList<Integer>(queryDimOrdinals.length);

        for(int i = 0;i < queryDimOrdinals.length;i++)
        {
            list.add(queryDimOrdinals[i]);
        }
        return list;
    }
    
    /**
     * Converts int list to int[]
     * @param integers
     * @return
     */
    public static int[] convertIntegerListToIntArray(Collection<Integer> integers)
    {
        int[] ret = new int[integers.size()];
        Iterator<Integer> iterator = integers.iterator();
        for (int i = 0; i < ret.length; i++)
        {//CHECKSTYLE:OFF    Approval No:Approval-284
            ret[i] = iterator.next().intValue();
        }//CHECKSTYLE:ON
        return ret;
    }
    
    /**
     * getMaskedByte
     * @param queryDimensions
     * @param generator
     * @return
     */
    public static int[] getMaskedByte(Dimension[] queryDimensions, KeyGenerator generator)
    {

        Set<Integer> integers = new TreeSet<Integer>();
        //
        for(int i = 0;i < queryDimensions.length;i++)
        {

            
			if(queryDimensions[i].isHighCardinalityDim())
            {
                continue;
            }
			else if(queryDimensions[i].getDataType() == SqlStatement.Type.ARRAY)
            {
                continue;
            }
            else if(queryDimensions[i].getDataType() == SqlStatement.Type.STRUCT)
                continue;
            else if(queryDimensions[i].getParentName() != null)
                continue;
            else
            {
                int[] range = generator.getKeyByteOffsets(queryDimensions[i].getOrdinal());
                for(int j = range[0];j <= range[1];j++)
                {
                    integers.add(j);
                }
            }

        }
        //
        int[] byteIndexs = new int[integers.size()];
        int j = 0;
        for(Iterator<Integer> iterator = integers.iterator();iterator.hasNext();)
        {
            Integer integer = (Integer)iterator.next();
            byteIndexs[j++] = integer.intValue();
        }

        return byteIndexs;
    }
    
    public static int[] getMaskedByte(Dimension[] queryDimensions, KeyGenerator generator,HybridStoreModel hybridStoreModel)
    {

        Set<Integer> integers = new TreeSet<Integer>();
        boolean isRowAdded=false;
        //
        for(int i = 0;i < queryDimensions.length;i++)
        {
            if(queryDimensions[i].isHighCardinalityDim())
            {
                continue;
            }
         	else if(queryDimensions[i].getDataType() == SqlStatement.Type.ARRAY)
            {
                continue;
            }
            else if(queryDimensions[i].getDataType() == SqlStatement.Type.STRUCT)
                continue;
            else if(queryDimensions[i].getParentName() != null)
                continue;
         
            //if querydimension is row store based, than add all row store ordinal in mask, because row store ordinal rangesare overalapped
            //for e.g its possible
            //dimension1 range: 0-1
            //dimension2 range: 1-2
            //hence to read only dimension2, you have to mask dimension1 also
            if(!queryDimensions[i].isColumnar())
            {
                //if all row store ordinal is already added in range than no need to consider it again
                if(!isRowAdded)
                {
                    isRowAdded=true;
                    int[] rowOrdinals=hybridStoreModel.getRowStoreOrdinals();
                    for(int r=0;r<rowOrdinals.length;r++)
                    {
                        int[] range = generator
                                .getKeyByteOffsets(hybridStoreModel.getMdKeyOrdinal(rowOrdinals[r]));
                        for(int j = range[0];j <= range[1];j++)
                        {
                            integers.add(j);
                        }
                        
                    }
                }
                continue;
                
            }
            int[] range = generator
                    .getKeyByteOffsets(hybridStoreModel.getMdKeyOrdinal(queryDimensions[i].getOrdinal()));
            for(int j = range[0];j <= range[1];j++)
            {
                integers.add(j);
            }

        }
        //
        int[] byteIndexs = convertIntegerListToIntArray(integers);
        return byteIndexs;
    }
    
    
    
    
    
    /**
     * getMaskedByte
     * @param queryDimensions
     * @param generator
     * @return
     */
    public static int[] getMaskedByte(int[] queryDimensions, KeyGenerator generator)
    {

        return getRanges(queryDimensions, generator);
    }
    
    /**
     * updateMaskedKeyRanges
     * @param maskedKey
     * @param maskedKeyRanges
     */
    public static void updateMaskedKeyRanges(int[] maskedKey, int[] maskedKeyRanges)
    {
        Arrays.fill(maskedKey, -1);
        for(int i = 0;i < maskedKeyRanges.length;i++)
        {
            maskedKey[maskedKeyRanges[i]] = i;
        }
    }

    /**
     * 
     * @param segmentOrdinals
     * @param dimensions
     * @return
     */
    public static boolean isSubsetDimensionLevels(int[] segmentOrdinals, Dimension[] dimensions)
    {
        int [] queryOrdinals = new int[dimensions.length];
        for(int i = 0;i < queryOrdinals.length;i++)
        {
            queryOrdinals[i] = dimensions[i].getOrdinal();
        }
        
//        Arrays.sort(queryOrdinals);
        
        for(int i = 0;i < queryOrdinals.length;i++)
        {
            if(Arrays.binarySearch(segmentOrdinals, queryOrdinals[i]) < 0)
            {
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * 
     * @param segmentOrdinals
     * @param dimensions
     * @return
     */
    public static boolean isSameDimensionLevels(int[] segmentOrdinals, Dimension[] dimensions)
    {
        int [] queryOrdinals = new int[dimensions.length];
        for(int i = 0;i < queryOrdinals.length;i++)
        {
            queryOrdinals[i] = dimensions[i].getOrdinal();
        }
        
//        Arrays.sort(queryOrdinals);
        Arrays.sort(queryOrdinals);
        Arrays.sort(segmentOrdinals);
        
        return Arrays.equals(queryOrdinals, segmentOrdinals);
    }
    
    public static boolean isSameDimensionLevels(Dimension[] otherDims, Dimension[] dimensions)
    {
        int [] queryOrdinals = new int[dimensions.length];
        for(int i = 0;i < queryOrdinals.length;i++)
        {
            queryOrdinals[i] = dimensions[i].getOrdinal();
        }
        
        int [] segmentOrdinals = new int[otherDims.length];
        for(int i = 0;i < segmentOrdinals.length;i++)
        {
            segmentOrdinals[i] = otherDims[i].getOrdinal();
        }
        
//        Arrays.sort(queryOrdinals);
        Arrays.sort(queryOrdinals);
        Arrays.sort(segmentOrdinals);
        
        return Arrays.equals(queryOrdinals, segmentOrdinals);
    }
    
   /* public static int [] removeHighCardinalityDimOrdinal(int[] queryDimOrdinal, Dimension[] queryDimensions)
    {
        List<Integer> listOfOrdinal=new ArrayList<Integer>(queryDimensions.length);
        int[] result = null;
        for(int ordinal:queryDimOrdinal)
        {
            for(Dimension queryDimension:queryDimensions)
            {
                if(queryDimension.getOrdinal()==ordinal && !queryDimension.isHighCardinalityDim())
                {
                    listOfOrdinal.add(ordinal);
                    //break;
                }
            }
            
        }
        Integer[] arraysOfOrdinal=listOfOrdinal.toArray(new Integer [listOfOrdinal.size()]);
        result = new int[arraysOfOrdinal.length];
        for (int i = 0; i < arraysOfOrdinal.length; i++) {
            result[i] = arraysOfOrdinal[i].intValue();
        }
        return result;
    }*/
    
}
