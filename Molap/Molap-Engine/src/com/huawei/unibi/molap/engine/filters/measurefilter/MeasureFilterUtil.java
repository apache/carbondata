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
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbweRARwUrjYxPx0CUk3mVB7mxOcZSaagKrMQNlhB
QO/t7G8sJvs0DEOxya/OlPGazf0uhOxVhOPgCM97GtKsBzIAzhyT9OivrTvmZ2zXfzqaRgNO
Z4JA2lzDOWRNUHF2158nZVHaJvGqvV1iquyuXu2xCIsn6eAlPmn4+hiGmq2dTQ==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.filters.measurefilter;

import java.util.ArrayList;
import java.util.List;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.engine.filters.measurefilter.util.MeasureFilterFactory;
import com.huawei.unibi.molap.metadata.MolapMetadata.Measure;

/**
 * MeasureFilterUtil class
 * 
 * @author R00900208
 *
 */
public final class MeasureFilterUtil
{
    private MeasureFilterUtil()
    {
        
    }
    
    /**
     * filterMeasures
     * @param result
     * @param msrConstaints
     * @param msrStartIndex
     * @return
     */
    public static double[][] filterMeasures(double[][] result, GroupMeasureFilterModel[] msrConstaints, int msrStartIndex,List<Measure> measures)
    {

        MeasureFilter[] measureFilters = MeasureFilterFactory.getFilterMeasures(msrConstaints,measures);
        
        if(!isMsrFilterEnabled(measureFilters))
        {
            return result;
        }
        List<double[]> tempResult = new ArrayList<double[]>(result.length);

        LOOP: for(int i = 0;i < result.length;i++)
        {

                for(int k = 0;k < measureFilters.length;k++)
                {
                    if(!(measureFilters[k].filter(result[i],msrStartIndex)))
                    {
                        continue LOOP;
                    }
                }
            tempResult.add(result[i]);
        }

        return tempResult.toArray(new double[tempResult.size()][]);
    }

//    /**
//     * Create the measure filters
//     * @param msrConstaints
//     * @return
//     */
//    public static MeasureFilter[][] getMsrFilters(MeasureFilterModel[][] msrConstaints)
//    {
//        MeasureFilter[][] measureFilters = new MeasureFilter[msrConstaints.length][];
//        for(int i = 0;i < measureFilters.length;i++)
//        {
//            if(msrConstaints[i] != null)
//            {
//                measureFilters[i] = MeasureFilterFactory.getMeasureFilter(msrConstaints[i]);
//            }
//        }
//        return measureFilters;
//    }
    
    /**
     * calculateAvgsAndUpdateData
     * @param result
     * @param avgIndexes
     * @param countMsrIndex
     * @param msrCount
     * @param dimCount
     */
    public static void calculateAvgsAndUpdateData(double[][] result,List<Integer> avgIndexes,int countMsrIndex,int msrCount, int dimCount)
    {
        if(avgIndexes.size() == 0 || countMsrIndex < 0)
        {
            return;
        }
        
        for(int k = 0;k < result.length;k++)
        {
            
            for(int i = 0;i < msrCount;i++)
            {
                if(avgIndexes.contains(i))
                {
                    if(result[k][dimCount + countMsrIndex]==0)
                    {
                        result[k][dimCount + i]=0;
                    }
                    else
                    {
                        result[k][dimCount + i] = result[k][dimCount + i]
                                / result[k][dimCount + countMsrIndex];
                    }
                }
            }
        }
        
        
    }
    
    /**
     * getMsrFilterIndexes
     * @param measureFilters
     * @return
     */
    public static int[] getMsrFilterIndexes(MeasureFilter[][] measureFilters)
    {
        
        List<Integer> msrInexes = new ArrayList<Integer>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        
        for(int i = 0;i < measureFilters.length;i++)
        {
            if(measureFilters[i] != null)
            {
                msrInexes.add(i);
            }
        }
        
        return convertListToArray(msrInexes);
        
    }
    
    /**
     * Checks whether measure filter is enabled or not.
     * 
     * @param measureFilters
     * @return, true if measure filter is enabled, false otherwise.
     *
     */
    public static boolean isMsrFilterEnabled(MeasureFilter[] measureFilters)
    {
        if(measureFilters == null)
        {
            return false;
        }
        for(int i = 0;i < measureFilters.length;i++)
        {
            if(measureFilters[i] instanceof MeasureGroupFilter)
            {
                if(((MeasureGroupFilter)measureFilters[i]).isMsrFilterEnabled())
                {
                    return true;
                }
            }
            
        }
        
        return false;
        
    }
    
    /**
     * 
     * @param msrInexes
     * @return
     */
    public static int[] convertListToArray(List<Integer> msrInexes)
    {
        int[] indxes = new int[msrInexes.size()];
        
        int i = 0;
        for(Integer integer : msrInexes)
        {
            indxes[i++] = integer;
        }
        return indxes;
    }
    
}
