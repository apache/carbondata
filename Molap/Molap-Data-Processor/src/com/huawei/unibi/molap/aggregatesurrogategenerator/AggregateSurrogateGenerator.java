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

/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2014
 * =====================================
 *
 */
package com.huawei.unibi.molap.aggregatesurrogategenerator;

import java.util.List;
//import java.util.Arrays;




/**
 * Project Name NSE V3R8C10 
 * Module Name : MOLAP Data Processor
 * Author :k00900841 
 * Created Date:10-Aug-2014
 * FileName : AggregateSurrogateGenerator.java
 * Class Description : This class is generating surrogate keys for aggregate tables
 * Class Version 1.0
 */
public class AggregateSurrogateGenerator
{
	/**
     * LOGGER
     */
//    private static final LogService LOGGER = LogServiceFactory
//            .getLogService(MolapAggregateSurrogateGeneratorStep.class.getName());
    /**
     * measureIndex
     */
    private int[] measureIndex;

    /**
     * isMdkeyInOutRowRequired
     */
    private boolean isMdkeyInOutRowRequired;
    
//    private byte[] maxKey;
//    
//    private int [] maskedByteRanges;
//    
//    private KeyGenerator keyGenerator;
    

    /**
     * AggregateSurrogateGenerator constructor
     * @param factReaderIterator
     * @param factLevels
     * @param aggreateLevels
     * @param factMeasures
     * @param aggregateMeasures
     * @param dimLens
     */
    public AggregateSurrogateGenerator(
            String[] factLevels, String[] aggreateLevels,
            String[] factMeasures, String[] aggregateMeasures,boolean isMdkeyInOutRowRequired, int[] aggDimensioncardinality)
    {
//    	int[] surrogateIndex = new int[aggreateLevels.length];
//        Arrays.fill(surrogateIndex, -1);
//        for(int i = 0;i < aggreateLevels.length;i++)
//        {
//            for(int j = 0;j < factLevels.length;j++)
//            {
//                if(aggreateLevels[i].equals(factLevels[j]))
//                {
//                    surrogateIndex[i] = j;
//                    break;
//                }
//            }
//        }
//        keyGenerator = KeyGeneratorFactory.getKeyGenerator(aggDimensioncardinality);
//        this.maskedByteRanges=MolapDataProcessorUtil.getMaskedByte(surrogateIndex, keyGenerator);
//        long [] max = new long[factLevels.length];
//        for (int i = 0; i < surrogateIndex.length; i++) 
//        {
//        	max[surrogateIndex[i]]=Long.MAX_VALUE;
//		}
//        try 
//        {
//			this.maxKey=keyGenerator.generateKey(max);
//		} 
//        catch (KeyGenException e) 
//		{
//        	LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "Problem while generating the max key");
//		}
//        measureIndex = new int[aggregateMeasures.length];
        measureIndex = new int[2];
//        Arrays.fill(measureIndex, -1);
//        for(int i = 0;i < aggregateMeasures.length-1;i++)
//        {
//            for(int j = 0;j < factMeasures.length;j++)
//            {
//                if(aggregateMeasures[i].equals(factMeasures[j]))
//                {
//                    measureIndex[i] = j;
//                    break;
//                }
//            }
//        }
//        measureIndex[measureIndex.length-1]=measureIndex[0];
        this.isMdkeyInOutRowRequired=isMdkeyInOutRowRequired;
        
    }
    
    /**
     * Below method will be used to generate the surrogate for aggregate table
     * @param factTuple
     * @return aggregate tuple
     */
    public Object[] generateSurrogate(Object[] factTuple)
    {
        // added 1 for the high card dims
        int size=measureIndex.length
                + 1 +1;
        if(isMdkeyInOutRowRequired)
        {
            size+=1;
        }
        Object[] records = new Object[size];
        int count = 0;
        int i = 0;
        for(;i < measureIndex.length-1;i++)
        {
            records[count++] = factTuple[i];
        }
        records[count++]=factTuple[i++];
        // for high card cols.
        records[count++]=(byte[])factTuple[i++];
        byte[] mdkey= (byte[])factTuple[i++];
        records[count++]=mdkey;
        if(isMdkeyInOutRowRequired)
        {
            records[records.length-1]=mdkey;
        }
        return records;
    }
}
