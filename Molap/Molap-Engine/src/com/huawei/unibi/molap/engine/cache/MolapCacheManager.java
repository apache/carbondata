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
GSGxBnLAgIPvXIY0b2yWiu8f8JPgEAXQm2P19li3452YPtAs5XASXQXeTTt1h0A/Qo0YXphi
3DbJY2uW0VM2ee+W3GBJ0oS7Zh+0TWgdZXf0vnh3ZOiq6C3aBndeb8Gc9vwVSQ==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
package com.huawei.unibi.molap.engine.cache;


/**
 * @author R00900208
 * 
 */
public final class MolapCacheManager
{
//
//    /**
//     * cacheManager
//     */
//    private static MolapCacheManager cacheManager;
//
//    /**
//     * if file based cache has to be used at all
//     * {@link MolapCommonConstants.MOLAP_CACHE_USED}
//     */
//    private static boolean cacheToBeUsed = getCacheProperty();
//
//    /**
//     * segmentCache
//     */
//    private FileBasedSegmentCache segmentCache;
//    
//    /**
//     * inMemoryCache
//     */
//    private boolean inMemoryCache;
//    
//    private InMemoryLRUCache inMemoryLRUCache;
//
//    /**
//     * 
//     */
//    private static final LogService LOGGER = LogServiceFactory.getLogService(MolapCacheManager.class.getName());
//
//    private MolapCacheManager()
//    {
//        segmentCache = new FileBasedSegmentCache();
//        inMemoryCache =  Boolean.parseBoolean(MolapProperties.getInstance().getProperty("molap.inmemory.cache.use",
//                Boolean.FALSE.toString()));
//        if(inMemoryCache)
//        {
//            inMemoryLRUCache = InMemoryLRUCache.getInstance();
//        }
//    }
//
//    private MolapCacheManager(String string)
//    {
//
//    }
//
//    /**
//     * Get the Cache property
//     * 
//     * @return boolean
//     */
//    private static boolean getCacheProperty()
//    {
//        String value = MolapProperties.getInstance().getProperty(MolapCommonConstants.MOLAP_CACHE_USED,
//                Boolean.TRUE.toString());
//        if(value != null && value.equalsIgnoreCase(Boolean.TRUE.toString()))
//        {
//            return true;
//        }
//        return false;
//    }
//
//    /**
//     * Get the singleton instance of cache manager
//     * 
//     * @return MolapCacheManager
//     */
//    public static MolapCacheManager getInstance()
//    {
//        if(!cacheToBeUsed)
//        {
//            return new MolapCacheManager("dummy");
//        }
//        if(cacheManager == null)
//        {
//            cacheManager = new MolapCacheManager();
//        }
//        return cacheManager;
//    }
//
//    /**
//     * Create Segment header
//     * 
//     * @param cubeUniqueName
//     * @param factTable
//     * @param queryDims
//     * @param constraints
//     * @return
//     */
//    public MolapSegmentHeader createMolapSegmentHeader(String cubeUniqueName, String factTable, Dimension[] queryDims,
//            Map<Dimension, MolapPredicates> constraints, Dimension[] dimTables, boolean isPaginationEnabled)
//    {
//        if(!cacheToBeUsed && !isPaginationEnabled)
//        {
//            return null;
//        }
//        MolapSegmentHeader header = new MolapSegmentHeader(cubeUniqueName, factTable);
//        // header.setCubeName(cubeUniqueName);
//        // header.setFactTableName(factTable);
//
//        int[] dims = new int[queryDims.length];
//        for(int i = 0;i < dims.length;i++)
//        {
//            dims[i] = queryDims[i].getOrdinal();
//        }
//        Arrays.sort(dims);
//
//        header.setDims(dims);
//
//        List<MolapPredicates> predList = new ArrayList<MolapPredicates>();
//
//        for(Dimension dim : dimTables)
//        {
//            for(Entry<Dimension, MolapPredicates> entry : constraints.entrySet())
//            {
//                Dimension key = entry.getKey();
//                if(key.getDimName().equals(dim.getDimName()) && key.getHierName().equals(dim.getHierName())
//                        && key.getName().equals(dim.getName()))
//                {
//                    predList.add(entry.getValue());
//                }
//            }
//        }
//
//        header.setPreds(predList);
//
//        return header;
//    }
//
//    /**
//     * @param queryDims
//     * @param sortOrder
//     * @param sortedDims
//     */
//    public byte[] getUpdatedSortOrder(Dimension[] queryDims, byte[] sortOrder, int[] sortedDims)
//    {
//        if(queryDims.length == sortOrder.length)
//        {
//            byte[] updatedSortOrder = new byte[sortOrder.length];
//
//            for(int i = 0;i < sortedDims.length;i++)
//            {
//                for(int j = 0;j < queryDims.length;j++)
//                {
//                    if(queryDims[j].getOrdinal() == sortedDims[i])
//                    {
//                        updatedSortOrder[i] = sortOrder[j];
//                        break;
//                    }
//                }
//            }
//            return updatedSortOrder;
//        }
//        return sortOrder;
//    }
//
//    /**
//     * Get segment fro cache
//     * 
//     * @param header
//     * @return MolapSegmentBody if found or null
//     */
//    public MolapSegmentBody getSegmentBodyFromCache(MolapSegmentHeader header)
//    {
//        if(!cacheToBeUsed)
//        {
//            return null;
//        }
//        MolapSegmentBody body = null;
//        try
//        {
//            if(inMemoryCache)
//            {
//                body = inMemoryLRUCache.get(header);
//                if(body != null)
//                {
//                    body = body.getCopy();
//                }
//            }
//            if(body == null)
//            {
//                body = segmentCache.get(header).get();
//            }
//        }
//        catch(Exception e)
//        {
//            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Ignore it", e);
//        }
//
//        return body;
//    }
//
//    // public List<MolapSegmentHeader> getAllHeaders()
//    // {
//    // try
//    // {
//    // return segmentCache.getSegmentHeaders().get();
//    // }
//    // catch(Exception e)
//    // {
//    //
//    // }
//    // return null;
//    // }
//
//    /**
//     * Roll up the data and return the rolled up data.
//     * 
//     * @param header
//     * @param predMisses
//     *            The predicates which are missed in this rollup. so we need to
//     *            query them.
//     * @return
//     * @return
//     */
//    // public MolapSegmentBody rollUp(MolapSegmentHeader
//    // header,List<MolapPredicates> predMisses,KeyGenerator keyGenerator)
//    // {
//    // if(!cacheToBeUsed)
//    // {
//    // return null;
//    // }
//    //
//    // try
//    // {
//    // Set<MolapSegmentHeader> allHeaders =
//    // segmentCache.getSegmentHeaders(header.getCubeName()).get();
//    // List<MolapSegmentHeader> foundHeads = new
//    // ArrayList<MolapSegmentHeader>();
//    // for(MolapSegmentHeader iterHead : allHeaders)
//    // {
//    // if(header.equalsWithOutPreds(iterHead) && equalsPreds(header.getPreds(),
//    // iterHead.getPreds()))
//    // {
//    // foundHeads.add(iterHead);
//    // }
//    // }
//    //
//    // List<MolapPredicates> filterInfo = header.getPreds();
//    // if(foundHeads.size() > 0)
//    // {
//    // MolapSegmentBody body = new MolapSegmentBody(new
//    // HashMap<ByteArrayWrapper, MeasureAggregator[]>(), null);
//    // for(MolapSegmentHeader foundHead : foundHeads)
//    // {
//    // List<MolapPredicates> preds = foundHead.getPreds();
//    //
//    // }
//    //
//    // }
//    //
//    // }
//    // catch(Exception e)
//    // {
//    // LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,"Ignore it", e);
//    // }
//    //
//    //
//    // return null;
//    //
//    // }
//
//    // private boolean equalsPreds(List<MolapPredicates> leftPreds,
//    // List<MolapPredicates> rightPreds)
//    // {
//    // if(leftPreds.size() != rightPreds.size())
//    // {
//    // return false;
//    // }
//    //
//    // for(MolapPredicates leftPred : leftPreds)
//    // {
//    // boolean exist = false;
//    // for(MolapPredicates rightPred : rightPreds)
//    // {
//    // if(leftPred.getOrdinal() == rightPred.getOrdinal())
//    // {
//    // exist = true;
//    // }
//    // }
//    //
//    // if(!exist)
//    // {
//    // return false;
//    // }
//    // }
//    // return true;
//    // }
//    /**
//     * Aggregate the data when the asked dimensions are subset of any of the
//     * cache data dimensions. Like if asked dimensions are d1,d2 and cache data
//     * dimensions are d1,d2,d3 then it aggregates get the data out as d1,d2
//     * 
//     * @param header
//     * @param keyGenerator
//     * @param msrs
//     * @return
//     */
//    public Pair<MolapSegmentBody, Map<ByteArrayWrapper, MeasureAggregator[]>> aggregate(MolapSegmentHeader header,
//            KeyGenerator keyGenerator, List<Measure> msrs)
//    {
//        if(!cacheToBeUsed)
//        {
//            return null;
//        }
//        
//        boolean parseBoolean = Boolean.parseBoolean(MolapProperties.getInstance().getProperty("molap.member.rollup.enabled", "true"));
//        if(!parseBoolean)
//        {
//            return null;
//        }
//
//        try
//        {
//            Set<MolapSegmentHeader> allHeaders = segmentCache.getSegmentHeaders(header.getCubeName()).get();
//            if(allHeaders == null || allHeaders.size() == 0)
//            {
//                return null;
//            }
//            List<MolapSegmentHeader> foundHeads = new ArrayList<MolapSegmentHeader>();
//          //CHECKSTYLE:OFF    Approval No:V3R8C00_012
//            for(MolapSegmentHeader iterHead : allHeaders)
//            {
//                if(iterHead.equalsForSubSet(header))
//                {
//                    foundHeads.add(iterHead);
//                }
//            }
//
//            MolapSegmentHeader finalHeader = null;
//            MolapSegmentBody molapSegmentBody = null;
//            long size = Long.MAX_VALUE;
//            if(foundHeads.size() > 0)
//            {
//                for(MolapSegmentHeader foundHead : foundHeads)
//                {
//                    molapSegmentBody = segmentCache.getWithOutData(foundHead).get();
//                    
//                    if(isAllMeasuresPresent(msrs, molapSegmentBody.getMeasures()))
//                    {
//                        if(size > molapSegmentBody.getSize())
//                        {
//                            size = molapSegmentBody.getSize();
//                            finalHeader = foundHead;
//                        }
//                    }
//                }
//                if(finalHeader != null)
//                {
//                    MolapSegmentBody body = null;
//                    if(inMemoryCache)
//                    {
//                        body = inMemoryLRUCache.get(finalHeader);
//                        if(body != null)
//                        {
//                            body = body.getCopy();
//                        }
//                    }
//                    if(body == null)
//                    {
//                        body = segmentCache.get(finalHeader).get();
//                    }
//                    molapSegmentBody = body;
//                }
//
//            }
//            /**
//             * Fortify fix: NULL_DEREFRENCE
//             */
//            if(molapSegmentBody != null && null != finalHeader)
//            {
//                List<Integer> bytePos = new ArrayList<Integer>();
//                byte[] maskedBytesForRollUp = QueryExecutorUtil.getMaskedBytesForRollUp(header.getDims(), keyGenerator,
//                        QueryExecutorUtil.getRangesForMaskedByte(finalHeader.getDims(), keyGenerator), bytePos);
//                int[] bytePosArray = QueryExecutorUtil.convertIntegerListToIntArray(bytePos);
//                LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Result rolling out after member rollup");
//                return new Pair<MolapSegmentBody, Map<ByteArrayWrapper, MeasureAggregator[]>>(molapSegmentBody,
//                        PostQueryAggregatorUtil.aggregateData(molapSegmentBody, keyGenerator, msrs,
//                                maskedBytesForRollUp, bytePosArray));
//            }
//
//          //CHECKSTYLE:ON
//        }
//        catch(Exception e)
//        {
//            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Ignore it", e);
//        }
//        return null;
//
//    }
//
//    /**
//     * Aggregate the data when the asked dimensions are same and filters are
//     * subset of any of the cache data dimensions filters.
//     * 
//     * @param header
//     * @param keyGenerator
//     * @param msrs
//     * @return
//     */
//    public Pair<MolapSegmentBody, Map<ByteArrayWrapper, MeasureAggregator[]>> aggregate(MolapSegmentHeader header,
//            KeyGenerator keyGenerator, List<Measure> msrs, InMemFilterModel filterModel, Dimension[] allDims,
//            List<InMemoryCube> slices, boolean isSegmentCall)
//    {
//        if(!cacheToBeUsed)
//        {
//            return null;
//        }
//        
//        boolean parseBoolean = Boolean.parseBoolean(MolapProperties.getInstance().getProperty("molap.filter.rollup.enabled", "true"));
//        if(!parseBoolean)
//        {
//            return null;
//        }
//
//        try
//        {
//            int[] dims = header.getDims();
//            List<MolapPredicates> preds = header.getPreds();
//            if(!isSegmentCall && ( preds == null || !header.isAllPredPresentOnDims(dims, preds) && dims != null))
//            {
//                return null;
//            }
//            Set<MolapSegmentHeader> allHeaders = segmentCache.getSegmentHeaders(header.getCubeName()).get();
//            if(allHeaders == null || allHeaders.size() == 0)
//            {
//                return null;
//            }
//            List<MolapSegmentHeader> foundHeads = new ArrayList<MolapSegmentHeader>();
//          //CHECKSTYLE:OFF    Approval No:V3R8C00_012
//            getHeader(header, isSegmentCall, allHeaders, foundHeads);
//
//            MolapSegmentHeader finalHeader = null;
//            MolapSegmentBody molapSegmentBody = null;
//            long size = Long.MAX_VALUE;
//            if(foundHeads.size() > 0)
//            {
//                for(MolapSegmentHeader foundHead : foundHeads)
//                {
//                    molapSegmentBody = segmentCache.getWithOutData(foundHead).get();
//                    if(isAllMeasuresPresent(msrs, molapSegmentBody.getMeasures()))
//                    {
//                        if(size > molapSegmentBody.getSize())
//                        {
//                            size = molapSegmentBody.getSize();
//                            finalHeader = foundHead;
//                        }
//                    }
//                }
//                if(finalHeader != null)
//                {
//                    MolapSegmentBody body = null;
//                    if(inMemoryCache)
//                    {
//                        body = inMemoryLRUCache.get(finalHeader);
//                        if(body != null)
//                        {
//                            body = body.getCopy();
//                        }
//                    }
//                    if(body == null)
//                    {
//                        body = segmentCache.get(finalHeader).get();
//                    }
//                    molapSegmentBody = body;
//                }
//
//            }
//
//            /**
//             * Fortify Fix: FORWARD_NULL
//             */
//            if(molapSegmentBody != null && null != finalHeader)
//            {
//                List<Integer> bytePos = new ArrayList<Integer>();
//                byte[] maskedBytesForRollUp = null;
//                int[] bytePosArray = null;
//                int[] maskedBytePos = new int[keyGenerator.getKeySizeInBytes()];
//                QueryExecutorUtil.updateMaskedKeyRanges(maskedBytePos,
//                        QueryExecutorUtil.getMaskedByte(finalHeader.getDims(), keyGenerator));
//                // if(dims.length > 0)
//                // {
//                maskedBytesForRollUp = QueryExecutorUtil.getMaskedBytesForRollUp(dims, keyGenerator,
//                        QueryExecutorUtil.getRangesForMaskedByte(finalHeader.getDims(), keyGenerator), bytePos);
//                bytePosArray = QueryExecutorUtil.convertIntegerListToIntArray(bytePos);
//                
//                LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Result rolling out after filter rollup");
//                // }
//                return new Pair<MolapSegmentBody, Map<ByteArrayWrapper, MeasureAggregator[]>>(molapSegmentBody,
//                        PostQueryAggregatorUtil.aggregateData(molapSegmentBody, keyGenerator, msrs,
//                                maskedBytesForRollUp, bytePosArray, filterModel, maskedBytePos,
//                                getHeaderDims(finalHeader.getDims(), allDims), slices));
//            }
//
//        }
//      //CHECKSTYLE:ON
//        catch(Exception e)
//        {
//            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Ignore it", e);
//        }
//        return null;
//
//    }
//
//    private void getHeader(MolapSegmentHeader header, boolean isSegmentCall, Set<MolapSegmentHeader> allHeaders,
//            List<MolapSegmentHeader> foundHeads)
//    {
//        //CHECKSTYLE:OFF    Approval No:V3R8C00_012
//        if(!isSegmentCall)
//        {
//            for(MolapSegmentHeader iterHead : allHeaders)
//            {
//                if(header.equalsForSubSetOfPreds(iterHead))
//                {
//                    foundHeads.add(iterHead);
//                }
//            }
//        }
//        else
//        {
//            for(MolapSegmentHeader iterHead : allHeaders)
//            {
//                if(header.equalsForSubSetOfPredsForSegmentQuery(iterHead))
//                {
//                    foundHeads.add(iterHead);
//                }
//            }
//        }//CHECKSTYLE:ON
//    }
//    
//    
//    private boolean isAllMeasuresPresent(List<Measure> msrs,String[] measures)
//    {
//        
//        for(Measure measure : msrs)
//        {
//            boolean found = false;
//            for(String msrName : measures)
//            {
//                if(measure.getName().equals(msrName))
//                {
//                    found = true;
//                    break;
//                }
//            }
//            if(!found)
//            {
//                return false;
//            }
//        }
//        return true;
//        
//    }
//
//    private Dimension[] getHeaderDims(int[] dimOrdinals, Dimension[] allDims)
//    {
//        Dimension[] dimensions = new Dimension[dimOrdinals.length];
//        for(int i = 0;i < dimensions.length;i++)
//        {
//            for(int j = 0;j < allDims.length;j++)
//            {
//                if(allDims[j].getOrdinal() == dimOrdinals[i])
//                {
//                    dimensions[i] = allDims[j];
//                    break;
//                }
//            }
//        }
//        return dimensions;
//    }
//
//    /**
//     * Store the result data to file map
//     * 
//     * @param header
//     * @param measures
//     * @param data
//     */
//    public void storeData(MolapSegmentHeader header, List<Measure> measures,
//            Map<ByteArrayWrapper, MeasureAggregator[]> data, int[] maskKey, byte[] maxKey,
//            CalculatedMeasure[] calculatedMeasures, int keySize, byte[] updatedSortOrder,KeyGenerator generator)
//    {
//        if(!cacheToBeUsed)
//        {
//            return;
//        }
//        String[] msrNames = new String[measures.size()];
//        for(int i = 0;i < msrNames.length;i++)
//        {
//            msrNames[i] = measures.get(i).getName();
//        }
//
//        String[] aggNames = new String[measures.size()];
//        for(int i = 0;i < aggNames.length;i++)
//        {
//            aggNames[i] = measures.get(i).getAggName();
//        }
//
//        String[] calMsrNames = new String[calculatedMeasures.length];
//        for(int i = 0;i < calMsrNames.length;i++)
//        {
//            calMsrNames[i] = calculatedMeasures[i].getName();
//        }
//
//        MolapSegmentBody body = new MolapSegmentBody((Serializable)data, msrNames);
//        body.setMaskedKeyRanges(maskKey);
//        body.setMaxKey(maxKey);
//        body.setCalcMsrs(calMsrNames);
//        body.setKeySize(keySize);
//        body.setAggNames(aggNames);
//        body.setSortOrder(updatedSortOrder);
//        Boolean hasSegmentAlready = false;
//        try
//        {
//            hasSegmentAlready = segmentCache.contains(header).get();
//            if(hasSegmentAlready)
//            {
//                body.merge(segmentCache.get(header).get());
//            }
//        }
//        catch(Exception e)
//        {
//            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Ignore it", e);
//        }
//        body.setSize(((Map)body.getData()).size());
//        body.setGenerator(generator);
//        if(inMemoryCache)
//        {
//            inMemoryLRUCache.put(header, body.getCopy());
//        }
//        segmentCache.put(header, body);
//    }
//
//    /**
//     * Flush All cubes cache
//     * 
//     * @return boolean
//     */
//    public boolean flushAllCubes()
//    {
//        if(!cacheToBeUsed)
//        {
//            return true;
//        }
//        try
//        {
//            if(inMemoryCache)
//            {
//                inMemoryLRUCache.clear();
//            }
//            return segmentCache.flush(null).get();
//        }
//        catch(Exception e)
//        {
//            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Ignore it", e);
//        }
//
//        return false;
//    }
//
//    /**
//     * Flush the passed cube cache
//     * 
//     * @param schemaName
//     * @param cubeName
//     * @return
//     */
//    public boolean flushCube(String schemaName, String cubeName)
//    {
//        if(!cacheToBeUsed)
//        {
//            return true;
//        }
//        try
//        {
//            if(inMemoryCache)
//            {
//                inMemoryLRUCache.clear();
//            }
//            return segmentCache.flushCube(schemaName, cubeName).get();
//        }
//        catch(Exception e)
//        {
//            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Ignore it", e);
//        }
//        return false;
//    }
//
//    /**
//     * Flush the cache starting with passed key
//     * 
//     * @param cubeUniqueName
//     * @param startKey
//     * @param keyGen
//     * @param tableName
//     * @return
//     */
//    public boolean flushCubeStartingWithKey(String cubeUniqueName, byte[] startKey, KeyGenerator keyGen,
//            String tableName)
//    {
//        if(!cacheToBeUsed)
//        {
//            return true;
//        }
//        try
//        {
//            if(inMemoryCache)
//            {
//                inMemoryLRUCache.clear();
//            }
//            return segmentCache.flushCubeStartingWithKey(cubeUniqueName, startKey, keyGen, tableName).get();
//        }
//        catch(Exception e)
//        {
//            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Ignore it", e);
//        }
//        return false;
//    }

}
