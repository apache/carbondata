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
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwdEVzw1icjfRowqz2DW4XzUpEhhSzBOwVynEHjc
u0090XF3IsG6wwheovqlq6ZgI9KUkA5nHMVBTd3b+r2VKz/tUI7C5U4YGIEbyxGxoaQv1Xzm
Ms4audO0xEYJm0KAeIirAe58CeEY5drk0NSa+jSzkM3WHaUw3MUyEepgf8Vaeg==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.cache;


/**
 * @author R00900208
 *
 */
public class CustomFileBasedSegmentCacheWorker
{
//    private String path = MolapCommonConstants.MOLAP_CACHE_LOCATION+File.separator+"custom";
//    
//    private CustomFileLRUCache customFileLRUCache;
//    
//    private Map<String,Map<MolapSegmentHeader,String>> cache;
//    
//    private static Byte const_byte = Byte.valueOf((byte)1);
//    
//    private MolapSegmentBodySerializer bodySerializer;
//    
//    private MolapSegmentHeaderSerializer headerSerializer;
//    
//    private int fileWriteBufferSize = Integer.parseInt(MolapProperties.getInstance().getProperty(
//            MolapCommonConstants.MOLAP_SORT_FILE_WRITE_BUFFER_SIZE,
//            MolapCommonConstants.MOLAP_SORT_FILE_WRITE_BUFFER_SIZE_DEFAULT_VALUE));
//    
//    /**
//     * LOGGER
//     */
//    private static final LogService LOGGER = LogServiceFactory.getLogService(CustomFileBasedSegmentCacheWorker.class.getName());
//    
//    public CustomFileBasedSegmentCacheWorker()
//    {
//
//        File cacheFile = new File(path);
//        if(!cacheFile.exists())
//        {
//            boolean mkdirs = cacheFile.mkdirs();
//            if(!mkdirs)
//            {
//                LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Not Able to create Custom cache File.");
//            }
//        }
//        cache = new HashMap<String, Map<MolapSegmentHeader,String>>();
//        bodySerializer = new MolapSegmentBodySerializer();
//        headerSerializer = new MolapSegmentHeaderSerializer();
//        customFileLRUCache = CustomFileLRUCache.getInstance(cache);
//        loadMaps();
//    }
//    
//    private void loadMaps()
//    {
//        File cacheFile = new File(path);
//        File[] dirs = cacheFile.listFiles();
//        for(int i = 0;i < dirs.length;i++)
//        {
//            Map<MolapSegmentHeader, String> headerMap = new HashMap<MolapSegmentHeader, String>();
//            cache.put(dirs[i].getName(), headerMap);
//            
//            File[] files = dirs[i].listFiles();
//            for(int j = 0;j < files.length;j++)
//            {
//                DataInputStream in = null;
//                try
//                {
//                    in = new DataInputStream(new BufferedInputStream(new FileInputStream(files[j]),fileWriteBufferSize));
//                    headerMap.put(headerSerializer.deserialize(in),files[j].getAbsolutePath());
//                }
//                catch(Exception e)
//                {
//                    LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,e.getMessage());
//                }
//                finally
//                {
//                    if(in != null)
//                    {
//                        try
//                        {
//                            in.close();
//                        }
//                        catch(IOException e)
//                        {
//                            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,e.getMessage());
//                        }
//                    }
//                }
//            }
//            loadToLRU(headerMap);
//        }
//    }
//    
//    Boolean contains(MolapSegmentHeader header) 
//    {
//        return customFileLRUCache.get(header) != null;
//    }
//
//    MolapSegmentBody get(MolapSegmentHeader header) 
//    {
//        if(customFileLRUCache.get(header) != null)
//        {
//            long st = System.currentTimeMillis();
//             Map<MolapSegmentHeader, String> cubeCache = cache.get(header.getCubeName());
//            if(cubeCache == null)
//            {
//                return null;
//            }
//            String filePath = cubeCache.get(header);
//            
//            File file = new File(filePath);
//            if(file.exists())
//            {
//                DataInputStream inputStream = null;
//                try
//                {
//                    inputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(file),fileWriteBufferSize));
//                    
//                    headerSerializer.deserialize(inputStream);
//                    MolapSegmentBody body = bodySerializer.deserialize(inputStream);
//                    readData(inputStream, body,header.getCubeName());
//                    LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,"Time taken for get from cache : "+(System.currentTimeMillis()-st) );
//                    return body;
//                }
//                catch (Exception e) 
//                {
//                    LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,e.getMessage());
//                }
//                finally
//                {
//                    if(inputStream != null)
//                    {
//                        try
//                        {
//                            inputStream.close();
//                        }
//                        catch(IOException e)
//                        {
//                            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,e.getMessage());
//                        }
//                    }
//                }
//            }
//            
//        }
//        return null;
//    }
//    
//    
//    private Measure[] getMeasures(Cube cube,String[] msrs,String[] aggNames)
//    {
//        Measure[] measures = new Measure[msrs.length];
//        
//        List<Measure> actualMsrs = cube.getMeasures(cube.getFactTableName());
//        
//        for(int i = 0;i < measures.length;i++)
//        {
//            String msr = msrs[i];
//            if (MolapCommonConstants.GEN_COUNT_MEASURE.equals(msr))
//            {
//                measures[i] = new Measure();
//                measures[i].setAggName(aggNames[i]);
//                continue;
//            }
//            for(Measure actMsr : actualMsrs)
//            {
//                if(actMsr.getName().equals(msr))
//                {
//                    measures[i] = actMsr.getCopy();
//                    measures[i].setAggName(aggNames[i]);
//                    break;
//                }
//            }
//        }
//        return measures;
//    }
//    
//    MolapSegmentBody getWithoutData(MolapSegmentHeader header) 
//    {
//        if(customFileLRUCache.get(header) != null)
//        {
////            long st = System.currentTimeMillis();
//             Map<MolapSegmentHeader, String> cubeCache = cache.get(header.getCubeName());
//            if(cubeCache == null)
//            {
//                return null;
//            }
//            String filePath = cubeCache.get(header);
//            
//            File file = new File(filePath);
//            if(file.exists())
//            {
//                DataInputStream inputStream = null;
//                try
//                {
//                    inputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(file),fileWriteBufferSize));
//                    
//                    headerSerializer.deserialize(inputStream);
//                    MolapSegmentBody body = bodySerializer.deserialize(inputStream);
//                    //readData(inputStream, body);
//                   // LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,"Time taken for get from cache : "+(System.currentTimeMillis()-st) );
//                    return body;
//                }
//                catch (Exception e) 
//                {
//                    LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,e.getMessage());
//                }
//                finally
//                {
//                    if(inputStream != null)
//                    {
//                        try
//                        {
//                            inputStream.close();
//                        }
//                        catch(IOException e)
//                        {
//                            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,e.getMessage());
//                        }
//                    }
//                }
//            }
//            
//        }
//        return null;
//    }
//
//    Set<MolapSegmentHeader> getSegmentHeaders(String cubeName) 
//    {
//       Map<MolapSegmentHeader, String> cubeCache = cache.get(cubeName);
//       if(cubeCache == null)
//       {
//           return null;
//       }
//       return cubeCache.keySet();
//    }
//    
//    private void loadToLRU(Map<MolapSegmentHeader, String> headerMap)
//    {
//        for(MolapSegmentHeader header:headerMap.keySet())
//        {
//            customFileLRUCache.put(header, const_byte);
//        }
//    }
//
//    Boolean put(MolapSegmentHeader header, MolapSegmentBody body) 
//    {
//        Map<MolapSegmentHeader, String> cubeCache = cache.get(header.getCubeName());
//        if(cubeCache == null)
//        {
//            cubeCache = new HashMap<MolapSegmentHeader, String>();
//            cache.put(header.getCubeName(),cubeCache);
//        }
//        long st = System.currentTimeMillis();
//        if(customFileLRUCache.get(header) != null)
//        {
//            String filePath = cubeCache.get(header);
//            if(filePath != null)
//            {
//                boolean isDelete = new File(filePath).delete();
//                if(!isDelete)
//                {
//                    LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Not Able to delete the Cache File.");
//                }
//                cubeCache.remove(header);
//                customFileLRUCache.remove(header);
//            }
//        }
//        customFileLRUCache.put(header, const_byte);
//        DataOutputStream dataOutputStream = null;
//        try
//        {
//            String segmentPath = path+File.separator+header.getCubeName();
//            boolean isMkdirs = new File(segmentPath).mkdirs();
//            if(!isMkdirs)
//            {
//                LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Not Able to create Custom LRU Cache File.");
//            }
//            segmentPath = segmentPath+File.separator+System.nanoTime();
//            dataOutputStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(segmentPath)),fileWriteBufferSize));
//            
//            cubeCache.put(header, segmentPath);
//            
//            headerSerializer.serialize(dataOutputStream, header);
//            
//            Map data = (Map)body.getData();
//            body.setData(null);
//            bodySerializer.serialize(dataOutputStream, body);
//            writeData(data, dataOutputStream,body.getAggNames().length);
//            LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,"Time taken to put to cache : "+(System.currentTimeMillis()-st) );
//        }
//        catch (Throwable e) 
//        {
//            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,e.getMessage());
//
//        }
//        finally
//        {
//            if(dataOutputStream != null)
//            {
//                try
//                {
//                    dataOutputStream.close();
//                }
//                catch(IOException e)
//                {
//                    LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,e.getMessage());
//                }
//            }
//        }
//        return true;
//    }
//    
//    private void writeData( Map<ByteArrayWrapper, MeasureAggregator[]> data,DataOutputStream dataOutput,int msrCount) throws IOException
//    {
//        dataOutput.writeInt(data.size());
//        for(Entry<ByteArrayWrapper, MeasureAggregator[]> entrySet : data.entrySet())
//        {
//            ByteArrayWrapper key = entrySet.getKey();
//            dataOutput.write(key.getMaskedKey());
//            MeasureAggregator[] value = entrySet.getValue();
//            for(int i = 0;i < msrCount;i++)
//            {
//                value[i].writeData(dataOutput);
//            }
//        }
//    }
//    
//    private void readData(DataInputStream dataInput, MolapSegmentBody body,String cubeName) throws IOException
//    {
//        int size = dataInput.readInt();
//        String[] aggNames = body.getAggNames();
//        int keySize = body.getKeySize();
//        int msrCount = aggNames.length;
//        Map<ByteArrayWrapper, MeasureAggregator[]> data = new LinkedHashMap<ByteArrayWrapper, MeasureAggregator[]>(size+1,1.0f);
//        Cube cube = MolapMetadata.getInstance().getCube(cubeName);
//        Measure[] measures = getMeasures(cube, body.getMeasures(),aggNames);
//        for(int i = 0;i < size;i++)
//        {
//            ByteArrayWrapper key = new ByteArrayWrapper();
//            byte[] keyArray = new byte[keySize];
//            if(dataInput.read(keyArray) < 0)
//            {
//                LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Problme while reading the Custom File based Segment cache.");
//            }
//            key.setMaskedKey(keyArray);
//            MeasureAggregator[] aggregators = AggUtil.getAggregatorsWithCubeName(measures, false, body.getGenerator(), cubeName);
//            for(int j = 0;j < msrCount;j++)
//            {
//                aggregators[j].readData(dataInput);
//            }
//            data.put(key, aggregators);
//        }
//        body.setData((Serializable)data);
//    }
//
// 
//    /**
//     * remove.
//     * @param header
//     * @return
//     */
//    Boolean remove(MolapSegmentHeader header) 
//    {
//        customFileLRUCache.remove(header);
//        Map<MolapSegmentHeader, String> cubeCache = cache.get(header.getCubeName());
//        if(cubeCache == null)
//        {
//            return false;
//        }
//        String filePath = cubeCache.remove(header);
//        
//        if(! new File(filePath).delete())
//        {
//            return false;
//        }
//        
//        return true;
//    }
//
//    /**
//     * flush.
//     * @param region
//     * @return
//     */
//    public boolean flush(final ConstrainedColumn[] region) 
//    {
//        if (region == null || region.length == 0) 
//        {
//            // Special case: An empty region means everything.
//            customFileLRUCache.clear();
//            cache.clear();
//            
//            File[] files = new File(path).listFiles();
//            
//            try
//            {
//                MolapUtil.deleteFoldersAndFiles(files);
//            }
//            catch(MolapUtilException e)
//            {
//                LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,e.getMessage());
//            }
//            
//        }
//    
//        return true;
//    }
//    
//    /**
//     * flushCube.
//     * @param schemaName
//     * @param cubeName
//     * @return
//     */
//    public Boolean flushCube(String schemaName, String cubeName)
//    {
//        String cubeUniqueName = schemaName+'_'+cubeName;
//        Map<MolapSegmentHeader, String> cubeCache = cache.get(cubeUniqueName);
//        if(cubeCache == null)
//        {
//            return true;
//        }
//        cubeCache.clear();
//        File[] files = new File(path+File.separator+cubeUniqueName).listFiles();
//        try
//        {
//            MolapUtil.deleteFiles(files);
//        }
//        catch(MolapUtilException e)
//        {
//            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,e.getMessage());
//        }
//        
//        return true;
//    }
//
//
//    void tearDown() 
//    {
//        
//    }
//
//
//    /**
//     * Clears segments without time dim
//     * Clears segments without predicate
//     * Clears any segment which has time dim key greater dan new time dim key
//     * @param cubeUniqueName
//     * @param startKey
//     * @param keygen
//     * @param tableName
//     * @return
//     */
//    public Boolean flushCube(String cubeUniqueName, byte[] startKey, KeyGenerator keygen,String tableName)
//    {
//        Map<MolapSegmentHeader, String> map = cache.get(cubeUniqueName);
//        if(null == map)
//        {
//            //cube present but no data loaded.!! nothing to flush
//            return true;
//        }
//        Set<MolapSegmentHeader> headers = map.keySet();
//        Cube cube = MolapMetadata.getInstance().getCube(cubeUniqueName);
//       // long[] startKeyArray = keygen.getKeyArray(startKey);
//        List<Dimension> dimensions = cube.getDimensions(cube.getFactTableName());
//        
//        
//       //TODO List<InMemoryCube> memoryCubes = InMemoryCubeStore.getInstance().getActiveSlices(cubeUniqueName);
//        
//        List<Dimension> timeDimensions = new ArrayList<MolapMetadata.Dimension>(1);
//        //list all time dims
//        for(Dimension dim : dimensions)
//        {
//            if(dim.getLevelType().isTime())
//            {
//                timeDimensions.add(dim);
//            }
//        }
//
//        //if no timedim flush all in the cube
//        if(timeDimensions.isEmpty())
//        {
//            flushCube(cube.getSchemaName(),cube.getCubeName().substring(cube.getSchemaName().length()+1,cube.getCubeName().length())); 
//        }
//        
//        int[] timeDimOrdinal = new int[timeDimensions.size()];
//        int count=0;
//        for(Dimension dim : timeDimensions)
//        {
//            timeDimOrdinal[count++] = dim.getOrdinal();
//        }
//        List<MolapSegmentHeader> toRemove = new ArrayList<MolapSegmentHeader>();
//        for(MolapSegmentHeader header : headers)
//        {
//            if(!header.getFactTableName().equals(tableName))
//            {
//                continue;
//            }
//            boolean retain = false;
//            int[] dims = header.getDims();
//
//            if(dims.length > 0 && dims[0] == timeDimensions.get(0).getOrdinal())
//            {
//                retain = true;
//            }
//
//            if(!retain)
//            {
//                toRemove.add(header);
//                continue;
//            }
//           
//            boolean flush = false;
///**TODO : Smart cache flush would be implemented later.           
////            long[] startKeyFromHeader = header.getStartKey();
////            long[] endKeyFromHeader = header.getEndKey();
//            
////            int foundTimeDim = -1;
//            //TODO : Smart cache flush would be implemented later.
////            int timeDimIndex=timeDimOrdinal.length-2;
////            for(;timeDimIndex >= 0 && foundTimeDim == -1;timeDimIndex--)
////            {
////                for(int j = dims.length - 1;j >= 0;j--)
////                {
////                    if(dims[j] == timeDimOrdinal[timeDimIndex])
////                    {
////                        foundTimeDim = timeDimIndex;
////                        break;
////                    }
////                }
////            }
//
////            if(foundTimeDim > -1)
////            {
////                long[] startKeySurrogate = Arrays.copyOfRange(startKeyArray, 0, foundTimeDim + 1);
////                long[] headerKeySurrogate = Arrays.copyOfRange(startKeyFromHeader, 0, foundTimeDim + 1);
////                if(Arrays.equals(startKeySurrogate, headerKeySurrogate))
////                {
////                    flush = true;
////                }
////                else
////                {
////                    long[] headerEndKeySurrogate = Arrays.copyOfRange(endKeyFromHeader, 0, foundTimeDim + 1);
////                    if(compare(startKeySurrogate, headerEndKeySurrogate) <= 0)
////                    {
////                       MolapSegmentBody molapSegmentBody = get(header);
////                       //Coverity Fix add null check
////                       if(molapSegmentBody == null)
////                       {
////                           continue;
////                       }
////                       molapSegmentBody.trim(startKey);
////                      
////                       Map<ByteArrayWrapper, MeasureAggregator[]> data = (Map<ByteArrayWrapper, MeasureAggregator[]>)molapSegmentBody.getData();
////                       if(null == data || data.isEmpty())
////                       {
////                           flush = true;
////                       }
////                       else
////                       {
////                           //Fortify Fix
////                           put(header,molapSegmentBody);
////                       }
////                      
////                    }
////                    
////                }
////            }
////            else
////            {**/
//                flush = true;
///**           }**/
//            
//            if(flush)
//            {
//                toRemove.add(header);
//            }
//        }
//        
//        if(!toRemove.isEmpty())
//        {
//           Iterator<MolapSegmentHeader> iterator = toRemove.iterator();
//           while(iterator.hasNext())
//           {
//               remove(iterator.next());
//           }
//        }
//        return false;
//    }
//    
//    
//    /**
//     * Compares two long arrays
//     * <p>+ if buffer 1 > buffer 2
//     * <p>- if buffer 1 < buffer 2
//     * <p>0 on equals
//     * @param buffer1
//     * @param buffer2
//     * @return
//     */
//    public int compare(long[] buffer1, long[] buffer2)
//    {
//        // Short circuit equal case
//        if(buffer1 == buffer2)
//        {
//            return 0;
//        }
//        // Bring WritableComparator code local
//        int i = 0;
//        int j = 0;
//        for(;i < buffer1.length && j < buffer2.length;i++, j++)
//        {
//            long a = buffer1[i];
//            long b = buffer2[j];
//            if(a != b)
//            {//CHECKSTYLE:OFF    Approval No:Approval-356
//                return (int)(a - b);
//            }//CHECKSTYLE:ON
//        }
//        return 0;
//    }
    
}
