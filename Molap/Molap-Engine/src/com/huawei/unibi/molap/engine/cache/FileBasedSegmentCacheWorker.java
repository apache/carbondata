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
u0090VgvPJ+TQv5e+5CAQuFICXzTuYnvtcQbaeIVMqIjbzqj0e9rFcKXvOjoAmc+hWoguTAA
mMepOne7qwogFjA/0SXiOLsOm/o3CIZhKFL6UD1QU41vrfiaNW291AB2HE5dMA==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
package com.huawei.unibi.molap.engine.cache;

import java.io.Serializable;

/**
 * 
 * @author R00900208
 *
 */
public class FileBasedSegmentCacheWorker implements Serializable {

//    private static final long serialVersionUID  = -5744665059683205118L;
//    
//    /**
//     * Attribute for Molap LOGGER
//     */
//    private static final LogService LOGGER = LogServiceFactory
//            .getLogService(FileBasedSegmentCacheWorker.class.getName());
//
//    
//    private Map<String,Map<MolapSegmentHeader, MolapSegmentBody>>  cubeSegmentMap;
//    
//    private Map<String,DB> cubeDBMap;
//    
//    private LRUCache lruCache = null;
//    
//    private static Byte const_byte = new Byte((byte)1);
//    
//    private String path = MolapCommonConstants.MOLAP_CACHE_LOCATION;
//
//
//    private static final FileBasedSegmentCacheWorker instance =
//        new FileBasedSegmentCacheWorker();
//
////    private static final String CACHE_NAME = "MOLAP_CACHE";
//
//    static FileBasedSegmentCacheWorker instance() {
//        return instance;
//    }
//
//    /**
//     * Constructor
//     */
//    FileBasedSegmentCacheWorker() 
//    {
//        cubeSegmentMap = new HashMap<String, Map<MolapSegmentHeader,MolapSegmentBody>>();
//        cubeDBMap = new HashMap<String,DB>();
//
//        File cacheFile = new File(path);
//        if(!cacheFile.exists())
//        {
//            if(!cacheFile.mkdirs())
//            {
//                LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Problem while creating the directory");
//            }
//        }
//        lruCache = LRUCache.getInstance(cubeSegmentMap);
//        loadMaps();
//    }
//
//    private void loadToLRU(ConcurrentMap<MolapSegmentHeader,MolapSegmentBody> hashMap)
//    {
//        for(MolapSegmentHeader header:hashMap.keySet())
//        {
//            lruCache.put(header, const_byte);
//        }
//    }
//
//    
//    private void loadMaps()
//    {
//        File file = new File(path);
//        File[] cubeNameFolders = file.listFiles();
//        for(int i = 0;i < cubeNameFolders.length;i++)
//        {
//          DB db = DBMaker.openFile(cubeNameFolders[i].getAbsolutePath()+'/'+cubeNameFolders[i].getName()).disableCache().closeOnExit().make();
//          cubeDBMap.put(cubeNameFolders[i].getName(), db);
//          ConcurrentMap<MolapSegmentHeader,MolapSegmentBody> hashMap = db.getHashMap(cubeNameFolders[i].getName());
//          if(hashMap  != null)
//          {
//              cubeSegmentMap.put(cubeNameFolders[i].getName(), hashMap);
//              loadToLRU(hashMap);
//          }
//        }
//    }
//
//    /**
//     * Below method will be used to check header is present or not 
//     * @param header
//     * @return boolean
//     */
//    Boolean contains(MolapSegmentHeader header) 
//    {
//        return lruCache.get(header) != null;
//    }
//
//    /**
//     * Below method will be used to get the segement body from cache
//     * @param header
//     * @return MolapSegmentBody
//     */
//    MolapSegmentBody get(MolapSegmentHeader header) 
//    {
//        if(lruCache.get(header) != null)
//        {
//            Map<MolapSegmentHeader, MolapSegmentBody> cache = cubeSegmentMap.get(header.getCubeName());
//            if(cache == null)
//            {
//                return null;
//            }
//            return cache.get(header);
//        }
//        return null;
//    }
//
//    /**
//     * Below method will be used to get the molap segment headers
//     * @param cubeName
//     * @return set of headers
//     */
//    Set<MolapSegmentHeader> getSegmentHeaders(String cubeName) 
//    {
//        Map<MolapSegmentHeader, MolapSegmentBody> cache = cubeSegmentMap.get(cubeName);
//        return cache.keySet();
//    }
//
//    /**
//     * Below method will be used to to put the segment header and body to cache 
//     * @param header
//     * @param body
//     * @return Boolean
//     */
//    Boolean put(MolapSegmentHeader header, MolapSegmentBody body) 
//    {
//        lruCache.put(header, const_byte);
//        Map<MolapSegmentHeader, MolapSegmentBody> cache = cubeSegmentMap.get(header.getCubeName());
//        if(cache == null)
//        {
//            cache = createCache(header.getCubeName());
//            cubeSegmentMap.put(header.getCubeName(), cache);
//        }
//        cache.put(header, body);
//        commitDB(header.getCubeName());
//        return true;
//    }
//    
//    private Map<MolapSegmentHeader, MolapSegmentBody> createCache(String cubeName)
//    {
//        File file = new File(path+'/'+cubeName);
//        if(!file.mkdirs())
//        {
//            LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Problem while creating the directory");
//        }
//        DB db = DBMaker.openFile(file.getAbsolutePath()+'/'+cubeName).disableCache().closeOnExit().make();
//        cubeDBMap.put(cubeName, db);
//        ConcurrentMap<MolapSegmentHeader, MolapSegmentBody> hashMap = null;
//        hashMap = db.getHashMap(cubeName);
////      if(hashMap == null)
////      {
////          //hashMap = db.createHashMap(cubeName,new MolapSegmentHeaderSerializer(),new MolapSegmentBodySerializer());
////      }
//    return hashMap;
//        
//    }
//
//    /**
//     * Below method will be used to remove the header
//     * @param header
//     * @return boolean 
//     */
//    Boolean remove(MolapSegmentHeader header) 
//    {
//        lruCache.remove(header);
//        Map<MolapSegmentHeader, MolapSegmentBody> cache = cubeSegmentMap.get(header.getCubeName());
//        if(cache == null)
//        {
//            return false;
//        }
//        cache.remove(header);
//        commitDB(header.getCubeName());
//        return true;
//    }
//
//    /**
//     * Below method will be used to flush the cube
//     * @param region
//     * @return boolean 
//     */
//    public boolean flush(final ConstrainedColumn[] region) 
//    {
//        if (region == null || region.length == 0) 
//        {
//            // Special case: An empty region means everything.
//            lruCache.clear();
//            for(Map<MolapSegmentHeader, MolapSegmentBody> cache : cubeSegmentMap.values())
//            {
//                cache.clear();
//            }
//        }
//        Collection<DB> values = cubeDBMap.values();
//        for(DB db : values)
//        {
//            db.commit();
//        }
//        return true;
//    }
//    
//    /**
//     * Below method will be used to flush the cube
//     * @param schemaName
//     * @param cubeName
//     * @return boolean
//     */
//    public Boolean flushCube(String schemaName, String cubeName)
//    {
//        List<MolapSegmentHeader> headers = lruCache.getHeaders();
//        String cubeUniqueName = schemaName+'_'+cubeName;
//        Map<MolapSegmentHeader, MolapSegmentBody> map = cubeSegmentMap.get(cubeUniqueName);
//        map.clear();
//        for(MolapSegmentHeader header:headers)
//        {
//            if(header.getCubeName().equals(cubeUniqueName))
//            {
//                lruCache.remove(header);
//            }
//        }
//        commitDB(cubeUniqueName);
//        return true;
//    }
//
//    /**
//     * Commit if DB registered for this cube
//     * @param cubeUniqueName
//     */
//    private void commitDB(String cubeUniqueName)
//    {
//        DB db = cubeDBMap.get(cubeUniqueName);
//        if(db != null)
//        {
//            db.commit();
//        }
//    }
//
//    /**
//     * tear down 
//     */
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
//        Map<MolapSegmentHeader, MolapSegmentBody> map = cubeSegmentMap.get(cubeUniqueName);
//        if(null == map)
//        {
//            //cube present but no data loaded.!! nothing to flush
//            return true;
//        }
//        Set<MolapSegmentHeader> headers = map.keySet();
//        Cube cube = MolapMetadata.getInstance().getCube(cubeUniqueName);
//        long[] startKeyArray = keygen.getKeyArray(startKey);
//        List<Dimension> dimensions = cube.getDimensions(cube.getFactTableName());
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
//            
//            long[] startKeyFromHeader = header.getStartKey();
//            long[] endKeyFromHeader = header.getEndKey();
//            
//            int foundTimeDim = -1;
//            int timeDimIndex=timeDimOrdinal.length-2;
//            for(;timeDimIndex >= 0 && foundTimeDim == -1;timeDimIndex--)
//            {
//                for(int j = dims.length - 1;j >= 0;j--)
//                {
//                    if(dims[j] == timeDimOrdinal[timeDimIndex])
//                    {
//                        foundTimeDim = timeDimIndex;
//                        break;
//                    }
//                }
//            }
//
//            if(foundTimeDim > -1)
//            {
//                long[] startKeySurrogate = Arrays.copyOfRange(startKeyArray, 0, foundTimeDim + 1);
//                long[] headerKeySurrogate = Arrays.copyOfRange(startKeyFromHeader, 0, foundTimeDim + 1);
//                if(Arrays.equals(startKeySurrogate, headerKeySurrogate))
//                {
//                    flush = true;
//                }
//                else
//                {
//                    long[] headerEndKeySurrogate = Arrays.copyOfRange(endKeyFromHeader, 0, foundTimeDim + 1);
//                    if(compare(startKeySurrogate, headerEndKeySurrogate) <= 0)
//                    {
//                       MolapSegmentBody molapSegmentBody = get(header);
//                       //Coverity Fix add null check
//                       if(molapSegmentBody == null)
//                       {
//                           continue;
//                       }
//                       molapSegmentBody.trim(startKey);
//                       HashMap<ByteArrayWrapper, MeasureAggregator[]> data = (HashMap<ByteArrayWrapper, MeasureAggregator[]>)molapSegmentBody.getData();
//                       if(null == data || data.isEmpty())
//                       {
//                           flush = true;
//                       }
//                       else
//                       {
//                           put(header,molapSegmentBody);
//                       }
//                      
//                    }
//                    
//                }
//            }
//            else
//            {
//                flush = true;
//            }
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
