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
u0090SfnUnT2goUAHLN9f9o1aaWxy0dILwz2QL9CO3jw3pDZSCRPepwCEQqWLFsUybw7TZtT
DvnIfZo1D73h+0KYxOzAh1arwHhul/JPCsh5FN4cgtLdp6zrV5ThLaQ8NcPv/w==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
package com.huawei.unibi.molap.engine.cache;

/**
 * @author R00900208
 *
 */
public final class InMemoryLRUCache 
{
    
//
//    /**
//     * fCacheMap
//     */
//    private Map<MolapSegmentHeader, MolapSegmentBody> fCacheMap;
//   
//	
//    /**
//     * 
//     */
//    private static final LogService LOGGER = LogServiceFactory.getLogService(InMemoryLRUCache.class.getName());
//
//    
//	/**
//	 * Get instance of class
//	 * @param hashMap
//	 * @return
//	 */
//	public static synchronized InMemoryLRUCache getInstance()
//	{
//	    long mem = 0;
//	    try
//	    {
//    	    mem = Long.parseLong(MondrianProperties.instance().getProperty(
//                    "com.huawei.datastore.inmemory.lrusize", "10"));
//	    }
//	    catch (NumberFormatException e) 
//	    {
//	        mem = 10;
//        }
//	    mem = MolapProperties.getInstance().validate(mem, 1000, 5, 10);
//		return new InMemoryLRUCache(3000, mem);
//	}
//
//	/**
//	 * Instantiate LRU cache.
//	 * @param size
//	 * @param memSize
//	 * @param hashMap
//	 */
//    @SuppressWarnings("unchecked")
//    private InMemoryLRUCache(int size,final long memSize)
//    {
//
//        // If the cache is to be used by multiple threads,
//        // the hashMap must be wrapped with code to synchronize 
//        fCacheMap = Collections.synchronizedMap
//        (
//            //true = use access order instead of insertion order
//            new LinkedHashMap<MolapSegmentHeader,MolapSegmentBody>(size, .75F, true)
//            { 
//            	private long size;
//                @Override
//                public boolean removeEldestEntry(Map.Entry<MolapSegmentHeader, MolapSegmentBody> eldest)  
//                {
//                	if(size > memSize)
//                	{
//                		size --;
//                		return true;
//                	}
//                    //when to remove the eldest entry
//                    return false;   //size exceeded the max allowed
//                }
//                
//                public MolapSegmentBody put(MolapSegmentHeader key,MolapSegmentBody value) 
//                {
//                	size ++;
//                	return super.put(key, value);
//                }
//                
//                public void clear() 
//                {
//                    size =0;
//                    super.clear();
//                }
//            }
//        );
//    }
//
//    /**
//     * Put the key
//     * @param key
//     * @param elem
//     */
//    public void put(MolapSegmentHeader key, MolapSegmentBody elem)
//    {
//        fCacheMap.put(key, elem);
//    }
//
//    /**
//     * Get the key
//     * @param key
//     * @return
//     */
//    public MolapSegmentBody get(MolapSegmentHeader key)
//    {
//        MolapSegmentBody molapSegmentBody = fCacheMap.get(key);
//        if(molapSegmentBody != null)
//        {
//            LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Got Segment from inMemory cache");
//        }
//        return molapSegmentBody;
//    }
//    
//    /**
//     * Get headers
//     * @return
//     */
//    public List<MolapSegmentHeader> getHeaders()
//    {
//        return new ArrayList<MolapSegmentHeader>(fCacheMap.keySet());
//    }
//    
//    /**
//     * Remove key
//     * @param key
//     * @return
//     */
//    public MolapSegmentBody remove(MolapSegmentHeader key)
//    {
//        return fCacheMap.remove(key);
//    }
//    
//    /**
//     * To string
//     */
//    @Override
//    public String toString() {
//    	// TODO Auto-generated method stub
//    	return fCacheMap.toString();
//    }
//    
//    
////    public static void main(String[] args) 
////    {
////    	LRUCache cache = new LRUCache(1,100,null);
////    	for (long i = 0; i < 500; i++) {
////    		//cache.put(i+"", new byte[]{1,2});
////		}
////    	
////    	System.out.println(cache);
////	}
//
//    /**
//     * Clear cache
//     */
//    public void clear()
//    {
//        fCacheMap.clear();
//    }
    
    
    
    
    
}
