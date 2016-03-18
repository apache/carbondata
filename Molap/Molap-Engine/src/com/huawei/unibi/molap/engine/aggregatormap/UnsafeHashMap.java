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

package com.huawei.unibi.molap.engine.aggregatormap;

import static org.apache.spark.unsafe.Platform.BYTE_ARRAY_OFFSET;
import static org.apache.spark.unsafe.Platform.INT_ARRAY_OFFSET;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkEnv;
//import org.apache.spark.shuffle.ShuffleMemoryManager;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.map.BytesToBytesMap;
import org.apache.spark.unsafe.map.BytesToBytesMap.Location;
import org.apache.spark.unsafe.memory.ExecutorMemoryManager;
import org.apache.spark.unsafe.memory.MemoryAllocator;
import org.apache.spark.unsafe.memory.TaskMemoryManager;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.wrappers.ByteArrayWrapper;

public class UnsafeHashMap
{
//    private final long PAGE_SIZE_BYTES = 1L << 26;
    
    private BytesToBytesMap map;
    
    private int keySize;
    
    private int counter=-1;
    
    protected List<MeasureAggregator[]> measuresList;
    
    public UnsafeHashMap(int initialCapacity, int keySize)
    {
        TaskMemoryManager memoryManager = new TaskMemoryManager(new ExecutorMemoryManager(MemoryAllocator.UNSAFE));
//        ShuffleMemoryManager shuffleMemoryManager = ShuffleMemoryManager.create(Long.MAX_VALUE, PAGE_SIZE_BYTES);
        map = new BytesToBytesMap(memoryManager,SparkEnv.get().shuffleMemoryManager(), 10000000,1.0d,SparkEnv.get().shuffleMemoryManager().pageSizeBytes(), false);
        this.keySize=keySize;
        measuresList = new ArrayList<MeasureAggregator[]>(100000);
    }
    
    public int putKey(byte[] key)
    {
        Location lookup = map.lookup(key, BYTE_ARRAY_OFFSET, this.keySize);
        if(!lookup.isDefined())
        {
            ++counter;
            lookup.putNewKey(
                    key,
                    BYTE_ARRAY_OFFSET,
                    keySize,
                    new int[]{counter},
                    INT_ARRAY_OFFSET,
                    4
              );
            return counter;
        }
        else
        {
            int[] index = new int[1];
            Platform.copyMemory(
                    lookup.getValueAddress().getBaseObject(),
                    lookup.getValueAddress().getBaseOffset(),
                    index,
                    INT_ARRAY_OFFSET,
                    12
                  );
            return index[0];
            
        }
    }
    
    public void putValue(MeasureAggregator[] msrs)
    {
        measuresList.add(msrs);
    }
    
    public MeasureAggregator[] getValue(int index)
    {
        return measuresList.get(index);
    }
    
    public Map<ByteArrayWrapper,MeasureAggregator[]> getData()
    {
        Iterator<Location> iterator = map.iterator();
        Map<ByteArrayWrapper,MeasureAggregator[]> finalMap = new HashMap<ByteArrayWrapper, MeasureAggregator[]>(map.numElements(),1.0f);
        byte[] key = null;
        int[] index= new int[1];
        Location next = null;
        ByteArrayWrapper byteArrayWrapper = null;
        while(iterator.hasNext())
        {
            next = iterator.next();
            key = new byte[keySize];
            Platform.copyMemory(
                    next.getValueAddress().getBaseObject(),
                    next.getValueAddress().getBaseOffset(),
                    index,
                    INT_ARRAY_OFFSET,
                    4
                  );
            
            Platform.copyMemory(
                    next.getKeyAddress().getBaseObject(),
                    next.getKeyAddress().getBaseOffset(),
                    key,
                    INT_ARRAY_OFFSET,
                    keySize
                  );
            byteArrayWrapper = new ByteArrayWrapper();
            byteArrayWrapper.setMaskedKey(key);
            finalMap.put(byteArrayWrapper, measuresList.get(index[0]));
        }
        map.free();
        return finalMap;
    }
}
