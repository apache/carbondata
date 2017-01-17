/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.core.memory;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;

/**
 * Factory class to to get the memory allocator instance
 */
public class MemoryAllocatorFactory {

  private MemoryAllocator memoryAllocator;

  public static final MemoryAllocatorFactory INSATANCE = new MemoryAllocatorFactory();

  private MemoryAllocatorFactory() {
    boolean offHeap = Boolean.parseBoolean(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.USE_OFFHEAP_IN_QUERY_PROCSSING,
            CarbonCommonConstants.USE_OFFHEAP_IN_QUERY_PROCSSING_DEFAULT));
    if (offHeap) {
      memoryAllocator = MemoryAllocator.UNSAFE;
    } else {
      memoryAllocator = MemoryAllocator.HEAP;
    }
  }

  public MemoryAllocator getMemoryAllocator() {
    return memoryAllocator;
  }
}
