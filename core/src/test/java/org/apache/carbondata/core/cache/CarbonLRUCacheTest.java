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

package org.apache.carbondata.core.cache;

import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.testing.FakeTicker;
import mockit.Mock;
import mockit.MockUp;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CarbonLRUCacheTest {

  private static CarbonLRUCache carbonLRUCache;
  private static Cacheable cacheable;

  @BeforeClass public static void setUp() {
    carbonLRUCache = new CarbonLRUCache("prop1", "2");
    cacheable = new MockUp<Cacheable>() {
      @SuppressWarnings("unused") @Mock long getMemorySize() {
        return 15L;
      }
    }.getMockInstance();
  }

  @Test public void testPut() {
    boolean result = carbonLRUCache.put("Column1", cacheable, 10L);
    assertTrue(result);
  }

  @Test public void testPutWhenSizeIsNotAvailable() {
    boolean result = carbonLRUCache.put("Column2", cacheable, 11111110L);
    assertFalse(result);
  }

  @Test public void testPutWhenKeysHaveToBeRemoved() {
    boolean result = carbonLRUCache.put("Column3", cacheable, 2097153L);
    assertTrue(result);
  }

  @Test public void testRemove() {
    carbonLRUCache.remove("Column2");
    assertNull(carbonLRUCache.get("Column2"));
  }

  @Test public void testBiggerThanMaxSizeConfiguration() {
    CarbonLRUCache carbonLRUCacheForConfig =
            new CarbonLRUCache("prop2", "200000");//200GB
    assertTrue(carbonLRUCacheForConfig.put("Column1", cacheable, 10L));
    assertFalse(carbonLRUCacheForConfig.put("Column2", cacheable, 107374182400L));//100GB
  }

  @Test public void testCacheExpiration_expireAfterWrite() {
    FakeTicker ticker = new FakeTicker();
    Cache<String, Cacheable> cache =
        CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.MINUTES).ticker(ticker).build();
    cache.put("column1", cacheable);
    assertEquals(1, cache.size());
    assertNotNull(cache.getIfPresent("column1"));
    ticker.advance(2, TimeUnit.MINUTES);
    assertNull(cache.getIfPresent("column1"));
  }

  @AfterClass public static void cleanUp() {
    carbonLRUCache.clear();
    assertNull(carbonLRUCache.get("Column1"));
  }

}
