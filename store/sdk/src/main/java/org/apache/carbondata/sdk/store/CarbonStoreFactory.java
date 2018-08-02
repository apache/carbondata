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

package org.apache.carbondata.sdk.store;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.sdk.store.conf.StoreConf;
import org.apache.carbondata.sdk.store.exception.CarbonException;

/**
 * Factory class to create {@link CarbonStore}
 */
@InterfaceAudience.User
@InterfaceStability.Unstable
public class CarbonStoreFactory {
  private static Map<String, CarbonStore> remoteStore = new ConcurrentHashMap<>();
  private static Map<String, CarbonStore> localStores = new ConcurrentHashMap<>();

  private CarbonStoreFactory() {
  }

  public static CarbonStore getRemoteStore(String storeName, StoreConf storeConf)
      throws CarbonException {
    if (remoteStore.containsKey(storeName)) {
      return remoteStore.get(storeName);
    }

    // create a new instance
    try {
      String className = "org.apache.carbondata.sdk.store.RemoteCarbonStore";
      CarbonStore store = createCarbonStore(storeConf, className);
      remoteStore.put(storeName, store);
      return store;
    } catch (ClassNotFoundException | IllegalAccessException | InvocationTargetException |
        InstantiationException e) {
      throw new CarbonException(e);
    }
  }

  public static void removeDistributedStore(String storeName) throws IOException {
    if (remoteStore.containsKey(storeName)) {
      remoteStore.get(storeName).close();
      remoteStore.remove(storeName);
    }
  }

  public static CarbonStore getLocalStore(String storeName, StoreConf storeConf)
      throws CarbonException {
    if (localStores.containsKey(storeName)) {
      return localStores.get(storeName);
    }

    // create a new instance
    try {
      String className = "org.apache.carbondata.store.impl.LocalCarbonStore";
      CarbonStore store = createCarbonStore(storeConf, className);
      localStores.put(storeName, store);
      return store;
    } catch (ClassNotFoundException | IllegalAccessException | InvocationTargetException |
        InstantiationException e) {
      throw new CarbonException(e);
    }
  }

  private static CarbonStore createCarbonStore(StoreConf storeConf, String className)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException,
      InvocationTargetException {
    Constructor[] constructor = Class.forName(className).getDeclaredConstructors();
    constructor[0].setAccessible(true);
    return (CarbonStore) constructor[0].newInstance(storeConf);
  }
}
