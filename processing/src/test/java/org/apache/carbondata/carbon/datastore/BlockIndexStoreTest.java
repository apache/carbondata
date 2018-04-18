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

package org.apache.carbondata.carbon.datastore;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.BlockIndexStore;
import org.apache.carbondata.core.datastore.block.AbstractIndex;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.block.TableBlockUniqueIdentifier;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.processing.StoreCreator;

import junit.framework.TestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class BlockIndexStoreTest extends TestCase {

  // private BlockIndexStore indexStore;
  BlockIndexStore<TableBlockUniqueIdentifier, AbstractIndex> cache;

  private static final LogService LOGGER =
          LogServiceFactory.getLogService(BlockIndexStoreTest.class.getName());

  @BeforeClass public void setUp() {
    StoreCreator.createCarbonStore();
    CarbonProperties.getInstance().
        addProperty(CarbonCommonConstants.CARBON_MAX_DRIVER_LRU_CACHE_SIZE, "10");
    CacheProvider cacheProvider = CacheProvider.getInstance();
    cache = (BlockIndexStore) cacheProvider.createCache(CacheType.EXECUTOR_BTREE);
  }

  @AfterClass public void tearDown() {
  }

  @Test public void testEmpty() {

  }

  private List<TableBlockUniqueIdentifier> getTableBlockUniqueIdentifierList(List<TableBlockInfo> tableBlockInfos,
      AbsoluteTableIdentifier absoluteTableIdentifier) {
    List<TableBlockUniqueIdentifier> tableBlockUniqueIdentifiers = new ArrayList<>();
    for (TableBlockInfo tableBlockInfo : tableBlockInfos) {
      tableBlockUniqueIdentifiers.add(new TableBlockUniqueIdentifier(absoluteTableIdentifier, tableBlockInfo));
    }
    return tableBlockUniqueIdentifiers;
  }

  private class BlockLoaderThread implements Callable<Void> {
    private List<TableBlockInfo> tableBlockInfoList;
    private AbsoluteTableIdentifier absoluteTableIdentifier;

    public BlockLoaderThread(List<TableBlockInfo> tableBlockInfoList,
        AbsoluteTableIdentifier absoluteTableIdentifier) {
      this.tableBlockInfoList = tableBlockInfoList;
      this.absoluteTableIdentifier = absoluteTableIdentifier;
    }

    @Override public Void call() throws Exception {
      List<TableBlockUniqueIdentifier> tableBlockUniqueIdentifierList =
          getTableBlockUniqueIdentifierList(tableBlockInfoList, absoluteTableIdentifier);
      cache.getAll(tableBlockUniqueIdentifierList);
      return null;
    }

  }

  private static File getPartFile() {
    String path = StoreCreator.getIdentifier().getTablePath()
        + "/Fact/Part0/Segment_0";
    File file = new File(path);
    File[] files = file.listFiles();
    File part = null;
    for (int i = 0; i < files.length; i++) {
      if (files[i].getName().startsWith("part")) {
        part = files[i];
      }
    }
    return part;
  }

}
