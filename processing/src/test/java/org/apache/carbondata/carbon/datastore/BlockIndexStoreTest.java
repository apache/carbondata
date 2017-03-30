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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.datastore.BlockIndexStore;
import org.apache.carbondata.core.datastore.block.AbstractIndex;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.block.TableBlockUniqueIdentifier;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.processing.StoreCreator;

import junit.framework.TestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class BlockIndexStoreTest extends TestCase {

  // private BlockIndexStore indexStore;
  BlockIndexStore<TableBlockUniqueIdentifier, AbstractIndex> cache;

  private String property;

  private static final LogService LOGGER =
          LogServiceFactory.getLogService(BlockIndexStoreTest.class.getName());

  @BeforeClass public void setUp() {
	property = CarbonProperties.getInstance().getProperty(CarbonCommonConstants.CARBON_DATA_FILE_VERSION);
	
	CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_DATA_FILE_VERSION, "1");
    StoreCreator.createCarbonStore();
    CarbonProperties.getInstance().
        addProperty(CarbonCommonConstants.CARBON_MAX_DRIVER_LRU_CACHE_SIZE, "10");
    CacheProvider cacheProvider = CacheProvider.getInstance();
    cache = (BlockIndexStore) cacheProvider.createCache(CacheType.EXECUTOR_BTREE, "");
  }
  
  @AfterClass public void tearDown() {
	    if(null!=property) {
		CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_DATA_FILE_VERSION, property);
	    }else {
	    	CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_DATA_FILE_VERSION, CarbonCommonConstants.CARBON_DATA_FILE_DEFAULT_VERSION);
	    }
	  }

  @Test public void testLoadAndGetTaskIdToSegmentsMapForSingleSegment()
      throws IOException {
    String canonicalPath =
        new File(this.getClass().getResource("/").getPath() + "/../../").getCanonicalPath();
    File file = getPartFile();
    TableBlockInfo info =
        new TableBlockInfo(file.getAbsolutePath(), 0, "0", new String[] { "loclhost" },
            file.length(), ColumnarFormatVersion.V1);
    CarbonTableIdentifier carbonTableIdentifier =
            new CarbonTableIdentifier(CarbonCommonConstants.DATABASE_DEFAULT_NAME, "t3", "1");
    AbsoluteTableIdentifier absoluteTableIdentifier =
        new AbsoluteTableIdentifier("/src/test/resources", carbonTableIdentifier);
    try {

      List<TableBlockUniqueIdentifier> tableBlockInfoList =
          getTableBlockUniqueIdentifierList(Arrays.asList(new TableBlockInfo[] { info }), absoluteTableIdentifier);
      List<AbstractIndex> loadAndGetBlocks = cache.getAll(tableBlockInfoList);
      assertTrue(loadAndGetBlocks.size() == 1);
    } catch (Exception e) {
      assertTrue(false);
    }
    List<String> segmentIds = new ArrayList<>();
      segmentIds.add(info.getSegmentId());
    cache.removeTableBlocks(segmentIds, absoluteTableIdentifier);
  }

  private List<TableBlockUniqueIdentifier> getTableBlockUniqueIdentifierList(List<TableBlockInfo> tableBlockInfos,
      AbsoluteTableIdentifier absoluteTableIdentifier) {
    List<TableBlockUniqueIdentifier> tableBlockUniqueIdentifiers = new ArrayList<>();
    for (TableBlockInfo tableBlockInfo : tableBlockInfos) {
      tableBlockUniqueIdentifiers.add(new TableBlockUniqueIdentifier(absoluteTableIdentifier, tableBlockInfo));
    }
    return tableBlockUniqueIdentifiers;
  }

  @Test public void testloadAndGetTaskIdToSegmentsMapForSameBlockLoadedConcurrently()
      throws IOException {
    String canonicalPath =
        new File(this.getClass().getResource("/").getPath() + "/../../").getCanonicalPath();
    File file = getPartFile();
    TableBlockInfo info =
        new TableBlockInfo(file.getAbsolutePath(), 0, "0", new String[] { "loclhost" },
            file.length(), ColumnarFormatVersion.V1);
    TableBlockInfo info1 =
        new TableBlockInfo(file.getAbsolutePath(), 0, "0", new String[] { "loclhost" },
            file.length(), ColumnarFormatVersion.V1);

    TableBlockInfo info2 =
        new TableBlockInfo(file.getAbsolutePath(), 0, "1", new String[] { "loclhost" },
            file.length(), ColumnarFormatVersion.V1);
    TableBlockInfo info3 =
        new TableBlockInfo(file.getAbsolutePath(), 0, "1", new String[] { "loclhost" },
            file.length(), ColumnarFormatVersion.V1);
    TableBlockInfo info4 =
        new TableBlockInfo(file.getAbsolutePath(), 0, "1", new String[] { "loclhost" },
            file.length(), ColumnarFormatVersion.V1);

    CarbonTableIdentifier carbonTableIdentifier =
            new CarbonTableIdentifier(CarbonCommonConstants.DATABASE_DEFAULT_NAME, "t3", "1");
    AbsoluteTableIdentifier absoluteTableIdentifier =
        new AbsoluteTableIdentifier("/src/test/resources", carbonTableIdentifier);
    ExecutorService executor = Executors.newFixedThreadPool(3);
    executor.submit(new BlockLoaderThread(Arrays.asList(new TableBlockInfo[] { info, info1 }),
        absoluteTableIdentifier));
    executor.submit(
        new BlockLoaderThread(Arrays.asList(new TableBlockInfo[] { info2, info3, info4 }),
            absoluteTableIdentifier));
    executor.submit(new BlockLoaderThread(Arrays.asList(new TableBlockInfo[] { info, info1 }),
        absoluteTableIdentifier));
    executor.submit(
        new BlockLoaderThread(Arrays.asList(new TableBlockInfo[] { info2, info3, info4 }),
            absoluteTableIdentifier));
    executor.shutdown();
    try {
      executor.awaitTermination(1, TimeUnit.DAYS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    List<TableBlockInfo> tableBlockInfos =
        Arrays.asList(new TableBlockInfo[] { info, info1, info2, info3, info4 });
    try {
      List<TableBlockUniqueIdentifier> tableBlockUniqueIdentifiers =
          getTableBlockUniqueIdentifierList(tableBlockInfos, absoluteTableIdentifier);
      List<AbstractIndex> loadAndGetBlocks = cache.getAll(tableBlockUniqueIdentifiers);
      assertTrue(loadAndGetBlocks.size() == 5);
    } catch (Exception e) {
      assertTrue(false);
    }
    List<String> segmentIds = new ArrayList<>();
    for (TableBlockInfo tableBlockInfo : tableBlockInfos) {
      segmentIds.add(tableBlockInfo.getSegmentId());
    }
    cache.removeTableBlocks(segmentIds, absoluteTableIdentifier);
  }

  @Test public void testloadAndGetTaskIdToSegmentsMapForDifferentSegmentLoadedConcurrently()
      throws IOException {
    String canonicalPath =
        new File(this.getClass().getResource("/").getPath() + "/../../").getCanonicalPath();
    File file = getPartFile();
    TableBlockInfo info =
        new TableBlockInfo(file.getAbsolutePath(), 0, "0", new String[] { "loclhost" },
            file.length(), ColumnarFormatVersion.V1);
    TableBlockInfo info1 =
        new TableBlockInfo(file.getAbsolutePath(), 0, "0", new String[] { "loclhost" },
            file.length(), ColumnarFormatVersion.V1);

    TableBlockInfo info2 =
        new TableBlockInfo(file.getAbsolutePath(), 0, "1", new String[] { "loclhost" },
            file.length(), ColumnarFormatVersion.V1);
    TableBlockInfo info3 =
        new TableBlockInfo(file.getAbsolutePath(), 0, "1", new String[] { "loclhost" },
            file.length(), ColumnarFormatVersion.V1);
    TableBlockInfo info4 =
        new TableBlockInfo(file.getAbsolutePath(), 0, "1", new String[] { "loclhost" },
            file.length(), ColumnarFormatVersion.V1);

    TableBlockInfo info5 =
        new TableBlockInfo(file.getAbsolutePath(), 0, "2", new String[] { "loclhost" },
            file.length(),ColumnarFormatVersion.V1);
    TableBlockInfo info6 =
        new TableBlockInfo(file.getAbsolutePath(), 0, "2", new String[] { "loclhost" },
            file.length(), ColumnarFormatVersion.V1);

    TableBlockInfo info7 =
        new TableBlockInfo(file.getAbsolutePath(), 0, "3", new String[] { "loclhost" },
            file.length(), ColumnarFormatVersion.V1);

    CarbonTableIdentifier carbonTableIdentifier =
            new CarbonTableIdentifier(CarbonCommonConstants.DATABASE_DEFAULT_NAME, "t3", "1");
    AbsoluteTableIdentifier absoluteTableIdentifier =
        new AbsoluteTableIdentifier("/src/test/resources", carbonTableIdentifier);
    ExecutorService executor = Executors.newFixedThreadPool(3);
    executor.submit(new BlockLoaderThread(Arrays.asList(new TableBlockInfo[] { info, info1 }),
        absoluteTableIdentifier));
    executor.submit(
        new BlockLoaderThread(Arrays.asList(new TableBlockInfo[] { info2, info3, info4 }),
            absoluteTableIdentifier));
    executor.submit(new BlockLoaderThread(Arrays.asList(new TableBlockInfo[] { info5, info6 }),
        absoluteTableIdentifier));
    executor.submit(new BlockLoaderThread(Arrays.asList(new TableBlockInfo[] { info7 }),
        absoluteTableIdentifier));

    executor.shutdown();
    try {
      executor.awaitTermination(1, TimeUnit.DAYS);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    List<TableBlockInfo> tableBlockInfos = Arrays
        .asList(new TableBlockInfo[] { info, info1, info2, info3, info4, info5, info6, info7 });
    try {
      List<TableBlockUniqueIdentifier> blockUniqueIdentifierList =
          getTableBlockUniqueIdentifierList(tableBlockInfos, absoluteTableIdentifier);
      List<AbstractIndex> loadAndGetBlocks = cache.getAll(blockUniqueIdentifierList);
      assertTrue(loadAndGetBlocks.size() == 8);
    } catch (Exception e) {
      assertTrue(false);
    }
    List<String> segmentIds = new ArrayList<>();
    for (TableBlockInfo tableBlockInfo : tableBlockInfos) {
      segmentIds.add(tableBlockInfo.getSegmentId());
    }
    cache.removeTableBlocks(segmentIds, absoluteTableIdentifier);
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
    String path = StoreCreator.getAbsoluteTableIdentifier().getStorePath() + "/" + StoreCreator
        .getAbsoluteTableIdentifier().getCarbonTableIdentifier().getDatabaseName() + "/"
        + StoreCreator.getAbsoluteTableIdentifier().getCarbonTableIdentifier().getTableName()
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
