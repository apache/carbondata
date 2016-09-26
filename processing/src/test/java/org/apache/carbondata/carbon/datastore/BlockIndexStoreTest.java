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

package org.apache.carbondata.carbon.datastore;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.carbon.datastore.BlockIndexStore;
import org.apache.carbondata.core.carbon.datastore.block.AbstractIndex;
import org.apache.carbondata.core.carbon.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.carbon.datastore.exception.IndexBuilderException;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.test.util.StoreCreator;

import junit.framework.TestCase;
import org.junit.BeforeClass;
import org.junit.Test;

public class BlockIndexStoreTest extends TestCase {

  private BlockIndexStore indexStore;

  @BeforeClass public void setUp() {
    StoreCreator.createCarbonStore();
    indexStore = BlockIndexStore.getInstance();
  }

  @Test public void testloadAndGetTaskIdToSegmentsMapForSingleSegment() throws IOException {
    String canonicalPath =
        new File(this.getClass().getResource("/").getPath() + "/../../").getCanonicalPath();
    File file = getPartFile();
    TableBlockInfo info =
        new TableBlockInfo(file.getAbsolutePath(), 0, "0", new String[] { "loclhost" },
            file.length());
    CarbonTableIdentifier carbonTableIdentifier =
            new CarbonTableIdentifier(CarbonCommonConstants.DATABASE_DEFAULT_NAME, "t3", "1");
    AbsoluteTableIdentifier absoluteTableIdentifier =
        new AbsoluteTableIdentifier("/src/test/resources", carbonTableIdentifier);
    try {
      List<AbstractIndex> loadAndGetBlocks = indexStore
          .loadAndGetBlocks(Arrays.asList(new TableBlockInfo[] { info }), absoluteTableIdentifier);
      assertTrue(loadAndGetBlocks.size() == 1);
    } catch (IndexBuilderException e) {
      assertTrue(false);
    }
    indexStore.clear(absoluteTableIdentifier);
  }

  @Test public void testloadAndGetTaskIdToSegmentsMapForSameBlockLoadedConcurrently()
      throws IOException {
    String canonicalPath =
        new File(this.getClass().getResource("/").getPath() + "/../../").getCanonicalPath();
    File file = getPartFile();
    TableBlockInfo info =
        new TableBlockInfo(file.getAbsolutePath(), 0, "0", new String[] { "loclhost" },
            file.length());
    TableBlockInfo info1 =
        new TableBlockInfo(file.getAbsolutePath(), 0, "0", new String[] { "loclhost" },
            file.length());

    TableBlockInfo info2 =
        new TableBlockInfo(file.getAbsolutePath(), 0, "1", new String[] { "loclhost" },
            file.length());
    TableBlockInfo info3 =
        new TableBlockInfo(file.getAbsolutePath(), 0, "1", new String[] { "loclhost" },
            file.length());
    TableBlockInfo info4 =
        new TableBlockInfo(file.getAbsolutePath(), 0, "1", new String[] { "loclhost" },
            file.length());

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

    try {
      List<AbstractIndex> loadAndGetBlocks = indexStore.loadAndGetBlocks(
          Arrays.asList(new TableBlockInfo[] { info, info1, info2, info3, info4 }),
          absoluteTableIdentifier);
      assertTrue(loadAndGetBlocks.size() == 5);
    } catch (IndexBuilderException e) {
      assertTrue(false);
    }
    indexStore.clear(absoluteTableIdentifier);
  }

  @Test public void testloadAndGetTaskIdToSegmentsMapForDifferentSegmentLoadedConcurrently()
      throws IOException {
    String canonicalPath =
        new File(this.getClass().getResource("/").getPath() + "/../../").getCanonicalPath();
    File file = getPartFile();
    TableBlockInfo info =
        new TableBlockInfo(file.getAbsolutePath(), 0, "0", new String[] { "loclhost" },
            file.length());
    TableBlockInfo info1 =
        new TableBlockInfo(file.getAbsolutePath(), 0, "0", new String[] { "loclhost" },
            file.length());

    TableBlockInfo info2 =
        new TableBlockInfo(file.getAbsolutePath(), 0, "1", new String[] { "loclhost" },
            file.length());
    TableBlockInfo info3 =
        new TableBlockInfo(file.getAbsolutePath(), 0, "1", new String[] { "loclhost" },
            file.length());
    TableBlockInfo info4 =
        new TableBlockInfo(file.getAbsolutePath(), 0, "1", new String[] { "loclhost" },
            file.length());

    TableBlockInfo info5 =
        new TableBlockInfo(file.getAbsolutePath(), 0, "2", new String[] { "loclhost" },
            file.length());
    TableBlockInfo info6 =
        new TableBlockInfo(file.getAbsolutePath(), 0, "2", new String[] { "loclhost" },
            file.length());

    TableBlockInfo info7 =
        new TableBlockInfo(file.getAbsolutePath(), 0, "3", new String[] { "loclhost" },
            file.length());

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
    try {
      List<AbstractIndex> loadAndGetBlocks = indexStore.loadAndGetBlocks(Arrays
              .asList(new TableBlockInfo[] { info, info1, info2, info3, info4, info5, info6, info7 }),
          absoluteTableIdentifier);
      assertTrue(loadAndGetBlocks.size() == 8);
    } catch (IndexBuilderException e) {
      assertTrue(false);
    }
    indexStore.clear(absoluteTableIdentifier);
  }

  private class BlockLoaderThread implements Callable<Void> {
    private List<TableBlockInfo> tableBlockInfoList;
    private AbsoluteTableIdentifier absoluteTableIdentifier;

    public BlockLoaderThread(List<TableBlockInfo> tableBlockInfoList,
        AbsoluteTableIdentifier absoluteTableIdentifier) {
      // TODO Auto-generated constructor stub
      this.tableBlockInfoList = tableBlockInfoList;
      this.absoluteTableIdentifier = absoluteTableIdentifier;
    }

    @Override public Void call() throws Exception {
      indexStore.loadAndGetBlocks(tableBlockInfoList, absoluteTableIdentifier);
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
