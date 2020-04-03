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

package org.apache.carbondata.core.indexstore.blockletindex;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.index.IndexInputSplit;
import org.apache.carbondata.core.index.Segment;
import org.apache.carbondata.core.indexstore.BlockletIndexWrapper;
import org.apache.carbondata.core.indexstore.TableBlockIndexUniqueIdentifier;
import org.apache.carbondata.core.indexstore.TableBlockIndexUniqueIdentifierWrapper;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.IndexSchema;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.readcommitter.TableStatusReadCommittedScope;

import mockit.Deencapsulation;
import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

public class TestBlockletIndexFactory {

  private CarbonTable carbonTable;

  private AbsoluteTableIdentifier absoluteTableIdentifier;

  private TableInfo tableInfo;

  private BlockletIndexFactory blockletIndexFactory;

  private TableBlockIndexUniqueIdentifier tableBlockIndexUniqueIdentifier;

  private TableBlockIndexUniqueIdentifierWrapper tableBlockIndexUniqueIdentifierWrapper;

  private Cache<TableBlockIndexUniqueIdentifierWrapper, BlockletIndexWrapper> cache;

  private TableSchema factTable;

  @Before public void setUp()
      throws ClassNotFoundException, IllegalAccessException, InvocationTargetException,
      InstantiationException {
    tableInfo = new TableInfo();
    factTable = new TableSchema();
    Constructor<?> constructor =
        Class.forName("org.apache.carbondata.core.metadata.schema.table.CarbonTable")
            .getDeclaredConstructors()[0];
    constructor.setAccessible(true);
    carbonTable = (CarbonTable) constructor.newInstance();
    absoluteTableIdentifier = AbsoluteTableIdentifier
        .from("/opt/store/default/carbon_table/", "default", "carbon_table",
            UUID.randomUUID().toString());
    Deencapsulation.setField(tableInfo, "identifier", absoluteTableIdentifier);
    Deencapsulation.setField(tableInfo, "factTable", factTable);
    Deencapsulation.setField(carbonTable, "tableInfo", tableInfo);
    new MockUp<CarbonTable>() {
      @Mock
      public AbsoluteTableIdentifier getAbsoluteTableIdentifier(){
        return absoluteTableIdentifier;
      }
    };
    blockletIndexFactory = new BlockletIndexFactory(carbonTable, new IndexSchema());
    Deencapsulation.setField(blockletIndexFactory, "cache",
        CacheProvider.getInstance().createCache(CacheType.DRIVER_BLOCKLET_INDEX));
    tableBlockIndexUniqueIdentifier =
        new TableBlockIndexUniqueIdentifier("/opt/store/default/carbon_table/Fact/Part0/Segment_0",
            "0_batchno0-0-1521012756709.carbonindex", null, "0");
    tableBlockIndexUniqueIdentifierWrapper =
        new TableBlockIndexUniqueIdentifierWrapper(tableBlockIndexUniqueIdentifier, carbonTable);
    cache = CacheProvider.getInstance().createCache(CacheType.DRIVER_BLOCKLET_INDEX);
  }

  @Test public void addDataMapToCache()
      throws IOException, MemoryException, NoSuchMethodException, InvocationTargetException,
      IllegalAccessException {
    List<BlockIndex> dataMaps = new ArrayList<>();
    Method method = BlockletIndexFactory.class
        .getDeclaredMethod("cache", TableBlockIndexUniqueIdentifierWrapper.class,
            BlockletIndexWrapper.class);
    method.setAccessible(true);
    method.invoke(blockletIndexFactory, tableBlockIndexUniqueIdentifierWrapper,
        new BlockletIndexWrapper(tableBlockIndexUniqueIdentifier.getSegmentId(), dataMaps));
    BlockletIndexWrapper result = cache.getIfPresent(tableBlockIndexUniqueIdentifierWrapper);
    assert null != result;
  }

  @Test public void getValidDistributables() throws IOException {
    BlockletIndexInputSplit blockletDataMapDistributable = new BlockletIndexInputSplit(
        "/opt/store/default/carbon_table/Fact/Part0/Segment_0/0_batchno0-0-1521012756709.carbonindex");
    Segment segment = new Segment("0", null, new TableStatusReadCommittedScope(carbonTable
        .getAbsoluteTableIdentifier(), new Configuration(false)));
    blockletDataMapDistributable.setSegment(segment);
    BlockletIndexInputSplit blockletDataMapDistributable1 = new BlockletIndexInputSplit(
        "/opt/store/default/carbon_table/Fact/Part0/Segment_0/0_batchno0-0-1521012756701.carbonindex");
    blockletDataMapDistributable1.setSegment(segment);
    List<IndexInputSplit> indexInputSplits = new ArrayList<>(2);
    indexInputSplits.add(blockletDataMapDistributable);
    indexInputSplits.add(blockletDataMapDistributable1);
    new MockUp<BlockletIndexFactory>() {
      @Mock Set<TableBlockIndexUniqueIdentifier> getTableBlockIndexUniqueIdentifiers(
          Segment segment) {
        TableBlockIndexUniqueIdentifier tableBlockIndexUniqueIdentifier1 =
            new TableBlockIndexUniqueIdentifier(
                "/opt/store/default/carbon_table/Fact/Part0/Segment_0",
                "0_batchno0-0-1521012756701.carbonindex", null, "0");
        Set<TableBlockIndexUniqueIdentifier> tableBlockIndexUniqueIdentifiers = new HashSet<>(3);
        tableBlockIndexUniqueIdentifiers.add(tableBlockIndexUniqueIdentifier);
        tableBlockIndexUniqueIdentifiers.add(tableBlockIndexUniqueIdentifier1);
        return tableBlockIndexUniqueIdentifiers;
      }
    };
    List<IndexInputSplit> validDistributables =
        blockletIndexFactory.getAllUncachedDistributables(indexInputSplits);
    assert 1 == validDistributables.size();
  }
}