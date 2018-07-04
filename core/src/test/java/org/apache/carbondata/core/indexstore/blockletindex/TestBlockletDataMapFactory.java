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
import org.apache.carbondata.core.datamap.DataMapDistributable;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.indexstore.BlockletDataMapIndexWrapper;
import org.apache.carbondata.core.indexstore.TableBlockIndexUniqueIdentifier;
import org.apache.carbondata.core.indexstore.TableBlockIndexUniqueIdentifierWrapper;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;

import mockit.Deencapsulation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Before;
import org.junit.Test;

public class TestBlockletDataMapFactory {

  private CarbonTable carbonTable;

  private AbsoluteTableIdentifier absoluteTableIdentifier;

  private TableInfo tableInfo;

  private BlockletDataMapFactory blockletDataMapFactory;

  private TableBlockIndexUniqueIdentifier tableBlockIndexUniqueIdentifier;

  private TableBlockIndexUniqueIdentifierWrapper tableBlockIndexUniqueIdentifierWrapper;

  private Cache<TableBlockIndexUniqueIdentifierWrapper, BlockletDataMapIndexWrapper> cache;

  @Before public void setUp()
      throws ClassNotFoundException, IllegalAccessException, InvocationTargetException,
      InstantiationException {
    tableInfo = new TableInfo();
    Constructor<?> constructor =
        Class.forName("org.apache.carbondata.core.metadata.schema.table.CarbonTable")
            .getDeclaredConstructors()[0];
    constructor.setAccessible(true);
    carbonTable = (CarbonTable) constructor.newInstance();
    absoluteTableIdentifier = AbsoluteTableIdentifier
        .from("/opt/store/default/carbon_table/", "default", "carbon_table",
            UUID.randomUUID().toString());
    Deencapsulation.setField(tableInfo, "identifier", absoluteTableIdentifier);
    Deencapsulation.setField(carbonTable, "tableInfo", tableInfo);
    new MockUp<CarbonTable>() {
      @Mock
      public AbsoluteTableIdentifier getAbsoluteTableIdentifier(){
        return absoluteTableIdentifier;
      }
    };
    blockletDataMapFactory = new BlockletDataMapFactory(carbonTable, new DataMapSchema());
    Deencapsulation.setField(blockletDataMapFactory, "cache",
        CacheProvider.getInstance().createCache(CacheType.DRIVER_BLOCKLET_DATAMAP));
    tableBlockIndexUniqueIdentifier =
        new TableBlockIndexUniqueIdentifier("/opt/store/default/carbon_table/Fact/Part0/Segment_0",
            "0_batchno0-0-1521012756709.carbonindex", null, "0");
    tableBlockIndexUniqueIdentifierWrapper =
        new TableBlockIndexUniqueIdentifierWrapper(tableBlockIndexUniqueIdentifier, carbonTable);
    cache = CacheProvider.getInstance().createCache(CacheType.DRIVER_BLOCKLET_DATAMAP);
  }

  @Test public void addDataMapToCache()
      throws IOException, MemoryException, NoSuchMethodException, InvocationTargetException,
      IllegalAccessException {
    List<BlockDataMap> dataMaps = new ArrayList<>();
    Method method = BlockletDataMapFactory.class
        .getDeclaredMethod("cache", TableBlockIndexUniqueIdentifierWrapper.class,
            BlockletDataMapIndexWrapper.class);
    method.setAccessible(true);
    method.invoke(blockletDataMapFactory, tableBlockIndexUniqueIdentifierWrapper,
        new BlockletDataMapIndexWrapper(tableBlockIndexUniqueIdentifier.getSegmentId(), dataMaps));
    BlockletDataMapIndexWrapper result = cache.getIfPresent(tableBlockIndexUniqueIdentifierWrapper);
    assert null != result;
  }

  @Test public void getValidDistributables() throws IOException {
    BlockletDataMapDistributable blockletDataMapDistributable = new BlockletDataMapDistributable(
        "/opt/store/default/carbon_table/Fact/Part0/Segment_0/0_batchno0-0-1521012756709.carbonindex");
    Segment segment = new Segment("0", null);
    blockletDataMapDistributable.setSegment(segment);
    BlockletDataMapDistributable blockletDataMapDistributable1 = new BlockletDataMapDistributable(
        "/opt/store/default/carbon_table/Fact/Part0/Segment_0/0_batchno0-0-1521012756701.carbonindex");
    blockletDataMapDistributable1.setSegment(segment);
    List<DataMapDistributable> dataMapDistributables = new ArrayList<>(2);
    dataMapDistributables.add(blockletDataMapDistributable);
    dataMapDistributables.add(blockletDataMapDistributable1);
    new MockUp<BlockletDataMapFactory>() {
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
    List<DataMapDistributable> validDistributables =
        blockletDataMapFactory.getAllUncachedDistributables(dataMapDistributables);
    assert 1 == validDistributables.size();
  }
}