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

import java.lang.reflect.Method;
import java.util.BitSet;

import org.apache.carbondata.core.cache.dictionary.AbstractDictionaryCacheTest;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonImplicitDimension;
import org.apache.carbondata.core.scan.filter.executer.FilterExecuter;
import org.apache.carbondata.core.scan.filter.executer.ImplicitIncludeFilterExecutorImpl;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.apache.carbondata.core.util.ByteUtil;

import mockit.Mock;
import mockit.MockUp;
import org.junit.Before;
import org.junit.Test;

public class TestBlockletDataMap extends AbstractDictionaryCacheTest {

  ImplicitIncludeFilterExecutorImpl implicitIncludeFilterExecutor;
  @Before public void setUp() throws Exception {
    CarbonImplicitDimension carbonImplicitDimension =
        new CarbonImplicitDimension(0, CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_POSITIONID);
    DimColumnResolvedFilterInfo dimColumnEvaluatorInfo = new DimColumnResolvedFilterInfo();
    dimColumnEvaluatorInfo.setColumnIndex(0);
    dimColumnEvaluatorInfo.setRowIndex(0);
    dimColumnEvaluatorInfo.setDimension(carbonImplicitDimension);
    dimColumnEvaluatorInfo.setDimensionExistsInCurrentSilce(false);
    implicitIncludeFilterExecutor =
        new ImplicitIncludeFilterExecutorImpl(dimColumnEvaluatorInfo);
  }

  @Test public void testaddBlockBasedOnMinMaxValue() throws Exception {

    new MockUp<ImplicitIncludeFilterExecutorImpl>() {
      @Mock BitSet isFilterValuesPresentInBlockOrBlocklet(byte[][] maxValue, byte[][] minValue,
          String uniqueBlockPath, boolean[] isMinMaxSet) {
        BitSet bitSet = new BitSet(1);
        bitSet.set(8);
        return bitSet;
      }
    };

    BlockDataMap blockletDataMap = new BlockletDataMap();
    Method method = BlockDataMap.class
        .getDeclaredMethod("addBlockBasedOnMinMaxValue", FilterExecuter.class, byte[][].class,
            byte[][].class, boolean[].class, String.class, int.class);
    method.setAccessible(true);

    byte[][] minValue = { ByteUtil.toBytes("sfds") };
    byte[][] maxValue = { ByteUtil.toBytes("resa") };
    boolean[] minMaxFlag = new boolean[] {true};
    Object result = method
        .invoke(blockletDataMap, implicitIncludeFilterExecutor, minValue, maxValue, minMaxFlag,
            "/opt/store/default/carbon_table/Fact/Part0/Segment_0/part-0-0_batchno0-0-1514989110586.carbondata",
            0);
    assert ((boolean) result);
  }
}
