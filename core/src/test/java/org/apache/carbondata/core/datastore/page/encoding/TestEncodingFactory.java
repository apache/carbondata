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

package org.apache.carbondata.core.datastore.page.encoding;

import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.page.encoding.adaptive.AdaptiveDeltaIntegralCodec;
import org.apache.carbondata.core.datastore.page.encoding.adaptive.AdaptiveIntegralCodec;
import org.apache.carbondata.core.datastore.page.encoding.compress.DirectCompressCodec;
import org.apache.carbondata.core.datastore.page.statistics.PrimitivePageStatsCollector;
import org.apache.carbondata.core.metadata.datatype.DataTypes;

import junit.framework.TestCase;
import org.junit.Test;

 /**
 * The class is meant to test the different type of ColumnPageCodec
 * base datatype and min and max values.
 */
public class TestEncodingFactory extends TestCase {

  @Test public void testSelectProperDeltaType() {
    PrimitivePageStatsCollector primitivePageStatsCollector =
        PrimitivePageStatsCollector.newInstance(DataTypes.LONG);
    // for Byte
    primitivePageStatsCollector.update((long) Byte.MAX_VALUE);
    ColumnPageCodec columnPageCodec =
        DefaultEncodingFactory.selectCodecByAlgorithmForIntegral(primitivePageStatsCollector, false, null);
    assert (columnPageCodec instanceof AdaptiveIntegralCodec);
    assert (DataTypes.BYTE == ((AdaptiveIntegralCodec) columnPageCodec).getTargetDataType());
    // for Short
    primitivePageStatsCollector.update((long) Short.MAX_VALUE);
    columnPageCodec =
        DefaultEncodingFactory.selectCodecByAlgorithmForIntegral(primitivePageStatsCollector, false, null);
    assert (columnPageCodec instanceof AdaptiveIntegralCodec);
    assert (DataTypes.SHORT == ((AdaptiveIntegralCodec) columnPageCodec).getTargetDataType());
    // for int
    primitivePageStatsCollector.update((long) Integer.MAX_VALUE);
    columnPageCodec =
        DefaultEncodingFactory.selectCodecByAlgorithmForIntegral(primitivePageStatsCollector, false, null);
    assert (columnPageCodec instanceof AdaptiveIntegralCodec);
    assert (DataTypes.INT == ((AdaptiveIntegralCodec) columnPageCodec).getTargetDataType());
    // for long
    primitivePageStatsCollector.update(Long.MAX_VALUE);
    columnPageCodec =
        DefaultEncodingFactory.selectCodecByAlgorithmForIntegral(primitivePageStatsCollector, false, null);
    assert (columnPageCodec instanceof DirectCompressCodec);
    assert ("DirectCompressCodec".equals(columnPageCodec.getName()));
  }

  @Test public void testSelectProperDeltaType2() {
    PrimitivePageStatsCollector primitivePageStatsCollector =
        PrimitivePageStatsCollector.newInstance(DataTypes.LONG);
    // for Byte
    primitivePageStatsCollector.update((long) 200);
    ColumnPageCodec columnPageCodec =
        DefaultEncodingFactory.selectCodecByAlgorithmForIntegral(primitivePageStatsCollector, false, null);
    assert (columnPageCodec instanceof AdaptiveDeltaIntegralCodec);
    assert (DataTypes.BYTE == ((AdaptiveDeltaIntegralCodec) columnPageCodec).getTargetDataType());
    // for Short
    primitivePageStatsCollector.update((long) 634767);
    columnPageCodec =
        DefaultEncodingFactory.selectCodecByAlgorithmForIntegral(primitivePageStatsCollector, false, null);
    assert (columnPageCodec instanceof AdaptiveIntegralCodec);
    assert (DataTypes.SHORT_INT == ((AdaptiveIntegralCodec) columnPageCodec).getTargetDataType());
    // for int
    primitivePageStatsCollector.update((long) (Integer.MAX_VALUE + 200));
    columnPageCodec =
        DefaultEncodingFactory.selectCodecByAlgorithmForIntegral(primitivePageStatsCollector, false, null);
    assert (columnPageCodec instanceof AdaptiveIntegralCodec);
    assert (DataTypes.INT == ((AdaptiveIntegralCodec) columnPageCodec).getTargetDataType());
    // for int
    primitivePageStatsCollector.update(Long.MAX_VALUE);
    columnPageCodec =
        DefaultEncodingFactory.selectCodecByAlgorithmForIntegral(primitivePageStatsCollector, false, null);
    assert (columnPageCodec instanceof DirectCompressCodec);
    assert ("DirectCompressCodec".equals(columnPageCodec.getName()));
  }
}
