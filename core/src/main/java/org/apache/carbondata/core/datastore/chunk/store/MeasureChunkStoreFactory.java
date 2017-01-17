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

package org.apache.carbondata.core.datastore.chunk.store;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.chunk.store.impl.safe.SafeByteMeasureChunkStore;
import org.apache.carbondata.core.datastore.chunk.store.impl.safe.SafeDoubleMeasureChunkStore;
import org.apache.carbondata.core.datastore.chunk.store.impl.safe.SafeIntMeasureChunkStore;
import org.apache.carbondata.core.datastore.chunk.store.impl.safe.SafeLongMeasureChunkStore;
import org.apache.carbondata.core.datastore.chunk.store.impl.safe.SafeShortMeasureChunkStore;
import org.apache.carbondata.core.datastore.chunk.store.impl.unsafe.UnsafeByteMeasureChunkStore;
import org.apache.carbondata.core.datastore.chunk.store.impl.unsafe.UnsafeDoubleMeasureChunkStore;
import org.apache.carbondata.core.datastore.chunk.store.impl.unsafe.UnsafeIntMeasureChunkStore;
import org.apache.carbondata.core.datastore.chunk.store.impl.unsafe.UnsafeLongMeasureChunkStore;
import org.apache.carbondata.core.datastore.chunk.store.impl.unsafe.UnsafeShortMeasureChunkStore;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.ValueCompressionUtil.DataType;

/**
 * Factory class for getting the measure store type
 */
public class MeasureChunkStoreFactory {

  /**
   * instance type
   */
  public static final MeasureChunkStoreFactory INSTANCE = new MeasureChunkStoreFactory();

  /**
   * is unsafe
   */
  private static final boolean isUnsafe;

  static {
    isUnsafe = Boolean.parseBoolean(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.ENABLE_UNSAFE_IN_QUERY_EXECUTION,
            CarbonCommonConstants.ENABLE_UNSAFE_IN_QUERY_EXECUTION_DEFAULTVALUE));
  }

  private MeasureChunkStoreFactory() {
  }

  /**
   * Below method will be used to get the measure data chunk store based on data type
   *
   * @param dataType     data type
   * @param numberOfRows number of rows
   * @return measure chunk store
   */
  public MeasureDataChunkStore getMeasureDataChunkStore(DataType dataType, int numberOfRows) {
    if (!isUnsafe) {
      switch (dataType) {
        case DATA_BYTE:
          return new SafeByteMeasureChunkStore(numberOfRows);
        case DATA_SHORT:
          return new SafeShortMeasureChunkStore(numberOfRows);
        case DATA_INT:
          return new SafeIntMeasureChunkStore(numberOfRows);
        case DATA_LONG:
          return new SafeLongMeasureChunkStore(numberOfRows);
        case DATA_DOUBLE:
          return new SafeDoubleMeasureChunkStore(numberOfRows);
        default:
          return new SafeDoubleMeasureChunkStore(numberOfRows);
      }
    } else {
      switch (dataType) {
        case DATA_BYTE:
          return new UnsafeByteMeasureChunkStore(numberOfRows);
        case DATA_SHORT:
          return new UnsafeShortMeasureChunkStore(numberOfRows);
        case DATA_INT:
          return new UnsafeIntMeasureChunkStore(numberOfRows);
        case DATA_LONG:
          return new UnsafeLongMeasureChunkStore(numberOfRows);
        case DATA_DOUBLE:
          return new UnsafeDoubleMeasureChunkStore(numberOfRows);
        default:
          return new UnsafeDoubleMeasureChunkStore(numberOfRows);
      }
    }
  }
}
