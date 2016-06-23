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

package org.carbondata.core.datastorage.util;

import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.NodeMeasureDataStore;
import org.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.carbondata.core.datastorage.store.impl.data.compressed.HeavyCompressedDoubleArrayDataInMemoryStore;

import org.carbondata.core.datastorage.store.impl.data.uncompressed.DoubleArrayDataInMemoryStore;
import org.carbondata.core.util.CarbonProperties;

public final class StoreFactory {
  /**
   * Double array data store.
   */
  private static final String COMPRESSED_DOUBLE_ARRAY = "COMPRESSED_DOUBLE_ARRAY";
  /**
   * value type.
   */
  private static StoreType valueType;

  static {
    String valuetype = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.VALUESTORE_TYPE,
            CarbonCommonConstants.VALUESTORE_TYPE_DEFAULT_VAL);

    // set value type
    if (COMPRESSED_DOUBLE_ARRAY.equals(valuetype)) {
      valueType = StoreType.COMPRESSED_DOUBLE_ARRAY;
    } else {
      valueType = StoreType.HEAVY_VALUE_COMPRESSION;
    }
  }

  private StoreFactory() {

  }

  public static NodeMeasureDataStore createDataStore(ValueCompressionModel compressionModel) {
    switch (valueType) {
      case COMPRESSED_DOUBLE_ARRAY:
        return new DoubleArrayDataInMemoryStore(compressionModel);

      case HEAVY_VALUE_COMPRESSION:
        return new HeavyCompressedDoubleArrayDataInMemoryStore(compressionModel);
      default:
        return new HeavyCompressedDoubleArrayDataInMemoryStore(compressionModel);
    }
  }

  /**
   * enum defined.
   */
  public enum StoreType {
    COMPRESSED_SINGLE_ARRAY,
    COMPRESSED_DOUBLE_ARRAY,
    HEAVY_VALUE_COMPRESSION
  }

}
