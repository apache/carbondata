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

package org.apache.carbondata.core.datastorage.util;

import org.apache.carbondata.core.datastorage.store.NodeMeasureDataStore;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.apache.carbondata.core.datastorage.store.impl.data.compressed.HeavyCompressedDoubleArrayDataInMemoryStore;
import org.apache.carbondata.core.datastorage.store.impl.data.uncompressed.DoubleArrayDataInMemoryStore;

public final class StoreFactory {
  /**
   * value type.
   */
  private static StoreType valueType;

  static {
    valueType = StoreType.HEAVY_VALUE_COMPRESSION;
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
