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

import org.carbondata.core.datastorage.store.FileHolder;
import org.carbondata.core.datastorage.store.NodeKeyStore;
import org.carbondata.core.datastorage.store.NodeMeasureDataStore;
import org.carbondata.core.datastorage.store.columnar.ColumnarKeyStore;
import org.carbondata.core.datastorage.store.columnar.ColumnarKeyStoreInfo;
import org.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.carbondata.core.datastorage.store.impl.data.compressed.HeavyCompressedDoubleArrayDataFileStore;
import org.carbondata.core.datastorage.store.impl.data.compressed.HeavyCompressedDoubleArrayDataInMemoryStore;
import org.carbondata.core.datastorage.store.impl.data.uncompressed.DoubleArrayDataFileStore;
import org.carbondata.core.datastorage.store.impl.data.uncompressed.DoubleArrayDataInMemoryStore;
import org.carbondata.core.datastorage.store.impl.key.columnar.compressed.CompressedColumnarFileKeyStore;
import org.carbondata.core.datastorage.store.impl.key.columnar.compressed.CompressedColumnarInMemoryStore;
import org.carbondata.core.datastorage.store.impl.key.columnar.uncompressed.UnCompressedColumnarFileKeyStore;
import org.carbondata.core.datastorage.store.impl.key.columnar.uncompressed.UnCompressedColumnarInMemoryStore;
import org.carbondata.core.datastorage.store.impl.key.compressed.CompressedSingleArrayKeyFileStore;
import org.carbondata.core.datastorage.store.impl.key.compressed.CompressedSingleArrayKeyInMemoryStore;
import org.carbondata.core.datastorage.store.impl.key.uncompressed.SingleArrayKeyFileStore;
import org.carbondata.core.datastorage.store.impl.key.uncompressed.SingleArrayKeyInMemoryStore;

public final class StoreFactory {
  /**
   * key type.
   */
  private static StoreType keyType;
  /**
   * value type.
   */
  private static StoreType valueType;

  static {
    keyType = StoreType.COMPRESSED_SINGLE_ARRAY;
    valueType = StoreType.HEAVY_VALUE_COMPRESSION;
  }

  private StoreFactory() {

  }

  public static NodeKeyStore createKeyStore(int size, int elementSize, boolean isLeaf,
      boolean isFileStore, long offset, String fileName, int length, FileHolder fileHolder) {
    switch (keyType) {
      case SINGLE_ARRAY:

        if (isFileStore) {
          return new SingleArrayKeyFileStore(size, elementSize, offset, fileName, length);
        } else {
          return new SingleArrayKeyInMemoryStore(size, elementSize, offset, fileName, fileHolder,
              length);
        }
      default:

        if (isLeaf) {
          if (isFileStore) {
            return new CompressedSingleArrayKeyFileStore(size, elementSize, offset, fileName,
                length);
          } else {
            return new CompressedSingleArrayKeyInMemoryStore(size, elementSize, offset, fileName,
                fileHolder, length);
          }
        } else {
          if (isFileStore) {
            return new SingleArrayKeyFileStore(size, elementSize, offset, fileName, length);
          } else {
            return new SingleArrayKeyInMemoryStore(size, elementSize, offset, fileName, fileHolder,
                length);
          }
        }
    }
  }

  public static NodeKeyStore createKeyStore(int size, int elementSize, boolean isLeaf) {
    switch (keyType) {
      case SINGLE_ARRAY:

        return new SingleArrayKeyInMemoryStore(size, elementSize);

      default:

        if (isLeaf) {
          return new CompressedSingleArrayKeyInMemoryStore(size, elementSize);
        } else {
          return new SingleArrayKeyInMemoryStore(size, elementSize);
        }

    }
  }

  public static ColumnarKeyStore createColumnarKeyStore(ColumnarKeyStoreInfo columnarKeyStoreInfo,
      FileHolder fileHolder, boolean isFileStore) {
    switch (keyType) {
      case SINGLE_ARRAY:

        if (isFileStore) {
          return new UnCompressedColumnarFileKeyStore(columnarKeyStoreInfo);
        } else {
          return new UnCompressedColumnarInMemoryStore(columnarKeyStoreInfo, fileHolder);
        }
      default:

        if (isFileStore) {
          return new CompressedColumnarFileKeyStore(columnarKeyStoreInfo);
        } else {
          return new CompressedColumnarInMemoryStore(columnarKeyStoreInfo, fileHolder);
        }
    }
  }

  public static NodeMeasureDataStore createDataStore(boolean isFileStore,
      ValueCompressionModel compressionModel, long[] offset, int[] length, String filePath,
      FileHolder fileHolder) {
    switch (valueType) {

      case COMPRESSED_DOUBLE_ARRAY:

        if (isFileStore) {
          return new DoubleArrayDataFileStore(compressionModel, offset, filePath, length);
        } else {
          return new DoubleArrayDataInMemoryStore(compressionModel, offset, length, filePath,
              fileHolder);
        }

      case HEAVY_VALUE_COMPRESSION:

        if (isFileStore) {
          return new HeavyCompressedDoubleArrayDataFileStore(compressionModel, offset, length,
              filePath);
        } else {
          return new HeavyCompressedDoubleArrayDataInMemoryStore(compressionModel, offset, length,
              filePath, fileHolder);
        }
      default:

        if (isFileStore) {
          return new HeavyCompressedDoubleArrayDataFileStore(compressionModel, offset, length,
              filePath);
        } else {
          return new HeavyCompressedDoubleArrayDataInMemoryStore(compressionModel, offset, length,
              filePath, fileHolder);
        }

    }
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
    SINGLE_ARRAY,
    COMPRESSED_SINGLE_ARRAY,
    COMPRESSED_DOUBLE_ARRAY,
    HEAVY_VALUE_COMPRESSION
  }

}
