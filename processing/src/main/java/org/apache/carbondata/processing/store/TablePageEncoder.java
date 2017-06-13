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

package org.apache.carbondata.processing.store;

import java.security.Key;
import java.util.Iterator;

import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.columnar.BlockIndexerStorageForInt;
import org.apache.carbondata.core.datastore.columnar.BlockIndexerStorageForNoInvertedIndex;
import org.apache.carbondata.core.datastore.columnar.BlockIndexerStorageForNoInvertedIndexForShort;
import org.apache.carbondata.core.datastore.columnar.BlockIndexerStorageForShort;
import org.apache.carbondata.core.datastore.columnar.ColGroupBlockStorage;
import org.apache.carbondata.core.datastore.columnar.IndexStorage;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageCodec;
import org.apache.carbondata.core.datastore.page.encoding.DefaultEncodingStrategy;
import org.apache.carbondata.core.datastore.page.encoding.EncodedData;
import org.apache.carbondata.core.datastore.page.encoding.EncodingStrategy;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonProperties;

public class TablePageEncoder {

  private ColumnarFormatVersion version;

  private boolean[] isUseInvertedIndex;

  private CarbonFactDataHandlerModel model;

  private static final EncodingStrategy encodingStrategy = new DefaultEncodingStrategy();

  public TablePageEncoder(CarbonFactDataHandlerModel model) {
    this.version = CarbonProperties.getInstance().getFormatVersion();
    this.model = model;
    this.isUseInvertedIndex = model.getIsUseInvertedIndex();
  }

  // function to apply all columns in one table page
  public EncodedData encode(TablePage tablePage) throws KeyGenException {
    EncodedData encodedData = new EncodedData();
    encodeAndCompressDimensions(tablePage, encodedData);
    encodeAndCompressMeasures(tablePage, encodedData);
    return encodedData;
  }

  // apply measure and set encodedData in `encodedData`
  private void encodeAndCompressMeasures(TablePage tablePage, EncodedData encodedData) {
    ColumnPage[] measurePage = tablePage.getMeasurePage();
    byte[][] encodedMeasures = new byte[measurePage.length][];
    for (int i = 0; i < measurePage.length; i++) {
      ColumnPageCodec encoder = encodingStrategy.createCodec(measurePage[i].getStatistics());
      encodedMeasures[i] = encoder.encode(measurePage[i]);
    }
    encodedData.measures = encodedMeasures;
  }

  private IndexStorage encodeAndCompressDictDimension(byte[][] data, boolean isSort,
      boolean isUseInvertedIndex) throws KeyGenException {
    if (isUseInvertedIndex) {
      if (version == ColumnarFormatVersion.V3) {
        return new BlockIndexerStorageForShort(data, true, false, isSort);
      } else {
        return new BlockIndexerStorageForInt(data, true, false, isSort);
      }
    } else {
      if (version == ColumnarFormatVersion.V3) {
        return new BlockIndexerStorageForNoInvertedIndexForShort(data, false);
      } else {
        return new BlockIndexerStorageForNoInvertedIndex(data);
      }
    }
  }

  private IndexStorage encodeAndCompressDirectDictDimension(byte[][] data, boolean isSort,
      boolean isUseInvertedIndex) throws KeyGenException {
    if (isUseInvertedIndex) {
      if (version == ColumnarFormatVersion.V3) {
        return new BlockIndexerStorageForShort(data, false, false, isSort);
      } else {
        return new BlockIndexerStorageForInt(data, false, false, isSort);
      }
    } else {
      if (version == ColumnarFormatVersion.V3) {
        return new BlockIndexerStorageForNoInvertedIndexForShort(data, false);
      } else {
        return new BlockIndexerStorageForNoInvertedIndex(data);
      }
    }
  }

  private IndexStorage encodeAndCompressComplexDimension(byte[][] data) {
    if (version == ColumnarFormatVersion.V3) {
      return new BlockIndexerStorageForShort(data, false, false, false);
    } else {
      return new BlockIndexerStorageForInt(data, false, false, false);
    }
  }

  private IndexStorage encodeAndCompressNoDictDimension(byte[][] data, boolean isSort,
      boolean isUseInvertedIndex) {
    if (isUseInvertedIndex) {
      if (version == ColumnarFormatVersion.V3) {
        return new BlockIndexerStorageForShort(data, false, true, isSort);
      } else {
        return new BlockIndexerStorageForInt(data, false, true, isSort);
      }
    } else {
      if (version == ColumnarFormatVersion.V3) {
        return new BlockIndexerStorageForNoInvertedIndexForShort(data, true);
      } else {
        return new BlockIndexerStorageForNoInvertedIndex(data);
      }
    }
  }

  // apply and compress each dimension, set encoded data in `encodedData`
  private void encodeAndCompressDimensions(TablePage tablePage, EncodedData encodedData)
      throws KeyGenException{
    TableSpec.DimensionSpec dimensionSpec = model.getTableSpec().getDimensionSpec();
    int dictionaryColumnCount = -1;
    int noDictionaryColumnCount = -1;
    int indexStorageOffset = 0;
    IndexStorage[] indexStorages = new IndexStorage[dimensionSpec.getNumExpandedDimensions()];
    Compressor compressor = CompressorFactory.getInstance().getCompressor();
    byte[][] compressedColumns = new byte[indexStorages.length][];
    for (int i = 0; i < dimensionSpec.getNumSimpleDimensions(); i++) {
      byte[] flattened;
      boolean isSortColumn = model.isSortColumn(i);
      switch (dimensionSpec.getType(i)) {
        case GLOBAL_DICTIONARY:
          // dictionary dimension
          indexStorages[indexStorageOffset] =
              encodeAndCompressDictDimension(
                  tablePage.getDictDimensionPage()[++dictionaryColumnCount].getByteArrayPage(),
                  isSortColumn,
                  isUseInvertedIndex[i] & isSortColumn);
          flattened = ByteUtil.flatten(indexStorages[indexStorageOffset].getKeyBlock());
          break;
        case DIRECT_DICTIONARY:
          // timestamp and date column
          indexStorages[indexStorageOffset] =
              encodeAndCompressDirectDictDimension(
                  tablePage.getDictDimensionPage()[++dictionaryColumnCount].getByteArrayPage(),
                  isSortColumn,
                  isUseInvertedIndex[i] & isSortColumn);
          flattened = ByteUtil.flatten(indexStorages[indexStorageOffset].getKeyBlock());
          break;
        case PLAIN_VALUE:
          // high cardinality dimension, encoded as plain string
          indexStorages[indexStorageOffset] =
              encodeAndCompressNoDictDimension(
                  tablePage.getNoDictDimensionPage()[++noDictionaryColumnCount].getByteArrayPage(),
                  isSortColumn,
                  isUseInvertedIndex[i] & isSortColumn);
          flattened = ByteUtil.flatten(indexStorages[indexStorageOffset].getKeyBlock());
          break;
        case COMPLEX:
          // we need to add complex column at last, so skipping it here
          continue;
        default:
          throw new RuntimeException("unsupported dimension type: " + dimensionSpec.getType(i));
      }
      compressedColumns[indexStorageOffset] = compressor.compressByte(flattened);
      indexStorageOffset++;
    }

    // handle complex type column
    for (int i = 0; i < dimensionSpec.getNumComplexDimensions(); i++) {
      Iterator<byte[][]> iterator = tablePage.getComplexDimensionPage()[i].iterator();
      while (iterator.hasNext()) {
        byte[][] data = iterator.next();
        indexStorages[indexStorageOffset] = encodeAndCompressComplexDimension(data);
        byte[] flattened = ByteUtil.flatten(indexStorages[indexStorageOffset].getKeyBlock());
        compressedColumns[indexStorageOffset] = compressor.compressByte(flattened);
        indexStorageOffset++;
      }
    }

    encodedData.indexStorages = indexStorages;
    encodedData.dimensions = compressedColumns;
  }

}
