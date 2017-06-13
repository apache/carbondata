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

import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.carbondata.core.compression.ValueCompressor;
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
import org.apache.carbondata.core.datastore.compression.ValueCompressionHolder;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.statistics.ColumnPageStatistics;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CompressionFinder;
import org.apache.carbondata.core.util.ValueCompressionUtil;
import org.apache.carbondata.processing.store.writer.Encoder;

// Default encoder for encoding dimension and measures. For dimensions, it applies RLE and
// inverted index encoding. For measures, it applies delta encoding or adaptive encoding
public class DefaultEncoder implements Encoder {

  private ColumnarFormatVersion version;

  private boolean[] isUseInvertedIndex;

  private CarbonFactDataHandlerModel model;

  public DefaultEncoder(CarbonFactDataHandlerModel model) {
    this.version = CarbonProperties.getInstance().getFormatVersion();
    this.model = model;
    this.isUseInvertedIndex = model.getIsUseInvertedIndex();
  }

  // function to encode all columns in one table page
  public Encoder.EncodedData encode(TablePage tablePage) {
    Encoder.EncodedData encodedData = new Encoder.EncodedData();
    encodeAndCompressDimensions(tablePage, encodedData);
    encodeAndCompressMeasures(tablePage, encodedData);
    return encodedData;
  }

  // encode measure and set encodedData in `encodedData`
  private void encodeAndCompressMeasures(TablePage tablePage, Encoder.EncodedData encodedData) {
    // TODO: following conversion is required only because compress model requires them,
    // remove then after the compress framework is refactoried
    ColumnPage[] measurePage = tablePage.getMeasurePage();
    int measureCount = measurePage.length;
    byte[] dataTypeSelected = new byte[measureCount];
    CompressionFinder[] finders = new CompressionFinder[measureCount];
    for (int i = 0; i < measureCount; i++) {
      ColumnPageStatistics stats = measurePage[i].getStatistics();
      finders[i] = ValueCompressionUtil
          .getCompressionFinder(stats.getMax(), stats.getMin(), stats.getDecimal(),
              measurePage[i].getDataType(), dataTypeSelected[i]);
    }

    //CompressionFinder[] finders = compressionModel.getCompressionFinders();
    ValueCompressionHolder[] holders = ValueCompressionUtil.getValueCompressionHolder(finders);
    encodedData.measures = encodeMeasure(holders, finders, measurePage);
  }

  // this method first invokes encoding routine to encode the data chunk,
  // followed by invoking compression routine for preparing the data chunk for writing.
  private byte[][] encodeMeasure(ValueCompressionHolder[] holders, CompressionFinder[] finders,
      ColumnPage[] columnPages) {
    ValueCompressionHolder[] values = new ValueCompressionHolder[columnPages.length];
    byte[][] encodedMeasures = new byte[values.length][];
    for (int i = 0; i < columnPages.length; i++) {
      values[i] = holders[i];
      if (columnPages[i].getDataType() != DataType.DECIMAL) {
        ValueCompressor compressor = ValueCompressionUtil.getValueCompressor(finders[i]);
        Object compressed = compressor.getCompressedValues(finders[i], columnPages[i],
            columnPages[i].getStatistics().getMax(), columnPages[i].getStatistics().getDecimal());
        values[i].setValue(compressed);
      } else {
        // in case of decimal, 'flatten' the byte[][] to byte[]
        byte[][] decimalPage = columnPages[i].getDecimalPage();
        int totalSize = 0;
        for (byte[] decimal : decimalPage) {
          totalSize += decimal.length;
        }
        ByteBuffer temp = ByteBuffer.allocate(totalSize);
        for (byte[] decimal : decimalPage) {
          temp.put(decimal);
        }
        values[i].setValue(temp.array());
      }
      values[i].compress();
      encodedMeasures[i] = values[i].getCompressedData();
    }

    return encodedMeasures;
  }

  private IndexStorage encodeAndCompressDictDimension(byte[][] data, boolean isSort,
      boolean isUseInvertedIndex) {
    if (isUseInvertedIndex) {
      if (version == ColumnarFormatVersion.V3) {
        return new BlockIndexerStorageForShort(data, true, false, isSort);
      } else {
        return new BlockIndexerStorageForInt(data, true, false, isSort);
      }
    } else {
      if (version == ColumnarFormatVersion.V3) {
        return new BlockIndexerStorageForNoInvertedIndexForShort(false, data);
      } else {
        return new BlockIndexerStorageForNoInvertedIndex(data);
      }
    }
  }

  private IndexStorage encodeAndCompressDirectDictDimension(byte[][] data, boolean isSort,
      boolean isUseInvertedIndex) {
    if (isUseInvertedIndex) {
      if (version == ColumnarFormatVersion.V3) {
        return new BlockIndexerStorageForShort(data, false, false, isSort);
      } else {
        return new BlockIndexerStorageForInt(data, false, false, isSort);
      }
    } else {
      if (version == ColumnarFormatVersion.V3) {
        return new BlockIndexerStorageForNoInvertedIndexForShort(false, data);
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
        return new BlockIndexerStorageForNoInvertedIndexForShort(true, data);
      } else {
        return new BlockIndexerStorageForNoInvertedIndex(data);
      }
    }
  }

  // encode and compress each dimension, set encoded data in `encodedData`
  private void encodeAndCompressDimensions(TablePage tablePage, Encoder.EncodedData encodedData) {
    TableSpec.DimensionSpec dimensionSpec = model.getTableSpec().getDimensionSpec();
    int dictionaryColumnCount = -1;
    int noDictionaryColumnCount = -1;
    int colGrpId = -1;
    int indexStorageOffset = 0;
    IndexStorage[] indexStorages = new IndexStorage[dimensionSpec.getNumExpandedDimensions()];
    SegmentProperties segmentProperties = model.getSegmentProperties();
    Compressor compressor = CompressorFactory.getInstance().getCompressor();
    byte[][] compressedColumns = new byte[indexStorages.length][];
    for (int i = 0; i < dimensionSpec.getNumSimpleDimensions(); i++) {
      byte[] flattened;
      boolean isSortColumn = model.isSortColumn(i);
      switch (dimensionSpec.getType(i)) {
        case GLOBAL_DICTIONARY:
          // dictionary dimension
          indexStorages[indexStorageOffset] = encodeAndCompressDictDimension(
              tablePage.getKeyColumnPage().getKeyVector(++dictionaryColumnCount), isSortColumn,
              isUseInvertedIndex[i] & isSortColumn);
          flattened = ByteUtil.flatten(indexStorages[indexStorageOffset].getKeyBlock());
          break;
        case DIRECT_DICTIONARY:
          // timestamp and date column
          indexStorages[indexStorageOffset] = encodeAndCompressDirectDictDimension(
              tablePage.getKeyColumnPage().getKeyVector(++dictionaryColumnCount), isSortColumn,
              isUseInvertedIndex[i] & isSortColumn);
          flattened = ByteUtil.flatten(indexStorages[indexStorageOffset].getKeyBlock());
          break;
        case PLAIN_VALUE:
          // high cardinality dimension, encoded as plain string
          indexStorages[indexStorageOffset] = encodeAndCompressNoDictDimension(
              tablePage.getNoDictDimensionPage()[++noDictionaryColumnCount].getStringPage(),
              isSortColumn, isUseInvertedIndex[i] & isSortColumn);
          flattened = ByteUtil.flatten(indexStorages[indexStorageOffset].getKeyBlock());
          break;
        case COLUMN_GROUP:
          // column group
          indexStorages[indexStorageOffset] =
              new ColGroupBlockStorage(segmentProperties, ++colGrpId,
                  tablePage.getKeyColumnPage().getKeyVector(++dictionaryColumnCount));
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