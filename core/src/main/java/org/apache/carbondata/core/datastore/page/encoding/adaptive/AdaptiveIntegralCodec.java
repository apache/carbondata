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

package org.apache.carbondata.core.datastore.page.encoding.adaptive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.ColumnPageValueConverter;
import org.apache.carbondata.core.datastore.page.LazyColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageDecoder;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoder;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.datastore.page.statistics.SimpleStatsResult;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;
import org.apache.carbondata.format.DataChunk2;
import org.apache.carbondata.format.Encoding;

/**
 * Codec for integer (byte, short, int, long) data type page.
 * This converter will do type casting on page data to make storage minimum.
 */
public class AdaptiveIntegralCodec extends AdaptiveCodec {

  public AdaptiveIntegralCodec(DataType srcDataType, DataType targetDataType,
      SimpleStatsResult stats, boolean isInvertedIndex) {
    super(srcDataType, targetDataType, stats, isInvertedIndex);
  }

  @Override
  public String getName() {
    return "AdaptiveIntegralCodec";
  }

  @Override
  public ColumnPageEncoder createEncoder(Map<String, String> parameter) {
    return new ColumnPageEncoder() {
      byte[] result = null;
      @Override
      protected byte[] encodeData(ColumnPage input) throws MemoryException, IOException {
        if (encodedPage != null) {
          throw new IllegalStateException("already encoded");
        }
        Compressor compressor =
            CompressorFactory.getInstance().getCompressor(input.getColumnCompressorName());
        result = encodeAndCompressPage(input, converter, compressor);
        byte[] bytes = writeInvertedIndexIfRequired(result);
        encodedPage.freeMemory();
        if (bytes.length != 0) {
          return bytes;
        }
        return result;
      }

      @Override
      protected List<Encoding> getEncodingList() {
        List<Encoding> encodings = new ArrayList<Encoding>();
        encodings.add(Encoding.ADAPTIVE_INTEGRAL);
        if (null != indexStorage && indexStorage.getRowIdPageLengthInBytes() > 0) {
          encodings.add(Encoding.INVERTED_INDEX);
        }
        return encodings;
      }

      @Override
      protected ColumnPageEncoderMeta getEncoderMeta(ColumnPage inputPage) {
        return new ColumnPageEncoderMeta(inputPage.getColumnSpec(), targetDataType, stats,
            inputPage.getColumnCompressorName());
      }

      @Override
      protected void fillLegacyFields(DataChunk2 dataChunk) throws IOException {
        fillLegacyFieldsIfRequired(dataChunk, result);
      }
    };
  }

  @Override
  public ColumnPageDecoder createDecoder(final ColumnPageEncoderMeta meta) {
    return new ColumnPageDecoder() {
      @Override
      public ColumnPage decode(byte[] input, int offset, int length)
          throws MemoryException, IOException {
        ColumnPage page = null;
        if (DataTypes.isDecimal(meta.getSchemaDataType())) {
          page = ColumnPage.decompressDecimalPage(meta, input, offset, length);
        } else {
          page = ColumnPage.decompress(meta, input, offset, length, false);
        }
        return LazyColumnPage.newPage(page, converter);
      }

      @Override
      public ColumnPage decode(byte[] input, int offset, int length, ColumnVectorInfo vectorInfo,
          BitSet nullBits, boolean isLVEncoded) throws MemoryException, IOException {
        ColumnPage page = null;
        if (DataTypes.isDecimal(meta.getSchemaDataType())) {
          page = ColumnPage.decompressDecimalPage(meta, input, offset, length);
        } else {
          page = ColumnPage.decompress(meta, input, offset, length, isLVEncoded);
        }
        page.setNullBits(nullBits);
        return LazyColumnPage.newPage(page, converter, vectorInfo);
      }

      @Override public ColumnPage decode(byte[] input, int offset, int length, boolean isLVEncoded)
          throws MemoryException, IOException {
        return decode(input, offset, length);
      }
    };
  }

  // encoded value = (type cast page value to target data type)
  private ColumnPageValueConverter converter = new ColumnPageValueConverter() {
    @Override
    public void encode(int rowId, byte value) {
      if (targetDataType == DataTypes.BYTE) {
        encodedPage.putByte(rowId, value);
      } else {
        throw new RuntimeException("internal error: " + debugInfo());
      }
    }

    @Override
    public void encode(int rowId, short value) {
      if (targetDataType == DataTypes.BYTE) {
        encodedPage.putByte(rowId, (byte) value);
      } else if (targetDataType == DataTypes.SHORT) {
        encodedPage.putShort(rowId, value);
      } else {
        throw new RuntimeException("internal error: " + debugInfo());
      }
    }

    @Override
    public void encode(int rowId, int value) {
      if (targetDataType == DataTypes.BYTE) {
        encodedPage.putByte(rowId, (byte) value);
      } else if (targetDataType == DataTypes.SHORT) {
        encodedPage.putShort(rowId, (short) value);
      } else if (targetDataType == DataTypes.SHORT_INT) {
        encodedPage.putShortInt(rowId, value);
      } else if (targetDataType == DataTypes.INT) {
        encodedPage.putInt(rowId, value);
      } else {
        throw new RuntimeException("internal error: " + debugInfo());
      }
    }

    @Override
    public void encode(int rowId, long value) {
      if (targetDataType == DataTypes.BYTE) {
        encodedPage.putByte(rowId, (byte) value);
      } else if (targetDataType == DataTypes.SHORT) {
        encodedPage.putShort(rowId, (short) value);
      } else if (targetDataType == DataTypes.SHORT_INT) {
        encodedPage.putShortInt(rowId, (int) value);
      } else if (targetDataType == DataTypes.INT) {
        encodedPage.putInt(rowId, (int) value);
      } else if (targetDataType == DataTypes.LONG) {
        encodedPage.putLong(rowId, (long) value);
      } else {
        throw new RuntimeException("internal error: " + debugInfo());
      }
    }

    @Override
    public void encode(int rowId, float value) {
      if (targetDataType == DataTypes.BYTE) {
        encodedPage.putByte(rowId, (byte) value);
      } else if (targetDataType == DataTypes.SHORT) {
        encodedPage.putShort(rowId, (short) value);
      } else if (targetDataType == DataTypes.SHORT_INT) {
        encodedPage.putShortInt(rowId, (int) value);
      } else if (targetDataType == DataTypes.INT) {
        encodedPage.putInt(rowId, (int) value);
      } else {
        throw new RuntimeException("internal error: " + debugInfo());
      }
    }

    @Override
    public void encode(int rowId, double value) {
      if (targetDataType == DataTypes.BYTE) {
        encodedPage.putByte(rowId, (byte) value);
      } else if (targetDataType == DataTypes.SHORT) {
        encodedPage.putShort(rowId, (short) value);
      } else if (targetDataType == DataTypes.SHORT_INT) {
        encodedPage.putShortInt(rowId, (int) value);
      } else if (targetDataType == DataTypes.INT) {
        encodedPage.putInt(rowId, (int) value);
      } else if (targetDataType == DataTypes.LONG) {
        encodedPage.putLong(rowId, (long) value);
      } else {
        throw new RuntimeException("internal error: " + debugInfo());
      }
    }

    @Override
    public long decodeLong(byte value) {
      return value;
    }

    @Override
    public long decodeLong(short value) {
      return value;
    }

    @Override
    public long decodeLong(int value) {
      return value;
    }

    @Override
    public double decodeDouble(byte value) {
      return value;
    }

    @Override
    public double decodeDouble(short value) {
      return value;
    }

    @Override
    public double decodeDouble(int value) {
      return value;
    }

    @Override
    public double decodeDouble(long value) {
      return value;
    }

    @Override
    public double decodeDouble(float value) {
      throw new RuntimeException("internal error: " + debugInfo());
    }

    @Override
    public double decodeDouble(double value) {
      throw new RuntimeException("internal error: " + debugInfo());
    }

    @Override
    public void decode(ColumnPage columnPage, ColumnVectorInfo vectorInfo) {
      CarbonColumnVector vector = vectorInfo.vector;
      BitSet nullBits = columnPage.getNullBits();
      DataType dataType = vector.getType();
      DataType type = columnPage.getDataType();
      int pageSize = columnPage.getPageSize();
      BitSet deletedRows = vectorInfo.deletedRows;
      if (vectorInfo.isExplictSorted) {
        if (deletedRows != null && !deletedRows.isEmpty()) {
          fillVectorWithInvertedIndexWithDelta(columnPage, vectorInfo, vector, dataType, type,
              pageSize, deletedRows, nullBits);
        } else {
          fillVectorWithInvertedIndex(columnPage, vectorInfo, vector, dataType, type, pageSize);
        }
      } else {
        if (deletedRows != null && !deletedRows.isEmpty()) {
          fillVectorWithDelta(columnPage, vector, dataType, type, pageSize, deletedRows, nullBits);
        } else {
          fillVector(columnPage, vector, dataType, type, pageSize);
        }
      }
      if (deletedRows == null || deletedRows.isEmpty()) {
        for (int i = nullBits.nextSetBit(0); i >= 0; i = nullBits.nextSetBit(i + 1)) {
          vector.putNull(i);
        }
      }

    }

    private void fillVector(ColumnPage columnPage, CarbonColumnVector vector, DataType dataType,
        DataType type, int pageSize) {
      if (type == DataTypes.BOOLEAN || type == DataTypes.BYTE) {
        byte[] byteData = columnPage.getByteData();
        if (dataType == DataTypes.SHORT) {
          for (int i = 0; i < pageSize; i++) {
            vector.putShort(i, (short) byteData[i]);
          }
        } else if (dataType == DataTypes.INT) {
          for (int i = 0; i < pageSize; i++) {
            vector.putInt(i, (int) byteData[i]);
          }
        } else if (dataType == DataTypes.LONG) {
          for (int i = 0; i < pageSize; i++) {
            vector.putLong(i, byteData[i]);
          }
        } else if (dataType == DataTypes.TIMESTAMP) {
          for (int i = 0; i < pageSize; i++) {
            vector.putLong(i, byteData[i] * 1000);
          }
        } else if (dataType == DataTypes.BOOLEAN) {
          for (int i = 0; i < pageSize; i++) {
            vector.putByte(i, byteData[i]);
          }
        } else {
          for (int i = 0; i < pageSize; i++) {
            vector.putDouble(i, byteData[i]);
          }
        }
      } else if (type == DataTypes.SHORT) {
        short[] shortData = columnPage.getShortData();
        if (dataType == DataTypes.SHORT) {
          for (int i = 0; i < pageSize; i++) {
            vector.putShort(i, (short) shortData[i]);
          }
        } else if (dataType == DataTypes.INT) {
          for (int i = 0; i < pageSize; i++) {
            vector.putInt(i, (int) shortData[i]);
          }
        } else if (dataType == DataTypes.LONG) {
          for (int i = 0; i < pageSize; i++) {
            vector.putLong(i, shortData[i]);
          }
        } else if (dataType == DataTypes.TIMESTAMP) {
          for (int i = 0; i < pageSize; i++) {
            vector.putLong(i, shortData[i] * 1000);
          }
        } else {
          for (int i = 0; i < pageSize; i++) {
            vector.putDouble(i, shortData[i]);
          }
        }

      } else if (type == DataTypes.SHORT_INT) {
        int[] shortIntData = columnPage.getShortIntData();
        if (dataType == DataTypes.SHORT) {
          for (int i = 0; i < pageSize; i++) {
            vector.putShort(i, (short) shortIntData[i]);
          }
        } else if (dataType == DataTypes.INT) {
          for (int i = 0; i < pageSize; i++) {
            vector.putInt(i, (int) shortIntData[i]);
          }
        } else if (dataType == DataTypes.LONG) {
          for (int i = 0; i < pageSize; i++) {
            vector.putLong(i, shortIntData[i]);
          }
        } else if (dataType == DataTypes.TIMESTAMP) {
          for (int i = 0; i < pageSize; i++) {
            vector.putLong(i, shortIntData[i] * 1000);
          }
        } else {
          for (int i = 0; i < pageSize; i++) {
            vector.putDouble(i, shortIntData[i]);
          }
        }
      } else if (type == DataTypes.INT) {
        int[] intData = columnPage.getIntData();
        if (dataType == DataTypes.SHORT) {
          for (int i = 0; i < pageSize; i++) {
            vector.putShort(i, (short) intData[i]);
          }
        } else if (dataType == DataTypes.INT) {
          for (int i = 0; i < pageSize; i++) {
            vector.putInt(i, intData[i]);
          }
        } else if (dataType == DataTypes.LONG) {
          for (int i = 0; i < pageSize; i++) {
            vector.putLong(i, intData[i]);
          }
        } else if (dataType == DataTypes.TIMESTAMP) {
          for (int i = 0; i < pageSize; i++) {
            vector.putLong(i, intData[i] * 1000);
          }
        } else {
          for (int i = 0; i < pageSize; i++) {
            vector.putDouble(i, intData[i]);
          }
        }
      } else {
        double[] doubleData = columnPage.getDoubleData();
        for (int i = 0; i < pageSize; i++) {
          vector.putDouble(i, doubleData[i]);
        }
      }
    }

    private void fillVectorWithInvertedIndex(ColumnPage columnPage, ColumnVectorInfo vectorInfo,
        CarbonColumnVector vector, DataType dataType, DataType type, int pageSize) {
      int[] invertedIndex = vectorInfo.invertedIndex;
      if (type == DataTypes.BOOLEAN || type == DataTypes.BYTE) {
        byte[] byteData = columnPage.getByteData();
        if (dataType == DataTypes.SHORT) {
          for (int i = 0; i < pageSize; i++) {
            vector.putShort(invertedIndex[i], (short) byteData[i]);
          }
        } else if (dataType == DataTypes.INT) {
          for (int i = 0; i < pageSize; i++) {
            vector.putInt(invertedIndex[i], (int) byteData[i]);
          }
        } else if (dataType == DataTypes.LONG) {
          for (int i = 0; i < pageSize; i++) {
            vector.putLong(invertedIndex[i], byteData[i]);
          }
        }  else if (dataType == DataTypes.TIMESTAMP) {
          for (int i = 0; i < pageSize; i++) {
            vector.putLong(invertedIndex[i], byteData[i] * 1000);
          }
        } else if (dataType == DataTypes.BOOLEAN) {
          for (int i = 0; i < pageSize; i++) {
            vector.putByte(invertedIndex[i], byteData[i]);
          }
        } else {
          for (int i = 0; i < pageSize; i++) {
            vector.putDouble(invertedIndex[i], byteData[i]);
          }
        }
      } else if (type == DataTypes.SHORT) {
        short[] shortData = columnPage.getShortData();
        if (dataType == DataTypes.SHORT) {
          for (int i = 0; i < pageSize; i++) {
            vector.putShort(invertedIndex[i], shortData[i]);
          }
        } else if (dataType == DataTypes.INT) {
          for (int i = 0; i < pageSize; i++) {
            vector.putInt(invertedIndex[i], (int) shortData[i]);
          }
        } else if (dataType == DataTypes.LONG) {
          for (int i = 0; i < pageSize; i++) {
            vector.putLong(invertedIndex[i], shortData[i]);
          }
        } else if (dataType == DataTypes.TIMESTAMP) {
          for (int i = 0; i < pageSize; i++) {
            vector.putLong(invertedIndex[i], shortData[i] * 1000);
          }
        } else {
          for (int i = 0; i < pageSize; i++) {
            vector.putDouble(invertedIndex[i], shortData[i]);
          }
        }

      } else if (type == DataTypes.SHORT_INT) {
        int[] shortIntData = columnPage.getShortIntData();
        if (dataType == DataTypes.SHORT) {
          for (int i = 0; i < pageSize; i++) {
            vector.putShort(invertedIndex[i], (short) shortIntData[i]);
          }
        } else if (dataType == DataTypes.INT) {
          for (int i = 0; i < pageSize; i++) {
            vector.putInt(invertedIndex[i], shortIntData[i]);
          }
        } else if (dataType == DataTypes.LONG) {
          for (int i = 0; i < pageSize; i++) {
            vector.putLong(invertedIndex[i], shortIntData[i]);
          }
        } else if (dataType == DataTypes.TIMESTAMP) {
          for (int i = 0; i < pageSize; i++) {
            vector.putLong(invertedIndex[i], shortIntData[i] * 1000);
          }
        } else {
          for (int i = 0; i < pageSize; i++) {
            vector.putDouble(invertedIndex[i], shortIntData[i]);
          }
        }
      } else if (type == DataTypes.INT) {
        int[] intData = columnPage.getIntData();
        if (dataType == DataTypes.SHORT) {
          for (int i = 0; i < pageSize; i++) {
            vector.putShort(invertedIndex[i], (short) intData[i]);
          }
        } else if (dataType == DataTypes.INT) {
          for (int i = 0; i < pageSize; i++) {
            vector.putInt(invertedIndex[i], intData[i]);
          }
        } else if (dataType == DataTypes.LONG) {
          for (int i = 0; i < pageSize; i++) {
            vector.putLong(invertedIndex[i], intData[i]);
          }
        } else if (dataType == DataTypes.TIMESTAMP) {
          for (int i = 0; i < pageSize; i++) {
            vector.putLong(invertedIndex[i], intData[i] * 1000);
          }
        } else {
          for (int i = 0; i < pageSize; i++) {
            vector.putDouble(invertedIndex[i], intData[i]);
          }
        }
      } else {
        double[] doubleData = columnPage.getDoubleData();
        for (int i = 0; i < pageSize; i++) {
          vector.putDouble(invertedIndex[i], doubleData[i]);
        }
      }
    }

    private void fillVectorWithDelta(ColumnPage columnPage, CarbonColumnVector vector,
        DataType dataType,
        DataType type, int pageSize,
        BitSet deletedRows,
        BitSet nullBitset) {
      int k = 0;
      if (type == DataTypes.BOOLEAN || type == DataTypes.BYTE) {
        byte[] byteData = columnPage.getByteData();
        if (dataType == DataTypes.SHORT) {
          for (int i = 0; i < pageSize; i++) {
            if (!deletedRows.get(i)) {
              if (nullBitset.get(i)) {
                vector.putNull(k++);
              } else {
                vector.putShort(k++, (short) (byteData[i]));
              }
            }
          }
        } else if (dataType == DataTypes.INT) {
          for (int i = 0; i < pageSize; i++) {
            if (!deletedRows.get(i)) {
              if (nullBitset.get(i)) {
                vector.putNull(k++);
              } else {
                vector.putInt(k++, (int) (byteData[i]));
              }
            }
          }
        } else if (dataType == DataTypes.LONG) {
          for (int i = 0; i < pageSize; i++) {
            if (!deletedRows.get(i)) {
              if (nullBitset.get(i)) {
                vector.putNull(k++);
              } else {
                vector.putLong(k++, (byteData[i]));
              }
            }
          }
        } else if (dataType == DataTypes.TIMESTAMP) {
          for (int i = 0; i < pageSize; i++) {
            if (!deletedRows.get(i)) {
              if (nullBitset.get(i)) {
                vector.putNull(k++);
              } else {
                vector.putLong(k++, (byteData[i] * 1000));
              }
            }
          }
        } else if (dataType == DataTypes.BOOLEAN) {
          for (int i = 0; i < pageSize; i++) {
            if (!deletedRows.get(i)) {
              if (nullBitset.get(i)) {
                vector.putNull(k++);
              } else {
                vector.putByte(k++, (byte) (byteData[i]));
              }
            }
          }
        } else {
          for (int i = 0; i < pageSize; i++) {
            if (!deletedRows.get(i)) {
              if (nullBitset.get(i)) {
                vector.putNull(k++);
              } else {
                vector.putDouble(k++, (byteData[i]));
              }
            }
          }
        }
      } else if (type == DataTypes.SHORT) {
        short[] shortData = columnPage.getShortData();
        if (dataType == DataTypes.SHORT) {
          for (int i = 0; i < pageSize; i++) {
            if (!deletedRows.get(i)) {
              if (nullBitset.get(i)) {
                vector.putNull(k++);
              } else {
                vector.putShort(k++, (shortData[i]));
              }
            }
          }
        } else if (dataType == DataTypes.INT) {
          for (int i = 0; i < pageSize; i++) {
            if (!deletedRows.get(i)) {
              if (nullBitset.get(i)) {
                vector.putNull(k++);
              } else {
                vector.putInt(k++, (int) (shortData[i]));
              }
            }
          }
        } else if (dataType == DataTypes.LONG) {
          for (int i = 0; i < pageSize; i++) {
            if (!deletedRows.get(i)) {
              if (nullBitset.get(i)) {
                vector.putNull(k++);
              } else {
                vector.putLong(k++, (shortData[i]));
              }
            }
          }
        } else if (dataType == DataTypes.TIMESTAMP) {
          for (int i = 0; i < pageSize; i++) {
            if (!deletedRows.get(i)) {
              if (nullBitset.get(i)) {
                vector.putNull(k++);
              } else {
                vector.putLong(k++, (shortData[i] * 1000));
              }
            }
          }
        } else {
          for (int i = 0; i < pageSize; i++) {
            if (!deletedRows.get(i)) {
              if (nullBitset.get(i)) {
                vector.putNull(k++);
              } else {
                vector.putDouble(k++, (shortData[i]));
              }
            }
          }
        }

      } else if (type == DataTypes.SHORT_INT) {
        int[] shortIntData = columnPage.getShortIntData();
        if (dataType == DataTypes.SHORT) {
          for (int i = 0; i < pageSize; i++) {
            if (!deletedRows.get(i)) {
              if (nullBitset.get(i)) {
                vector.putNull(k++);
              } else {
                vector.putShort(k++, (short) (shortIntData[i]));
              }
            }
          }
        } else if (dataType == DataTypes.INT) {
          for (int i = 0; i < pageSize; i++) {
            if (!deletedRows.get(i)) {
              if (nullBitset.get(i)) {
                vector.putNull(k++);
              } else {
                vector.putInt(k++, (int) (shortIntData[i]));
              }
            }
          }
        } else if (dataType == DataTypes.LONG) {
          for (int i = 0; i < pageSize; i++) {
            if (!deletedRows.get(i)) {
              if (nullBitset.get(i)) {
                vector.putNull(k++);
              } else {
                vector.putLong(k++, (shortIntData[i]));
              }
            }
          }
        } else if (dataType == DataTypes.TIMESTAMP) {
          for (int i = 0; i < pageSize; i++) {
            if (!deletedRows.get(i)) {
              if (nullBitset.get(i)) {
                vector.putNull(k++);
              } else {
                vector.putLong(k++, (shortIntData[i] * 1000));
              }
            }
          }
        } else {
          for (int i = 0; i < pageSize; i++) {
            if (!deletedRows.get(i)) {
              if (nullBitset.get(i)) {
                vector.putNull(k++);
              } else {
                vector.putDouble(k++, (shortIntData[i]));
              }
            }
          }
        }
      } else if (type == DataTypes.INT) {
        int[] intData = columnPage.getIntData();
        if (dataType == DataTypes.SHORT) {
          for (int i = 0; i < pageSize; i++) {
            if (!deletedRows.get(i)) {
              if (nullBitset.get(i)) {
                vector.putNull(k++);
              } else {
                vector.putShort(k++, (short) (intData[i]));
              }
            }
          }
        } else if (dataType == DataTypes.INT) {
          for (int i = 0; i < pageSize; i++) {
            if (!deletedRows.get(i)) {
              if (nullBitset.get(i)) {
                vector.putNull(k++);
              } else {
                vector.putInt(k++, (int) (intData[i]));
              }
            }
          }
        } else if (dataType == DataTypes.LONG) {
          for (int i = 0; i < pageSize; i++) {
            if (!deletedRows.get(i)) {
              if (nullBitset.get(i)) {
                vector.putNull(k++);
              } else {
                vector.putLong(k++, (intData[i]));
              }
            }
          }
        } else if (dataType == DataTypes.TIMESTAMP) {
          for (int i = 0; i < pageSize; i++) {
            if (!deletedRows.get(i)) {
              if (nullBitset.get(i)) {
                vector.putNull(k++);
              } else {
                vector.putLong(k++, (intData[i] * 1000));
              }
            }
          }
        } else {
          for (int i = 0; i < pageSize; i++) {
            if (!deletedRows.get(i)) {
              if (nullBitset.get(i)) {
                vector.putNull(k++);
              } else {
                vector.putDouble(k++, (intData[i]));
              }
            }
          }
        }
      } else {
        double[] doubleData = columnPage.getDoubleData();
        for (int i = 0; i < pageSize; i++) {
          if (!deletedRows.get(i)) {
            if (nullBitset.get(i)) {
              vector.putNull(k++);
            } else {
              vector.putDouble(k++, doubleData[i]);
            }
          }
        }
      }
    }

    private void fillVectorWithInvertedIndexWithDelta(ColumnPage columnPage,
        ColumnVectorInfo vectorInfo, CarbonColumnVector vector, DataType dataType, DataType type,
        int pageSize, BitSet deletedRows, BitSet nullBitset) {
      int[] invertedIndex = vectorInfo.invertedIndex;
      int k = 0;
      if (type == DataTypes.BOOLEAN || type == DataTypes.BYTE) {
        byte[] byteData = columnPage.getByteData();
        if (dataType == DataTypes.SHORT) {
          short[] finalData = new short[pageSize];
          for (int i = 0; i < pageSize; i++) {
            finalData[invertedIndex[i]] = (short) (byteData[i]);
          }
          for (int i = 0; i < finalData.length; i++) {
            if (!deletedRows.get(i)) {
              if (nullBitset.get(i)) {
                vector.putNull(k++);
              } else {
                vector.putShort(k++, finalData[i]);
              }
            }
          }
        } else if (dataType == DataTypes.INT) {
          int[] finalData = new int[pageSize];
          for (int i = 0; i < pageSize; i++) {
            finalData[invertedIndex[i]] = (int) (byteData[i]);
          }
          for (int i = 0; i < finalData.length; i++) {
            if (!deletedRows.get(i)) {
              if (nullBitset.get(i)) {
                vector.putNull(k++);
              } else {
                vector.putInt(k++, finalData[i]);
              }
            }
          }
        } else if (dataType == DataTypes.LONG) {
          long[] finalData = new long[pageSize];
          for (int i = 0; i < pageSize; i++) {
            finalData[invertedIndex[i]] = (byteData[i]);
          }
          for (int i = 0; i < finalData.length; i++) {
            if (!deletedRows.get(i)) {
              if (nullBitset.get(i)) {
                vector.putNull(k++);
              } else {
                vector.putLong(k++, finalData[i]);
              }
            }
          }
        } else if (dataType == DataTypes.TIMESTAMP) {
          long[] finalData = new long[pageSize];
          for (int i = 0; i < pageSize; i++) {
            finalData[invertedIndex[i]] = (byteData[i]);
          }
          for (int i = 0; i < finalData.length; i++) {
            if (!deletedRows.get(i)) {
              if (nullBitset.get(i)) {
                vector.putNull(k++);
              } else {
                vector.putLong(k++, finalData[i] * 1000);
              }
            }
          }
        } else if (dataType == DataTypes.BOOLEAN) {
          byte[] finalData = new byte[pageSize];
          for (int i = 0; i < pageSize; i++) {
            finalData[invertedIndex[i]] = (byte) (byteData[i]);
          }
          for (int i = 0; i < finalData.length; i++) {
            if (!deletedRows.get(i)) {
              if (nullBitset.get(i)) {
                vector.putNull(k++);
              } else {
                vector.putByte(k++, finalData[i]);
              }
            }
          }
        } else {
          double[] finalData = new double[pageSize];
          for (int i = 0; i < pageSize; i++) {
            finalData[invertedIndex[i]] = (byteData[i]);
          }
          for (int i = 0; i < finalData.length; i++) {
            if (!deletedRows.get(i)) {
              if (nullBitset.get(i)) {
                vector.putNull(k++);
              } else {
                vector.putDouble(k++, finalData[i]);
              }
            }
          }
        }
      } else if (type == DataTypes.SHORT) {
        short[] shortData = columnPage.getShortData();
        if (dataType == DataTypes.SHORT) {
          short[] finalData = new short[pageSize];
          for (int i = 0; i < pageSize; i++) {
            finalData[invertedIndex[i]] = (short) (shortData[i]);
          }
          for (int i = 0; i < finalData.length; i++) {
            if (!deletedRows.get(i)) {
              if (nullBitset.get(i)) {
                vector.putNull(k++);
              } else {
                vector.putShort(k++, finalData[i]);
              }
            }
          }
        } else if (dataType == DataTypes.INT) {
          int[] finalData = new int[pageSize];
          for (int i = 0; i < pageSize; i++) {
            finalData[invertedIndex[i]] = (int) (shortData[i]);
          }
          for (int i = 0; i < finalData.length; i++) {
            if (!deletedRows.get(i)) {
              if (nullBitset.get(i)) {
                vector.putNull(k++);
              } else {
                vector.putInt(k++, finalData[i]);
              }
            }
          }
        } else if (dataType == DataTypes.LONG) {
          long[] finalData = new long[pageSize];
          for (int i = 0; i < pageSize; i++) {
            finalData[invertedIndex[i]] = (shortData[i]);
          }
          for (int i = 0; i < finalData.length; i++) {
            if (!deletedRows.get(i)) {
              if (nullBitset.get(i)) {
                vector.putNull(k++);
              } else {
                vector.putLong(k++, finalData[i]);
              }
            }
          }
        } else if (dataType == DataTypes.TIMESTAMP) {
          long[] finalData = new long[pageSize];
          for (int i = 0; i < pageSize; i++) {
            finalData[invertedIndex[i]] = (shortData[i]);
          }
          for (int i = 0; i < finalData.length; i++) {
            if (!deletedRows.get(i)) {
              if (nullBitset.get(i)) {
                vector.putNull(k++);
              } else {
                vector.putLong(k++, finalData[i] * 1000);
              }
            }
          }
        } else {
          double[] finalData = new double[pageSize];
          for (int i = 0; i < pageSize; i++) {
            finalData[invertedIndex[i]] = (shortData[i]);
          }
          for (int i = 0; i < finalData.length; i++) {
            if (!deletedRows.get(i)) {
              if (nullBitset.get(i)) {
                vector.putNull(k++);
              } else {
                vector.putDouble(k++, finalData[i]);
              }
            }
          }
        }

      } else if (type == DataTypes.SHORT_INT) {
        int[] shortIntData = columnPage.getShortIntData();
        if (dataType == DataTypes.SHORT) {
          short[] finalData = new short[pageSize];
          for (int i = 0; i < pageSize; i++) {
            finalData[invertedIndex[i]] = (short) (shortIntData[i]);
          }
          for (int i = 0; i < finalData.length; i++) {
            if (!deletedRows.get(i)) {
              if (nullBitset.get(i)) {
                vector.putNull(k++);
              } else {
                vector.putShort(k++, finalData[i]);
              }
            }
          }
        } else if (dataType == DataTypes.INT) {
          int[] finalData = new int[pageSize];
          for (int i = 0; i < pageSize; i++) {
            finalData[invertedIndex[i]] = (int) (shortIntData[i]);
          }
          for (int i = 0; i < finalData.length; i++) {
            if (!deletedRows.get(i)) {
              if (nullBitset.get(i)) {
                vector.putNull(k++);
              } else {
                vector.putInt(k++, finalData[i]);
              }
            }
          }
        } else if (dataType == DataTypes.LONG) {
          long[] finalData = new long[pageSize];
          for (int i = 0; i < pageSize; i++) {
            finalData[invertedIndex[i]] = (shortIntData[i]);
          }
          for (int i = 0; i < finalData.length; i++) {
            if (!deletedRows.get(i)) {
              if (nullBitset.get(i)) {
                vector.putNull(k++);
              } else {
                vector.putLong(k++, finalData[i]);
              }
            }
          }
        } else if (dataType == DataTypes.TIMESTAMP) {
          long[] finalData = new long[pageSize];
          for (int i = 0; i < pageSize; i++) {
            finalData[invertedIndex[i]] = (shortIntData[i]);
          }
          for (int i = 0; i < finalData.length; i++) {
            if (!deletedRows.get(i)) {
              if (nullBitset.get(i)) {
                vector.putNull(k++);
              } else {
                vector.putLong(k++, finalData[i] * 1000);
              }
            }
          }
        } else {
          double[] finalData = new double[pageSize];
          for (int i = 0; i < pageSize; i++) {
            finalData[invertedIndex[i]] = (shortIntData[i]);
          }
          for (int i = 0; i < finalData.length; i++) {
            if (!deletedRows.get(i)) {
              if (nullBitset.get(i)) {
                vector.putNull(k++);
              } else {
                vector.putDouble(k++, finalData[i]);
              }
            }
          }
        }
      } else if (type == DataTypes.INT) {
        int[] intData = columnPage.getIntData();
        if (dataType == DataTypes.SHORT) {
          short[] finalData = new short[pageSize];
          for (int i = 0; i < pageSize; i++) {
            finalData[invertedIndex[i]] = (short) (intData[i]);
          }
          for (int i = 0; i < finalData.length; i++) {
            if (!deletedRows.get(i)) {
              if (nullBitset.get(i)) {
                vector.putNull(k++);
              } else {
                vector.putShort(k++, finalData[i]);
              }
            }
          }
        } else if (dataType == DataTypes.INT) {
          int[] finalData = new int[pageSize];
          for (int i = 0; i < pageSize; i++) {
            finalData[invertedIndex[i]] = (int) (intData[i]);
          }
          for (int i = 0; i < finalData.length; i++) {
            if (!deletedRows.get(i)) {
              if (nullBitset.get(i)) {
                vector.putNull(k++);
              } else {
                vector.putInt(k++, finalData[i]);
              }
            }
          }
        } else if (dataType == DataTypes.LONG) {
          long[] finalData = new long[pageSize];
          for (int i = 0; i < pageSize; i++) {
            finalData[invertedIndex[i]] = (intData[i]);
          }
          for (int i = 0; i < finalData.length; i++) {
            if (!deletedRows.get(i)) {
              if (nullBitset.get(i)) {
                vector.putNull(k++);
              } else {
                vector.putLong(k++, finalData[i]);
              }
            }
          }
        } else if (dataType == DataTypes.TIMESTAMP) {
          long[] finalData = new long[pageSize];
          for (int i = 0; i < pageSize; i++) {
            finalData[invertedIndex[i]] = (intData[i]);
          }
          for (int i = 0; i < finalData.length; i++) {
            if (!deletedRows.get(i)) {
              if (nullBitset.get(i)) {
                vector.putNull(k++);
              } else {
                vector.putLong(k++, finalData[i] * 1000);
              }
            }
          }
        } else {
          double[] finalData = new double[pageSize];
          for (int i = 0; i < pageSize; i++) {
            finalData[invertedIndex[i]] = (intData[i]);
          }
          for (int i = 0; i < finalData.length; i++) {
            if (!deletedRows.get(i)) {
              if (nullBitset.get(i)) {
                vector.putNull(k++);
              } else {
                vector.putDouble(k++, finalData[i]);
              }
            }
          }
        }
      } else {
        double[] doubleData = columnPage.getDoubleData();
        double[] finalData = new double[pageSize];
        for (int i = 0; i < pageSize; i++) {
          finalData[invertedIndex[i]] = doubleData[i];
        }
        for (int i = 0; i < pageSize; i++) {
          if (!deletedRows.get(i)) {
            if (nullBitset.get(i)) {
              vector.putNull(k++);
            } else {
              vector.putDouble(k++, finalData[i]);
            }
          }
        }
      }
    }

  };

}
