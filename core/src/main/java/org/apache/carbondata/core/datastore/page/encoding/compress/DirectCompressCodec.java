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

package org.apache.carbondata.core.datastore.page.encoding.compress;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.ReusableDataBuffer;
import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.chunk.impl.VariableLengthDimensionColumnPage;
import org.apache.carbondata.core.datastore.chunk.store.DimensionChunkStoreFactory;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.ColumnPageValueConverter;
import org.apache.carbondata.core.datastore.page.LazyColumnPage;
import org.apache.carbondata.core.datastore.page.VarLengthColumnPageBase;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageCodec;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageDecoder;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoder;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.keygenerator.directdictionary.timestamp.DateDirectDictionaryGenerator;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.DecimalConverterFactory;
import org.apache.carbondata.core.metadata.datatype.DecimalType;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;
import org.apache.carbondata.core.scan.result.vector.impl.CarbonColumnVectorImpl;
import org.apache.carbondata.core.scan.result.vector.impl.directread.ColumnarVectorWrapperDirectFactory;
import org.apache.carbondata.core.scan.result.vector.impl.directread.ConvertibleVector;
import org.apache.carbondata.core.scan.result.vector.impl.directread.SequentialFill;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.format.Encoding;

/**
 * This codec directly apply compression on the input data
 */
public class DirectCompressCodec implements ColumnPageCodec {

  private DataType dataType;

  public DirectCompressCodec(DataType dataType) {
    this.dataType = dataType;
  }

  boolean isComplexPrimitiveIntLengthEncoding = false;

  public void setComplexPrimitiveIntLengthEncoding(boolean complexPrimitiveIntLengthEncoding) {
    isComplexPrimitiveIntLengthEncoding = complexPrimitiveIntLengthEncoding;
  }

  @Override
  public String getName() {
    return "DirectCompressCodec";
  }

  @Override
  public ColumnPageEncoder createEncoder(Map<String, String> parameter) {
    return new ColumnPageEncoder() {

      @Override
      protected ByteBuffer encodeData(ColumnPage input) throws IOException {
        Compressor compressor = CompressorFactory.getInstance().getCompressor(
            input.getColumnCompressorName());
        return input.compress(compressor);
      }

      @Override
      protected List<Encoding> getEncodingList() {
        List<Encoding> encodings = new ArrayList<>();
        encodings.add(dataType == DataTypes.VARCHAR ?
            Encoding.DIRECT_COMPRESS_VARCHAR :
            Encoding.DIRECT_COMPRESS);
        return encodings;
      }

      @Override
      protected ColumnPageEncoderMeta getEncoderMeta(ColumnPage inputPage) {
        return new ColumnPageEncoderMeta(inputPage.getColumnSpec(), inputPage.getDataType(),
            inputPage.getStatistics(), inputPage.getColumnCompressorName());
      }
    };
  }

  @Override
  public ColumnPageDecoder createDecoder(final ColumnPageEncoderMeta meta) {
    return new ColumnPageDecoder() {

      @Override
      public ColumnPage decode(byte[] input, int offset, int length) {
        ColumnPage decodedPage;
        if (DataTypes.isDecimal(dataType)) {
          decodedPage = ColumnPage.decompressDecimalPage(meta, input, offset, length);
        } else {
          decodedPage = ColumnPage
              .decompress(meta, input, offset, length, false, isComplexPrimitiveIntLengthEncoding);
        }
        return LazyColumnPage.newPage(decodedPage, converter);
      }

      @Override
      public void decodeAndFillVector(byte[] input, int offset, int length,
          ColumnVectorInfo vectorInfo, BitSet nullBits, boolean isLVEncoded, int pageSize,
          ReusableDataBuffer reusableDataBuffer) {
        Compressor compressor =
            CompressorFactory.getInstance().getCompressor(meta.getCompressorName());
        int uncompressedLength;
        byte[] unCompressData;
        if (null != reusableDataBuffer && compressor.supportReusableBuffer()) {
          uncompressedLength = compressor.unCompressedLength(input, offset, length);
          unCompressData = reusableDataBuffer.getDataBuffer(uncompressedLength);
          compressor.rawUncompress(input, offset, length, unCompressData);
        } else {
          unCompressData = compressor.unCompressByte(input, offset, length);
          uncompressedLength = unCompressData.length;
        }
        if (DataTypes.isDecimal(dataType)) {
          TableSpec.ColumnSpec columnSpec = meta.getColumnSpec();
          DecimalConverterFactory.DecimalConverter decimalConverter =
              DecimalConverterFactory.INSTANCE
                  .getDecimalConverter(columnSpec.getPrecision(), columnSpec.getScale());
          vectorInfo.decimalConverter = decimalConverter;
          if (DataTypes.isDecimal(meta.getStoreDataType())) {
            ColumnPage decimalColumnPage = VarLengthColumnPageBase
                .newDecimalColumnPage(meta, unCompressData, uncompressedLength);
            decimalConverter.fillVector(decimalColumnPage.getByteArrayPage(), pageSize, vectorInfo,
                nullBits, meta.getStoreDataType());
          } else {
            converter
                .decodeAndFillVector(unCompressData, vectorInfo, nullBits, meta.getStoreDataType(),
                    pageSize);
          }
        } else {
          converter
              .decodeAndFillVector(unCompressData, vectorInfo, nullBits, meta.getStoreDataType(),
                  pageSize);
        }
      }

      @Override
      public ColumnPage decode(byte[] input, int offset, int length, boolean isLVEncoded) {
        return LazyColumnPage.newPage(ColumnPage
            .decompress(meta, input, offset, length, isLVEncoded,
                isComplexPrimitiveIntLengthEncoding), converter);
      }
    };
  }

  private ColumnPageValueConverter converter = new ColumnPageValueConverter() {
    @Override
    public void encode(int rowId, byte value) {
      throw new RuntimeException("internal error");
    }

    @Override
    public void encode(int rowId, short value) {
      throw new RuntimeException("internal error");
    }

    @Override
    public void encode(int rowId, int value) {
      throw new RuntimeException("internal error");
    }

    @Override
    public void encode(int rowId, long value) {
      throw new RuntimeException("internal error");
    }

    @Override
    public void encode(int rowId, float value) {
      throw new RuntimeException("internal error");
    }

    @Override
    public void encode(int rowId, double value) {
      throw new RuntimeException("internal error");
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
      return value;
    }

    @Override
    public double decodeDouble(double value) {
      return value;
    }

    @Override
    public void decodeAndFillVector(byte[] pageData, ColumnVectorInfo vectorInfo, BitSet nullBits,
        DataType pageDataType, int pageSize) {
      CarbonColumnVector vector = vectorInfo.vector;
      DataType vectorDataType = vector.getType();
      BitSet deletedRows = vectorInfo.deletedRows;
      if (vectorDataType.isComplexType() && vectorInfo.vectorStack.isEmpty()) {
        // Only if vectorStack is empty, initialize with the parent vector
        vectorInfo.vectorStack.push(vectorInfo.vector);
      }
      // If top of vector stack is a complex vector,
      // then add their children into the stack and load them too.
      if (!vectorInfo.vectorStack.isEmpty() && vectorInfo.vectorStack.peek().getType()
          .isComplexType()) {
        CarbonColumnVector parentVector = vectorInfo.vectorStack.peek();
        CarbonColumnVectorImpl parentVectorImpl =
            (CarbonColumnVectorImpl) (parentVector.getColumnVector());
        int deletedRowCount = vectorInfo.deletedRows != null ?
            vectorInfo.deletedRows.cardinality() : 0;
        // parse the parent page data,
        // save the information about number of child in each row in parent vector
        if (DataTypes.isStructType(parentVectorImpl.getType())) {
          parentVectorImpl
              .setNumberOfElementsInEachRowForStruct(pageData, pageSize - deletedRowCount);
        } else {
          parentVectorImpl
              .setNumberOfElementsInEachRowForArray(pageData, pageSize - deletedRowCount);
        }
        for (CarbonColumnVector childVector : parentVector.getColumnVector().getChildrenVector()) {
          // push each child
          vectorInfo.vectorStack.push(childVector);
          // load the child page, here child page loading flow updated vector from top of the stack
          // and pop() the child vector once loading is finished.
          ((CarbonColumnVectorImpl) (vectorInfo.vectorStack.peek().getColumnVector())).loadPage();
        }
        vector = ColumnarVectorWrapperDirectFactory
            .getDirectVectorWrapperFactory(vectorInfo, parentVector, vectorInfo.invertedIndex,
                nullBits, vectorInfo.deletedRows, true, false);
        // In case of update there will be two wrappers enclosing columnVector
        if (vector.getColumnVector() != null
            && vector.getColumnVector().getColumnVector() != null) {
          vector = vector.getColumnVector();
        }
        fillVectorBasedOnType(pageData, vector, vectorDataType, pageDataType, pageSize,
            vectorInfo, nullBits);
      } else {
        pageSize = ColumnVectorInfo.getUpdatedPageSizeForChildVector(vectorInfo, pageSize);
        vector = ColumnarVectorWrapperDirectFactory
            .getDirectVectorWrapperFactory(vectorInfo, vector, vectorInfo.invertedIndex, nullBits,
                deletedRows, true, false);
        fillVectorBasedOnType(pageData, vector, vector.getType(), pageDataType, pageSize,
            vectorInfo, nullBits);
        if ((deletedRows == null || deletedRows.isEmpty())
            && !(vectorInfo.vector instanceof SequentialFill)) {
          for (int i = nullBits.nextSetBit(0); i >= 0; i = nullBits.nextSetBit(i + 1)) {
            vector.putNull(i);
          }
        }
        if (vector instanceof ConvertibleVector) {
          ((ConvertibleVector) vector).convert();
        }
      }
    }

    private void fillVectorBasedOnType(byte[] pageData, CarbonColumnVector vector,
        DataType vectorDataType, DataType pageDataType, int pageSize, ColumnVectorInfo vectorInfo,
        BitSet nullBits) {
      if (vectorInfo.vector.getColumnVector() != null
          && vector.getColumnVector() == vectorInfo.vector.getColumnVector() && vectorInfo.vector
          .getColumnVector().getType().isComplexType()) {
        List<Integer> childElementsForEachRow =
            ((CarbonColumnVectorImpl) vector.getColumnVector())
                .getNumberOfChildrenElementsInEachRow();
        vector.getColumnVector().putComplexObject(childElementsForEachRow);
      } else {
        fillPrimitiveType(pageData, vector, vectorDataType, pageDataType, pageSize, vectorInfo,
            nullBits);
      }
    }

    private void fillPrimitiveType(byte[] pageData, CarbonColumnVector vector,
        DataType vectorDataType, DataType pageDataType, int pageSize, ColumnVectorInfo vectorInfo,
        BitSet nullBits) {
      int intSizeInBytes = DataTypes.INT.getSizeInBytes();
      int shortSizeInBytes = DataTypes.SHORT.getSizeInBytes();
      int lengthStoredInBytes;
      // check if local dictionary is enabled for complex primitve type
      if (!vectorInfo.vectorStack.isEmpty()) {
        CarbonColumnVectorImpl tempVector =
            (CarbonColumnVectorImpl) (vectorInfo.vectorStack.peek().getColumnVector());
        if (tempVector.getLocalDictionary() != null) {
          DimensionChunkStoreFactory.DimensionStoreType dimStoreType =
              DimensionChunkStoreFactory.DimensionStoreType.LOCAL_DICT;
          // This will call fillVector() for local-dict eventually
          new VariableLengthDimensionColumnPage(pageData, new int[0], new int[0], pageSize,
              dimStoreType, tempVector.getLocalDictionary(), vectorInfo, pageSize);
          return;
        }
      }

      if (vectorInfo.encodings != null && vectorInfo.encodings.size() > 0 && CarbonUtil
          .hasEncoding(vectorInfo.encodings, Encoding.INT_LENGTH_COMPLEX_CHILD_BYTE_ARRAY)) {
        lengthStoredInBytes = intSizeInBytes;
      } else {
        // Before to carbon 2.0, complex child length is stored as SHORT
        // for string, varchar, binary, date, decimal types
        lengthStoredInBytes = shortSizeInBytes;
      }
      int rowId = 0;
      if (pageDataType == DataTypes.BOOLEAN || pageDataType == DataTypes.BYTE) {
        if (vectorDataType == DataTypes.SHORT) {
          for (int i = 0; i < pageSize; i++) {
            vector.putShort(i, pageData[i]);
          }
        } else if (vectorDataType == DataTypes.INT) {
          for (int i = 0; i < pageSize; i++) {
            vector.putInt(i, pageData[i]);
          }
        } else if (vectorDataType == DataTypes.LONG) {
          for (int i = 0; i < pageSize; i++) {
            vector.putLong(i, pageData[i]);
          }
        } else if (vectorDataType == DataTypes.TIMESTAMP) {
          for (int i = 0; i < pageSize; i++) {
            vector.putLong(i, (long) pageData[i] * 1000);
          }
        } else if (vectorDataType == DataTypes.BOOLEAN || vectorDataType == DataTypes.BYTE) {
          vector.putBytes(0, pageSize, pageData, 0);
        } else if (DataTypes.isDecimal(vectorDataType)) {
          DecimalConverterFactory.DecimalConverter decimalConverter = vectorInfo.decimalConverter;
          decimalConverter.fillVector(pageData, pageSize, vectorInfo, nullBits, pageDataType);
        } else {
          for (int i = 0; i < pageSize; i++) {
            vector.putDouble(i, pageData[i]);
          }
        }
      } else if (pageDataType == DataTypes.SHORT) {
        int size = pageSize * shortSizeInBytes;
        if (vectorDataType == DataTypes.SHORT) {
          for (int i = 0; i < size; i += shortSizeInBytes) {
            vector.putShort(rowId++, (ByteUtil.toShortLittleEndian(pageData, i)));
          }
        } else if (vectorDataType == DataTypes.INT) {
          for (int i = 0; i < size; i += shortSizeInBytes) {
            vector.putInt(rowId++, ByteUtil.toShortLittleEndian(pageData, i));
          }
        } else if (vectorDataType == DataTypes.LONG) {
          for (int i = 0; i < size; i += shortSizeInBytes) {
            vector.putLong(rowId++, ByteUtil.toShortLittleEndian(pageData, i));
          }
        } else if (vectorDataType == DataTypes.TIMESTAMP) {
          for (int i = 0; i < size; i += shortSizeInBytes) {
            vector.putLong(rowId++, (long) ByteUtil.toShortLittleEndian(pageData, i) * 1000);
          }
        } else if (DataTypes.isDecimal(vectorDataType)) {
          DecimalConverterFactory.DecimalConverter decimalConverter = vectorInfo.decimalConverter;
          decimalConverter.fillVector(pageData, pageSize, vectorInfo, nullBits, pageDataType);
        } else {
          for (int i = 0; i < size; i += shortSizeInBytes) {
            vector.putDouble(rowId++, ByteUtil.toShortLittleEndian(pageData, i));
          }
        }
      } else if (pageDataType == DataTypes.SHORT_INT) {
        if (vectorDataType == DataTypes.INT) {
          for (int i = 0; i < pageSize; i++) {
            int shortInt = ByteUtil.valueOf3Bytes(pageData, i * 3);
            vector.putInt(i, shortInt);
          }
        } else if (vectorDataType == DataTypes.LONG) {
          for (int i = 0; i < pageSize; i++) {
            int shortInt = ByteUtil.valueOf3Bytes(pageData, i * 3);
            vector.putLong(i, shortInt);
          }
        } else if (vectorDataType == DataTypes.TIMESTAMP) {
          for (int i = 0; i < pageSize; i++) {
            int shortInt = ByteUtil.valueOf3Bytes(pageData, i * 3);
            vector.putLong(i, (long) shortInt * 1000);
          }
        } else if (DataTypes.isDecimal(vectorDataType)) {
          DecimalConverterFactory.DecimalConverter decimalConverter = vectorInfo.decimalConverter;
          decimalConverter.fillVector(pageData, pageSize, vectorInfo, nullBits, pageDataType);
        } else {
          for (int i = 0; i < pageSize; i++) {
            int shortInt = ByteUtil.valueOf3Bytes(pageData, i * 3);
            vector.putDouble(i, shortInt);
          }
        }
      } else {
        if (pageDataType == DataTypes.INT) {
          int size = pageSize * intSizeInBytes;
          if (vectorDataType == DataTypes.INT) {
            for (int i = 0; i < size; i += intSizeInBytes) {
              vector.putInt(rowId++, ByteUtil.toIntLittleEndian(pageData, i));
            }
          } else if (vectorDataType == DataTypes.LONG) {
            for (int i = 0; i < size; i += intSizeInBytes) {
              vector.putLong(rowId++, ByteUtil.toIntLittleEndian(pageData, i));
            }
          } else if (vectorDataType == DataTypes.TIMESTAMP) {
            for (int i = 0; i < size; i += intSizeInBytes) {
              vector.putLong(rowId++, (long) ByteUtil.toIntLittleEndian(pageData, i) * 1000);
            }
          } else if (DataTypes.isDecimal(vectorDataType)) {
            DecimalConverterFactory.DecimalConverter decimalConverter = vectorInfo.decimalConverter;
            decimalConverter.fillVector(pageData, pageSize, vectorInfo, nullBits, pageDataType);
          } else {
            for (int i = 0; i < size; i += intSizeInBytes) {
              vector.putDouble(rowId++, ByteUtil.toIntLittleEndian(pageData, i));
            }
          }
        } else if (pageDataType == DataTypes.LONG) {
          int longSizeInBytes = DataTypes.LONG.getSizeInBytes();
          int size = pageSize * longSizeInBytes;
          if (vectorDataType == DataTypes.LONG) {
            for (int i = 0; i < size; i += longSizeInBytes) {
              vector.putLong(rowId++, ByteUtil.toLongLittleEndian(pageData, i));
            }
          } else if (vectorDataType == DataTypes.TIMESTAMP) {
            for (int i = 0; i < size; i += longSizeInBytes) {
              vector.putLong(rowId++, ByteUtil.toLongLittleEndian(pageData, i) * 1000);
            }
          } else if (DataTypes.isDecimal(vectorDataType)) {
            DecimalConverterFactory.DecimalConverter decimalConverter = vectorInfo.decimalConverter;
            decimalConverter.fillVector(pageData, pageSize, vectorInfo, nullBits, pageDataType);
          }
        } else if (pageDataType == DataTypes.BYTE_ARRAY) {
          // for complex primitive types
          if (vectorDataType == DataTypes.STRING || vectorDataType == DataTypes.BINARY
              || vectorDataType == DataTypes.VARCHAR) {
            // for complex primitive string, binary, varchar type
            int offset = 0;
            int length;
            for (int i = 0; i < pageSize; i++) {
              if (lengthStoredInBytes == intSizeInBytes) {
                length = ByteBuffer.wrap(pageData, offset, lengthStoredInBytes).getInt();
              } else {
                length = ByteBuffer.wrap(pageData, offset, lengthStoredInBytes).getShort();
              }
              offset += lengthStoredInBytes;
              if (vectorDataType == DataTypes.BINARY && length == 0) {
                vector.putNull(i);
                continue;
              }
              byte[] row = new byte[length];
              System.arraycopy(pageData, offset, row, 0, length);
              if (Arrays.equals(row, CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY)) {
                vector.putNull(i);
              } else {
                vector.putObject(i, row);
              }
              offset += length;
            }
          } else if (vectorDataType == DataTypes.DATE) {
            // for complex primitive date type
            int offset = 0;
            int length;
            for (int i = 0; i < pageSize; i++) {
              if (lengthStoredInBytes == intSizeInBytes) {
                length = ByteBuffer.wrap(pageData, offset, lengthStoredInBytes).getInt();
              } else {
                length = ByteBuffer.wrap(pageData, offset, lengthStoredInBytes).getShort();
              }
              offset += lengthStoredInBytes;
              if (length == 0) {
                vector.putObject(0, null);
              } else {
                // Calculating surrogate only in case of non-null values
                int surrogateInternal = ByteUtil.toXorInt(pageData, offset, intSizeInBytes);
                vector.putObject(0, surrogateInternal - DateDirectDictionaryGenerator.cutOffDate);
              }
              offset += length;
            }
          } else if (DataTypes.isDecimal(vectorDataType)) {
            // for complex primitive decimal type
            DecimalConverterFactory.DecimalConverter decimalConverter = vectorInfo.decimalConverter;
            if (decimalConverter == null) {
              decimalConverter = DecimalConverterFactory.INSTANCE
                  .getDecimalConverter(((DecimalType) vectorDataType).getPrecision(),
                      ((DecimalType) vectorDataType).getScale());
            }
            decimalConverter.fillVector(pageData, pageSize, vectorInfo, nullBits, pageDataType);
          }
        } else if (vectorDataType == DataTypes.FLOAT) {
          int floatSizeInBytes = DataTypes.FLOAT.getSizeInBytes();
          int size = pageSize * floatSizeInBytes;
          for (int i = 0; i < size; i += floatSizeInBytes) {
            vector.putFloat(rowId++, ByteUtil.toFloatLittleEndian(pageData, i));
          }
        } else {
          int doubleSizeInBytes = DataTypes.DOUBLE.getSizeInBytes();
          int size = pageSize * doubleSizeInBytes;
          for (int i = 0; i < size; i += doubleSizeInBytes) {
            vector.putDouble(rowId++, ByteUtil.toDoubleLittleEndian(pageData, i));
          }
        }
      }
    }
  };

}
