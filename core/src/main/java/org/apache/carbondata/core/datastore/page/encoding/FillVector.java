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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.DecimalConverterFactory;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;
import org.apache.carbondata.core.scan.result.vector.impl.CarbonColumnVectorImpl;
import org.apache.carbondata.core.util.ByteUtil;

public class FillVector {
  private byte[] pageData;
  private float floatFactor = 0;
  private double factor = 0;
  private ColumnVectorInfo vectorInfo;
  private BitSet nullBits;

  public FillVector(byte[] pageData, ColumnVectorInfo vectorInfo, BitSet nullBits) {
    this.pageData = pageData;
    this.vectorInfo = vectorInfo;
    this.nullBits = nullBits;
  }

  public void setFactor(double factor) {
    this.factor = factor;
  }

  public void setFloatFactor(float floatFactor) {
    this.floatFactor = floatFactor;
  }

  public void basedOnType(CarbonColumnVector vector, DataType vectorDataType, int pageSize,
      DataType pageDataType) {
    if (vectorInfo.vector.getColumnVector() != null && ((CarbonColumnVectorImpl) vectorInfo.vector
        .getColumnVector()).isComplex()) {
      fillComplexType(vector.getColumnVector(), pageDataType);
    } else {
      fillPrimitiveType(vector, vectorDataType, pageSize, pageDataType);
      vector.setIndex(0);
    }
  }

  private void fillComplexType(CarbonColumnVector vector, DataType pageDataType) {
    CarbonColumnVectorImpl vectorImpl = (CarbonColumnVectorImpl) vector;
    if (vector != null && vector.getChildrenVector() != null) {
      ArrayList<Integer> childElements = ((CarbonColumnVectorImpl) vector).getChildrenElements();
      for (int i = 0; i < childElements.size(); i++) {
        int count = childElements.get(i);
        typeComplexObject(vectorImpl.getChildrenVector().get(0), count, pageDataType);
        vector.putArrayObject();
      }
      vectorImpl.getChildrenVector().get(0).setIndex(0);
    }
  }

  private void fillPrimitiveType(CarbonColumnVector vector, DataType vectorDataType, int pageSize,
      DataType pageDataType) {
    // offset which denotes the start index for pageData
    int pageIndex = vector.getIndex();
    int rowId = 0;

    // Filling into vector is done based on page data type

    if (vectorDataType == DataTypes.FLOAT && floatFactor != 0.0) {
      if (pageDataType == DataTypes.BOOLEAN || pageDataType == DataTypes.BYTE) {
        for (int i = 0; i < pageSize; i++) {
          vector.putFloat(i, (pageData[pageIndex++] / floatFactor));
        }
      } else if (pageDataType == DataTypes.SHORT) {
        int size = pageSize * DataTypes.SHORT.getSizeInBytes();
        for (int i = 0; i < size; i += DataTypes.SHORT.getSizeInBytes()) {
          vector.putFloat(rowId++,
              (ByteUtil.toShortLittleEndian(pageData, pageIndex + i) / floatFactor));
        }
        pageIndex += size;
      } else if (pageDataType == DataTypes.SHORT_INT) {
        int size = pageSize * DataTypes.SHORT_INT.getSizeInBytes();
        for (int i = 0; i < size; i += DataTypes.SHORT_INT.getSizeInBytes()) {
          vector.putFloat(rowId++, (ByteUtil.valueOf3Bytes(pageData, pageIndex + i) / floatFactor));
        }
        pageIndex += size;
      } else if (pageDataType == DataTypes.INT) {
        int size = pageSize * DataTypes.INT.getSizeInBytes();
        for (int i = 0; i < size; i += DataTypes.INT.getSizeInBytes()) {
          vector.putFloat(rowId++,
              (ByteUtil.toIntLittleEndian(pageData, pageIndex + i) / floatFactor));
        }
        pageIndex += size;
      } else if (pageDataType == DataTypes.LONG) {
        int size = pageSize * DataTypes.LONG.getSizeInBytes();
        for (int i = 0; i < size; i += DataTypes.LONG.getSizeInBytes()) {
          vector.putFloat(rowId++,
              (float) (ByteUtil.toLongLittleEndian(pageData, pageIndex + i) / factor));
        }
        pageIndex += size;
      } else {
        throw new RuntimeException("internal error: " + this.toString());
      }
    } else if (vectorDataType == DataTypes.DOUBLE && factor != 0.0) {
      if (pageDataType == DataTypes.BOOLEAN || pageDataType == DataTypes.BYTE) {
        for (int i = 0; i < pageSize; i++) {
          vector.putDouble(i, (pageData[pageIndex++] / factor));
        }
      } else if (pageDataType == DataTypes.SHORT) {
        int size = pageSize * DataTypes.SHORT.getSizeInBytes();
        for (int i = 0; i < size; i += DataTypes.SHORT.getSizeInBytes()) {
          vector
              .putDouble(rowId++, (ByteUtil.toShortLittleEndian(pageData, pageIndex + i) / factor));
        }
        pageIndex += size;
      } else if (pageDataType == DataTypes.SHORT_INT) {
        int size = pageSize * DataTypes.SHORT_INT.getSizeInBytes();
        for (int i = 0; i < size; i += DataTypes.SHORT_INT.getSizeInBytes()) {
          vector.putDouble(rowId++, (ByteUtil.valueOf3Bytes(pageData, pageIndex + i) / factor));
        }
        pageIndex += size;
      } else if (pageDataType == DataTypes.INT) {
        int size = pageSize * DataTypes.INT.getSizeInBytes();
        for (int i = 0; i < size; i += DataTypes.INT.getSizeInBytes()) {
          vector.putDouble(rowId++, (ByteUtil.toIntLittleEndian(pageData, pageIndex + i) / factor));
        }
        pageIndex += size;
      } else if (pageDataType == DataTypes.LONG) {
        int size = pageSize * DataTypes.LONG.getSizeInBytes();
        for (int i = 0; i < size; i += DataTypes.LONG.getSizeInBytes()) {
          vector
              .putDouble(rowId++, (ByteUtil.toLongLittleEndian(pageData, pageIndex + i) / factor));
        }
        pageIndex += size;
      } else {
        throw new RuntimeException("Unsupported datatype : " + pageDataType);
      }
    } else if (pageDataType == DataTypes.BYTE_ARRAY) {
      if (vectorDataType == DataTypes.STRING) {
        int offset = pageIndex;
        for (int j = 0; j < pageSize; j++) {
          byte[] stringLen = new byte[4];
          for (int k = 0; k < stringLen.length; k++) {
            stringLen[k] = this.pageData[k + offset];
          }
          ByteBuffer wrapped = ByteBuffer.wrap(stringLen, 0, 4);
          int len = wrapped.getInt();
          offset += 4;
          byte[] row = new byte[len];
          System.arraycopy(this.pageData, offset, row, 0, len);
          vector.putObject(0, row);
          offset += len;
        }
        pageIndex = offset;
      }
    } else if (pageDataType == DataTypes.BOOLEAN || pageDataType == DataTypes.BYTE) {
      if (vectorDataType == DataTypes.SHORT) {
        for (int i = 0; i < pageSize; i++) {
          vector.putShort(i, (short) pageData[pageIndex++]);
        }
      } else if (vectorDataType == DataTypes.INT) {
        for (int i = 0; i < pageSize; i++) {
          vector.putInt(i, (int) pageData[pageIndex++]);
        }
      } else if (vectorDataType == DataTypes.LONG) {
        for (int i = 0; i < pageSize; i++) {
          vector.putLong(i, pageData[pageIndex++]);
        }
      } else if (vectorDataType == DataTypes.TIMESTAMP) {
        for (int i = 0; i < pageSize; i++) {
          vector.putLong(i, (long) pageData[pageIndex++] * 1000);
        }
      } else if (vectorDataType == DataTypes.BOOLEAN || vectorDataType == DataTypes.BYTE) {
        vector.putBytes(0, pageSize, pageData, pageIndex);
        pageIndex += pageSize;
      } else if (DataTypes.isDecimal(vectorDataType)) {
        DecimalConverterFactory.DecimalConverter decimalConverter = vectorInfo.decimalConverter;
        decimalConverter.fillVector(pageData, pageSize, vectorInfo, nullBits, pageDataType);
      } else if (vectorDataType == DataTypes.FLOAT) {
        for (int i = 0; i < pageSize; i++) {
          vector.putFloat(i, pageData[pageIndex++]);
        }
      } else {
        for (int i = 0; i < pageSize; i++) {
          vector.putDouble(i, pageData[pageIndex++]);
        }
      }
    } else if (pageDataType == DataTypes.SHORT) {
      int size = pageSize * DataTypes.SHORT.getSizeInBytes();
      if (vectorDataType == DataTypes.SHORT) {
        for (int i = 0; i < size; i += DataTypes.SHORT.getSizeInBytes()) {
          vector.putShort(rowId++, (ByteUtil.toShortLittleEndian(pageData, pageIndex + i)));
        }
      } else if (vectorDataType == DataTypes.INT) {
        for (int i = 0; i < size; i += DataTypes.SHORT.getSizeInBytes()) {
          vector.putInt(rowId++, (ByteUtil.toShortLittleEndian(pageData, pageIndex + i)));
        }
      } else if (vectorDataType == DataTypes.LONG) {
        for (int i = 0; i < size; i += DataTypes.SHORT.getSizeInBytes()) {
          vector.putLong(rowId++, (ByteUtil.toShortLittleEndian(pageData, pageIndex + i)));
        }
      } else if (vectorDataType == DataTypes.TIMESTAMP) {
        for (int i = 0; i < size; i += DataTypes.SHORT.getSizeInBytes()) {
          vector.putLong(rowId++,
              ((long) ByteUtil.toShortLittleEndian(pageData, pageIndex + i)) * 1000);
        }
      } else if (DataTypes.isDecimal(vectorDataType)) {
        DecimalConverterFactory.DecimalConverter decimalConverter = vectorInfo.decimalConverter;
        decimalConverter.fillVector(pageData, pageSize, vectorInfo, nullBits, pageDataType);
      } else if (vectorDataType == DataTypes.FLOAT) {
        for (int i = 0; i < size; i += DataTypes.SHORT.getSizeInBytes()) {
          vector.putFloat(rowId++, (float) (ByteUtil.toShortLittleEndian(pageData, pageIndex + i)));
        }
      } else {
        for (int i = 0; i < size; i += DataTypes.SHORT.getSizeInBytes()) {
          vector.putDouble(rowId++, (ByteUtil.toShortLittleEndian(pageData, pageIndex + i)));
        }
      }
      pageIndex += size;
    } else if (pageDataType == DataTypes.SHORT_INT) {
      if (vectorDataType == DataTypes.INT) {
        for (int i = 0; i < pageSize; i++) {
          int shortInt = ByteUtil.valueOf3Bytes(pageData, (pageIndex + i) * 3);
          vector.putInt(i, shortInt);
        }
      } else if (vectorDataType == DataTypes.LONG) {
        for (int i = 0; i < pageSize; i++) {
          int shortInt = ByteUtil.valueOf3Bytes(pageData, (pageIndex + i) * 3);
          vector.putLong(i, shortInt);
        }
      } else if (vectorDataType == DataTypes.TIMESTAMP) {
        for (int i = 0; i < pageSize; i++) {
          int shortInt = ByteUtil.valueOf3Bytes(pageData, (pageIndex + i) * 3);
          vector.putLong(i, (long) shortInt * 1000);
        }
      } else if (DataTypes.isDecimal(vectorDataType)) {
        DecimalConverterFactory.DecimalConverter decimalConverter = vectorInfo.decimalConverter;
        decimalConverter.fillVector(pageData, pageSize, vectorInfo, nullBits, DataTypes.SHORT_INT);
      } else if (vectorDataType == DataTypes.FLOAT) {
        for (int i = 0; i < pageSize; i++) {
          int shortInt = ByteUtil.valueOf3Bytes(pageData, (pageIndex + i) * 3);
          vector.putFloat(i, shortInt);
        }
      } else {
        for (int i = 0; i < pageSize; i++) {
          int shortInt = ByteUtil.valueOf3Bytes(pageData, (pageIndex + i) * 3);
          vector.putDouble(i, shortInt);
        }
      }
      pageIndex += pageSize;
    } else if (pageDataType == DataTypes.INT) {
      int size = pageSize * DataTypes.INT.getSizeInBytes();
      if (vectorDataType == DataTypes.INT) {
        for (int i = 0; i < size; i += DataTypes.INT.getSizeInBytes()) {
          vector.putInt(rowId++, ByteUtil.toIntLittleEndian(pageData, pageIndex + i));
        }
      } else if (vectorDataType == DataTypes.LONG) {
        for (int i = 0; i < size; i += DataTypes.INT.getSizeInBytes()) {
          vector.putLong(rowId++, ByteUtil.toIntLittleEndian(pageData, pageIndex + i));
        }
      } else if (vectorDataType == DataTypes.TIMESTAMP) {
        for (int i = 0; i < size; i += DataTypes.INT.getSizeInBytes()) {
          vector
              .putLong(rowId++, (long) ByteUtil.toIntLittleEndian(pageData, pageIndex + i) * 1000);
        }
      } else if (DataTypes.isDecimal(vectorDataType)) {
        DecimalConverterFactory.DecimalConverter decimalConverter = vectorInfo.decimalConverter;
        decimalConverter.fillVector(pageData, pageSize, vectorInfo, nullBits, pageDataType);
      } else {
        for (int i = 0; i < size; i += DataTypes.INT.getSizeInBytes()) {
          vector.putDouble(rowId++, (ByteUtil.toIntLittleEndian(pageData, pageIndex + i)));
        }
      }
      pageIndex += size;
    } else if (pageDataType == DataTypes.LONG) {
      int size = pageSize * DataTypes.LONG.getSizeInBytes();
      if (vectorDataType == DataTypes.LONG) {
        for (int i = 0; i < size; i += DataTypes.LONG.getSizeInBytes()) {
          vector.putLong(rowId++, ByteUtil.toLongLittleEndian(pageData, pageIndex + i));
        }
      } else if (vectorDataType == DataTypes.TIMESTAMP) {
        for (int i = 0; i < size; i += DataTypes.LONG.getSizeInBytes()) {
          vector.putLong(rowId++, ByteUtil.toLongLittleEndian(pageData, pageIndex + i) * 1000);
        }
      } else if (DataTypes.isDecimal(vectorDataType)) {
        DecimalConverterFactory.DecimalConverter decimalConverter = vectorInfo.decimalConverter;
        decimalConverter.fillVector(pageData, pageSize, vectorInfo, nullBits, pageDataType);
      }
      pageIndex += size;
    } else if (vectorDataType == DataTypes.FLOAT) {
      int size = pageSize * DataTypes.FLOAT.getSizeInBytes();
      for (int i = 0; i < size; i += DataTypes.FLOAT.getSizeInBytes()) {
        vector.putFloat(rowId++, (ByteUtil.toFloatLittleEndian(this.pageData, pageIndex + i)));
      }
      pageIndex += size;
    } else {
      int size = pageSize * DataTypes.DOUBLE.getSizeInBytes();
      for (int i = 0; i < size; i += DataTypes.DOUBLE.getSizeInBytes()) {
        vector.putDouble(rowId++, (ByteUtil.toDoubleLittleEndian(pageData, pageIndex + i)));
      }
      pageIndex += size;
    }
    vector.setIndex(pageIndex);
  }

  private void typeArray(CarbonColumnVectorImpl vector, int len, DataType pageDataType) {
    ArrayList<Integer> childElements = vector.getChildrenElements();
    int childIndex = vector.getIndex();

    for (int i = 0; i < len; i++) {
      typeComplexObject(vector.getChildrenVector().get(0), childElements.get(childIndex),
          pageDataType);
      vector.putArrayObject();
      childIndex++;
    }
    vector.setIndex(childIndex);

  }

  // Filling of respective complex types - array/map/struct
  private void typeComplexObject(CarbonColumnVectorImpl vector, int len, DataType pageDataType) {
    if (vector.isComplex()) {
      if (vector.getDataTypeName().equals("ARRAY")) {
        typeArray(vector, len, pageDataType);
      }
      // TODO: Similarly should handle cases for Map and Struct too
    } else {
      fillPrimitiveType(vector, vector.getType(), len, pageDataType);
    }
  }
}
