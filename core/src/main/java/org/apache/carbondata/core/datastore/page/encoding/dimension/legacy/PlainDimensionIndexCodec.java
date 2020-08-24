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

package org.apache.carbondata.core.datastore.page.encoding.dimension.legacy;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.columnar.BinaryPageIndexGenerator;
import org.apache.carbondata.core.datastore.columnar.PageIndexGenerator;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoder;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.format.Encoding;

public class PlainDimensionIndexCodec extends IndexStorageCodec {

  private final List<Encoding> encodingList;

  public PlainDimensionIndexCodec(boolean isSort, boolean isVarCharType) {
    super(isSort);
    encodingList = new ArrayList<>();
    encodingList.add(Encoding.DIRECT_STRING);
    if (isVarCharType) {
      encodingList.add(Encoding.DIRECT_COMPRESS_VARCHAR);
    }
  }

  @Override
  public String getName() {
    return "PlainDimensionIndexCodec";
  }

  @Override
  public ColumnPageEncoder createEncoder(Map<String, Object> parameter) {
    return new IndexStorageEncoder(true, null, encodingList) {
      private final int THREE_BYTES_MAX = (int) Math.pow(2, 23) - 1;
      private final int THREE_BYTES_MIN = -THREE_BYTES_MAX - 1;

      @Override
      protected void encodeIndexStorage(ColumnPage input) {
        PageIndexGenerator<byte[][]> pageIndexGenerator;
        Compressor compressor =
            CompressorFactory.getInstance().getCompressor(input.getColumnCompressorName());
        boolean isLVByteBufferPage = input.isLVByteBufferPage() || input.isLVActualColumnPage();
        byte[][] data = input.getByteArrayPage();
        DataType storeDataType = input.getColumnPageEncoderMeta().getStoreDataType();
        int encodedLength = CarbonCommonConstants.SHORT_SIZE_IN_BYTE;
        if (storeDataType == DataTypes.VARCHAR || storeDataType == DataTypes.BINARY) {
          encodedLength = CarbonCommonConstants.INT_SIZE_IN_BYTE;
        }
        // fill length array
        int[] lengthArray = new int[data.length];
        int max = Integer.MIN_VALUE;
        int min = Integer.MAX_VALUE;
        int currentDataLength;
        int size = 0;
        for (int i = 0; i < lengthArray.length; i++) {
          if (isLVByteBufferPage) {
            currentDataLength = data[i].length - encodedLength;
          } else {
            currentDataLength = data[i].length;
          }
          lengthArray[i] = currentDataLength;
          size += currentDataLength;
          if (max < currentDataLength) {
            max = currentDataLength;
          }
          if (min > currentDataLength) {
            min = currentDataLength;
          }
        }
        pageIndexGenerator = new BinaryPageIndexGenerator(data, isSort, lengthArray);
        // free memory
        selectedDataType = fitLongMinMax(max, min);
        byte[][] dataPage = pageIndexGenerator.getDataPage();
        ByteBuffer byteBuffer;
        if (DataTypes.BYTE == selectedDataType) {
          byteBuffer = ByteBuffer.allocate(lengthArray.length + size);
          for (int i = 0; i < lengthArray.length; i++) {
            byteBuffer.put((byte) lengthArray[i]);
            if (isLVByteBufferPage) {
              byte[] filteredByteArray =
                  Arrays.copyOfRange(dataPage[i], encodedLength, dataPage[i].length);
              byteBuffer.put(filteredByteArray);
            } else {
              byteBuffer.put(dataPage[i]);
            }
          }
        } else if (DataTypes.SHORT == selectedDataType) {
          byteBuffer = ByteBuffer.allocate((lengthArray.length * 2) + size);
          for (int i = 0; i < lengthArray.length; i++) {
            byteBuffer.putShort((short) lengthArray[i]);
            if (isLVByteBufferPage) {
              byte[] filteredByteArray =
                  Arrays.copyOfRange(dataPage[i], encodedLength, dataPage[i].length);
              byteBuffer.put(filteredByteArray);
            } else {
              byteBuffer.put(dataPage[i]);
            }
          }
        } else if (DataTypes.SHORT_INT == selectedDataType) {
          byteBuffer = ByteBuffer.allocate((lengthArray.length * 3) + size);
          for (int i = 0; i < lengthArray.length; i++) {
            byteBuffer.put(ByteUtil.to3Bytes(lengthArray[i]));
            if (isLVByteBufferPage) {
              byte[] filteredByteArray =
                  Arrays.copyOfRange(dataPage[i], encodedLength, dataPage[i].length);
              byteBuffer.put(filteredByteArray);
            } else {
              byteBuffer.put(dataPage[i]);
            }
          }
        } else {
          byteBuffer = ByteBuffer.allocate((lengthArray.length * 4) + size);
          for (int i = 0; i < lengthArray.length; i++) {
            byteBuffer.putInt(lengthArray[i]);
            if (isLVByteBufferPage) {
              byte[] filteredByteArray =
                  Arrays.copyOfRange(dataPage[i], encodedLength, dataPage[i].length);
              byteBuffer.put(filteredByteArray);
            } else {
              byteBuffer.put(dataPage[i]);
            }
          }
        }
        byteBuffer.rewind();
        byte[] flattened = byteBuffer.array();
        super.compressedDataPage = compressor.compressByte(flattened);
        super.pageIndexGenerator = pageIndexGenerator;
      }

      private DataType fitLongMinMax(int max, int min) {
        if (max <= Byte.MAX_VALUE && min >= Byte.MIN_VALUE) {
          return DataTypes.BYTE;
        } else if (max <= Short.MAX_VALUE && min >= Short.MIN_VALUE) {
          return DataTypes.SHORT;
        } else if (max <= THREE_BYTES_MAX && min >= THREE_BYTES_MIN) {
          return DataTypes.SHORT_INT;
        } else {
          return DataTypes.INT;
        }
      }
    };
  }
}
