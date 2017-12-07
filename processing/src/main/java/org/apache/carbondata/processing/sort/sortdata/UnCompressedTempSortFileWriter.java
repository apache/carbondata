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

package org.apache.carbondata.processing.sort.sortdata;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.processing.sort.exception.CarbonSortKeyAndGroupByException;

public class UnCompressedTempSortFileWriter extends AbstractTempSortFileWriter {

  /**
   * UnCompressedTempSortFileWriter
   *
   * @param tableFieldStat
   * @param writeBufferSize
   */
  public UnCompressedTempSortFileWriter(TableFieldStat tableFieldStat, int writeBufferSize) {
    super(tableFieldStat, writeBufferSize);
  }

  public static void writeDataOutputStream(Object[][] records, DataOutputStream dataOutputStream,
      TableFieldStat tableFieldStat)
      throws IOException {
    Object[] row;

    // write each row
    for (int recordIndex = 0; recordIndex < records.length; recordIndex++) {
      row = records[recordIndex];
      int fieldIndex = 0;

      // write dictionary and non dictionary dimensions
      for (; fieldIndex < tableFieldStat.getIsDimNoDictFlags().length; fieldIndex++) {
        if (tableFieldStat.getIsDimNoDictFlags()[fieldIndex]) {
          byte[] col = (byte[]) row[fieldIndex];
          dataOutputStream.writeShort(col.length);
          dataOutputStream.write(col);
        } else {
          dataOutputStream.writeInt((int) row[fieldIndex]);
        }
      }

      // write complex dimensions
      for (; fieldIndex < tableFieldStat.getDimCnt() + tableFieldStat.getComplexDimCnt();
           fieldIndex++) {
        byte[] value = (byte[]) row[fieldIndex];
        dataOutputStream.writeShort(value.length);
        dataOutputStream.write(value);
      }

      // write measures
      for (int idx = 0; idx < tableFieldStat.getMeasureCnt(); idx++) {
        Object value = row[fieldIndex + idx];
        if (null != value) {
          dataOutputStream.write((byte) 1);
          DataType dataType = tableFieldStat.getMeasureDataType()[idx];
          if (dataType == DataTypes.BOOLEAN) {
            dataOutputStream.writeBoolean((boolean) value);
          } else if (dataType == DataTypes.SHORT) {
            dataOutputStream.writeShort((Short) value);
          } else if (dataType == DataTypes.INT) {
            dataOutputStream.writeInt((Integer) value);
          } else if (dataType == DataTypes.LONG) {
            dataOutputStream.writeLong((Long) value);
          } else if (dataType == DataTypes.DOUBLE) {
            dataOutputStream.writeDouble((Double) value);
          } else if (DataTypes.isDecimal(dataType)) {
            BigDecimal val = (BigDecimal) value;
            byte[] bigDecimalInBytes = DataTypeUtil.bigDecimalToByte(val);
            dataOutputStream.writeInt(bigDecimalInBytes.length);
            dataOutputStream.write(bigDecimalInBytes);
          } else {
            throw new IllegalArgumentException("unsupported data type:"
                + tableFieldStat.getMeasureDataType()[idx]);
          }
        } else {
          dataOutputStream.write((byte) 0);
        }
      }
    }
  }

  /**
   * Below method will be used to write the sort temp file
   *
   * @param records
   */
  public void writeSortTempFile(Object[][] records) throws CarbonSortKeyAndGroupByException {
    ByteArrayOutputStream blockDataArray = null;
    DataOutputStream dataOutputStream = null;
    int totalSize = 0;
    int recordSize = 0;
    try {
      recordSize =
          (tableFieldStat.getMeasureCnt() * CarbonCommonConstants.DOUBLE_SIZE_IN_BYTE) + (
              tableFieldStat.getDimCnt() * CarbonCommonConstants.INT_SIZE_IN_BYTE);
      totalSize = records.length * recordSize;

      blockDataArray = new ByteArrayOutputStream(totalSize);
      dataOutputStream = new DataOutputStream(blockDataArray);

      writeDataOutputStream(records, dataOutputStream, tableFieldStat);
      stream.writeInt(records.length);
      byte[] byteArray = blockDataArray.toByteArray();
      stream.writeInt(byteArray.length);
      stream.write(byteArray);

    } catch (IOException e) {
      throw new CarbonSortKeyAndGroupByException(e);
    } finally {
      CarbonUtil.closeStreams(blockDataArray);
      CarbonUtil.closeStreams(dataOutputStream);
    }
  }
}
