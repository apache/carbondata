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

package org.apache.carbondata.processing.sortandgroupby.sortdata;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.processing.sortandgroupby.exception.CarbonSortKeyAndGroupByException;
import org.apache.carbondata.processing.util.NonDictionaryUtil;

public class UnCompressedTempSortFileWriter extends AbstractTempSortFileWriter {

  /**
   * UnCompressedTempSortFileWriter
   *
   * @param writeBufferSize
   * @param dimensionCount
   * @param measureCount
   */
  public UnCompressedTempSortFileWriter(int dimensionCount, int complexDimensionCount,
      int measureCount, int noDictionaryCount, int writeBufferSize) {
    super(dimensionCount, complexDimensionCount, measureCount, noDictionaryCount, writeBufferSize);
  }

  public static void writeDataOutputStream(Object[][] records, DataOutputStream dataOutputStream,
      int measureCount, int dimensionCount, int noDictionaryCount, int complexDimensionCount)
      throws IOException {
    Object[] row;
    for (int recordIndex = 0; recordIndex < records.length; recordIndex++) {
      row = records[recordIndex];
      int fieldIndex = 0;

      for (int counter = 0; counter < dimensionCount; counter++) {
        dataOutputStream.writeInt((Integer) NonDictionaryUtil.getDimension(fieldIndex++, row));
      }

      //write byte[] of high card dims
      if (noDictionaryCount > 0) {
        dataOutputStream.write(NonDictionaryUtil.getByteArrayForNoDictionaryCols(row));
      }
      fieldIndex = 0;
      for (int counter = 0; counter < complexDimensionCount; counter++) {
        int complexByteArrayLength = ((byte[]) row[fieldIndex]).length;
        dataOutputStream.writeInt(complexByteArrayLength);
        dataOutputStream.write(((byte[]) row[fieldIndex++]));
      }

      for (int counter = 0; counter < measureCount; counter++) {
        if (null != row[fieldIndex]) {
          dataOutputStream.write((byte) 1);
          dataOutputStream.writeDouble((Double) NonDictionaryUtil.getMeasure(fieldIndex, row));
        } else {
          dataOutputStream.write((byte) 0);
        }

        fieldIndex++;
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
      recordSize = (measureCount * CarbonCommonConstants.DOUBLE_SIZE_IN_BYTE) + (dimensionCount
          * CarbonCommonConstants.INT_SIZE_IN_BYTE);
      totalSize = records.length * recordSize;

      blockDataArray = new ByteArrayOutputStream(totalSize);
      dataOutputStream = new DataOutputStream(blockDataArray);

      writeDataOutputStream(records, dataOutputStream, measureCount, dimensionCount,
          noDictionaryCount, complexDimensionCount);
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
