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

package org.apache.carbondata.processing.loading.sort;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.apache.carbondata.core.memory.CarbonUnsafe;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.core.util.NonDictionaryUtil;
import org.apache.carbondata.processing.loading.row.IntermediateSortTempRow;
import org.apache.carbondata.processing.sort.sortdata.SortParameters;
import org.apache.carbondata.processing.sort.sortdata.TableFieldStat;

/**
 * This class is used to convert/write/read row in sort step in carbondata.
 * It consists the following function:
 * 1. convert raw row & intermediate sort temp row to 3-parted row
 * 2. read/write intermediate sort temp row to sort temp file & unsafe memory
 * 3. write raw row directly to sort temp file & unsafe memory as intermediate sort temp row
 */
public class SortStepRowHandler implements Serializable {
  private static final long serialVersionUID = 1L;
  private int dictSortDimCnt = 0;
  private int dictNoSortDimCnt = 0;
  private int noDictSortDimCnt = 0;
  private int noDictNoSortDimCnt = 0;
  private int measureCnt;

  // indices for dict & sort dimension columns
  private int[] dictSortDimIdx;
  // indices for dict & no-sort dimension columns
  private int[] dictNoSortDimIdx;
  // indices for no-dict & sort dimension columns
  private int[] noDictSortDimIdx;
  // indices for no-dict & no-sort dimension columns, including complex columns
  private int[] noDictNoSortDimIdx;
  // indices for measure columns
  private int[] measureIdx;

  private DataType[] dataTypes;

  /**
   * constructor
   * @param tableFieldStat table field stat
   */
  public SortStepRowHandler(TableFieldStat tableFieldStat) {
    this.dictSortDimCnt = tableFieldStat.getDictSortDimCnt();
    this.dictNoSortDimCnt = tableFieldStat.getDictNoSortDimCnt();
    this.noDictSortDimCnt = tableFieldStat.getNoDictSortDimCnt();
    this.noDictNoSortDimCnt = tableFieldStat.getNoDictNoSortDimCnt();
    this.measureCnt = tableFieldStat.getMeasureCnt();
    this.dictSortDimIdx = tableFieldStat.getDictSortDimIdx();
    this.dictNoSortDimIdx = tableFieldStat.getDictNoSortDimIdx();
    this.noDictSortDimIdx = tableFieldStat.getNoDictSortDimIdx();
    this.noDictNoSortDimIdx = tableFieldStat.getNoDictNoSortDimIdx();
    this.measureIdx = tableFieldStat.getMeasureIdx();
    this.dataTypes = tableFieldStat.getMeasureDataType();
  }

  /**
   * constructor
   * @param sortParameters sort parameters
   */
  public SortStepRowHandler(SortParameters sortParameters) {
    this(new TableFieldStat(sortParameters));
  }

  /**
   * Convert carbon row from raw format to 3-parted format.
   * This method is used in global-sort.
   *
   * @param row raw row whose length is the same as field number
   * @return 3-parted row whose length is 3. (1 for dict dims ,1 for non-dict and complex,
   * 1 for measures)
   */
  public Object[] convertRawRowTo3Parts(Object[] row) {
    Object[] holder = new Object[3];
    try {
      int[] dictDims
          = new int[this.dictSortDimCnt + this.dictNoSortDimCnt];
      byte[][] nonDictArray = new byte[this.noDictSortDimCnt + this.noDictNoSortDimCnt][];
      Object[] measures = new Object[this.measureCnt];

      // convert dict & data
      int idxAcc = 0;
      for (int idx = 0; idx < this.dictSortDimCnt; idx++) {
        dictDims[idxAcc++] = (int) row[this.dictSortDimIdx[idx]];
      }

      // convert dict & no-sort
      for (int idx = 0; idx < this.dictNoSortDimCnt; idx++) {
        dictDims[idxAcc++] = (int) row[this.dictNoSortDimIdx[idx]];
      }
      // convert no-dict & sort
      idxAcc = 0;
      for (int idx = 0; idx < this.noDictSortDimCnt; idx++) {
        nonDictArray[idxAcc++] = (byte[]) row[this.noDictSortDimIdx[idx]];
      }
      // convert no-dict & no-sort
      for (int idx = 0; idx < this.noDictNoSortDimCnt; idx++) {
        nonDictArray[idxAcc++] = (byte[]) row[this.noDictNoSortDimIdx[idx]];
      }

      // convert measure data
      for (int idx = 0; idx < this.measureCnt; idx++) {
        measures[idx] = row[this.measureIdx[idx]];
      }

      NonDictionaryUtil.prepareOutObj(holder, dictDims, nonDictArray, measures);
    } catch (Exception e) {
      throw new RuntimeException("Problem while converting row to 3 parts", e);
    }
    return holder;
  }

  /**
   * Convert intermediate sort temp row to 3-parted row.
   * This method is used in the final merge sort to feed rows to the next write step.
   *
   * @param sortTempRow intermediate sort temp row
   * @return 3-parted row
   */
  public Object[] convertIntermediateSortTempRowTo3Parted(IntermediateSortTempRow sortTempRow) {
    int[] dictDims
        = new int[this.dictSortDimCnt + this.dictNoSortDimCnt];
    byte[][] noDictArray
        = new byte[this.noDictSortDimCnt + this.noDictNoSortDimCnt][];

    int[] dictNoSortDims = new int[this.dictNoSortDimCnt];
    byte[][] noDictNoSortDims = new byte[this.noDictNoSortDimCnt][];
    Object[] measures = new Object[this.measureCnt];

    sortTempRow.unpackNoSortFromBytes(dictNoSortDims, noDictNoSortDims, measures, this.dataTypes);

    // dict dims
    System.arraycopy(sortTempRow.getDictSortDims(), 0 , dictDims,
        0, this.dictSortDimCnt);
    System.arraycopy(dictNoSortDims, 0, dictDims,
        this.dictSortDimCnt, this.dictNoSortDimCnt);;

    // no dict dims, including complex
    System.arraycopy(sortTempRow.getNoDictSortDims(), 0,
        noDictArray, 0, this.noDictSortDimCnt);
    System.arraycopy(noDictNoSortDims, 0, noDictArray,
        this.noDictSortDimCnt, this.noDictNoSortDimCnt);

    // measures are already here

    Object[] holder = new Object[3];
    NonDictionaryUtil.prepareOutObj(holder, dictDims, noDictArray, measures);
    return holder;
  }

  /**
   * Read intermediate sort temp row from InputStream.
   * This method is used during the merge sort phase to read row from sort temp file.
   *
   * @param inputStream input stream
   * @return a row that contains three parts
   * @throws IOException if error occrus while reading from stream
   */
  public IntermediateSortTempRow readIntermediateSortTempRowFromInputStream(
      DataInputStream inputStream) throws IOException {
    int[] dictSortDims = new int[this.dictSortDimCnt];
    byte[][] noDictSortDims = new byte[this.noDictSortDimCnt][];

    // read dict & sort dim data
    for (int idx = 0; idx < this.dictSortDimCnt; idx++) {
      dictSortDims[idx] = inputStream.readInt();
    }

    // read no-dict & sort data
    for (int idx = 0; idx < this.noDictSortDimCnt; idx++) {
      short len = inputStream.readShort();
      byte[] bytes = new byte[len];
      inputStream.readFully(bytes);
      noDictSortDims[idx] = bytes;
    }

    // read no-dict dims & measures
    int len = inputStream.readInt();
    byte[] noSortDimsAndMeasures = new byte[len];
    inputStream.readFully(noSortDimsAndMeasures);

    return new IntermediateSortTempRow(dictSortDims, noDictSortDims, noSortDimsAndMeasures);
  }

  /**
   * Write intermediate sort temp row to OutputStream
   * This method is used during the merge sort phase to write row to sort temp file.
   *
   * @param sortTempRow intermediate sort temp row
   * @param outputStream output stream
   * @throws IOException if error occurs while writing to stream
   */
  public void writeIntermediateSortTempRowToOutputStream(IntermediateSortTempRow sortTempRow,
      DataOutputStream outputStream) throws IOException {
    // write dict & sort dim
    for (int idx = 0; idx < this.dictSortDimCnt; idx++) {
      outputStream.writeInt(sortTempRow.getDictSortDims()[idx]);
    }

    // write no-dict & sort dim
    for (int idx = 0; idx < this.noDictSortDimCnt; idx++) {
      byte[] bytes = sortTempRow.getNoDictSortDims()[idx];
      outputStream.writeShort(bytes.length);
      outputStream.write(bytes);
    }

    // write packed no-sort dim & measure
    outputStream.writeInt(sortTempRow.getNoSortDimsAndMeasures().length);
    outputStream.write(sortTempRow.getNoSortDimsAndMeasures());
  }

  /**
   * Write raw row as an intermediate sort temp row to sort temp file.
   * This method is used in the beginning of the sort phase. Comparing with converting raw row to
   * intermediate sort temp row and then writing the converted one, Writing raw row directly will
   * save the intermediate trivial loss.
   * This method use an array backend buffer to save memory allocation. The buffer will be reused
   * for all rows (per thread).
   *
   * @param row raw row
   * @param outputStream output stream
   * @param rowBuffer array backend buffer
   * @throws IOException if error occurs while writing to stream
   */
  public void writeRawRowAsIntermediateSortTempRowToOutputStream(Object[] row,
      DataOutputStream outputStream, ByteBuffer rowBuffer) throws IOException {
    // write dict & sort
    for (int idx = 0; idx < this.dictSortDimCnt; idx++) {
      outputStream.writeInt((int) row[this.dictSortDimIdx[idx]]);
    }

    // write no-dict & sort
    for (int idx = 0; idx < this.noDictSortDimCnt; idx++) {
      byte[] bytes = (byte[]) row[this.noDictSortDimIdx[idx]];
      outputStream.writeShort(bytes.length);
      outputStream.write(bytes);
    }

    // pack no-sort
    rowBuffer.clear();
    packNoSortFieldsToBytes(row, rowBuffer);
    rowBuffer.flip();
    int packSize = rowBuffer.limit();

    // write no-sort
    outputStream.writeInt(packSize);
    outputStream.write(rowBuffer.array(), 0, packSize);
  }

  /**
   * Read intermediate sort temp row from unsafe memory.
   * This method is used during merge sort phase for off-heap sort.
   *
   * @param baseObject base object of memory block
   * @param address address of the row
   * @return intermediate sort temp row
   */
  public IntermediateSortTempRow readIntermediateSortTempRowFromUnsafeMemory(Object baseObject,
      long address) {
    int size = 0;

    int[] dictSortDims = new int[this.dictSortDimCnt];
    byte[][] noDictSortDims = new byte[this.noDictSortDimCnt][];

    // read dict & sort dim
    for (int idx = 0; idx < dictSortDims.length; idx++) {
      dictSortDims[idx] = CarbonUnsafe.getUnsafe().getInt(baseObject, address + size);
      size += 4;
    }

    // read no-dict & sort dim
    for (int idx = 0; idx < noDictSortDims.length; idx++) {
      short length = CarbonUnsafe.getUnsafe().getShort(baseObject, address + size);
      size += 2;
      byte[] bytes = new byte[length];
      CarbonUnsafe.getUnsafe().copyMemory(baseObject, address + size,
          bytes, CarbonUnsafe.BYTE_ARRAY_OFFSET, length);
      size += length;
      noDictSortDims[idx] = bytes;
    }

    // read no-sort dims & measures
    int len = CarbonUnsafe.getUnsafe().getInt(baseObject, address + size);
    size += 4;
    byte[] noSortDimsAndMeasures = new byte[len];
    CarbonUnsafe.getUnsafe().copyMemory(baseObject, address + size,
        noSortDimsAndMeasures, CarbonUnsafe.BYTE_ARRAY_OFFSET, len);

    return new IntermediateSortTempRow(dictSortDims, noDictSortDims, noSortDimsAndMeasures);
  }

  /**
   * Write intermediate sort temp row directly from unsafe memory to stream.
   * This method is used at the late beginning of the sort phase to write in-memory pages
   * to sort temp file. Comparing with reading intermediate sort temp row from memory and then
   * writing it, Writing directly from memory to stream will save the intermediate trivial loss.
   *
   * @param baseObject base object of the memory block
   * @param address base address of the row
   * @param outputStream output stream
   * @throws IOException if error occurs while writing to stream
   */
  public void writeIntermediateSortTempRowFromUnsafeMemoryToStream(Object baseObject,
      long address, DataOutputStream outputStream) throws IOException {
    int size = 0;

    // dict & sort
    for (int idx = 0; idx < dictSortDimCnt; idx++) {
      outputStream.writeInt(CarbonUnsafe.getUnsafe().getInt(baseObject, address + size));
      size += 4;
    }

    // no-dict & sort
    for (int idx = 0; idx < noDictSortDimCnt; idx++) {
      short length = CarbonUnsafe.getUnsafe().getShort(baseObject, address + size);
      size += 2;
      byte[] bytes = new byte[length];
      CarbonUnsafe.getUnsafe().copyMemory(baseObject, address + size,
          bytes, CarbonUnsafe.BYTE_ARRAY_OFFSET, length);
      size += length;

      outputStream.writeShort(length);
      outputStream.write(bytes);
    }

    // packed no-sort & measure
    int len = CarbonUnsafe.getUnsafe().getInt(baseObject, address + size);
    size += 4;
    byte[] noSortDimsAndMeasures = new byte[len];
    CarbonUnsafe.getUnsafe().copyMemory(baseObject, address + size,
        noSortDimsAndMeasures, CarbonUnsafe.BYTE_ARRAY_OFFSET, len);
    size += len;

    outputStream.writeInt(len);
    outputStream.write(noSortDimsAndMeasures);
  }

  /**
   * Write raw row as an intermediate sort temp row to memory.
   * This method is used in the beginning of the off-heap sort phase. Comparing with converting
   * raw row to intermediate sort temp row and then writing the converted one,
   * Writing raw row directly will save the intermediate trivial loss.
   * This method use an array backend buffer to save memory allocation. The buffer will be reused
   * for all rows (per thread).
   *
   * @param row raw row
   * @param baseObject base object of the memory block
   * @param address base address for the row
   * @param rowBuffer array backend buffer
   * @return number of bytes written to memory
   */
  public int writeRawRowAsIntermediateSortTempRowToUnsafeMemory(Object[] row,
      Object baseObject, long address, ByteBuffer rowBuffer) {
    int size = 0;
    // write dict & sort
    for (int idx = 0; idx < this.dictSortDimCnt; idx++) {
      CarbonUnsafe.getUnsafe()
          .putInt(baseObject, address + size, (int) row[this.dictSortDimIdx[idx]]);
      size += 4;
    }

    // write no-dict & sort
    for (int idx = 0; idx < this.noDictSortDimCnt; idx++) {
      byte[] bytes = (byte[]) row[this.noDictSortDimIdx[idx]];
      CarbonUnsafe.getUnsafe().putShort(baseObject, address + size, (short) bytes.length);
      size += 2;
      CarbonUnsafe.getUnsafe()
          .copyMemory(bytes, CarbonUnsafe.BYTE_ARRAY_OFFSET, baseObject, address + size,
              bytes.length);
      size += bytes.length;
    }

    // convert pack no-sort
    rowBuffer.clear();
    packNoSortFieldsToBytes(row, rowBuffer);
    rowBuffer.flip();
    int packSize = rowBuffer.limit();

    // write no-sort
    CarbonUnsafe.getUnsafe().putInt(baseObject, address + size, packSize);
    size += 4;
    CarbonUnsafe.getUnsafe()
        .copyMemory(rowBuffer.array(), CarbonUnsafe.BYTE_ARRAY_OFFSET, baseObject, address + size,
            packSize);
    size += packSize;
    return size;
  }

  /**
   * Pack to no-sort fields to byte array
   *
   * @param row raw row
   * @param rowBuffer byte array backend buffer
   */
  private void packNoSortFieldsToBytes(Object[] row, ByteBuffer rowBuffer) {
    // convert dict & no-sort
    for (int idx = 0; idx < this.dictNoSortDimCnt; idx++) {
      rowBuffer.putInt((int) row[this.dictNoSortDimIdx[idx]]);
    }
    // convert no-dict & no-sort
    for (int idx = 0; idx < this.noDictNoSortDimCnt; idx++) {
      byte[] bytes = (byte[]) row[this.noDictNoSortDimIdx[idx]];
      rowBuffer.putShort((short) bytes.length);
      rowBuffer.put(bytes);
    }

    // convert measure
    Object tmpValue;
    DataType tmpDataType;
    for (int idx = 0; idx < this.measureCnt; idx++) {
      tmpValue = row[this.measureIdx[idx]];
      tmpDataType = this.dataTypes[idx];
      if (null == tmpValue) {
        rowBuffer.put((byte) 0);
        continue;
      }
      rowBuffer.put((byte) 1);
      if (DataTypes.BOOLEAN == tmpDataType) {
        if ((boolean) tmpValue) {
          rowBuffer.put((byte) 1);
        } else {
          rowBuffer.put((byte) 0);
        }
      } else if (DataTypes.SHORT == tmpDataType) {
        rowBuffer.putShort((Short) tmpValue);
      } else if (DataTypes.INT == tmpDataType) {
        rowBuffer.putInt((Integer) tmpValue);
      } else if (DataTypes.LONG == tmpDataType) {
        rowBuffer.putLong((Long) tmpValue);
      } else if (DataTypes.DOUBLE == tmpDataType) {
        rowBuffer.putDouble((Double) tmpValue);
      } else if (DataTypes.isDecimal(tmpDataType)) {
        byte[] decimalBytes = DataTypeUtil.bigDecimalToByte((BigDecimal) tmpValue);
        rowBuffer.putShort((short) decimalBytes.length);
        rowBuffer.put(decimalBytes);
      } else {
        throw new IllegalArgumentException("Unsupported data type: " + tmpDataType);
      }
    }
  }
}
