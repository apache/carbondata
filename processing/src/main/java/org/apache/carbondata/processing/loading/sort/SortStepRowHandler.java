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
import java.nio.charset.Charset;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.memory.CarbonUnsafe;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.CarbonUnsafeUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.core.util.NonDictionaryUtil;
import org.apache.carbondata.core.util.ReUsableByteArrayDataOutputStream;
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
  private int varcharDimCnt = 0;
  private int complexDimCnt = 0;
  private int measureCnt;

  // indices for dict & sort dimension columns
  private int[] dictSortDimIdx;
  // indices for dict & no-sort dimension columns
  private int[] dictNoSortDimIdx;
  // indices for no-dict & sort dimension columns
  private int[] noDictSortDimIdx;
  // indices for no-dict & no-sort dimension columns, excluding complex/varchar columns
  private int[] noDictNoSortDimIdx;
  private int[] varcharDimIdx;
  private int[] complexDimIdx;
  // indices for measure columns
  private int[] measureIdx;

  private DataType[] dataTypes;

  private DataType[] noDictSortDataTypes;

  private boolean[] noDictSortColMapping;

  private DataType[] noDictNoSortDataTypes;

  private boolean[] noDictNoSortColMapping;

  /**
   * constructor
   * @param tableFieldStat table field stat
   */
  public SortStepRowHandler(TableFieldStat tableFieldStat) {
    this.dictSortDimCnt = tableFieldStat.getDictSortDimCnt();
    this.dictNoSortDimCnt = tableFieldStat.getDictNoSortDimCnt();
    this.noDictSortDimCnt = tableFieldStat.getNoDictSortDimCnt();
    this.noDictNoSortDimCnt = tableFieldStat.getNoDictNoSortDimCnt();
    this.varcharDimCnt = tableFieldStat.getVarcharDimCnt();
    this.complexDimCnt = tableFieldStat.getComplexDimCnt();
    this.measureCnt = tableFieldStat.getMeasureCnt();
    this.dictSortDimIdx = tableFieldStat.getDictSortDimIdx();
    this.dictNoSortDimIdx = tableFieldStat.getDictNoSortDimIdx();
    this.noDictSortDimIdx = tableFieldStat.getNoDictSortDimIdx();
    this.noDictNoSortDimIdx = tableFieldStat.getNoDictNoSortDimIdx();
    this.varcharDimIdx = tableFieldStat.getVarcharDimIdx();
    this.complexDimIdx = tableFieldStat.getComplexDimIdx();
    this.measureIdx = tableFieldStat.getMeasureIdx();
    this.dataTypes = tableFieldStat.getMeasureDataType();
    this.noDictSortDataTypes = tableFieldStat.getNoDictSortDataType();
    noDictSortColMapping = new boolean[noDictSortDataTypes.length];
    for (int i = 0; i < noDictSortDataTypes.length; i++) {
      noDictSortColMapping[i] = DataTypeUtil.isPrimitiveColumn(noDictSortDataTypes[i]);
    }
    this.noDictNoSortDataTypes = tableFieldStat.getNoDictNoSortDataType();
    noDictNoSortColMapping = new boolean[noDictNoSortDataTypes.length];
    for (int i = 0; i < noDictNoSortDataTypes.length; i++) {
      noDictNoSortColMapping[i] = DataTypeUtil.isPrimitiveColumn(noDictNoSortDataTypes[i]);
    }
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
      Object[] nonDictArray = new Object[this.noDictSortDimCnt + this.noDictNoSortDimCnt
                                       + this.varcharDimCnt + this.complexDimCnt];
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
        nonDictArray[idxAcc++] = row[this.noDictSortDimIdx[idx]];
      }
      // convert no-dict & no-sort
      for (int idx = 0; idx < this.noDictNoSortDimCnt; idx++) {
        nonDictArray[idxAcc++] = row[this.noDictNoSortDimIdx[idx]];
      }
      // convert varchar dims
      for (int idx = 0; idx < this.varcharDimCnt; idx++) {
        nonDictArray[idxAcc++] = row[this.varcharDimIdx[idx]];
      }
      // convert complex dims
      for (int idx = 0; idx < this.complexDimCnt; idx++) {
        nonDictArray[idxAcc++] = row[this.complexDimIdx[idx]];
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
    Object[] out = new Object[3];
    NonDictionaryUtil
        .prepareOutObj(out, sortTempRow.getDictSortDims(), sortTempRow.getNoDictSortDims(),
            sortTempRow.getMeasures());
    return out;
  }

  /**
   * Read intermediate sort temp row from InputStream.
   * This method is used during the intermediate merge sort phase to read row from sort temp file.
   *
   * @param inputStream input stream
   * @return a row that contains three parts
   * @throws IOException if error occrus while reading from stream
   */
  public IntermediateSortTempRow readWithoutNoSortFieldConvert(
      DataInputStream inputStream) throws IOException {
    int[] dictSortDims = new int[this.dictSortDimCnt];
    Object[] noDictSortDims = new Object[this.noDictSortDimCnt];

    // read dict & sort dim data
    for (int idx = 0; idx < this.dictSortDimCnt; idx++) {
      dictSortDims[idx] = inputStream.readInt();
    }

    // read no-dict & sort data
    for (int idx = 0; idx < this.noDictSortDimCnt; idx++) {
      // for no dict measure column get the original data
      noDictSortDims[idx] = getDataForNoDictSortColumn(inputStream, idx);
    }

    // read no-dict dims & measures
    int len = inputStream.readInt();
    byte[] noSortDimsAndMeasures = new byte[len];
    inputStream.readFully(noSortDimsAndMeasures);
    // keeping no sort fields and measure in pack byte array as it will not participate in sort
    return new IntermediateSortTempRow(dictSortDims, noDictSortDims, noSortDimsAndMeasures);
  }

  /**
   * Read intermediate sort temp row from InputStream.
   * This method is used during the final merge sort phase to read row from sort temp file and
   * merged sort temp file.
   *
   * @param inputStream input stream
   * @return a row that contains three parts
   * @throws IOException if error occrus while reading from stream
   */
  public IntermediateSortTempRow readWithNoSortFieldConvert(
      DataInputStream inputStream) throws IOException {
    int[] dictSortDims = new int[this.dictSortDimCnt + this.dictNoSortDimCnt];
    Object[] noDictSortDims =
        new Object[this.noDictSortDimCnt + this.noDictNoSortDimCnt + this.varcharDimCnt
            + this.complexDimCnt];

    // read dict & sort dim data
    for (int idx = 0; idx < this.dictSortDimCnt; idx++) {
      dictSortDims[idx] = inputStream.readInt();
    }

    // read no-dict & sort data
    for (int idx = 0; idx < this.noDictSortDimCnt; idx++) {
      // for no dict measure column get the original data
      noDictSortDims[idx] = getDataForNoDictSortColumn(inputStream, idx);
    }

    // read no-dict dims & measures
    int len = inputStream.readInt();
    byte[] noSortDimsAndMeasures = new byte[len];
    inputStream.readFully(noSortDimsAndMeasures);
    Object[] measure = new Object[this.measureCnt];
    // unpack the no sort fields and measure fields
    unpackNoSortFromBytes(noSortDimsAndMeasures, dictSortDims, noDictSortDims, measure);
    return new IntermediateSortTempRow(dictSortDims, noDictSortDims,measure);
  }

  /**
   * Return the data from the stream according to the column type
   *
   * @param inputStream
   * @param idx
   * @throws IOException
   */
  private Object getDataForNoDictSortColumn(DataInputStream inputStream, int idx)
      throws IOException {
    if (this.noDictSortColMapping[idx]) {
      return readDataFromStream(inputStream, idx);
    } else {
      short len = inputStream.readShort();
      byte[] bytes = new byte[len];
      inputStream.readFully(bytes);
      return bytes;
    }
  }

  /**
   * Read the data from the stream
   *
   * @param inputStream
   * @param idx
   * @return
   * @throws IOException
   */
  private Object readDataFromStream(DataInputStream inputStream, int idx) throws IOException {
    DataType dataType = noDictSortDataTypes[idx];
    Object data = null;
    if (!inputStream.readBoolean()) {
      return null;
    }
    if (dataType == DataTypes.BOOLEAN) {
      data = inputStream.readBoolean();
    } else if (dataType == DataTypes.BYTE) {
      data = inputStream.readByte();
    } else if (dataType == DataTypes.SHORT) {
      data = inputStream.readShort();
    } else if (dataType == DataTypes.INT) {
      data = inputStream.readInt();
    } else if (dataType == DataTypes.LONG || dataType == DataTypes.TIMESTAMP) {
      data = inputStream.readLong();
    } else if (dataType == DataTypes.DOUBLE) {
      data = inputStream.readDouble();
    } else if (dataType == DataTypes.FLOAT) {
      data = inputStream.readFloat();
    } else if (dataType == DataTypes.BYTE_ARRAY || DataTypes.isDecimal(dataType)) {
      byte[] bytes =
          inputStream.readUTF().getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
      data = bytes;
    }
    return data;
  }

  private void unpackNoSortFromBytes(byte[] noSortDimsAndMeasures, int[] dictDims,
      Object[] noDictDims, Object[] measures) {
    ByteBuffer rowBuffer = ByteBuffer.wrap(noSortDimsAndMeasures);
    // read dict_no_sort
    for (int i = dictSortDimCnt; i < dictDims.length; i++) {
      dictDims[i] = rowBuffer.getInt();
    }

    int noDictIndex = noDictSortDimCnt;
    // read no_dict_no_sort
    for (int i = 0; i < noDictNoSortDimCnt; i++) {
      // for no dict measure column get the original data
      if (this.noDictNoSortColMapping[i]) {
        noDictDims[noDictIndex++] = getDataFromRowBuffer(noDictNoSortDataTypes[i], rowBuffer);
      } else {
        short len = rowBuffer.getShort();
        byte[] bytes = new byte[len];
        rowBuffer.get(bytes);
        noDictDims[noDictIndex++] = bytes;
      }
    }

    // read varchar dims
    for (int i = 0; i < varcharDimCnt; i++) {
      int len = rowBuffer.getInt();
      byte[] bytes = new byte[len];
      rowBuffer.get(bytes);
      noDictDims[noDictIndex++] = bytes;
    }

    // read complex dims
    for (int i = 0; i < complexDimCnt; i++) {
      int len = rowBuffer.getInt();
      byte[] bytes = new byte[len];
      rowBuffer.get(bytes);
      noDictDims[noDictIndex++] = bytes;
    }

    // read measure
    int measureCnt = measures.length;
    Object tmpContent;
    for (short idx = 0 ; idx < measureCnt; idx++) {
      tmpContent = getDataFromRowBuffer(dataTypes[idx], rowBuffer);
      measures[idx] = tmpContent;
    }
  }

  /**
   * Retrieve/Get the data from the row buffer.
   *
   * @param tmpDataType
   * @param rowBuffer
   * @return
   */
  private Object getDataFromRowBuffer(DataType tmpDataType, ByteBuffer rowBuffer) {
    Object tmpContent;
    if ((byte) 0 == rowBuffer.get()) {
      return null;
    }

    if (DataTypes.BOOLEAN == tmpDataType) {
      if ((byte) 1 == rowBuffer.get()) {
        tmpContent = true;
      } else {
        tmpContent = false;
      }
    } else if (DataTypes.SHORT == tmpDataType) {
      tmpContent = rowBuffer.getShort();
    } else if (DataTypes.INT == tmpDataType) {
      tmpContent = rowBuffer.getInt();
    } else if (DataTypes.LONG == tmpDataType || DataTypes.TIMESTAMP == tmpDataType) {
      tmpContent = rowBuffer.getLong();
    } else if (DataTypes.DOUBLE == tmpDataType) {
      tmpContent = rowBuffer.getDouble();
    } else if (DataTypes.FLOAT == tmpDataType) {
      tmpContent = rowBuffer.getFloat();
    } else if (DataTypes.BYTE == tmpDataType) {
      tmpContent = rowBuffer.get();
    } else if (DataTypes.isDecimal(tmpDataType)) {
      short len = rowBuffer.getShort();
      byte[] decimalBytes = new byte[len];
      rowBuffer.get(decimalBytes);
      tmpContent = DataTypeUtil.byteToBigDecimal(decimalBytes);
    } else {
      throw new IllegalArgumentException("Unsupported data type: " + tmpDataType);
    }
    return tmpContent;
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
      if (this.noDictSortColMapping[idx]) {
        // write the original data to the stream
        writeDataToStream(sortTempRow.getNoDictSortDims()[idx], outputStream, idx);
      } else {
        byte[] bytes = (byte[]) sortTempRow.getNoDictSortDims()[idx];
        outputStream.writeShort(bytes.length);
        outputStream.write(bytes);
      }
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
   * @param reUsableByteArrayDataOutputStream DataOutputStream backend by ByteArrayOutputStream
   * @throws IOException if error occurs while writing to stream
   */
  public void writeRawRowAsIntermediateSortTempRowToOutputStream(Object[] row,
      DataOutputStream outputStream,
      ReUsableByteArrayDataOutputStream reUsableByteArrayDataOutputStream) throws IOException {
    // write dict & sort
    for (int idx = 0; idx < this.dictSortDimCnt; idx++) {
      outputStream.writeInt((int) row[this.dictSortDimIdx[idx]]);
    }

    // write no-dict & sort
    for (int idx = 0; idx < this.noDictSortDimCnt; idx++) {
      if (this.noDictSortColMapping[idx]) {
        // write the original data to the stream
        writeDataToStream(row[this.noDictSortDimIdx[idx]], outputStream, idx);
      } else {
        byte[] bytes = (byte[]) row[this.noDictSortDimIdx[idx]];
        outputStream.writeShort(bytes.length);
        outputStream.write(bytes);
      }
    }

    // pack no-sort
    reUsableByteArrayDataOutputStream.reset();
    packNoSortFieldsToBytes(row, reUsableByteArrayDataOutputStream);
    int packSize = reUsableByteArrayDataOutputStream.getSize();

    // write no-sort
    outputStream.writeInt(packSize);
    outputStream.write(reUsableByteArrayDataOutputStream.getByteArray(), 0, packSize);
  }

  /**
   * Write the data to stream
   *
   * @param data
   * @param outputStream
   * @param idx
   * @throws IOException
   */
  private void writeDataToStream(Object data, DataOutputStream outputStream, int idx)
      throws IOException {
    DataType dataType = noDictSortDataTypes[idx];
    if (null == data) {
      outputStream.writeBoolean(false);
    } else {
      outputStream.writeBoolean(true);
      if (dataType == DataTypes.BOOLEAN) {
        outputStream.writeBoolean((boolean) data);
      } else if (dataType == DataTypes.BYTE) {
        outputStream.writeByte((byte) data);
      } else if (dataType == DataTypes.SHORT) {
        outputStream.writeShort((short) data);
      } else if (dataType == DataTypes.INT) {
        outputStream.writeInt((int) data);
      } else if (dataType == DataTypes.LONG || dataType == DataTypes.TIMESTAMP) {
        outputStream.writeLong((long) data);
      } else if (dataType == DataTypes.DOUBLE) {
        outputStream.writeDouble((double) data);
      } else if (DataTypes.isDecimal(dataType)) {
        BigDecimal val = (BigDecimal) data;
        byte[] bigDecimalInBytes = DataTypeUtil.bigDecimalToByte(val);
        outputStream.writeShort(bigDecimalInBytes.length);
        outputStream.write(bigDecimalInBytes);
      } else if (dataType == DataTypes.FLOAT) {
        outputStream.writeFloat((float) data);
      } else if (dataType == DataTypes.BYTE_ARRAY) {
        outputStream.writeUTF(data.toString());
      }
    }
  }

  /**
   * Read intermediate sort temp row from unsafe memory.
   * This method is used during merge sort phase for off-heap sort.
   *
   * @param baseObject base object of memory block
   * @param address address of the row
   * @return intermediate sort temp row
   */
  public IntermediateSortTempRow readFromMemoryWithoutNoSortFieldConvert(Object baseObject,
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
   * Read intermediate sort temp row from unsafe memory.
   * This method is used during merge sort phase for off-heap sort.
   *
   * @param baseObject base object of memory block
   * @param address address of the row
   * @return intermediate sort temp row
   */
  public IntermediateSortTempRow readRowFromMemoryWithNoSortFieldConvert(Object baseObject,
      long address) {
    int size = 0;

    int[] dictSortDims = new int[this.dictSortDimCnt + this.dictNoSortDimCnt];
    Object[] noDictSortDims =
        new Object[this.noDictSortDimCnt + this.noDictNoSortDimCnt + this.varcharDimCnt
            + this.complexDimCnt];

    // read dict & sort dim
    for (int idx = 0; idx < dictSortDimCnt; idx++) {
      dictSortDims[idx] = CarbonUnsafe.getUnsafe().getInt(baseObject, address + size);
      size += 4;
    }

    // read no-dict & sort dim
    for (int idx = 0; idx < this.noDictSortDimCnt; idx++) {
      short length = CarbonUnsafe.getUnsafe().getShort(baseObject, address + size);
      size += 2;
      if (this.noDictSortColMapping[idx]) {
        // get the original data from the unsafe memory
        if (0 == length) {
          // if the length is 0, the the data is null
          noDictSortDims[idx] = null;
        } else {
          Object data = CarbonUnsafeUtil
              .getDataFromUnsafe(noDictSortDataTypes[idx], baseObject, address, size, length);
          size += length;
          noDictSortDims[idx] = data;
        }
      } else {
        byte[] bytes = new byte[length];
        CarbonUnsafe.getUnsafe()
            .copyMemory(baseObject, address + size, bytes, CarbonUnsafe.BYTE_ARRAY_OFFSET, length);
        size += length;
        noDictSortDims[idx] = bytes;
      }
    }

    // read no-sort dims & measures
    int len = CarbonUnsafe.getUnsafe().getInt(baseObject, address + size);
    size += 4;
    byte[] noSortDimsAndMeasures = new byte[len];
    CarbonUnsafe.getUnsafe().copyMemory(baseObject, address + size,
        noSortDimsAndMeasures, CarbonUnsafe.BYTE_ARRAY_OFFSET, len);
    Object[] measures = new Object[measureCnt];
    unpackNoSortFromBytes(noSortDimsAndMeasures, dictSortDims, noDictSortDims, measures);
    return new IntermediateSortTempRow(dictSortDims, noDictSortDims, measures);
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
   * @param unsafeTotalLength
   * @throws IOException if error occurs while writing to stream
   */
  public void writeIntermediateSortTempRowFromUnsafeMemoryToStream(Object baseObject, long address,
      DataOutputStream outputStream, long unsafeRemainingLength, long unsafeTotalLength)
      throws IOException, MemoryException {
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
      if (this.noDictSortColMapping[idx]) {
        // get the original data from unsafe memory
        if (0 == length) {
          // if the length is 0, then the data is null
          writeDataToStream(null, outputStream, idx);
        } else {
          Object data = CarbonUnsafeUtil
              .getDataFromUnsafe(noDictSortDataTypes[idx], baseObject, address, size, length);
          size += length;
          writeDataToStream(data, outputStream, idx);
        }
      } else {
        validateUnsafeMemoryBlockSizeLimit(unsafeRemainingLength, length, unsafeTotalLength);
        byte[] bytes = new byte[length];
        CarbonUnsafe.getUnsafe()
            .copyMemory(baseObject, address + size, bytes, CarbonUnsafe.BYTE_ARRAY_OFFSET, length);
        size += length;
        outputStream.writeShort(length);
        outputStream.write(bytes);
      }
    }

    // packed no-sort & measure
    int len = CarbonUnsafe.getUnsafe().getInt(baseObject, address + size);
    size += 4;
    validateUnsafeMemoryBlockSizeLimit(unsafeRemainingLength, len, unsafeTotalLength);
    byte[] noSortDimsAndMeasures = new byte[len];
    CarbonUnsafe.getUnsafe().copyMemory(baseObject, address + size,
        noSortDimsAndMeasures, CarbonUnsafe.BYTE_ARRAY_OFFSET, len);

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
   * @param unsafeTotalLength
   * @return number of bytes written to memory
   */
  public int writeRawRowAsIntermediateSortTempRowToUnsafeMemory(Object[] row, Object baseObject,
      long address, ReUsableByteArrayDataOutputStream reUsableByteArrayDataOutputStream,
      long unsafeRemainingLength, long unsafeTotalLength) throws MemoryException, IOException {
    int size = 0;
    // write dict & sort
    for (int idx = 0; idx < this.dictSortDimCnt; idx++) {
      validateUnsafeMemoryBlockSizeLimit(unsafeRemainingLength, 4, unsafeTotalLength);
      CarbonUnsafe.getUnsafe()
          .putInt(baseObject, address + size, (int) row[this.dictSortDimIdx[idx]]);
      size += 4;
    }

    // write no-dict & sort
    for (int idx = 0; idx < this.noDictSortDimCnt; idx++) {
      if (this.noDictSortColMapping[idx]) {
        Object data = row[this.noDictSortDimIdx[idx]];
        if (null == data) {
          // if the data is null, then write only the length as 0.
          CarbonUnsafe.getUnsafe().putShort(baseObject, address + size, (short) 0);
          size += 2;
        } else {
          int sizeInBytes = this.noDictSortDataTypes[idx].getSizeInBytes();
          if (this.noDictSortDataTypes[idx] == DataTypes.TIMESTAMP) {
            sizeInBytes = DataTypes.LONG.getSizeInBytes();
          }
          CarbonUnsafe.getUnsafe().putShort(baseObject, address + size, (short) sizeInBytes);
          size += 2;
          // put data to unsafe according to the data types
          CarbonUnsafeUtil
              .putDataToUnsafe(noDictSortDataTypes[idx], data, baseObject, address, size,
                  sizeInBytes);
          size += sizeInBytes;
        }
      } else {
        byte[] bytes = (byte[]) row[this.noDictSortDimIdx[idx]];
        validateUnsafeMemoryBlockSizeLimit(unsafeRemainingLength, 2 + bytes.length,
            unsafeTotalLength);
        CarbonUnsafe.getUnsafe().putShort(baseObject, address + size, (short) bytes.length);
        size += 2;
        CarbonUnsafe.getUnsafe()
            .copyMemory(bytes, CarbonUnsafe.BYTE_ARRAY_OFFSET, baseObject, address + size,
                bytes.length);
        size += bytes.length;
      }
    }

    // convert pack no-sort
    reUsableByteArrayDataOutputStream.reset();
    packNoSortFieldsToBytes(row, reUsableByteArrayDataOutputStream);
    int packSize = reUsableByteArrayDataOutputStream.getSize();

    validateUnsafeMemoryBlockSizeLimit(unsafeRemainingLength, 4 + packSize, unsafeTotalLength);
    // write no-sort
    CarbonUnsafe.getUnsafe().putInt(baseObject, address + size, packSize);
    size += 4;
    CarbonUnsafe.getUnsafe().copyMemory(reUsableByteArrayDataOutputStream.getByteArray(),
        CarbonUnsafe.BYTE_ARRAY_OFFSET, baseObject, address + size, packSize);
    size += packSize;
    return size;
  }

  private void validateUnsafeMemoryBlockSizeLimit(long unsafeRemainingLength, int requestedSize,
      long unsafeTotalLength) throws MemoryException {
    if (unsafeTotalLength <= requestedSize) {
      throw new MemoryException(
          "not enough unsafe memory for sort: increase the 'offheap.sort.chunk.size.inmb' ");
    } else if (unsafeRemainingLength <= requestedSize) {
      throw new MemoryException("cannot handle this row. create new page");
    }
  }

  /**
   * Pack to no-sort fields to byte array
   *
   * @param row raw row
   * @param @param reUsableByteArrayDataOutputStream
   *        DataOutputStream backend by ByteArrayOutputStream
   */
  private void packNoSortFieldsToBytes(Object[] row,
      ReUsableByteArrayDataOutputStream reUsableByteArrayDataOutputStream) throws IOException {
    // convert dict & no-sort
    for (int idx = 0; idx < this.dictNoSortDimCnt; idx++) {
      reUsableByteArrayDataOutputStream.writeInt((int) row[this.dictNoSortDimIdx[idx]]);
    }
    // convert no-dict & no-sort
    for (int idx = 0; idx < this.noDictNoSortDimCnt; idx++) {
      if (this.noDictNoSortColMapping[idx]) {
        // put the original data to buffer
        putDataToRowBuffer(this.noDictNoSortDataTypes[idx], row[this.noDictNoSortDimIdx[idx]],
            reUsableByteArrayDataOutputStream);
      } else {
        byte[] bytes = (byte[]) row[this.noDictNoSortDimIdx[idx]];
        reUsableByteArrayDataOutputStream.writeShort((short) bytes.length);
        reUsableByteArrayDataOutputStream.write(bytes);
      }
    }
    // convert varchar dims
    for (int idx = 0; idx < this.varcharDimCnt; idx++) {
      byte[] bytes = (byte[]) row[this.varcharDimIdx[idx]];
      reUsableByteArrayDataOutputStream.writeInt(bytes.length);
      reUsableByteArrayDataOutputStream.write(bytes);
    }
    // convert complex dims
    for (int idx = 0; idx < this.complexDimCnt; idx++) {
      byte[] bytes = (byte[]) row[this.complexDimIdx[idx]];
      reUsableByteArrayDataOutputStream.writeInt(bytes.length);
      reUsableByteArrayDataOutputStream.write(bytes);
    }

    // convert measure
    for (int idx = 0; idx < this.measureCnt; idx++) {
      putDataToRowBuffer(this.dataTypes[idx], row[this.measureIdx[idx]],
          reUsableByteArrayDataOutputStream);
    }
  }

  /**
   * Put the data to the row buffer
   *  @param tmpDataType
   * @param tmpValue
   * @param reUsableByteArrayDataOutputStream
   */
  private void putDataToRowBuffer(DataType tmpDataType, Object tmpValue,
      ReUsableByteArrayDataOutputStream reUsableByteArrayDataOutputStream) throws IOException {
    if (null == tmpValue) {
      reUsableByteArrayDataOutputStream.write((byte) 0);
      return;
    }
    reUsableByteArrayDataOutputStream.write((byte) 1);
    if (DataTypes.BOOLEAN == tmpDataType) {
      if ((boolean) tmpValue) {
        reUsableByteArrayDataOutputStream.write((byte) 1);
      } else {
        reUsableByteArrayDataOutputStream.write((byte) 0);
      }
    } else if (DataTypes.SHORT == tmpDataType) {
      reUsableByteArrayDataOutputStream.writeShort((Short) tmpValue);
    } else if (DataTypes.INT == tmpDataType) {
      reUsableByteArrayDataOutputStream.writeInt((Integer) tmpValue);
    } else if (DataTypes.LONG == tmpDataType || DataTypes.TIMESTAMP == tmpDataType) {
      reUsableByteArrayDataOutputStream.writeLong((Long) tmpValue);
    } else if (DataTypes.DOUBLE == tmpDataType) {
      reUsableByteArrayDataOutputStream.writeDouble((Double) tmpValue);
    }  else if (DataTypes.FLOAT == tmpDataType) {
      reUsableByteArrayDataOutputStream.writeFloat((Float) tmpValue);
    } else if (DataTypes.BYTE == tmpDataType) {
      reUsableByteArrayDataOutputStream.write((byte) tmpValue);
    } else if (DataTypes.isDecimal(tmpDataType)) {
      byte[] decimalBytes = DataTypeUtil.bigDecimalToByte((BigDecimal) tmpValue);
      reUsableByteArrayDataOutputStream.writeShort((short) decimalBytes.length);
      reUsableByteArrayDataOutputStream.write(decimalBytes);
    } else {
      throw new IllegalArgumentException("Unsupported data type: " + tmpDataType);
    }
  }
}
