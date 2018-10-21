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

package org.apache.carbondata.core.datastore.page.encoding.rle;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageCodec;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageDecoder;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoder;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;
import org.apache.carbondata.format.Encoding;

/**
 * RLE encoding implementation for integral column page.
 * This encoding keeps track of repeated-run and non-repeated-run, and make use
 * of the highest bit of the length field to indicate the type of run.
 * The length field is encoded as 16 bits value. (Page size must be less than 65535 rows)
 *
 * For example: input data {5, 5, 1, 2, 3, 3, 3, 3, 3} will be encoded to
 * {0x00, 0x02, 0x05,             (repeated-run, 2 values of 5)
 *  0x80, 0x03, 0x01, 0x02, 0x03, (non-repeated-run, 3 values: 1, 2, 3)
 *  0x00, 0x04, 0x03}             (repeated-run, 4 values of 3)
 */
public class RLECodec implements ColumnPageCodec {

  enum RUN_STATE { INIT, START, REPEATED_RUN, NONREPEATED_RUN }

  @Override
  public String getName() {
    return "RLECodec";
  }

  @Override
  public ColumnPageEncoder createEncoder(Map<String, String> parameter) {
    return new RLEEncoder();
  }

  @Override
  public ColumnPageDecoder createDecoder(ColumnPageEncoderMeta meta) {
    assert meta instanceof RLEEncoderMeta;
    RLEEncoderMeta codecMeta = (RLEEncoderMeta) meta;
    return new RLEDecoder(meta.getColumnSpec(), codecMeta.getPageSize(), meta.getCompressorName());
  }

  // This codec supports integral type only
  private void validateDataType(DataType dataType) {
    if (! (dataType == DataTypes.BOOLEAN || dataType == DataTypes.BYTE ||
        dataType == DataTypes.SHORT || dataType == DataTypes.INT ||
        dataType == DataTypes.LONG)) {
      throw new UnsupportedOperationException(dataType + " is not supported for RLE");
    }
  }

  private class RLEEncoder extends ColumnPageEncoder {
    // While encoding RLE, this class internally work as a state machine
    // INIT state is the initial state before any value comes
    // START state is the start for each run
    // REPEATED_RUN state means it is collecting repeated values (`lastValue`)
    // NONREPEATED_RUN state means it is collecting non-repeated values (`nonRepeatValues`)
    private RUN_STATE runState;

    // count for each run, either REPEATED_RUN or NONREPEATED_RUN
    private short valueCount;

    // collected value for REPEATED_RUN
    private Object lastValue;

    // collected value for NONREPEATED_RUN
    private List<Object> nonRepeatValues;

    // data type of input page
    private DataType dataType;

    // output stream for encoded data
    private ByteArrayOutputStream bao;
    private DataOutputStream stream;

    private RLEEncoder() {
      this.runState = RUN_STATE.INIT;
      this.valueCount = 0;
      this.nonRepeatValues = new ArrayList<>();
      this.bao = new ByteArrayOutputStream();
      this.stream = new DataOutputStream(bao);
    }

    @Override
    protected byte[] encodeData(ColumnPage input) throws MemoryException, IOException {
      validateDataType(input.getDataType());
      this.dataType = input.getDataType();
      if (dataType == DataTypes.BYTE) {
        byte[] bytePage = input.getBytePage();
        for (int i = 0; i < bytePage.length; i++) {
          putValue(bytePage[i]);
        }
      } else if (dataType == DataTypes.SHORT) {
        short[] shortPage = input.getShortPage();
        for (int i = 0; i < shortPage.length; i++) {
          putValue(shortPage[i]);
        }
      } else if (dataType == DataTypes.INT) {
        int[] intPage = input.getIntPage();
        for (int i = 0; i < intPage.length; i++) {
          putValue(intPage[i]);
        }
      } else if (dataType == DataTypes.LONG) {
        long[] longPage = input.getLongPage();
        for (int i = 0; i < longPage.length; i++) {
          putValue(longPage[i]);
        }
      } else {
        throw new UnsupportedOperationException(input.getDataType() +
            " does not support RLE encoding");
      }
      return collectResult();
    }

    @Override
    protected List<Encoding> getEncodingList() {
      List<Encoding> encodings = new ArrayList<>();
      encodings.add(Encoding.RLE_INTEGRAL);
      return encodings;
    }

    @Override
    protected ColumnPageEncoderMeta getEncoderMeta(ColumnPage inputPage) {
      return new RLEEncoderMeta(inputPage.getColumnSpec(),
          inputPage.getDataType(),
          inputPage.getPageSize(),
          inputPage.getStatistics(),
          inputPage.getColumnCompressorName());
    }

    private void putValue(Object value) throws IOException {
      if (runState == RUN_STATE.INIT) {
        startNewRun(value);
      } else {
        if (lastValue.equals(value)) {
          putRepeatValue(value);
        } else {
          putNonRepeatValue(value);
        }
      }
    }

    // when last row is reached, write out all collected data
    private byte[] collectResult() throws IOException {
      switch (runState) {
        case REPEATED_RUN:
          writeRunLength(valueCount);
          writeRunValue(lastValue);
          break;
        case NONREPEATED_RUN:
          writeRunLength(valueCount | 0x8000);
          for (int i = 0; i < valueCount; i++) {
            writeRunValue(nonRepeatValues.get(i));
          }
          break;
        default:
          assert (runState == RUN_STATE.START);
          writeRunLength(1);
          writeRunValue(lastValue);
      }
      return bao.toByteArray();
    }

    private void writeRunLength(int length) throws IOException {
      stream.writeShort(length);
    }

    private void writeRunValue(Object value) throws IOException {
      if (dataType == DataTypes.BYTE) {
        stream.writeByte((byte) value);
      } else if (dataType == DataTypes.SHORT) {
        stream.writeShort((short) value);
      } else if (dataType == DataTypes.INT) {
        stream.writeInt((int) value);
      } else if (dataType == DataTypes.LONG) {
        stream.writeLong((long) value);
      } else {
        throw new RuntimeException("internal error");
      }
    }

    // for each run, call this to initialize the state and clear the collected data
    private void startNewRun(Object value) {
      runState = RUN_STATE.START;
      valueCount = 1;
      lastValue = value;
      nonRepeatValues.clear();
      nonRepeatValues.add(value);
    }

    // non-repeated run ends, put the collected data to result page
    private void encodeNonRepeatedRun() throws IOException {
      // put the value count (highest bit is 1) and all collected values
      writeRunLength(valueCount | 0x8000);
      for (int i = 0; i < valueCount; i++) {
        writeRunValue(nonRepeatValues.get(i));
      }
    }

    // repeated run ends, put repeated value to result page
    private void encodeRepeatedRun() throws IOException {
      // put the value count (highest bit is 0) and repeated value
      writeRunLength(valueCount);
      writeRunValue(lastValue);
    }

    private void putRepeatValue(Object value) throws IOException {
      switch (runState) {
        case REPEATED_RUN:
          valueCount++;
          break;
        case NONREPEATED_RUN:
          // non-repeated run ends, encode this run
          encodeNonRepeatedRun();
          startNewRun(value);
          break;
        default:
          assert (runState == RUN_STATE.START);
          // enter repeated run
          runState = RUN_STATE.REPEATED_RUN;
          valueCount++;
          break;
      }
    }

    private void putNonRepeatValue(Object value) throws IOException {
      switch (runState) {
        case NONREPEATED_RUN:
          // collect the non-repeated value
          nonRepeatValues.add(value);
          lastValue = value;
          valueCount++;
          break;
        case REPEATED_RUN:
          // repeated-run ends, encode this run
          encodeRepeatedRun();
          startNewRun(value);
          break;
        default:
          assert (runState == RUN_STATE.START);
          // enter non-repeated run
          runState = RUN_STATE.NONREPEATED_RUN;
          nonRepeatValues.add(value);
          lastValue = value;
          valueCount++;
          break;
      }
    }

  }

  // It decodes data in one shot. It is suitable for scan query
  // TODO: add a on-the-fly decoder for filter query with high selectivity
  private class RLEDecoder implements ColumnPageDecoder {

    private TableSpec.ColumnSpec columnSpec;
    private int pageSize;
    private String compressorName;

    private RLEDecoder(TableSpec.ColumnSpec columnSpec, int pageSize, String compressorName) {
      validateDataType(columnSpec.getSchemaDataType());
      this.columnSpec = columnSpec;
      this.pageSize = pageSize;
      this.compressorName = compressorName;
    }

    @Override
    public ColumnPage decode(byte[] input, int offset, int length)
        throws MemoryException, IOException {
      DataType dataType = columnSpec.getSchemaDataType();
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(input, offset, length));
      ColumnPage resultPage = ColumnPage.newPage(
          new ColumnPageEncoderMeta(columnSpec, dataType, compressorName), pageSize);
      if (dataType == DataTypes.BOOLEAN || dataType == DataTypes.BYTE) {
        decodeBytePage(in, resultPage);
      } else if (dataType == DataTypes.SHORT) {
        decodeShortPage(in, resultPage);
      } else if (dataType == DataTypes.INT) {
        decodeIntPage(in, resultPage);
      } else if (dataType == DataTypes.LONG) {
        decodeLongPage(in, resultPage);
      } else {
        throw new RuntimeException("unsupported datatype:" + dataType);
      }
      return resultPage;
    }

    @Override
    public void decodeAndFillVector(byte[] input, int offset, int length,
        ColumnVectorInfo vectorInfo, BitSet nullBits, boolean isLVEncoded)
        throws MemoryException, IOException {
      throw new UnsupportedOperationException("Not supposed to be called here");
    }

    @Override public ColumnPage decode(byte[] input, int offset, int length, boolean isLVEncoded)
        throws MemoryException, IOException {
      return decode(input, offset, length);
    }

    private void decodeBytePage(DataInputStream in, ColumnPage decodedPage)
        throws IOException {
      int rowId = 0;
      do {
        int runLength = in.readShort();
        int count = runLength & 0x7FFF;
        if (runLength < 0) {
          // non-repeated run
          for (int i = 0; i < count; i++) {
            decodedPage.putByte(rowId++, in.readByte());
          }
        } else {
          // repeated run
          byte value = in.readByte();
          for (int i = 0; i < count; i++) {
            decodedPage.putByte(rowId++, value);
          }
        }
      } while (in.available() > 0);
    }

    private void decodeShortPage(DataInputStream in, ColumnPage decodedPage)
        throws IOException {
      int rowId = 0;
      do {
        int runLength = in.readShort();
        int count = runLength & 0x7FFF;
        if (runLength < 0) {
          // non-repeated run
          for (int i = 0; i < count; i++) {
            decodedPage.putShort(rowId++, in.readShort());
          }
        } else {
          // repeated run
          short value = in.readShort();
          for (int i = 0; i < count; i++) {
            decodedPage.putShort(rowId++, value);
          }
        }
      } while (in.available() > 0);
    }

    private void decodeIntPage(DataInputStream in, ColumnPage decodedPage)
        throws IOException {
      int rowId = 0;
      do {
        int runLength = in.readShort();
        int count = runLength & 0x7FFF;
        if (runLength < 0) {
          // non-repeated run
          for (int i = 0; i < count; i++) {
            decodedPage.putInt(rowId++, in.readInt());
          }
        } else {
          // repeated run
          int value = in.readInt();
          for (int i = 0; i < count; i++) {
            decodedPage.putInt(rowId++, value);
          }
        }
      } while (in.available() > 0);
    }

    private void decodeLongPage(DataInputStream in, ColumnPage decodedPage)
        throws IOException {
      int rowId = 0;
      do {
        int runLength = in.readShort();
        int count = runLength & 0x7FFF;
        if (runLength < 0) {
          // non-repeated run
          for (int i = 0; i < count; i++) {
            decodedPage.putLong(rowId++, in.readLong());
          }
        } else {
          // repeated run
          long value = in.readLong();
          for (int i = 0; i < count; i++) {
            decodedPage.putLong(rowId++, value);
          }
        }
      } while (in.available() > 0);
    }
  }

}
