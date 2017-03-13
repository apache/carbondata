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
package org.apache.carbondata.core.scan.result.vector;

import java.math.BigDecimal;
import java.util.BitSet;

import org.apache.carbondata.core.datastore.chunk.MeasureColumnDataChunk;
import org.apache.carbondata.core.metadata.datatype.DataType;

import org.apache.spark.sql.types.Decimal;

public class MeasureDataVectorProcessor {

  public interface MeasureVectorFiller {

    void fillMeasureVector(MeasureColumnDataChunk dataChunk, ColumnVectorInfo info);

    void fillMeasureVectorForFilter(int[] rowMapping, MeasureColumnDataChunk dataChunk,
        ColumnVectorInfo info);
  }

  public static class IntegralMeasureVectorFiller implements MeasureVectorFiller {

    @Override
    public void fillMeasureVector(MeasureColumnDataChunk dataChunk, ColumnVectorInfo info) {
      int offset = info.offset;
      int len = offset + info.size;
      int vectorOffset = info.vectorOffset;
      CarbonColumnVector vector = info.vector;
      BitSet nullBitSet = dataChunk.getNullValueIndexHolder().getBitSet();
      for (int i = offset; i < len; i++) {
        if (nullBitSet.get(i)) {
          vector.putNull(vectorOffset);
        } else {
          vector.putInt(vectorOffset,
              (int)dataChunk.getMeasureDataHolder().getReadableLongValueByIndex(i));
        }
        vectorOffset++;
      }
    }

    @Override
    public void fillMeasureVectorForFilter(int[] rowMapping, MeasureColumnDataChunk dataChunk,
        ColumnVectorInfo info) {
      int offset = info.offset;
      int len = offset + info.size;
      int vectorOffset = info.vectorOffset;
      CarbonColumnVector vector = info.vector;
      BitSet nullBitSet = dataChunk.getNullValueIndexHolder().getBitSet();
      for (int i = offset; i < len; i++) {
        int currentRow = rowMapping[i];
        if (nullBitSet.get(currentRow)) {
          vector.putNull(vectorOffset);
        } else {
          vector.putInt(vectorOffset,
              (int)dataChunk.getMeasureDataHolder().getReadableLongValueByIndex(currentRow));
        }
        vectorOffset++;
      }
    }
  }

  public static class ShortMeasureVectorFiller implements MeasureVectorFiller {

    @Override
    public void fillMeasureVector(MeasureColumnDataChunk dataChunk, ColumnVectorInfo info) {
      int offset = info.offset;
      int len = offset + info.size;
      int vectorOffset = info.vectorOffset;
      CarbonColumnVector vector = info.vector;
      BitSet nullBitSet = dataChunk.getNullValueIndexHolder().getBitSet();
      for (int i = offset; i < len; i++) {
        if (nullBitSet.get(i)) {
          vector.putNull(vectorOffset);
        } else {
          vector.putShort(vectorOffset,
              (short) dataChunk.getMeasureDataHolder().getReadableLongValueByIndex(i));
        }
        vectorOffset++;
      }
    }

    @Override
    public void fillMeasureVectorForFilter(int[] rowMapping, MeasureColumnDataChunk dataChunk,
        ColumnVectorInfo info) {
      int offset = info.offset;
      int len = offset + info.size;
      int vectorOffset = info.vectorOffset;
      CarbonColumnVector vector = info.vector;
      BitSet nullBitSet = dataChunk.getNullValueIndexHolder().getBitSet();
      for (int i = offset; i < len; i++) {
        int currentRow = rowMapping[i];
        if (nullBitSet.get(currentRow)) {
          vector.putNull(vectorOffset);
        } else {
          vector.putShort(vectorOffset,
              (short) dataChunk.getMeasureDataHolder().getReadableLongValueByIndex(currentRow));
        }
        vectorOffset++;
      }
    }
  }

  public static class LongMeasureVectorFiller implements MeasureVectorFiller {

    @Override
    public void fillMeasureVector(MeasureColumnDataChunk dataChunk, ColumnVectorInfo info) {
      int offset = info.offset;
      int len = offset + info.size;
      int vectorOffset = info.vectorOffset;
      CarbonColumnVector vector = info.vector;
      BitSet nullBitSet = dataChunk.getNullValueIndexHolder().getBitSet();
      for (int i = offset; i < len; i++) {
        if (nullBitSet.get(i)) {
          vector.putNull(vectorOffset);
        } else {
          vector.putLong(vectorOffset,
              dataChunk.getMeasureDataHolder().getReadableLongValueByIndex(i));
        }
        vectorOffset++;
      }
    }

    @Override
    public void fillMeasureVectorForFilter(int[] rowMapping, MeasureColumnDataChunk dataChunk,
        ColumnVectorInfo info) {
      int offset = info.offset;
      int len = offset + info.size;
      int vectorOffset = info.vectorOffset;
      CarbonColumnVector vector = info.vector;
      BitSet nullBitSet = dataChunk.getNullValueIndexHolder().getBitSet();
      for (int i = offset; i < len; i++) {
        int currentRow = rowMapping[i];
        if (nullBitSet.get(currentRow)) {
          vector.putNull(vectorOffset);
        } else {
          vector.putLong(vectorOffset,
              dataChunk.getMeasureDataHolder().getReadableLongValueByIndex(currentRow));
        }
        vectorOffset++;
      }
    }
  }

  public static class DecimalMeasureVectorFiller implements MeasureVectorFiller {

    @Override
    public void fillMeasureVector(MeasureColumnDataChunk dataChunk, ColumnVectorInfo info) {
      int offset = info.offset;
      int len = offset + info.size;
      int vectorOffset = info.vectorOffset;
      CarbonColumnVector vector = info.vector;
      int precision = info.measure.getMeasure().getPrecision();
      int newMeasureScale = info.measure.getMeasure().getScale();
      BitSet nullBitSet = dataChunk.getNullValueIndexHolder().getBitSet();
      for (int i = offset; i < len; i++) {
        if (nullBitSet.get(i)) {
          vector.putNull(vectorOffset);
        } else {
          BigDecimal decimal =
              dataChunk.getMeasureDataHolder().getReadableBigDecimalValueByIndex(i);
          if (decimal.scale() < newMeasureScale) {
            decimal = decimal.setScale(newMeasureScale);
          }
          Decimal toDecimal = org.apache.spark.sql.types.Decimal.apply(decimal);
          vector.putDecimal(vectorOffset, toDecimal, precision);
        }
        vectorOffset++;
      }
    }

    @Override
    public void fillMeasureVectorForFilter(int[] rowMapping, MeasureColumnDataChunk dataChunk,
        ColumnVectorInfo info) {
      int offset = info.offset;
      int len = offset + info.size;
      int vectorOffset = info.vectorOffset;
      CarbonColumnVector vector = info.vector;
      int precision = info.measure.getMeasure().getPrecision();
      BitSet nullBitSet = dataChunk.getNullValueIndexHolder().getBitSet();
      for (int i = offset; i < len; i++) {
        int currentRow = rowMapping[i];
        if (nullBitSet.get(currentRow)) {
          vector.putNull(vectorOffset);
        } else {
          BigDecimal decimal =
              dataChunk.getMeasureDataHolder().getReadableBigDecimalValueByIndex(currentRow);
          Decimal toDecimal = org.apache.spark.sql.types.Decimal.apply(decimal);
          vector.putDecimal(vectorOffset, toDecimal, precision);
        }
        vectorOffset++;
      }
    }
  }

  public static class DefaultMeasureVectorFiller implements MeasureVectorFiller {

    @Override
    public void fillMeasureVector(MeasureColumnDataChunk dataChunk, ColumnVectorInfo info) {
      int offset = info.offset;
      int len = offset + info.size;
      int vectorOffset = info.vectorOffset;
      CarbonColumnVector vector = info.vector;
      BitSet nullBitSet = dataChunk.getNullValueIndexHolder().getBitSet();
      for (int i = offset; i < len; i++) {
        if (nullBitSet.get(i)) {
          vector.putNull(vectorOffset);
        } else {
          vector.putDouble(vectorOffset,
              dataChunk.getMeasureDataHolder().getReadableDoubleValueByIndex(i));
        }
        vectorOffset++;
      }
    }

    @Override
    public void fillMeasureVectorForFilter(int[] rowMapping, MeasureColumnDataChunk dataChunk,
        ColumnVectorInfo info) {
      int offset = info.offset;
      int len = offset + info.size;
      int vectorOffset = info.vectorOffset;
      CarbonColumnVector vector = info.vector;
      BitSet nullBitSet = dataChunk.getNullValueIndexHolder().getBitSet();
      for (int i = offset; i < len; i++) {
        int currentRow = rowMapping[i];
        if (nullBitSet.get(currentRow)) {
          vector.putNull(vectorOffset);
        } else {
          vector.putDouble(vectorOffset,
              dataChunk.getMeasureDataHolder().getReadableDoubleValueByIndex(currentRow));
        }
        vectorOffset++;
      }
    }
  }

  public static class MeasureVectorFillerFactory {

    public static MeasureVectorFiller getMeasureVectorFiller(DataType dataType) {
      switch (dataType) {
        case SHORT:
          return new ShortMeasureVectorFiller();
        case INT:
          return new IntegralMeasureVectorFiller();
        case LONG:
          return new LongMeasureVectorFiller();
        case DECIMAL:
          return new DecimalMeasureVectorFiller();
        default:
          return new DefaultMeasureVectorFiller();
      }
    }
  }

}
