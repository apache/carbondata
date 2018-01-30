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

import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;

public class MeasureDataVectorProcessor {

  public interface MeasureVectorFiller {

    void fillMeasureVector(ColumnPage dataChunk, ColumnVectorInfo info);

    void fillMeasureVector(int[] filteredRowId, ColumnPage dataChunk,
        ColumnVectorInfo info);
  }

  public static class IntegralMeasureVectorFiller implements MeasureVectorFiller {

    @Override
    public void fillMeasureVector(ColumnPage dataChunk, ColumnVectorInfo info) {
      int offset = info.offset;
      int len = offset + info.size;
      int vectorOffset = info.vectorOffset;
      CarbonColumnVector vector = info.vector;
      BitSet nullBitSet = dataChunk.getNullBits();
      if (nullBitSet.isEmpty()) {
        for (int i = offset; i < len; i++) {
          vector.putInt(vectorOffset, (int)dataChunk.getLong(i));
          vectorOffset++;
        }
      } else {
        for (int i = offset; i < len; i++) {
          if (nullBitSet.get(i)) {
            vector.putNull(vectorOffset);
          } else {
            vector.putInt(vectorOffset, (int)dataChunk.getLong(i));
          }
          vectorOffset++;
        }
      }
    }

    @Override
    public void fillMeasureVector(int[] filteredRowId, ColumnPage dataChunk,
        ColumnVectorInfo info) {
      int offset = info.offset;
      int len = offset + info.size;
      int vectorOffset = info.vectorOffset;
      CarbonColumnVector vector = info.vector;
      BitSet nullBitSet = dataChunk.getNullBits();
      if (nullBitSet.isEmpty()) {
        for (int i = offset; i < len; i++) {
          int currentRow = filteredRowId[i];
          vector.putInt(vectorOffset, (int)dataChunk.getLong(currentRow));
          vectorOffset++;
        }
      } else {
        for (int i = offset; i < len; i++) {
          int currentRow = filteredRowId[i];
          if (nullBitSet.get(currentRow)) {
            vector.putNull(vectorOffset);
          } else {
            vector.putInt(vectorOffset, (int)dataChunk.getLong(currentRow));
          }
          vectorOffset++;
        }
      }
    }
  }

  /**
   * Fill Measure Vector For Boolean data type
   */
  public static class BooleanMeasureVectorFiller implements MeasureVectorFiller {

    @Override
    public void fillMeasureVector(ColumnPage dataChunk, ColumnVectorInfo info) {
      int offset = info.offset;
      int len = offset + info.size;
      int vectorOffset = info.vectorOffset;
      CarbonColumnVector vector = info.vector;
      BitSet nullBitSet = dataChunk.getNullBits();
      if (nullBitSet.isEmpty()) {
        for (int i = offset; i < len; i++) {
          vector.putBoolean(vectorOffset, dataChunk.getBoolean(i));
          vectorOffset++;
        }
      } else {
        for (int i = offset; i < len; i++) {
          if (nullBitSet.get(i)) {
            vector.putNull(vectorOffset);
          } else {
            vector.putBoolean(vectorOffset, dataChunk.getBoolean(i));
          }
          vectorOffset++;
        }
      }
    }

    @Override
    public void fillMeasureVector(int[] filteredRowId,
        ColumnPage dataChunk, ColumnVectorInfo info) {
      int offset = info.offset;
      int len = offset + info.size;
      int vectorOffset = info.vectorOffset;
      CarbonColumnVector vector = info.vector;
      BitSet nullBitSet = dataChunk.getNullBits();
      if (nullBitSet.isEmpty()) {
        for (int i = offset; i < len; i++) {
          int currentRow = filteredRowId[i];
          vector.putBoolean(vectorOffset, dataChunk.getBoolean(currentRow));
          vectorOffset++;
        }
      } else {
        for (int i = offset; i < len; i++) {
          int currentRow = filteredRowId[i];
          if (nullBitSet.get(currentRow)) {
            vector.putNull(vectorOffset);
          } else {
            vector.putBoolean(vectorOffset, dataChunk.getBoolean(currentRow));
          }
          vectorOffset++;
        }
      }
    }
  }

  public static class ShortMeasureVectorFiller implements MeasureVectorFiller {

    @Override
    public void fillMeasureVector(ColumnPage dataChunk, ColumnVectorInfo info) {
      int offset = info.offset;
      int len = offset + info.size;
      int vectorOffset = info.vectorOffset;
      CarbonColumnVector vector = info.vector;
      BitSet nullBitSet = dataChunk.getNullBits();
      if (nullBitSet.isEmpty()) {
        for (int i = offset; i < len; i++) {
          vector.putShort(vectorOffset, (short) dataChunk.getLong(i));
          vectorOffset++;
        }
      } else {
        for (int i = offset; i < len; i++) {
          if (nullBitSet.get(i)) {
            vector.putNull(vectorOffset);
          } else {
            vector.putShort(vectorOffset, (short) dataChunk.getLong(i));
          }
          vectorOffset++;
        }
      }
    }

    @Override
    public void fillMeasureVector(int[] filteredRowId, ColumnPage dataChunk,
        ColumnVectorInfo info) {
      int offset = info.offset;
      int len = offset + info.size;
      int vectorOffset = info.vectorOffset;
      CarbonColumnVector vector = info.vector;
      BitSet nullBitSet = dataChunk.getNullBits();
      if (nullBitSet.isEmpty()) {
        for (int i = offset; i < len; i++) {
          int currentRow = filteredRowId[i];
          vector.putShort(vectorOffset, (short) dataChunk.getLong(currentRow));
          vectorOffset++;
        }
      } else {
        for (int i = offset; i < len; i++) {
          int currentRow = filteredRowId[i];
          if (nullBitSet.get(currentRow)) {
            vector.putNull(vectorOffset);
          } else {
            vector.putShort(vectorOffset, (short) dataChunk.getLong(currentRow));
          }
          vectorOffset++;
        }
      }
    }
  }

  public static class LongMeasureVectorFiller implements MeasureVectorFiller {

    @Override
    public void fillMeasureVector(ColumnPage dataChunk, ColumnVectorInfo info) {
      int offset = info.offset;
      int len = offset + info.size;
      int vectorOffset = info.vectorOffset;
      CarbonColumnVector vector = info.vector;
      BitSet nullBitSet = dataChunk.getNullBits();
      if (nullBitSet.isEmpty()) {
        for (int i = offset; i < len; i++) {
          vector.putLong(vectorOffset, dataChunk.getLong(i));
          vectorOffset++;
        }
      } else {
        for (int i = offset; i < len; i++) {
          if (nullBitSet.get(i)) {
            vector.putNull(vectorOffset);
          } else {
            vector.putLong(vectorOffset, dataChunk.getLong(i));
          }
          vectorOffset++;
        }
      }
    }

    @Override
    public void fillMeasureVector(int[] filteredRowId, ColumnPage dataChunk,
        ColumnVectorInfo info) {
      int offset = info.offset;
      int len = offset + info.size;
      int vectorOffset = info.vectorOffset;
      CarbonColumnVector vector = info.vector;
      BitSet nullBitSet = dataChunk.getNullBits();
      if (nullBitSet.isEmpty()) {
        for (int i = offset; i < len; i++) {
          int currentRow = filteredRowId[i];
          vector.putLong(vectorOffset, dataChunk.getLong(currentRow));
          vectorOffset++;
        }
      } else {
        for (int i = offset; i < len; i++) {
          int currentRow = filteredRowId[i];
          if (nullBitSet.get(currentRow)) {
            vector.putNull(vectorOffset);
          } else {
            vector.putLong(vectorOffset, dataChunk.getLong(currentRow));
          }
          vectorOffset++;
        }
      }
    }
  }

  public static class DecimalMeasureVectorFiller implements MeasureVectorFiller {

    @Override
    public void fillMeasureVector(ColumnPage dataChunk, ColumnVectorInfo info) {
      int offset = info.offset;
      int len = offset + info.size;
      int vectorOffset = info.vectorOffset;
      CarbonColumnVector vector = info.vector;
      int precision = info.measure.getMeasure().getPrecision();
      int newMeasureScale = info.measure.getMeasure().getScale();
      BitSet nullBitSet = dataChunk.getNullBits();
      for (int i = offset; i < len; i++) {
        if (nullBitSet.get(i)) {
          vector.putNull(vectorOffset);
        } else {
          BigDecimal decimal =
              dataChunk.getDecimal(i);
          if (decimal.scale() < newMeasureScale) {
            decimal = decimal.setScale(newMeasureScale);
          }
          vector.putDecimal(vectorOffset, decimal, precision);
        }
        vectorOffset++;
      }
    }

    @Override
    public void fillMeasureVector(int[] filteredRowId, ColumnPage dataChunk,
        ColumnVectorInfo info) {
      int offset = info.offset;
      int len = offset + info.size;
      int vectorOffset = info.vectorOffset;
      CarbonColumnVector vector = info.vector;
      int precision = info.measure.getMeasure().getPrecision();
      BitSet nullBitSet = dataChunk.getNullBits();
      for (int i = offset; i < len; i++) {
        int currentRow = filteredRowId[i];
        if (nullBitSet.get(currentRow)) {
          vector.putNull(vectorOffset);
        } else {
          BigDecimal decimal = dataChunk.getDecimal(currentRow);
          if (info.measure.getMeasure().getScale() > decimal.scale()) {
            decimal = decimal.setScale(info.measure.getMeasure().getScale());
          }
          vector.putDecimal(vectorOffset, decimal, precision);
        }
        vectorOffset++;
      }
    }
  }

  public static class DefaultMeasureVectorFiller implements MeasureVectorFiller {

    @Override
    public void fillMeasureVector(ColumnPage dataChunk, ColumnVectorInfo info) {
      int offset = info.offset;
      int len = offset + info.size;
      int vectorOffset = info.vectorOffset;
      CarbonColumnVector vector = info.vector;
      BitSet nullBitSet = dataChunk.getNullBits();
      if (nullBitSet.isEmpty()) {
        for (int i = offset; i < len; i++) {
          vector.putDouble(vectorOffset, dataChunk.getDouble(i));
          vectorOffset++;
        }
      } else {
        for (int i = offset; i < len; i++) {
          if (nullBitSet.get(i)) {
            vector.putNull(vectorOffset);
          } else {
            vector.putDouble(vectorOffset, dataChunk.getDouble(i));
          }
          vectorOffset++;
        }
      }
    }

    @Override
    public void fillMeasureVector(int[] filteredRowId, ColumnPage dataChunk,
        ColumnVectorInfo info) {
      int offset = info.offset;
      int len = offset + info.size;
      int vectorOffset = info.vectorOffset;
      CarbonColumnVector vector = info.vector;
      BitSet nullBitSet = dataChunk.getNullBits();
      if (nullBitSet.isEmpty()) {
        for (int i = offset; i < len; i++) {
          int currentRow = filteredRowId[i];
          vector.putDouble(vectorOffset, dataChunk.getDouble(currentRow));
          vectorOffset++;
        }
      } else {
        for (int i = offset; i < len; i++) {
          int currentRow = filteredRowId[i];
          if (nullBitSet.get(currentRow)) {
            vector.putNull(vectorOffset);
          } else {
            vector.putDouble(vectorOffset, dataChunk.getDouble(currentRow));
          }
          vectorOffset++;
        }
      }
    }
  }

  public static class MeasureVectorFillerFactory {

    public static MeasureVectorFiller getMeasureVectorFiller(DataType dataType) {
      if (dataType == DataTypes.BOOLEAN) {
        return new BooleanMeasureVectorFiller();
      } else if (dataType == DataTypes.SHORT) {
        return new ShortMeasureVectorFiller();
      } else if (dataType == DataTypes.INT) {
        return new IntegralMeasureVectorFiller();
      } else if (dataType == DataTypes.LONG) {
        return new LongMeasureVectorFiller();
      } else if (DataTypes.isDecimal(dataType)) {
        return new DecimalMeasureVectorFiller();
      } else {
        return new DefaultMeasureVectorFiller();
      }
    }
  }

}
