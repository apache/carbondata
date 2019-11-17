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

package org.apache.carbondata.sdk.file.arrow;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.apache.carbondata.core.constants.CarbonCommonConstants;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;

public abstract class ArrowFieldWriter {

  private ValueVector valueVector;

  protected int count;

  public ArrowFieldWriter(ValueVector valueVector) {
    this.valueVector = valueVector;
  }

  public abstract void setNull();

  public abstract void setValue(Object data, int ordinal);

  public void write(Object data, int ordinal) {
    if (data == null) {
      setNull();
    } else {
      setValue(data, ordinal);
    }
    count += 1;
  }

  public void finish() {
    valueVector.setValueCount(count);
  }

  public void reset() {
    valueVector.reset();
    count = 0;
  }
}

class BooleanWriter extends ArrowFieldWriter {

  private BitVector bitVector;

  public BooleanWriter(BitVector bitVector) {
    super(bitVector);
    this.bitVector = bitVector;
  }

  @Override
  public void setNull() {
    bitVector.setNull(count);
  }

  @Override
  public void setValue(Object data, int ordinal) {
    bitVector.setSafe(count, (Boolean) data ? 1 : 0);
  }
}

class ByteWriter extends ArrowFieldWriter {
  private TinyIntVector tinyIntVector;

  public ByteWriter(TinyIntVector tinyIntVector) {
    super(tinyIntVector);
    this.tinyIntVector = tinyIntVector;
  }

  @Override
  public void setNull() {
    this.tinyIntVector.setNull(count);
  }

  @Override
  public void setValue(Object data, int ordinal) {
    this.tinyIntVector.setSafe(count, (byte) data);
  }
}

class ShortWriter extends ArrowFieldWriter {
  private SmallIntVector smallIntVector;

  public ShortWriter(SmallIntVector smallIntVector) {
    super(smallIntVector);
    this.smallIntVector = smallIntVector;
  }

  @Override
  public void setNull() {
    this.smallIntVector.setNull(count);
  }

  @Override
  public void setValue(Object data, int ordinal) {
    this.smallIntVector.setSafe(count, (short) data);
  }
}

class IntWriter extends ArrowFieldWriter {
  private IntVector intVector;

  public IntWriter(IntVector intVector) {
    super(intVector);
    this.intVector = intVector;
  }

  @Override
  public void setNull() {
    this.intVector.setNull(count);
  }

  @Override
  public void setValue(Object data, int ordinal) {
    this.intVector.setSafe(count, (int) data);
  }
}

class LongWriter extends ArrowFieldWriter {
  private BigIntVector bigIntVector;

  public LongWriter(BigIntVector bigIntVector) {
    super(bigIntVector);
    this.bigIntVector = bigIntVector;
  }

  @Override
  public void setNull() {
    this.bigIntVector.setNull(count);
  }

  @Override
  public void setValue(Object data, int ordinal) {
    this.bigIntVector.setSafe(count, (long) data);
  }
}

class FloatWriter extends ArrowFieldWriter {
  private Float4Vector float4Vector;

  public FloatWriter(Float4Vector float4Vector) {
    super(float4Vector);
    this.float4Vector = float4Vector;
  }

  @Override
  public void setNull() {
    this.float4Vector.setNull(count);
  }

  @Override
  public void setValue(Object data, int ordinal) {
    this.float4Vector.setSafe(count, (float) data);
  }
}

class DoubleWriter extends ArrowFieldWriter {
  private Float8Vector float8Vector;

  public DoubleWriter(Float8Vector float8Vector) {
    super(float8Vector);
    this.float8Vector = float8Vector;
  }

  @Override
  public void setNull() {
    this.float8Vector.setNull(count);
  }

  @Override
  public void setValue(Object data, int ordinal) {
    this.float8Vector.setSafe(count, (double) data);
  }
}

class DateWriter extends ArrowFieldWriter {
  private DateDayVector dateDayVector;

  public DateWriter(DateDayVector dateDayVector) {
    super(dateDayVector);
    this.dateDayVector = dateDayVector;
  }

  @Override
  public void setNull() {
    this.dateDayVector.setNull(count);
  }

  @Override
  public void setValue(Object data, int ordinal) {
    this.dateDayVector.setSafe(count, (int)data);
  }
}

class TimeStampWriter extends ArrowFieldWriter {
  private TimeStampMicroTZVector timeStampMicroTZVector;

  public TimeStampWriter(TimeStampMicroTZVector timeStampMicroTZVector) {
    super(timeStampMicroTZVector);
    this.timeStampMicroTZVector = timeStampMicroTZVector;
  }

  @Override
  public void setNull() {
    this.timeStampMicroTZVector.setNull(count);
  }

  @Override
  public void setValue(Object data, int ordinal) {
    this.timeStampMicroTZVector.setSafe(count, (long)data);
  }
}

class StringWriter extends ArrowFieldWriter {
  private VarCharVector varCharVector;

  public StringWriter(VarCharVector varCharVector) {
    super(varCharVector);
    this.varCharVector = varCharVector;
  }

  @Override
  public void setNull() {
    this.varCharVector.setNull(count);
  }

  @Override
  public void setValue(Object data, int ordinal) {
    byte[] bytes =
        (String.valueOf(data)).getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    this.varCharVector.setSafe(count, byteBuffer, byteBuffer.position(), bytes.length);
  }
}

class BinaryWriter extends ArrowFieldWriter {
  private VarBinaryVector varBinaryVector;

  public BinaryWriter(VarBinaryVector varBinaryVector) {
    super(varBinaryVector);
    this.varBinaryVector = varBinaryVector;
  }

  @Override
  public void setNull() {
    this.varBinaryVector.setNull(count);
  }

  @Override
  public void setValue(Object data, int ordinal) {
    byte[] bytes = (byte[]) data;
    varBinaryVector.setSafe(count, bytes, 0, bytes.length);
  }
}

class DecimalWriter extends ArrowFieldWriter {
  private final int precision;
  private final int scale;
  private DecimalVector decimalVector;

  public DecimalWriter(DecimalVector decimalVector, int precision, int scale) {
    super(decimalVector);
    this.decimalVector = decimalVector;
    this.precision = precision;
    this.scale = scale;
  }

  @Override
  public void setNull() {
    this.decimalVector.setNull(count);
  }

  @Override
  public void setValue(Object data, int ordinal) {
    BigDecimal decimal = (BigDecimal) data;
    decimalVector.setSafe(count, decimal);
  }
}

class ArrayWriter extends ArrowFieldWriter {
  private ListVector listVector;
  private ArrowFieldWriter elementWriter;

  public ArrayWriter(ListVector listVector, ArrowFieldWriter elementWriter) {
    super(listVector);
    this.listVector = listVector;
    this.elementWriter = elementWriter;
  }

  @Override
  public void setNull() {

  }

  @Override
  public void setValue(Object data, int ordinal) {
    Object[] array = (Object[]) data;
    int i = 0;
    listVector.startNewValue(count);
    while (i < array.length) {
      elementWriter.write(array, i);
      i += 1;
    }
    listVector.endValue(count, array.length);
  }

  public void finish() {
    super.finish();
    elementWriter.finish();
  }

  public void reset() {
    super.reset();
    elementWriter.reset();
  }
}

class StructWriter extends ArrowFieldWriter {
  private StructVector structVector;
  private ArrowFieldWriter[] children;

  public StructWriter(StructVector structVector, ArrowFieldWriter[] children) {
    super(structVector);
    this.structVector = structVector;
    this.children = children;
  }

  @Override
  public void setNull() {
    int i = 0;
    while (i < children.length) {
      children[i].setNull();
      children[i].count += 1;
      i += 1;
    }
    structVector.setNull(count);
  }

  @Override
  public void setValue(Object data, int ordinal) {
    Object[] struct = (Object[]) data;
    int i = 0;
    while (i < struct.length) {
      children[i].write(struct[i], i);
      i += 1;
    }
    structVector.setIndexDefined(count);
  }

  public void finish() {
    super.finish();
    for (int i = 0; i < children.length; i++) {
      children[i].finish();
    }
  }

  public void reset() {
    super.reset();
    for (int i = 0; i < children.length; i++) {
      children[i].reset();
    }
  }
}
