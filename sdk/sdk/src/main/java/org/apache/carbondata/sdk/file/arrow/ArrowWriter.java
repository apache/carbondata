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

import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.sdk.file.Schema;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

public class ArrowWriter {

  private VectorSchemaRoot root;

  private ArrowFieldWriter[] children;

  private int count;

  public void write(Object[] data) {
    int i = 0;
    while (i < children.length) {
      children[i].write(data[i], i);
      i += 1;
    }
    count += 1;
  }

  public void finish() {
    root.setRowCount(count);
    for (int i = 0; i < children.length; i++) {
      children[i].finish();
    }
  }

  public void reset() {
    root.setRowCount(0);
    count = 0;
    for (int i = 0; i < children.length; i++) {
      children[i].reset();
    }
  }

  private ArrowWriter(VectorSchemaRoot root, ArrowFieldWriter[] children) {
    this.root = root;
    this.children = children;
  }

  public static ArrowWriter create(Schema schema, String timeZoneId) {
    org.apache.arrow.vector.types.pojo.Schema arrowSchema =
        ArrowUtils.toArrowSchema(schema, timeZoneId);
    VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, ArrowUtils.rootAllocator);
    return create(root);
  }

  public static ArrowWriter create(VectorSchemaRoot root) {
    final List<FieldVector> fieldVectors = root.getFieldVectors();
    ArrowFieldWriter[] fieldWriters = new ArrowFieldWriter[fieldVectors.size()];
    int i = 0;
    for (FieldVector fieldVector : fieldVectors) {
      fieldWriters[i] = createFieldWriter(fieldVector);
      i++;
    }
    return new ArrowWriter(root, fieldWriters);
  }

  private static ArrowFieldWriter createFieldWriter(ValueVector valueVector) {
    if (valueVector instanceof BitVector) {
      return new BooleanWriter((BitVector) valueVector);
    } else if (valueVector instanceof TinyIntVector) {
      return new ByteWriter((TinyIntVector) valueVector);
    } else if (valueVector instanceof SmallIntVector) {
      return new ShortWriter((SmallIntVector) valueVector);
    } else if (valueVector instanceof IntVector) {
      return new IntWriter((IntVector) valueVector);
    } else if (valueVector instanceof BigIntVector) {
      return new LongWriter((BigIntVector) valueVector);
    } else if (valueVector instanceof DecimalVector) {
      DecimalVector decimalVector = (DecimalVector) valueVector;
      final Field field = decimalVector.getField();
      ArrowType.Decimal c = (ArrowType.Decimal) field.getType();
      return new DecimalWriter((DecimalVector) valueVector, c.getPrecision(), c.getScale());
    } else if (valueVector instanceof VarCharVector) {
      return new StringWriter((VarCharVector) valueVector);
    } else if (valueVector instanceof Float4Vector) {
      return new FloatWriter((Float4Vector) valueVector);
    } else if (valueVector instanceof Float8Vector) {
      return new DoubleWriter((Float8Vector) valueVector);
    } else if (valueVector instanceof ListVector) {
      ArrowFieldWriter elementVector =
          createFieldWriter(((ListVector) valueVector).getDataVector());
      return new ArrayWriter((ListVector) valueVector, elementVector);
    } else if (valueVector instanceof StructVector) {
      StructVector s = (StructVector) valueVector;
      List<ArrowFieldWriter> arrowFieldWriters = new ArrayList<>();
      for (int i = 0; i < s.size(); i++) {
        arrowFieldWriters.add(createFieldWriter(s.getChildByOrdinal(i)));
      }
      return new StructWriter(s,
          arrowFieldWriters.toArray(new ArrowFieldWriter[arrowFieldWriters.size()]));
    } else if (valueVector instanceof VarBinaryVector) {
      return new BinaryWriter((VarBinaryVector) valueVector);
    } else if (valueVector instanceof DateDayVector) {
      return new DateWriter((DateDayVector) valueVector);
    } else if (valueVector instanceof TimeStampMicroTZVector) {
      return new TimeStampWriter((TimeStampMicroTZVector) valueVector);
    } else {
      throw new UnsupportedOperationException("Invalid data type");
    }
  }
}

