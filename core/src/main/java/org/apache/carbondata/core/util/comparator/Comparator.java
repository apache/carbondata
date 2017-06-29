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

package org.apache.carbondata.core.util.comparator;

import java.math.BigDecimal;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.util.ByteUtil;

public final class Comparator {

  private static SerializableComparator comparator;

  public static SerializableComparator getComparator(DataType dataType) {
    switch (dataType) {
      case INT:
        comparator = new IntSerializableComparator();
        break;
      case SHORT:
        comparator = new ShortSerializableComparator();
        break;
      case DOUBLE:
        comparator = new DoubleSerializableComparator();
        break;
      case LONG:
      case DATE:
      case TIMESTAMP:
        comparator = new LongSerializableComparator();
        break;
      case DECIMAL:
        comparator = new BigDecimalSerializableComparator();
        break;
      default:
        comparator = new ByteArraySerializableComparator();
    }
    return comparator;
  }
}

class ByteArraySerializableComparator implements SerializableComparator {
  @Override public boolean compareTo(Object key1, Object key2) {
    return ByteUtil.compare((byte[]) key1, (byte[]) key2) < 0;
  }
}

class IntSerializableComparator implements SerializableComparator {
  @Override public boolean compareTo(Object key1, Object key2) {
    return (int) key1 - (int) key2 < 0;
  }
}

class ShortSerializableComparator implements SerializableComparator {
  @Override public boolean compareTo(Object key1, Object key2) {
    return (short) key1 - (short) key2 < 0;
  }
}

class DoubleSerializableComparator implements SerializableComparator {
  @Override public boolean compareTo(Object key1, Object key2) {
    return (double) key1 - (double) key2 < 0;
  }
}

class LongSerializableComparator implements SerializableComparator {
  @Override public boolean compareTo(Object key1, Object key2) {
    return (long) key1 - (long) key2 < 0;
  }
}

class BigDecimalSerializableComparator implements SerializableComparator {
  @Override public boolean compareTo(Object key1, Object key2) {
    return ((BigDecimal) key1).compareTo((BigDecimal) key2) < 0;
  }
}