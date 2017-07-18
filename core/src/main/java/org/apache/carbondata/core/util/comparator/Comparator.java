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

  public static SerializableComparator getComparator(DataType dataType) {
    switch (dataType) {
      case INT:
        return new IntSerializableComparator();
      case SHORT:
        return new ShortSerializableComparator();
      case DOUBLE:
        return new DoubleSerializableComparator();
      case LONG:
      case DATE:
      case TIMESTAMP:
        return new LongSerializableComparator();
      case DECIMAL:
        return new BigDecimalSerializableComparator();
      default:
        return new ByteArraySerializableComparator();
    }
  }
}

class ByteArraySerializableComparator implements SerializableComparator {
  @Override public int compare(Object key1, Object key2) {
    return ByteUtil.compare(key1.toString().getBytes(), key2.toString().getBytes());
  }
}

class IntSerializableComparator implements SerializableComparator {
  @Override public int compare(Object key1, Object key2) {
    if ((int) key1 < (int) key2) {
      return -1;
    } else if ((int) key1 > (int) key2) {
      return 1;
    } else {
      return 0;
    }
  }
}

class ShortSerializableComparator implements SerializableComparator {
  @Override public int compare(Object key1, Object key2) {
    if ((short) key1 < (short) key2) {
      return -1;
    } else if ((short) key1 > (short) key2) {
      return 1;
    } else {
      return 0;
    }
  }
}

class DoubleSerializableComparator implements SerializableComparator {
  @Override public int compare(Object key1, Object key2) {
    if ((double) key1 < (double) key2) {
      return -1;
    } else if ((double) key1 > (double) key2) {
      return 1;
    } else {
      return 0;
    }
  }
}

class LongSerializableComparator implements SerializableComparator {
  @Override public int compare(Object key1, Object key2) {
    if ((long) key1 < (long) key2) {
      return -1;
    } else if ((long) key1 > (long) key2) {
      return 1;
    } else {
      return 0;
    }
  }
}

class BigDecimalSerializableComparator implements SerializableComparator {
  @Override public int compare(Object key1, Object key2) {
    return ((BigDecimal) key1).compareTo((BigDecimal) key2);
  }
}