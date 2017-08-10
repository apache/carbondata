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
package org.apache.carbondata.presto.readers;

import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;

/**
 * This class creates streamReader
 * Based on type.
 */
public final class StreamReaders {
  /**
   *
   * @param type
   * @param data
   * @return StreamReader
   */
  public static StreamReader createStreamReader(Type type, Object[] data) {
    Class<?> javaType = type.getJavaType();
    if (javaType == long.class) {
      if (type instanceof IntegerType || type instanceof DateType) {
        return new IntegerStreamReader();
      } else if (type instanceof DecimalType) {
        return new DecimalSliceStreamReader();
      }
      return new LongStreamReader();
    } else if (javaType == double.class) {
      return new DoubleStreamReader();
    } else if (javaType == Slice.class) {
      if (type instanceof DecimalType) {
        return new DecimalSliceStreamReader();
      } else {
        return new SliceStreamReader();
      }
    } else {
      return new ObjectStreamReader();
    }
  }

}