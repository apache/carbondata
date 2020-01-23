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

package org.apache.carbondata.core.metadata.encoder;

import java.util.List;

/**
 * Encoding type supported in carbon
 */
public enum Encoding {
  DICTIONARY,
  DELTA,
  RLE,
  INVERTED_INDEX,
  BIT_PACKED,
  DIRECT_DICTIONARY,  // currently, only DATE data type use this encoding
  IMPLICIT,
  DIRECT_COMPRESS,
  ADAPTIVE_INTEGRAL,
  ADAPTIVE_DELTA_INTEGRAL,
  RLE_INTEGRAL,
  DIRECT_STRING,
  ADAPTIVE_FLOATING,
  BOOL_BYTE,
  ADAPTIVE_DELTA_FLOATING,
  DIRECT_COMPRESS_VARCHAR,
  INT_LENGTH_COMPLEX_CHILD_BYTE_ARRAY;

  public static Encoding valueOf(int ordinal) {
    if (ordinal == DICTIONARY.ordinal()) {
      return DICTIONARY;
    } else if (ordinal == DELTA.ordinal()) {
      return DELTA;
    } else if (ordinal == RLE.ordinal()) {
      return RLE;
    } else if (ordinal == INVERTED_INDEX.ordinal()) {
      return INVERTED_INDEX;
    } else if (ordinal == BIT_PACKED.ordinal()) {
      return BIT_PACKED;
    } else if (ordinal == DIRECT_DICTIONARY.ordinal()) {
      return DIRECT_DICTIONARY;
    } else if (ordinal == IMPLICIT.ordinal()) {
      return IMPLICIT;
    } else if (ordinal == DIRECT_COMPRESS.ordinal()) {
      return DIRECT_COMPRESS;
    } else if (ordinal == ADAPTIVE_INTEGRAL.ordinal()) {
      return ADAPTIVE_INTEGRAL;
    } else if (ordinal == ADAPTIVE_DELTA_INTEGRAL.ordinal()) {
      return ADAPTIVE_DELTA_INTEGRAL;
    } else if (ordinal == RLE_INTEGRAL.ordinal()) {
      return RLE_INTEGRAL;
    } else if (ordinal == DIRECT_STRING.ordinal()) {
      return DIRECT_STRING;
    } else if (ordinal == ADAPTIVE_FLOATING.ordinal()) {
      return ADAPTIVE_FLOATING;
    } else if (ordinal == BOOL_BYTE.ordinal()) {
      return BOOL_BYTE;
    } else if (ordinal == ADAPTIVE_DELTA_FLOATING.ordinal()) {
      return ADAPTIVE_DELTA_FLOATING;
    } else if (ordinal == DIRECT_COMPRESS_VARCHAR.ordinal()) {
      return DIRECT_COMPRESS_VARCHAR;
    } else if (ordinal == INT_LENGTH_COMPLEX_CHILD_BYTE_ARRAY.ordinal()) {
      return INT_LENGTH_COMPLEX_CHILD_BYTE_ARRAY;
    } else {
      throw new RuntimeException("create Encoding with invalid ordinal: " + ordinal);
    }
  }

  /**
   * Method to validate for supported encoding types that can be read using the current version
   *
   * @param encodings
   */
  public static void validateEncodingTypes(List<org.apache.carbondata.format.Encoding> encodings) {
    if (null != encodings && !encodings.isEmpty()) {
      for (org.apache.carbondata.format.Encoding encoder : encodings) {
        // if case is handle unsupported encoding type. An encoding not supported for read will
        // be added as null by thrift during deserialization
        // if given encoding name is not supported exception will be thrown
        if (null == encoder) {
          throw new UnsupportedOperationException(
              "There is mismatch between the encodings in data file and the encodings supported"
                  + " for read in the current version");
        } else {
          try {
            Encoding.valueOf(encoder.name());
          } catch (IllegalArgumentException ex) {
            throw new UnsupportedOperationException(
                "There is mismatch between the encodings in data file and the encodings supported"
                    + " for read in the current version. Encoding: " + encoder.name());
          }
        }
      }
    }
  }
}
