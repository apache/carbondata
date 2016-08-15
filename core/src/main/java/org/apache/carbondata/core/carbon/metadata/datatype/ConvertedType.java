/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.carbondata.core.carbon.metadata.datatype;

public enum ConvertedType {

  /**
   * a BYTE_ARRAY actually contains UTF8 encoded chars
   */
  UTF8,
  /**
   * a map is converted as an optional field containing a repeated key/value pair
   */
  MAP,
  /**
   * a key/value pair is converted into a group of two fields
   */
  MAP_KEY_VALUE,
  /**
   * a list is converted into an optional field containing a repeated field for its
   * values
   */
  LIST,
  /**
   * an enum is converted into a binary field
   */
  ENUM,
  /**
   * A decimal value.
   * This may be used to annotate binary or fixed primitive types. The
   * underlying byte array stores the unscaled value encoded as two's
   * complement using big-endian byte order (the most significant byte is the
   * zeroth element). The value of the decimal is the value * 10^{-scale}.
   * This must be accompanied by a (maximum) precision and a scale in the
   * SchemaElement. The precision specifies the number of digits in the decimal
   * and the scale stores the location of the decimal point. For example 1.23
   * would have precision 3 (3 total digits) and scale 2 (the decimal point is
   * 2 digits over).
   */
  DECIMAL,
  /**
   * A Date
   * Stored as days since Unix epoch, encoded as the INT32 physical type.
   */
  DATE,
  /**
   * A time
   * The total number of milliseconds since midnight.  The value is stored
   * as an INT32 physical type.
   */
  TIME_MILLIS,
  /**
   * A date/time combination
   * Date and time recorded as milliseconds since the Unix epoch.  Recorded as
   * a physical type of INT64.
   */
  TIMESTAMP_MILLIS,

  RESERVED,
  /**
   * An unsigned integer value.
   * The number describes the maximum number of meainful data bits in
   * the stored value. 8, 16 and 32 bit values are stored using the
   * INT32 physical type.  64 bit values are stored using the INT64
   * physical type.
   */
  UINT_8,
  UINT_16,
  UINT_32,
  UINT_64,
  /**
   * A signed integer value.
   * The number describes the maximum number of meainful data bits in
   * the stored value. 8, 16 and 32 bit values are stored using the
   * INT32 physical type.  64 bit values are stored using the INT64
   * physical type.
   */
  INT_8,
  INT_16,
  INT_32,
  INT_64,
  /**
   * An embedded JSON document
   * A JSON document embedded within a single UTF8 column.
   */
  JSON,

  /**
   * An embedded BSON document
   * A BSON document embedded within a single BINARY column.
   */
  BSON,

  /**
   * An interval of time
   * This type annotates data stored as a FIXED_LEN_BYTE_ARRAY of length 12
   * This data is composed of three separate little endian unsigned
   * integers.  Each stores a component of a duration of time.  The first
   * integer identifies the number of months associated with the duration,
   * the second identifies the number of days associated with the duration
   * and the third identifies the number of milliseconds associated with
   * the provided duration.  This duration of time is independent of any
   * particular timezone or date.
   */
  INTERVAL;
}
