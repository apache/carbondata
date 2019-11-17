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

package org.apache.carbondata.core.keygenerator.directdictionary.timestamp;

/**
 * Enum constant having the milli second for second, minute, hour, day
 */
public enum TimeStampGranularityTypeValue {
  /**
   * 1 second value in ms
   */
  MILLIS_SECONDS(1000),
  /**
   * 1 minute value in ms
   */
  MILLIS_MINUTE(1000 * 60),
  /**
   * 1 hour value in ms
   */
  MILLIS_HOUR(1000 * 60 * 60),
  /**
   * 1 day value in ms
   */
  MILLIS_DAY(1000 * 60 * 60 * 24);

  /**
   * enum constant value
   */
  private final long value;

  /**
   * constructor of enum constant
   *
   * @param value
   */
  TimeStampGranularityTypeValue(long value) {
    this.value = value;
  }

  /**
   * @return return the value of enum constant
   */
  public long getValue() {
    return this.value;
  }

}
