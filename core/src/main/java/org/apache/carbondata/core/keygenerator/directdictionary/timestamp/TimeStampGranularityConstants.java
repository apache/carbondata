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
 * Constant related to timestamp conversion
 */
public interface TimeStampGranularityConstants {

  /**
   * The property to set the date to be considered as start date for calculating the timestamp
   * java counts the number of milliseconds from  start of "January 1, 1970", this property is
   * customized the start of position. for example "January 1, 2000"
   */
  String CARBON_CUTOFF_TIMESTAMP = "carbon.cutOffTimestamp";
  /**
   * The property to set the timestamp (ie millisecond) conversion to the SECOND, MINUTE, HOUR
   * or DAY level
   */
  String CARBON_TIME_GRANULARITY = "carbon.timegranularity";

  /**
   * Second level key
   */
  String TIME_GRAN_SEC = "SECOND";
  /**
   * minute level key
   */
  String TIME_GRAN_MIN = "MINUTE";
  /**
   * hour level key
   */
  String TIME_GRAN_HOUR = "HOUR";
  /**
   * day level key
   */
  String TIME_GRAN_DAY = "DAY";
}
