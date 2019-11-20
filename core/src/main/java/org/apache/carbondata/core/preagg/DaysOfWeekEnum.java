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

package org.apache.carbondata.core.preagg;

/**
 * Class to define the days of the week inline with the Java Calender class DAYs.
 */
public enum DaysOfWeekEnum {

  SUNDAY("SUNDAY", 1),
  MONDAY("MONDAY", 2),
  TUESDAY("TUESDAY", 3),
  WEDNESDAY("WEDNESDAY", 4),
  THURSDAY("THURSDAY", 5),
  FRIDAY("FRIDAY", 6),
  SATURDAY("SATURDAY", 7);

  public String getDay() {
    return day;
  }

  public int getOrdinal() {
    return ordinal;
  }

  private String day;

  private int ordinal;

  DaysOfWeekEnum(String day, int ordinal) {
    this.day = day;
    this.ordinal = ordinal;
  }
}
