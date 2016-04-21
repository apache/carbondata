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

package org.carbondata.core.carbon;

/**
 * Enumerates the types of levels.
 */
public enum LevelType {

  /**
   * Indicates that the level is not related to time.
   */
  Regular,

  /**
   * Indicates that a level refers to years.
   * It must be used in a dimension whose type is
   * {@link DimensionType#TimeDimension}.
   */
  TimeYears,

  /**
   * Indicates that a level refers to half years.
   * It must be used in a dimension whose type is
   * {@link DimensionType#TimeDimension}.
   */
  TimeHalfYears,

  /**
   * Indicates that a level refers to quarters.
   * It must be used in a dimension whose type is
   * {@link DimensionType#TimeDimension}.
   */
  TimeQuarters,

  /**
   * Indicates that a level refers to months.
   * It must be used in a dimension whose type is
   * {@link DimensionType#TimeDimension}.
   */
  TimeMonths,

  /**
   * Indicates that a level refers to weeks.
   * It must be used in a dimension whose type is
   * {@link DimensionType#TimeDimension}.
   */
  TimeWeeks,

  /**
   * Indicates that a level refers to days.
   * It must be used in a dimension whose type is
   * {@link DimensionType#TimeDimension}.
   */
  TimeDays,

  /**
   * Indicates that a level refers to hours.
   * It must be used in a dimension whose type is
   * {@link DimensionType#TimeDimension}.
   */
  TimeHours,

  /**
   * Indicates that a level refers to minutes.
   * It must be used in a dimension whose type is
   * {@link DimensionType#TimeDimension}.
   */
  TimeMinutes,

  /**
   * Indicates that a level refers to seconds.
   * It must be used in a dimension whose type is
   * {@link DimensionType#TimeDimension}.
   */
  TimeSeconds,

  /**
   * Indicates that a level is an unspecified time period.
   * It must be used in a dimension whose type is
   * {@link DimensionType#TimeDimension}.
   */
  TimeUndefined,

  /**
   * Indicates that a level holds the null member.
   */
  Null;

  /**
   * Returns whether this is a time level.
   *
   * @return Whether this is a time level.
   */
  public boolean isTime() {
    return ordinal() >= TimeYears.ordinal() && ordinal() <= TimeUndefined.ordinal();
  }
}
