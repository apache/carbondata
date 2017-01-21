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

package org.apache.carbondata.common.logging.impl;

import org.apache.log4j.Level;

/**
 * Extended log level class to log the statistic details
 */
public class StatisticLevel extends Level {

  public static final StatisticLevel STATISTIC = new StatisticLevel(55000, "STATISTIC", 0);

  private static final long serialVersionUID = -209614723183147373L;

  /**
   * Constructor
   *
   * @param level            log level
   * @param levelStr         log level string
   * @param syslogEquivalent syslogEquivalent
   */
  protected StatisticLevel(int level, String levelStr, int syslogEquivalent) {
    super(level, levelStr, syslogEquivalent);
  }

  /**
   * Returns custom level for debug type log message
   *
   * @param val          value
   * @param defaultLevel level
   * @return custom level
   */
  public static StatisticLevel toLevel(int val, Level defaultLevel) {
    return STATISTIC;
  }

  /**
   * Returns custom level for debug type log message
   *
   * @param sArg         sArg
   * @param defaultLevel level
   * @return custom level
   */
  public static StatisticLevel toLevel(String sArg, Level defaultLevel) {
    return STATISTIC;
  }
}
