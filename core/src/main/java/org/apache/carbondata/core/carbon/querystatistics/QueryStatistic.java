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
package org.apache.carbondata.core.carbon.querystatistics;

import java.io.Serializable;

/**
 * Wrapper class to maintain the query statistics for each phase of the query
 */
public class QueryStatistic implements Serializable {

  /**
   * serialization id
   */
  private static final long serialVersionUID = -5667106646135905848L;

  /**
   * statistic message
   */
  private String message;

  /**
   * total time take of the phase
   */
  private long timeTaken;

  /**
   * starttime of the phase
   */
  private long startTime;

  /**
   * number of count
   */
  private long count;

  public QueryStatistic() {
    this.startTime = System.currentTimeMillis();
  }

  /**
   * below method will be used to add the statistic
   *
   * @param message     Statistic message
   * @param currentTime current time
   */
  public void addStatistics(String message, long currentTime) {
    this.timeTaken = currentTime - startTime;
    this.message = message;
  }

  /**
   * Below method will be used to add fixed time statistic.
   * For example total time taken for scan or result preparation
   *
   * @param message   statistic message
   * @param timetaken
   */
  public void addFixedTimeStatistic(String message, long timetaken) {
    this.timeTaken = timetaken;
    this.message = message;
  }

  public void addCountStatistic(String message, long count) {
    this.timeTaken = -1;
    this.count = count;
    this.message = message;
  }

  /**
   * Below method will be used to get the statistic message, which will
   * be used to log
   *
   * @param queryWithTaskId query with task id to append in the message
   * @return statistic message
   */
  public String getStatistics(String queryWithTaskId) {
    return message + " for the taskid : " + queryWithTaskId + " Is : " + timeTaken;
  }

  public String getMessage() {
    return this.message;
  }

  public long getTimeTaken() {
    return  this.timeTaken;
  }

  public long getCount() {
    return this.count;
  }

}
