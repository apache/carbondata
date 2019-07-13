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

package org.apache.carbondata.processing.store;

/**
 * This class contains attributes of file which are required to
 * construct file name like taskId, factTimeStamp
 */
public class CarbonDataFileAttributes {
  /**
   * task Id which is unique for each spark task
   */
  private String taskId;

  /**
   * load start time
   */
  private long factTimeStamp;

  /**
   * @param taskId
   * @param factTimeStamp
   */
  public CarbonDataFileAttributes(String taskId, long factTimeStamp) {
    this.taskId = taskId;
    this.factTimeStamp = factTimeStamp;
  }

  /**
   * @param taskId
   * @param factTimeStamp
   */
  public CarbonDataFileAttributes(long taskId, long factTimeStamp) {
    this.taskId = String.valueOf(taskId);
    this.factTimeStamp = factTimeStamp;
  }

  /**
   * @return
   */
  public String getTaskId() {
    return taskId;
  }

  /**
   * @return fact time stamp which is load start time
   */
  public long getFactTimeStamp() {
    return factTimeStamp;
  }

}
