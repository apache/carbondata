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

package org.apache.carbondata.processing.constants;

public final class DataProcessorConstants {
  /**
   *
   */
  public static final String CSV_DATALOADER = "CSV_DATALOADER";
  /**
   *
   */
  public static final String DATARESTRUCT = "DATARESTRUCT";
  /**
   * UPDATEMEMBER
   */
  public static final String UPDATEMEMBER = "UPDATEMEMBER";
  /**
   * number of days task should be in DB table
   */
  public static final String TASK_RETENTION_DAYS = "dataload.taskstatus.retention";
  /**
   * LOAD_FOLDER
   */
  public static final String LOAD_FOLDER = "Load_";
  /**
   * if bad record found
   */
  public static final long BAD_REC_FOUND = 223732673;
  /**
   * if bad record found
   */
  public static final long CSV_VALIDATION_ERRROR_CODE = 113732678;
  /**
   * Year Member val for data retention.
   */
  public static final String YEAR = "YEAR";

  /**
   * if data load fails due to bad record
   */
  public static final long BAD_REC_FAILURE_ERROR_CODE = 223732674;

  private DataProcessorConstants() {

  }
}
