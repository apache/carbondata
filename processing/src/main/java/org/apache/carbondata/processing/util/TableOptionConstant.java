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

package org.apache.carbondata.processing.util;

/**
 * enum holds the value related to the ddl option
 */
public enum TableOptionConstant {
  SERIALIZATION_NULL_FORMAT("serialization_null_format"),
  BAD_RECORDS_LOGGER_ENABLE("bad_records_logger_enable"),
  BAD_RECORDS_ACTION("bad_records_action");

  private String name;

  /**
   * constructor to initialize the enum value
   * @param name
   */
  TableOptionConstant(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }
}
