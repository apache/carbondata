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

package org.apache.carbondata.processing.loading.converter;

import java.util.HashMap;
import java.util.Map;

/**
 * It is holder for reason of bad records.
 */
public class BadRecordLogHolder {

  /**
   * this map will hold the bad record unified message for columns
   */
  private Map<String, String> columnMessageMap = new HashMap<>();

  private String reason;

  private boolean badRecordAdded;

  private boolean isLogged;

  public String getReason() {
    return reason;
  }

  public void setReason(String reason) {
    this.reason = reason;
    badRecordAdded = true;
  }

  public boolean isBadRecordNotAdded() {
    return badRecordAdded;
  }

  public void clear() {
    this.badRecordAdded = false;
  }

  public boolean isLogged() {
    return isLogged;
  }

  public void setLogged(boolean logged) {
    isLogged = logged;
  }

  public Map<String, String> getColumnMessageMap() {
    return columnMessageMap;
  }

  /**
   * this method will clear the map entries
   */
  public void finish() {
    if (null != columnMessageMap) {
      columnMessageMap.clear();
    }
  }
}
