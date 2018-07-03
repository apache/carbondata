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

package org.apache.carbondata.store.rest.model.dto;

import java.util.Map;

public class Load {

  private String databaseName;
  private String tableName;
  private String inputPath;
  private Map<String, String> options;
  private boolean isOverwrite;

  public Load() {
  }

  public Load(String databaseName, String tableName, String inputPaths,
      Map<String, String> options, boolean isOverwrite) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.inputPath = inputPaths;
    this.options = options;
    this.isOverwrite = isOverwrite;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getInputPath() {
    return inputPath;
  }

  public void setInputPath(String inputPath) {
    this.inputPath = inputPath;
  }

  public Map<String, String> getOptions() {
    return options;
  }

  public void setOptions(Map<String, String> options) {
    this.options = options;
  }

  public boolean isOverwrite() {
    return isOverwrite;
  }

  public void setOverwrite(boolean overwrite) {
    isOverwrite = overwrite;
  }
}
