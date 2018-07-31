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

package org.apache.carbondata.horizon.rest.model.view;

import java.util.HashMap;
import java.util.Map;

import org.apache.carbondata.sdk.store.descriptor.LoadDescriptor;
import org.apache.carbondata.sdk.store.descriptor.TableIdentifier;

public class LoadRequest extends Request {

  private String databaseName;
  private String tableName;
  private String inputPath;
  private Map<String, String> options;
  private boolean isOverwrite;

  public LoadRequest() {
  }

  public LoadRequest(String databaseName, String tableName, String inputPaths,
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

  public LoadDescriptor convertToDto() {
    return new LoadDescriptor(new TableIdentifier(tableName, databaseName),
        inputPath, options, isOverwrite);
  }

  public static class Builder {
    private LoadRequest load;
    private Map<String, String> options;

    private Builder() {
      load = new LoadRequest();
      options = new HashMap<>();
    }

    public Builder databaseName(String databaseName) {
      load.setDatabaseName(databaseName);
      return this;
    }

    public Builder tableName(String tableName) {
      load.setTableName(tableName);
      return this;
    }

    public Builder overwrite(boolean isOverwrite) {
      load.setOverwrite(isOverwrite);
      return this;
    }

    public Builder inputPath(String inputPath) {
      load.setInputPath(inputPath);
      return this;
    }

    public Builder options(String key, String value) {
      options.put(key, value);
      return this;
    }

    public LoadRequest create() {
      load.setOptions(options);
      return load;
    }
  }

  public static Builder builder() {
    return new Builder();
  }
}
