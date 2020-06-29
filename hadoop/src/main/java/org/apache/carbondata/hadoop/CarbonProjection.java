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

package org.apache.carbondata.hadoop;

import java.io.Serializable;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * User can add required columns
 */
public class CarbonProjection implements Serializable {

  private static final long serialVersionUID = -4328676723039530713L;

  private Set<String> columns = new LinkedHashSet<>();

  public CarbonProjection() {
  }

  public CarbonProjection(String[] columnNames) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1998
    Objects.requireNonNull(columnNames);
    for (String columnName : columnNames) {
      columns.add(columnName);
    }
  }

  public void addColumn(String column) {
    columns.add(column);
  }

  public String[] getAllColumns() {
    return columns.toArray(new String[columns.size()]);
  }

  public boolean isEmpty() {
    return columns.isEmpty();
  }
}
