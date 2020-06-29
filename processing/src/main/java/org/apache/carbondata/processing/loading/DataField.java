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

package org.apache.carbondata.processing.loading;

import java.io.Serializable;

import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;

/**
 * Metadata class for each column of table.
 */
public class DataField implements Serializable {

  public DataField(CarbonColumn column) {
    this.column = column;
  }

  private CarbonColumn column;

  private String dateFormat;

  private String timestampFormat;

  private boolean useActualData;

  public boolean isDateDataType() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3674
    return column.getDataType() == DataTypes.DATE;
  }

  public CarbonColumn getColumn() {
    return column;
  }

  public String getDateFormat() {
    return dateFormat;
  }

  public void setDateFormat(String dateFormat) {
    this.dateFormat = dateFormat;
  }

  public String getTimestampFormat() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1762
    return timestampFormat;
  }

  public void setTimestampFormat(String timestampFormat) {
    this.timestampFormat = timestampFormat;
  }

  public boolean isUseActualData() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2168
    return useActualData;
  }

  public void setUseActualData(boolean useActualData) {
    this.useActualData = useActualData;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2388
    this.column.setUseActualData(useActualData);
  }
}
