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

package org.apache.carbondata.presto;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.MoreObjects.toStringHelper;


public class CarbondataColumnHandle implements ColumnHandle {
  private final String connectorId;
  private final String columnName;

  public boolean isInvertedIndex() {
    return isInvertedIndex;
  }

  private final Type columnType;
  private final int ordinalPosition;
  private final int keyOrdinal;

  private final String columnUniqueId;
  private final boolean isInvertedIndex;

  /**
   * Used when this column contains decimal data.
   */
  private int scale;

  private int precision;


  public boolean isMeasure() {
    return isMeasure;
  }

  private final boolean isMeasure;

  public int getKeyOrdinal() {
    return keyOrdinal;
  }

  @JsonCreator public CarbondataColumnHandle(
      @JsonProperty("connectorId") String connectorId,
      @JsonProperty("columnName") String columnName,
      @JsonProperty("columnType") Type columnType,
      @JsonProperty("ordinalPosition") int ordinalPosition,
      @JsonProperty("keyOrdinal") int keyOrdinal,
      @JsonProperty("isMeasure") boolean isMeasure,
      @JsonProperty("columnUniqueId") String columnUniqueId,
      @JsonProperty("isInvertedIndex") boolean isInvertedIndex,
      @JsonProperty("precision") int precision,
      @JsonProperty("scale") int scale) {
    this.connectorId = requireNonNull(connectorId, "connectorId is null");
    this.columnName = requireNonNull(columnName, "columnName is null");
    this.columnType = requireNonNull(columnType, "columnType is null");

    this.ordinalPosition = requireNonNull(ordinalPosition, "ordinalPosition is null");
    this.keyOrdinal = requireNonNull(keyOrdinal, "keyOrdinal is null");

    this.isMeasure = isMeasure;
    this.columnUniqueId = columnUniqueId;//requireNonNull(columnUniqueId, "columnUniqueId is null");
    this.isInvertedIndex = requireNonNull(isInvertedIndex, "isInvertedIndex is null");
    this.precision = precision;
    this.scale = scale;
  }

  @JsonProperty public String getConnectorId() {
    return connectorId;
  }

  @JsonProperty public String getColumnName() {
    return columnName;
  }

  @JsonProperty public Type getColumnType() {
    return columnType;
  }

  @JsonProperty public int getOrdinalPosition() {
    return ordinalPosition;
  }

  public ColumnMetadata getColumnMetadata() {
    return new ColumnMetadata(columnName, columnType, null, false);
  }

  @Override public int hashCode() {
    return Objects.hash(connectorId, columnName);
  }

  @Override public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }

    CarbondataColumnHandle other = (CarbondataColumnHandle) obj;
    return Objects.equals(this.connectorId, other.connectorId) && Objects
        .equals(this.columnName, other.columnName);
  }

  @Override public String toString() {
    return toStringHelper(this).add("connectorId", connectorId).add("columnName", columnName)
        .add("columnType", columnType).add("ordinalPosition", ordinalPosition).toString();
  }

  @JsonProperty public int getScale() {
    return scale;
  }

  @JsonProperty public int getPrecision() {
    return precision;
  }



}
