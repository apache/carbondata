/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.carbondata;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CarbondataColumnHandle
    implements ColumnHandle
{
        private final String connectorId;
        private final String columnName;

    public boolean isInvertedIndex() {
        return isInvertedIndex;
    }

    private final Type columnType;
    private final int ordinalPosition;
    private final int keyOrdinal;
    private final int columnGroupOrdinal;

    private final int columnGroupId;
    private final String columnUniqueId;
    private final boolean isInvertedIndex;

    public boolean isMeasure() {
        return isMeasure;
    }

    private final boolean isMeasure;

    public int getKeyOrdinal() {
        return keyOrdinal;
    }

    public int getColumnGroupOrdinal() {
        return columnGroupOrdinal;
    }

    public int getColumnGroupId() {
        return columnGroupId;
    }

    public String getColumnUniqueId() {
        return columnUniqueId;
    }
    /* ordinalPosition of a columnhandle is the -> number of the column in the entire list of columns of this table
        IT DOESNT DEPEND ON THE QUERY (select clm3, clm0, clm1  from tablename)
        The columnhandle of clm3 : has ordinalposition = 3
     */

    @JsonCreator
    public CarbondataColumnHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnType") Type columnType,
            @JsonProperty("ordinalPosition") int ordinalPosition,
            @JsonProperty("keyOrdinal") int keyOrdinal,
            @JsonProperty("columnGroupOrdinal") int columnGroupOrdinal,
            @JsonProperty("isMeasure") boolean isMeasure,
            @JsonProperty("columnGroupId") int columnGroupId,
            @JsonProperty("columnUniqueId") String columnUniqueId,
            @JsonProperty("isInvertedIndex") boolean isInvertedIndex)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.columnType = requireNonNull(columnType, "columnType is null");

        this.ordinalPosition = requireNonNull(ordinalPosition, "ordinalPosition is null");
        this.keyOrdinal = requireNonNull(keyOrdinal, "keyOrdinal is null");
        this.columnGroupOrdinal = requireNonNull(columnGroupOrdinal, "columnGroupOrdinal is null");

        this.isMeasure = isMeasure;
        this.columnGroupId = requireNonNull(columnGroupId, "columnGroupId is null");
        this.columnUniqueId = columnUniqueId;//requireNonNull(columnUniqueId, "columnUniqueId is null");
        this.isInvertedIndex = requireNonNull(isInvertedIndex, "isInvertedIndex is null");
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public Type getColumnType()
    {
        return columnType;
    }

    @JsonProperty
    public int getOrdinalPosition()
    {
        return ordinalPosition;
    }

    public ColumnMetadata getColumnMetadata()
    {
        return new ColumnMetadata(columnName, columnType, null, false);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, columnName);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        CarbondataColumnHandle other = (CarbondataColumnHandle) obj;
        return Objects.equals(this.connectorId, other.connectorId) &&
                Objects.equals(this.columnName, other.columnName);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("columnName", columnName)
                .add("columnType", columnType)
                .add("ordinalPosition", ordinalPosition)
                .toString();
    }
}
