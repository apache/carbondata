/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.carbondata.core.carbon.metadata.schema.table.column;

import java.io.Serializable;
import java.util.Set;

import org.carbondata.core.carbon.metadata.datatype.ConvertedType;
import org.carbondata.core.carbon.metadata.datatype.DataType;
import org.carbondata.core.carbon.metadata.encoder.Encoding;

/**
 * Store the information about the column meta data present the table
 */
public class ColumnSchema implements Serializable {

    /**
     * serialization version
     */
    private static final long serialVersionUID = 7676766554874863763L;

    /**
     * dataType
     */
    private DataType dataType;
    /**
     * Name of the column. If it is a complex data type, we follow a naming rule
     * grand_parent_column.parent_column.child_column
     * For Array types, two columns will be stored one for
     * the array type and one for the primitive type with
     * the name parent_column.value
     */
    private String columnName;

    /**
     * Unique ID for a column. if this is dimension,
     * it is an unique ID that used in dictionary
     */
    private String columnUniqueId;

    /**
     * whether it is stored as columnar format or row format
     */
    private boolean isColumnar = true;

    /**
     * List of encoding that are chained to encode the data for this column
     */
    private Set<Encoding> encodingList;

    /**
     * Whether the column is a dimension or measure
     */
    private boolean isDimensionColumn;

    /**
     * The group ID for column used for row format columns,
     * where in columns in each group are chunked together.
     */
    private int columnGroupId = -1;

    /**
     * Optional When the schema is the result of a conversion from another model
     * Used to record the original type to help with cross conversion.
     */
    private ConvertedType convertedType;

    /**
     * Used when this column contains decimal data.
     */
    private int scale;

    private int precision;

    /**
     * Nested fields.  Since thrift does not support nested fields,
     * the nesting is flattened to a single list by a depth-first traversal.
     * The children count is used to construct the nested relationship.
     * This field is not set when the element is a primitive type
     */
    private int numberOfChild;

    /**
     * Used when this column is part of an aggregate function.
     */
    private String aggregateFunction;

    /**
     * used in case of schema restructuring
     */
    private byte[] defaultValue;

    /**
     * @return the columnName
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     * @param columnName the columnName to set
     */
    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    /**
     * @return the columnUniqueId
     */
    public String getColumnUniqueId() {
        return columnUniqueId;
    }

    /**
     * @param columnUniqueId the columnUniqueId to set
     */
    public void setColumnUniqueId(String columnUniqueId) {
        this.columnUniqueId = columnUniqueId;
    }

    /**
     * @return the isColumnar
     */
    public boolean isColumnar() {
        return isColumnar;
    }

    /**
     * @param isColumnar the isColumnar to set
     */
    public void setColumnar(boolean isColumnar) {
        this.isColumnar = isColumnar;
    }

    /**
     * @return the isDimensionColumn
     */
    public boolean isDimensionColumn() {
        return isDimensionColumn;
    }

    /**
     * @param isDimensionColumn the isDimensionColumn to set
     */
    public void setDimensionColumn(boolean isDimensionColumn) {
        this.isDimensionColumn = isDimensionColumn;
    }

    /**
     * @return the columnGroup
     */
    public int getColumnGroupId() {
        return columnGroupId;
    }

    /**
     * @param columnGroup the columnGroup to set
     */
    public void setColumnGroup(int columnGroupId) {
        this.columnGroupId = columnGroupId;
    }

    /**
     * @return the convertedType
     */
    public ConvertedType getConvertedType() {
        return convertedType;
    }

    /**
     * @param convertedType the convertedType to set
     */
    public void setConvertedType(ConvertedType convertedType) {
        this.convertedType = convertedType;
    }

    /**
     * @return the scale
     */
    public int getScale() {
        return scale;
    }

    /**
     * @param scale the scale to set
     */
    public void setScale(int scale) {
        this.scale = scale;
    }

    /**
     * @return the precision
     */
    public int getPrecision() {
        return precision;
    }

    /**
     * @param precision the precision to set
     */
    public void setPrecision(int precision) {
        this.precision = precision;
    }

    /**
     * @return the numberOfChild
     */
    public int getNumberOfChild() {
        return numberOfChild;
    }

    /**
     * @param numberOfChild the numberOfChild to set
     */
    public void setNumberOfChild(int numberOfChild) {
        this.numberOfChild = numberOfChild;
    }

    /**
     * @return the aggregator
     */
    public String getAggregateFunction() {
        return aggregateFunction;
    }

    /**
     * @param aggregator the aggregator to set
     */
    public void setAggregateFunction(String aggregateFunction) {
        this.aggregateFunction = aggregateFunction;
    }

    /**
     * @return the defaultValue
     */
    public byte[] getDefaultValue() {
        return defaultValue;
    }

    /**
     * @param defaultValue the defaultValue to set
     */
    public void setDefaultValue(byte[] defaultValue) {
        this.defaultValue = defaultValue;
    }

    /**
     * hash code method to check get the hashcode based.
     * for generating the hash code only column name and column unique id will considered
     */
    @Override public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((columnName == null) ? 0 : columnName.hashCode());
        return result;
    }

    /**
     * Overridden equals method for columnSchema
     */
    @Override public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof ColumnSchema)) {
            return false;
        }
        ColumnSchema other = (ColumnSchema) obj;
        if (columnName == null) {
            if (other.columnName != null) {
                return false;
            }
        } else if (!columnName.equals(other.columnName)) {
            return false;
        }
        return true;
    }

    /**
     * @return the dataType
     */
    public DataType getDataType() {
        return dataType;
    }

    /**
     * @param dataType the dataType to set
     */
    public void setDataType(DataType dataType) {
        this.dataType = dataType;
    }

	/**
	 * @return the encoderList
	 */
	public Set<Encoding> getEncodingList() {
		return encodingList;
	}

	/**
	 * @param encoderList the encoderList to set
	 */
	public void setEncodintList(Set<Encoding> encodingList) {
		this.encodingList = encodingList;
	}
}
