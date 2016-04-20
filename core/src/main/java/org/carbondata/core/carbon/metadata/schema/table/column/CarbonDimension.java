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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.carbondata.core.carbon.metadata.datatype.ConvertedType;
import org.carbondata.core.carbon.metadata.datatype.DataType;
import org.carbondata.core.carbon.metadata.encoder.Encoding;

/**
 * class to represent column(dimension) in table
 */
public class CarbonDimension implements Serializable {

    /**
     * serialization version
     */
    private static final long serialVersionUID = 3648269871656322681L;

    /**
     * column schema
     */
    protected ColumnSchema columnSchema;
    /**
     * queryOrder
     */
    protected int queryOrder;

    /**
     * isQueryForDistinctCount
     */
    protected boolean isDistinctQuery;

    /**
     * table ordinal
     */
    protected int ordinal;
    
    /**
     * List of child dimension for complex type
     */
    private List<CarbonDimension> listOfChildDimensions;
    
    /**
     * default value for in case of restructuring will be used 
     * when older segment does not have particular column
     */
    protected byte[] defaultValue;

    public CarbonDimension(ColumnSchema columnSchema, int ordinal) {
        this.columnSchema = columnSchema;
        this.ordinal = ordinal;
    }

    /**
     * this method will initialize list based on number of child dimensions Count
     */
    public void initializeChildDimensionsList(int childDimensions)
    {
        listOfChildDimensions = new ArrayList<CarbonDimension>(childDimensions);
    }
    
    /**
     * @return convertedType
     */
    public ConvertedType getConvertedType()
    {
    	return columnSchema.getConvertedType();
    }
    
    /**
     * @return number of children for complex type
     */
    public int getNumberOfChild()
    {
    	return columnSchema.getNumberOfChild();
    }
    
    /**
     * @return columnar or row based
     */
    public boolean isColumnar()
    {
    	return columnSchema.isColumnar();
    }

    /**
     * @return list of children dims for complex type
     */
    public List<CarbonDimension> getListOfChildDimensions() {
      return listOfChildDimensions;
    }
    
    /**
     * @param listOfChildDimensions
     */
    public void setListOfChildDimensions(List<CarbonDimension> listOfChildDimensions) {
      this.listOfChildDimensions = listOfChildDimensions;
    }
    /**
     * @return column unique id
     */
    public String getColumnId() {
        return columnSchema.getColumnUniqueId();
    }

    /**
     * @return the dataType
     */
    public DataType getDataType() {
        return columnSchema.getDataType();
    }

    /**
     * @return the colName
     */
    public String getColName() {
        return columnSchema.getColumnName();
    }

    /**
     * @return the queryOrder
     */
    public int getQueryOrder() {
        return queryOrder;
    }

    /**
     * @param queryOrder the queryOrder to set
     */
    public void setQueryOrder(int queryOrder) {
        this.queryOrder = queryOrder;
    }

    /**
     * @return the isDistinctQuery
     */
    public boolean isDistinctQuery() {
        return isDistinctQuery;
    }

    /**
     * @param isDistinctQuery the isDistinctQuery to set
     */
    public void setDistinctQuery(boolean isDistinctQuery) {
        this.isDistinctQuery = isDistinctQuery;
    }

    /**
     * @return the ordinal
     */
    public int getOrdinal() {
        return ordinal;
    }

    /**
     * @param ordinal the ordinal to set
     */
    public void setOrdinal(int ordinal) {
        this.ordinal = ordinal;
    }

    /**
     * @return the list of encoder used in dimension
     */
    public Set<Encoding> getEncoder() {
        return columnSchema.getEncodingList();
    }

    /**
     * @return column group id if it is row based
     */
    public int columnGroupId() {
        return columnSchema.getColumnGroupId();
    }

    /**
     * @return return the number of child present in case of complex type
     */
    public int numberOfChild() {
        return columnSchema.getNumberOfChild();
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
     * to generate the hash code for this class
     */
    @Override public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((columnSchema == null) ? 0 : columnSchema.hashCode());
        return result;
    }

    /**
     * to check whether to dimension are equal or not
     */
    @Override public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof CarbonDimension)) {
            return false;
        }
        CarbonDimension other = (CarbonDimension) obj;
        if (columnSchema == null) {
            if (other.columnSchema != null) {
                return false;
            }
        } else if (!columnSchema.equals(other.columnSchema)) {
            return false;
        }
        return true;
    }
    
    public boolean hasEncoding(Encoding encoding){
    	return columnSchema.getEncodingList().contains(encoding);
    }
}
