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

package com.huawei.unibi.molap.engine.holders;

import java.io.Serializable;
import java.util.List;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.scanner.impl.MolapKey;
import com.huawei.unibi.molap.engine.scanner.impl.MolapValue;
import com.huawei.unibi.molap.olap.SqlStatement.Type;

/**
* Class Description :This class will be used for holding the result after query execution.
* Version 1.0
*/
public class MolapResultHolder implements Serializable
{
    /**
     * 
     */
    private static final long serialVersionUID = -3092243950989231605L;

    /**
     * result
     */
    private Object[][] result = null;
    
    /**
     * pointer to result array
     */
    private int next = -1;
    
    /**
     * 
     */
    private boolean isLastValueNull;

    /**
     * SQL data types
     */
    private List<Type> dataTypes;
    
    private List<String> colHeaders;
    
    /**
     * totalRowCount
     */
    private int totalRowCount;
    
    private List<MolapKey> keys;
    
    private List<MolapValue> values;
    
    /**
     * 
     * This flag will be used to limit the results  to data readers from the result holder.  
     */
    private int rowLimit=-1;

    /**
     * 
     * @return Returns the rowLimit.
     * 
     */
    public int getRowLimit()
    {
        return rowLimit;
    }

    /**
     * 
     * @param rowLimit The rowLimit to set.
     * 
     */
    public void setRowLimit(int rowLimit)
    {
        this.rowLimit = rowLimit;
    }

    /**
     * 
     * MolapResultHolder Constructor
     * @param dataTypes
     *      SQL data types
     *
     */
    public MolapResultHolder(List<Type> dataTypes)
    {
        this.dataTypes = dataTypes;
    }

    /**
     * @param d
     */
    public void createData(MeasureAggregator... d)
    {
        result = new Object[d.length][1];
        for(int i = 0;i < d.length;i++)
        {
            result[i][0] = d[i].getValue();
        }
    }

    /**
     * Checks whether 
     * 
     * @return
     *
     */
    public boolean isNext()
    {
        next++;
        if(result== null || result.length==0 || next >= result[0].length)
        {
            return false;
        }
        
        if(rowLimit > 0 &&  next>rowLimit)
        {
            return false;
        }
        
        return true;
    }

    /**
     * Change the cursor position to -1
     *
     */
    public void reset()
    {
        next = -1;
    }

    /**
     * Set the result array 
     * @param data
     *
     */
    public void setObject(Object[][] data)
    {
        this.result = data;
    }

    /** This method returns the number of column
     * 
     */
    public int getColumnCount()
    {
        return result.length;
    }

    /**
     * Get the value for the column index
     * 
     * @param columnIndex
     *          column index
     * @return actual value
     *      
     *
     */
    public Object getObject(int columnIndex)
    {
        Object object = result[columnIndex - 1][next];
        // TODO Shall we consider SQL NULL value?
        isLastValueNull = (object == null || MolapCommonConstants.SQLNULLVALUE.equals(object));
        return object;
    }
    
    /**
     * Get the value for the column index
     * 
     * @param columnIndex
     *          column index
     * @return actual value
     *      
     *
     */
    public Object[][] getResult()
    {
        return result;
    }
    
    
    
    /**
     * Return the integer value for column
     * 
     * @param columnIndex
     *          column index
     * @return integer value
     *          
     *
     */
    public int getInt(int columnIndex)
    {
        Object object = getObject(columnIndex);
        if(object == null || MolapCommonConstants.SQLNULLVALUE.equals(object))
        {
            isLastValueNull=true;
            return 0;
        }
        String value = (String)object;
        int val=0;
        try
        {
             val = Integer.parseInt(value);
        }
        catch (NumberFormatException e)
        {
            isLastValueNull=true;
            return 0;
        }
        return val;
    }

    /**
     * Return the long value for column
     * 
     * @param column
     *          column index
     * @return long value
     *          
     *
     */
    public long getLong(int columnIndex)
    {
        Object object = getObject(columnIndex);
        if(object == null || MolapCommonConstants.SQLNULLVALUE.equals(object))
        {
            isLastValueNull = true;
            return 0;
        }
        String value = (String)object;
        long val = 0;
        try
        {
            val = Long.parseLong(value);
        }
        catch(NumberFormatException e)
        {
            isLastValueNull = true;
            return 0;
        }
        return val;
    }

    /**
     * Return the double value for column
     * 
     * @param column
     *          column index
     * @return double value
     *          
     *
     */
    public double getDouble(int columnIndex)
    {
        Object object = getObject(columnIndex);
        if(object == null || MolapCommonConstants.SQLNULLVALUE.equals(object))
        {
            isLastValueNull = true;
            return 0;
        }
        String value = (String)object;
        double val = 0;
        try
        {
            val = Double.parseDouble(value);
        }
        catch(NumberFormatException e)
        {
            isLastValueNull = true;
            return 0;
        }
        return val;
    }
    
    /**
     * Return the double value for column
     * 
     * @param column
     *          column index
     * @return double value
     *          
     *
     */
    public double getDoubleValue(int columnIndex)
    {
        Object object = getObject(columnIndex);
        if(object == null || MolapCommonConstants.SQLNULLVALUE.equals(object))
        {
            isLastValueNull = true;
            return 0;
        }
        double val = (Double)object;
        return val;
    }
    
    /**
     * Return the double value for column
     * 
     * @param column
     *          column index
     * @return double value
     *          
     *
     */
    public int getIntValue(int columnIndex)
    {
        Object object = getObject(columnIndex);
        if(object == null || MolapCommonConstants.SQLNULLVALUE.equals(object))
        {
            isLastValueNull = true;
            return 0;
        }
        int val = (Integer)object;
        return val;
    }
    
    /**
     * Return the double value for column
     * 
     * @param column
     *          column index
     * @return double value
     *          
     *
     */
    public long getLongValue(int columnIndex)
    {
        Object object = getObject(columnIndex);
        if(object == null || MolapCommonConstants.SQLNULLVALUE.equals(object))
        {
            isLastValueNull = true;
            return 0;
        }
        long val = (Long)object;
        return val;
    }

    /**
     * Check whether last value is null or not
     * 
     * @return last value was null or not
     *
     */
    public boolean wasNull()
    {
        return isLastValueNull;
    }

    /**
     * Return the sql data type for column
     * 
     * @param column
     *          column index
     * @return data type
     *
     */
    public Type getDataType(int column)
    {
        return dataTypes.get(column);
    }

    /**
     * Set the sql data type 
     * 
     * @param types
     *          sql data type
     *
     */
    public void setDataTypes(List<Type> types)
    {
        this.dataTypes = types;
    }

    /**
     * Return the Sql data type
     * 
     * @return Sql data type
     *
     */
    public List<Type> getDataTypes()
    {
        return dataTypes;
    }

    /**
     * @return the colHeaders
     */
    public List<String> getColHeaders()
    {
        return colHeaders;
    }

    /**
     * @param colHeaders the colHeaders to set
     */
    public void setColHeaders(List<String> colHeaders)
    {
        this.colHeaders = colHeaders;
    }

    /**
     * @return the totalRowCount
     */
    public int getTotalRowCount()
    {
        return totalRowCount;
    }

    /**
     * @param totalRowCount the totalRowCount to set
     */
    public void setTotalRowCount(int totalRowCount)
    {
        this.totalRowCount = totalRowCount;
    }

    /**
     * @return the keys
     */
    public List<MolapKey> getKeys()
    {
        return keys;
    }

    /**
     * @param keys the keys to set
     */
    public void setKeys(List<MolapKey> keys)
    {
        this.keys = keys;
    }

    /**
     * @return the values
     */
    public List<MolapValue> getValues()
    {
        return values;
    }

    /**
     * @param values the values to set
     */
    public void setValues(List<MolapValue> values)
    {
        this.values = values;
    }
    
    @Override
    public String toString()
    {
        StringBuffer buffer = new StringBuffer();
        for(int i = 0;i < result.length;i++)
        {
           StringBuffer b = new StringBuffer();
           for(int j = 0;j < result[i].length;j++)
           {
               b.append(result[i][j]).append(',');
           }
           buffer.append(b.toString()).append("\n");
        }
        return buffer.toString();
    }
}
