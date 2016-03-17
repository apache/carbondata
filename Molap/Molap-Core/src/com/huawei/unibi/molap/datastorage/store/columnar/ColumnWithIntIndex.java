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

package com.huawei.unibi.molap.datastorage.store.columnar;

import com.huawei.unibi.molap.util.ByteUtil.UnsafeComparer;

public class ColumnWithIntIndex implements Comparable<ColumnWithIntIndex>
{
    protected byte[] column;
    
    private int index;

    public ColumnWithIntIndex(byte[] column, int index)
    {
        this.column = column;
        this.index = index;
    }
    
    public ColumnWithIntIndex()
    {
    }    

    /**
     * @return the column
     */
    public byte[] getColumn()
    {
        return column;
    }

    /**
     * @return the index
     */
    public int getIndex()
    {
        return index;
    }
    
    //we are not comparing the values
    /*public boolean equals(ColumnWithIntIndex colIndex){
        return compareTo(colIndex)==0;
    }*/

    @Override
    public int compareTo(ColumnWithIntIndex o)
    {
        return UnsafeComparer.INSTANCE.compareTo(column, o.column);
    }

    /**
     * @param column the column to set
     */
    public void setColumn(byte[] column)
    {
        this.column = column;
    }

    /**
     * @param index the index to set
     */
    public void setIndex(int index)
    {
        this.index = index;
    }
    
}
