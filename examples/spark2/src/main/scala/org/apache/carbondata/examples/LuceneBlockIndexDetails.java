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

package org.apache.carbondata.examples;

import java.io.Serializable;

public class LuceneBlockIndexDetails implements Serializable {
    private static final long serialVersionUID = 1206104914911491724L;

    /**
     * Min value of a column of one blocklet Bit-Packed
     */
    private byte[] columnVal;


    /**
     * value of the blockletId
     */
    private String BlockletId;


    public String getBlockletId() {
        return BlockletId;
    }

    public void setBlockletId(String blockletId) {
        BlockletId = blockletId;
    }

    public byte[] getColumnVal() {
        return columnVal;
    }

    public void setColumnVal(byte[] columnVal) {
        this.columnVal = columnVal;
    }
}
