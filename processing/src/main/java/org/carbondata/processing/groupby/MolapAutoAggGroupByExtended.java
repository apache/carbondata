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

package org.carbondata.processing.groupby;

public class MolapAutoAggGroupByExtended extends MolapAutoAggGroupBy {

    /**
     * MolapAutoAggGroupByExtended Constructor
     *
     * @param aggType
     * @param aggClassName
     * @param schemaName
     * @param cubeName
     * @param tableName
     * @param factDims
     */
    public MolapAutoAggGroupByExtended(String[] aggType, String[] aggClassName, String schemaName,
            String cubeName, String tableName, int[] factDims, String extension,
            int currentRestructNum) {
        super(aggType, aggClassName, schemaName, cubeName, tableName, factDims, extension,
                currentRestructNum);
    }

    /**
     * Below method will be used to add new row
     *
     * @param row
     */
    protected void addNewRow(Object[] row) {
        for (int i = 0; i < aggregators.length; i++) {
            if (null != row[i]) {
                this.isNotNullValue[i] = true;
                aggregators[i].agg(row[i]);
            }
        }
        prvKey = (byte[]) row[this.keyIndex];
        calculateMaxMinUnique();
    }

    /**
     * This method will be used to update the measure value based on aggregator
     * type
     *
     * @param row row
     */
    protected void updateMeasureValue(Object[] row) {
        for (int i = 0; i < aggregators.length; i++) {
            if (null != row[i]) {
                aggregators[i].agg(row[i]);
            }
        }
        calculateMaxMinUnique();
    }
}
