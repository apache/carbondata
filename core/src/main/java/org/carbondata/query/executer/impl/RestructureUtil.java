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

package org.carbondata.query.executer.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.carbondata.core.metadata.CarbonMetadata.Dimension;
import org.carbondata.core.metadata.CarbonMetadata.Measure;
import org.carbondata.core.metadata.SliceMetaData;
import org.carbondata.query.aggregator.dimension.DimensionAggregatorInfo;

public final class RestructureUtil {
    private RestructureUtil() {

    }

    /**
     * Below method will be used to check if any new dimension is added after
     * restructure or not
     *
     * @param queryDims
     * @param sliceMataData
     * @param sMetaDims
     * @param currentDimList
     * @param holder
     * @return
     */
    public static Dimension[] updateRestructureHolder(Dimension[] queryDims,
            SliceMetaData sliceMataData, List<Dimension> currentDimList, RestructureHolder holder,
            QueryExecuterProperties executerProperties) {
        boolean found/* = false*/;
        int len = 0;
        String[] sMetaDims = sliceMataData.getDimensions();
        //if high
        List<Boolean> isNoDictionaryDims=new ArrayList<Boolean>(executerProperties.dimTables.length);
        boolean[] isNoDictionaryDimsArray=new boolean[executerProperties.dimTables.length];
        List<Dimension> crntDims = new ArrayList<Dimension>();
        for (int i = 0; i < executerProperties.dimTables.length; i++) {
            found = false;
            for (int j = 0; j < sMetaDims.length; j++) {
                if (sMetaDims[j].equals(executerProperties.dimTables[i].getActualTableName() + '_'
                        + executerProperties.dimTables[i].getColName())) {
                    //CHECKSTYLE:OFF    Approval No:Approval-V1R2C10_001,Approval-V1R2C10_002
                    //                    currentDimTables[len] = executerProperties.dimTables[i];
                    crntDims.add(executerProperties.dimTables[i]);
                    len++;
                    found = true;
                    break;
                }//CHECKSTYLE:ON
            }

            if (!found) {
                holder.updateRequired = true;
                if(executerProperties.dimTables[i].isNoDictionaryDim())
                {
                	isNoDictionaryDims.add(true);
                }
            }

        }
        len = 0;
        for (int i = 0; i < queryDims.length; i++) {
            if (queryDims[i].isNoDictionaryDim()) {
                currentDimList.add(queryDims[i]);
                continue;
            }
            for (int j = 0; j < sMetaDims.length; j++) {
                if (sMetaDims[j].equals(queryDims[i].getActualTableName() + '_' + queryDims[i]
                        .getColName())) {
                    currentDimList.add(queryDims[i]);
                    break;

                }
            }
            len++;
        }
        if(holder.updateRequired)
        {
        	isNoDictionaryDimsArray=ArrayUtils.toPrimitive(isNoDictionaryDims.toArray(new Boolean[isNoDictionaryDims.size()]));
        	holder.setIsNoDictionaryNewDims(isNoDictionaryDimsArray);
        }
        return crntDims.toArray(new Dimension[crntDims.size()]);
    }

    public static void updateMeasureInfo(SliceMetaData sliceMataData, Measure[] measures,
            int[] measureOrdinal, boolean[] msrExists, Object[] newMsrsDftVal) {
        String[] sMetaMsrs = sliceMataData.getMeasures();
        updateMsr(sliceMataData, measures, measureOrdinal, msrExists, newMsrsDftVal, sMetaMsrs);
    }

    /**
     * @param sliceMataData
     * @param measures
     * @param measureOrdinal
     * @param msrExists
     * @param newMsrsDftVal
     * @param sMetaMsrs
     */
    public static void updateMsr(SliceMetaData sliceMataData, Measure[] measures,
            int[] measureOrdinal, boolean[] msrExists, Object[] newMsrsDftVal, String[] sMetaMsrs) {
        for (int i = 0; i < measures.length; i++) {
            for (int j = 0; j < sMetaMsrs.length; j++) {
                if (measures[i].getColName() != null && measures[i].getColName()
                        .equals(sMetaMsrs[j])) {
                    measureOrdinal[i] = measures[i].getOrdinal();
                    msrExists[i] = true;
                    break;
                }
            }
            if (!msrExists[i]) {
                String[] newMeasures = sliceMataData.getNewMeasures();
                double[] newMsrDfts = sliceMataData.getNewMsrDfts();
                if (newMeasures != null) {
                    for (int j = 0; j < newMeasures.length; j++) {
                        if (measures[i].getColName() != null && measures[i].getColName()
                                .equals(newMeasures[j])) {
                            //CHECKSTYLE:OFF    Approval No:Approval-V1R2C10_003,Approval-V1R2C10_004
                            newMsrsDftVal[i] = newMsrDfts[j];
                            break;
                            //CHECKSTYLE:ON
                        }
                    }
                }
            }
        }
    }

    public static void updateDimensionAggInfo(
            List<DimensionAggregatorInfo> dimensionAggregationInfoList, String[] dimension) {
        Iterator<DimensionAggregatorInfo> iterator = dimensionAggregationInfoList.iterator();
        while (iterator.hasNext()) {
            DimensionAggregatorInfo next = iterator.next();
            Dimension dim = next.getDim();
            boolean isPresent = false;
            for (int i = 0; i < dimension.length; i++) {
                if (dimension[i].equals(dim.getActualTableName() + '_' + dim.getColName())) {
                    isPresent = true;
                }
            }
            if (!isPresent) {
                next.setDimensionPresentInCurrentSlice(false);
            }
        }
    }
}
