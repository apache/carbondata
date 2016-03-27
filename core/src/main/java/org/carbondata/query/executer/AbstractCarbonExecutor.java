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

package org.carbondata.query.executer;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.keygenerator.columnar.ColumnarSplitter;
import org.carbondata.core.keygenerator.factory.KeyGeneratorFactory;
import org.carbondata.core.metadata.CarbonMetadata.Dimension;
import org.carbondata.query.queryinterface.filter.CarbonFilterInfo;

/**
 * This class provides the common implementation required for Query Execution.
 */
public abstract class AbstractCarbonExecutor implements CarbonExecutor {

    /**
     * To determine what is the maximum available dimension value. THe return
     * value will be used to identify the max key.
     *
     * @param dim
     * @return returns the max value for dimension
     * @throws IOException
     */
    protected abstract Long getMaxValue(Dimension dim) throws IOException;

    /**
     * Returns Sorted surrogate ids for the dimension members for the given
     * column <code>colName</code>
     *
     * @param dimMem
     * @param dimName
     * @return the surrogate key for Dimension members.
     * @throws IOException
     */
    public abstract long[] getSurrogates(List<String> dimMem, Dimension dimName) throws IOException;

    /**
     * Set the start key considering the include and exclude Filters.
     * It will be used by the scanner for getting the start offset for the
     * scanner.
     *
     * @param key
     * @param incldPredKeys
     * @param excldPredKeys
     * @throws IOException
     */
    private void setStartKey(long[] key, long[][] incldPredKeys, long[][] incldOrPredKeys,
            long[][] excldPredKeys) throws IOException {
        for (int i = 0; i < key.length; i++) {
            long[] incldConstraints = incldPredKeys[i];
            long[] incldOrConstraints = incldOrPredKeys != null ? incldOrPredKeys[i] : null;
            long[] excldConstraints = excldPredKeys[i];
            boolean incld = incldConstraints != null && incldConstraints.length > 0;
            boolean incldOr = incldOrConstraints != null && incldOrConstraints.length > 0;
            if (incld || incldOr) {
                if (incld) {
                    key[i] = incldConstraints[0];
                }
                if (incldOr) {
                    key[i] = incldOrConstraints[0] < key[i] ? incldOrConstraints[0] : key[i];
                }
            } else if (excldConstraints != null && excldConstraints.length > 0) {
                for (long filter : excldConstraints) {
                    if (key[i] == filter) {
                        // if the column val is same as filter then increase the
                        // column val till the value
                        // is not equal to any of the filter keys
                        key[i]++;

                    } else if (key[i] < filter) {
                        break;
                    }
                }
            }
        }
    }

    /**
     * Set the start and end Key.
     *
     * @see CarbonExecutor#setStartAndEndKeys(long[], long[], long[][], long[][], com.huawei.unibi.carbon.metadata.CarbonMetadata.Dimension[])
     */
    public void setStartAndEndKeys(long[] startKey, long[] endKey, long[][] incldPredKeys,
            long[][] incldOrPredKeys, long[][] excldPredKeys, Dimension[] tables)
            throws IOException {
        setStartKey(startKey, incldPredKeys, incldOrPredKeys, excldPredKeys);

        setEndKey(endKey, incldPredKeys, incldOrPredKeys, excldPredKeys, tables);
    }

    /**
     * Set the end key considering the include and exclude Filters.
     * It will be used by the scanner for getting the end offset for the
     * scanner.
     *
     * @param key
     * @param incldPredKeys
     * @param excldPredKeys
     * @param tables
     * @throws IOException
     */
    private void setEndKey(long[] key, long[][] incldPredKeys, long[][] incldOrPredKeys,
            long[][] excldPredKeys, Dimension[] tables) throws IOException {
        boolean incldOrGlobal = false;
        for (int i = 0; i < key.length; i++) {
            key[i] = 0L;
            long[] incldConstraints = incldPredKeys[i];
            long[] incldOrConstraints = incldOrPredKeys != null ? incldOrPredKeys[i] : null;
            long[] excldConstraints = excldPredKeys[i];
            boolean incld = incldConstraints != null && incldConstraints.length > 0;
            boolean incldOr = incldOrConstraints != null && incldOrConstraints.length > 0;
            if (incld || incldOr) {
                if (incld) {
                    key[i] = incldConstraints[incldConstraints.length - 1];
                }
                if (incldOr) {
                    incldOrGlobal = true;
                    key[i] = incldOrConstraints[incldOrConstraints.length - 1] > key[i] ?
                            incldOrConstraints[incldOrConstraints.length - 1] :
                            key[i];
                }
                if (!incldOr && incldOrGlobal) {
                    key[i] = getMaxValue(tables[i]);
                }
            } else {
                key[i] = getMaxValue(tables[i]);
                if (excldConstraints != null && excldConstraints.length > 0) {
                    for (int filterIndex = excldConstraints.length - 1;
                         filterIndex >= 0; filterIndex--) {
                        if (key[i] > excldConstraints[filterIndex]) {
                            break;
                        }
                    }
                }
            }
        }
        key[key.length - 1]++;
    }

    /**
     * Returns the MDKey Generator
     *
     * @param dims
     * @return KeyGenerator
     */
    protected KeyGenerator getKeyGenerator(Dimension[] dims) {
        return getKeyGenerator(dims, true);
    }

    protected KeyGenerator getKeyGenerator(Dimension[] dims, boolean considerNormalized) {

        //        int[] lens = null;
        //        if(considerNormalized)
        //        {
        //            int lensLen = 0;
        //            // Find how many dimensions are not normalized
        //            for(int i = 0;i < dims.length;i++)
        //            {
        //                if(!dims[i].isNormalized())
        //                {
        //                    lensLen++;
        //                }
        //            }
        //
        //            // We only have to add dimensions which are not normalized
        //            lens = new int[lensLen];
        //            int j = 0;
        //            for(int i = 0; i < dims.length; i++)
        //            {
        //                if(!dims[i].isNormalized())
        //                {
        //                    lens[j] = dims[i].getNoOfbits();
        //                    j++;
        //                }
        //            }
        //        }
        //        else
        //        {
        //            lens = new int[dims.length];
        //            for(int i = 0; i < dims.length; i++)
        //            {
        //                lens[i] = dims[i].getNoOfbits();
        //            }
        //        }
        //return new MultiDimKeyVarLengthGenerator(lens);
        return KeyGeneratorFactory.getKeyGenerator(new int[] { 1, 2, 3 });
    }

    protected ColumnarSplitter getColumnarSplitter(Dimension[] dims, boolean considerNormalized) {

        //        int[] lens = null;
        //        if(considerNormalized)
        //        {
        //            int lensLen = 0;
        //            // Find how many dimensions are not normalized
        //            for(int i = 0;i < dims.length;i++)
        //            {
        //                if(!dims[i].isNormalized())
        //                {
        //                    lensLen++;
        //                }
        //            }
        //
        //            // We only have to add dimensions which are not normalized
        //            lens = new int[lensLen];
        //            int j = 0;
        //            for(int i = 0; i < dims.length; i++)
        //            {
        //                if(!dims[i].isNormalized())
        //                {
        //                    lens[j] = dims[i].getNoOfbits();
        //                    j++;
        //                }
        //            }
        //        }
        //        else
        //        {
        //            lens = new int[dims.length];
        //            for(int i = 0; i < dims.length; i++)
        //            {
        //                lens[i] = dims[i].getNoOfbits();
        //            }
        //        }
        //        byte dimSet=Byte.parseByte(CarbonProperties.getInstance().getProperty(
        //                CarbonCommonConstants.DIMENSION_SPLIT_VALUE_IN_COLUMNAR,
        //                CarbonCommonConstants.DIMENSION_SPLIT_VALUE_IN_COLUMNAR_DEFAULTVALUE));
        //        return new MultiDimKeyVarLengthEquiSplitGenerator(CarbonUtil.getIncrementedCardinalityFullyFilled(lens),(byte)1);
        return null;
    }

    /**
     * Check whether constrains exists in All dimensions. and this will be
     * used for checking whether query coming for execution is point query.
     *
     * @param constraints
     * @return false if constrains not exist on any dimension , true otherwise.
     */
    protected boolean constraintsExistsOnAllDimensions(
            Map<Dimension, CarbonFilterInfo> constraints) {
        for (Entry<Dimension, CarbonFilterInfo> dimensionConstraint : constraints.entrySet()) {
            CarbonFilterInfo info = dimensionConstraint.getValue();
            if (info.getEffectiveIncludedMembers().size() == 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Recursively adding the surrogate key for all dimension coming for point queries in the keyList based on
     * Filters selected on all dimensions.
     *
     * @param predKeys
     * @param count
     * @param builder
     * @param result
     */
    protected void addPointKeyInList(long[][] predKeys, int count, long[] builder,
            List<long[]> result) {
        //
        count++;
        if (count == predKeys.length - 1) {
            //
            long[] list = predKeys[count];
            for (int i = 0; i < list.length; i++) {
                builder[count] = predKeys[count][i];
                result.add(builder.clone());
            }
            return;
        }
        long[] list = predKeys[count];
        for (int i = 0; i < list.length; i++) {
            //
            builder[count] = list[i];
            addPointKeyInList(predKeys, count, builder, result);
        }
    }

}
