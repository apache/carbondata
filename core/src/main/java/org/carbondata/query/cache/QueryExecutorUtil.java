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

package org.carbondata.query.cache;

import java.util.*;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.SqlStatement;
import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.metadata.CarbonMetadata.Dimension;
import org.carbondata.core.vo.HybridStoreModel;
import org.carbondata.query.datastorage.InMemoryTable;
import org.carbondata.query.util.CarbonEngineLogEvent;

/**
 * Util class
 */
public final class QueryExecutorUtil {

    /**
     * LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(QueryExecutorUtil.class.getName());

    private QueryExecutorUtil() {

    }

    /**
     * To get the max key based on dimensions. i.e. all other dimensions will be
     * set to 0 bits and the required query dimension will be masked with all
     * 1's so that we can mask key and then compare while aggregating
     *
     * @param queryDimensions
     * @return
     * @throws KeyGenException
     */
    public static byte[] getMaxKeyBasedOnDimensions(Dimension[] queryDimensions,
            KeyGenerator generator, Dimension[] dimTables) throws KeyGenException {
        long[] max = new long[dimTables.length];
        Arrays.fill(max, 0L);
        for (int i = 0; i < queryDimensions.length; i++) {
            if (queryDimensions[i].isNoDictionaryDim()) {
                continue;
            }
            max[queryDimensions[i].getOrdinal()] = Long.MAX_VALUE;
        }
        return generator.generateKey(max);
    }

    /**
     * getDimension
     *
     * @param dimGet
     * @param dims
     * @return
     */
    public static Dimension getDimension(Dimension dimGet, Dimension[] dims) {
        for (Dimension dimension : dims) {
            if (dimGet.getDimName().equals(dimension.getDimName()) && dimGet.getHierName()
                    .equals(dimension.getHierName()) && dimGet.getName()
                    .equals(dimension.getName())) {
                return dimension;
            }
        }
        return dimGet;
    }

    /**
     * It checks whether the passed dimension is there in list or not.
     *
     * @param dimension
     * @param dimList
     * @return
     */
    public static boolean contain(Dimension dimension, List<Dimension> dimList) {
        for (Dimension dim : dimList) {
            if (dim.getHierName().equals(dimension.getHierName()) && dim.getDimName()
                    .equals(dimension.getDimName()) && dim.getName().equals(dimension.getName())) {
                return true;
            }
        }
        return false;
    }

    /**
     * It checks whether the passed dimension is there in list or not.
     *
     * @param dimension
     * @param dimList
     * @return
     */
    public static boolean contain(Dimension dimension, Dimension[] dimList) {
        for (Dimension dim : dimList) {
            if (dim.getHierName().equals(dimension.getHierName()) && dim.getDimName()
                    .equals(dimension.getDimName()) && dim.getName().equals(dimension.getName())) {
                return true;
            }
        }
        return false;
    }

    /**
     * @param generator
     * @param maskedKeyRanges
     * @param ranges
     * @param byteIndexs
     * @param key
     * @return
     * @throws KeyGenException
     */
    private static byte[] getMaskedKey(KeyGenerator generator, int[] maskedKeyRanges,
            List<Integer> ranges, int[] byteIndexs, long[] key) throws KeyGenException {
        byte[] mdKey = generator.generateKey(key);

        byte[] maskedKey = new byte[byteIndexs.length];

        for (int i = 0; i < byteIndexs.length; i++) {
            maskedKey[i] = mdKey[byteIndexs[i]];
        }
        for (int i = 0; i < byteIndexs.length; i++) {
            for (int k = 0; k < maskedKeyRanges.length; k++) {
                if (byteIndexs[i] == maskedKeyRanges[k]) {
                    ranges.add(k);
                    break;
                }
            }
        }

        return maskedKey;
    }

    /**
     * Converts int list to int[]
     *
     * @param integers
     * @return
     */
    public static int[] convertIntegerListToIntArray(Collection<Integer> integers) {
        int[] ret = new int[integers.size()];
        Iterator<Integer> iterator = integers.iterator();
        for (int i = 0; i < ret.length; i++) {//CHECKSTYLE:OFF    Approval No:Approval-284
            ret[i] = iterator.next().intValue();
        }//CHECKSTYLE:ON
        return ret;
    }

    /**
     * getMaskedByte
     *
     * @param queryDimensions
     * @param generator
     * @return
     */
    public static int[] getMaskedByte(Dimension[] queryDimensions, KeyGenerator generator,
            HybridStoreModel hm) {

        Set<Integer> integers = new TreeSet<Integer>();
        boolean isRowAdded = false;

        for (int i = 0; i < queryDimensions.length; i++) {

            if (queryDimensions[i].isNoDictionaryDim()) {
                continue;
            } else if (queryDimensions[i].getDataType() == SqlStatement.Type.ARRAY) {
                continue;
            } else if (queryDimensions[i].getDataType() == SqlStatement.Type.STRUCT) continue;
            else if (queryDimensions[i].getParentName() != null) continue;
            //if querydimension is row store based, than add all row store ordinal in mask, because
            //row store ordinal rangesare overalapped
            //for e.g its possible
            //dimension1 range: 0-1
            //dimension2 range: 1-2
            //hence to read only dimension2, you have to mask dimension1 also
            else if (!queryDimensions[i].isColumnar()) {
                //if all row store ordinal is already added in range than no need to consider
                // it again
                if (!isRowAdded) {
                    isRowAdded = true;
                    int[] rowOrdinals = hm.getRowStoreOrdinals();
                    for (int r = 0; r < rowOrdinals.length; r++) {
                        int[] range =
                                generator.getKeyByteOffsets(hm.getMdKeyOrdinal(rowOrdinals[r]));
                        for (int j = range[0]; j <= range[1]; j++) {
                            integers.add(j);
                        }

                    }
                }
                continue;

            } else {
                int[] range = generator
                        .getKeyByteOffsets(hm.getMdKeyOrdinal(queryDimensions[i].getOrdinal()));
                for (int j = range[0]; j <= range[1]; j++) {
                    integers.add(j);
                }
            }

        }
        //
        int[] byteIndexs = new int[integers.size()];
        int j = 0;
        for (Iterator<Integer> iterator = integers.iterator(); iterator.hasNext(); ) {
            Integer integer = (Integer) iterator.next();
            byteIndexs[j++] = integer.intValue();
        }

        return byteIndexs;
    }

    /**
     * updateMaskedKeyRanges
     *
     * @param maskedKey
     * @param maskedKeyRanges
     */
    public static void updateMaskedKeyRanges(int[] maskedKey, int[] maskedKeyRanges) {
        Arrays.fill(maskedKey, -1);
        for (int i = 0; i < maskedKeyRanges.length; i++) {
            maskedKey[maskedKeyRanges[i]] = i;
        }
    }

}
