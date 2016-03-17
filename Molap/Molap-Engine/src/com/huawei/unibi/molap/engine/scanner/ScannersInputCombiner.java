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

/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwdh/HjOjN0Brs7b7TRorj6S6iAIeaqK90lj7BAM
GSGxBtL5v/KKPfzM1vK43QeoKvH3oClqIe1OF+gF+9OUYOQq6IwFCEBySYALwsQ0+pjNOo9/
fRhqZMis03k9I+G96xYfafd9yJgKdVZlsUcsX+hMmJ/m4Z46jdHQSwwlFA2uvA==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 1997
 * =====================================
 *
 */
package com.huawei.unibi.molap.engine.scanner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressionModel;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.aggregator.util.AggUtil;
import com.huawei.unibi.molap.engine.datastorage.storeInterfaces.KeyValue;
import com.huawei.unibi.molap.engine.datastorage.streams.DataInputStream;
import com.huawei.unibi.molap.engine.schema.metadata.Pair;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;
import com.huawei.unibi.molap.metadata.LeafNodeInfo;
import com.huawei.unibi.molap.metadata.LeafNodeInfoColumnar;
import com.huawei.unibi.molap.util.ByteUtil;

/**
 * Merges the data from given list of scanners to combine to a single input
 * stream.
 * 
 * @author K00900207
 */
public class ScannersInputCombiner implements DataInputStream
{
    /**
     * 
     */
    private List<Scanner> scanners;

    /**
     * 
     */
    private List<RowTempFile> tempRows = new ArrayList<RowTempFile>(MolapCommonConstants.CONSTANT_SIZE_TEN);

    /**
     * 
     */
    private KeyGenerator keyGenerator;

    /**
     * 
     */
    private boolean hasFactCount;

    /**
     * If aggregation is required, data will be aggregated otherwise only unique
     * keys will be considered for result
     */
    private List<String> aggNames;

    /**
     * 
     */
//    private double[] maxData;

    /**
     * 
     */
//    private double[] minData;

    /**
     * 
     */
//    private int[] decimalLength;

    public ScannersInputCombiner(List<Scanner> scanners, KeyGenerator keyGenerator, List<String> aggNames,
            boolean hasFactCount)
    {
        this.aggNames = aggNames;
        this.scanners = scanners;
        this.keyGenerator = keyGenerator;
        this.hasFactCount = hasFactCount;
    }

    /**
     * 
     */
    private byte[] lastKey;

    /**
     * 
     */
    private MeasureAggregator[] lastAggs;

    @Override
    public Pair<byte[], double[]> getNextHierTuple()
    {
        Pair<byte[], double[]> data;

        double[] lastData = null;
        lastData = new double[aggNames.size()];

        while((data = getNextSortData()) != null)
        {
            byte[] key = data.getKey();
            double[] vals = data.getValue();

            if(lastKey == null)
            {
                lastKey = key;
                lastAggs = AggUtil.getAggregators(aggNames, hasFactCount, null);
            }

            else if(ByteUtil.compare(key, lastKey) != 0)
            {
                for(int k = 0;k < lastAggs.length;k++)
                {
                    lastData[k] = lastAggs[k].getValue();
//                    setDecimals(k, lastData[k]);
                }

                data.setKey(lastKey);
                data.setValue(lastData);

                lastKey = key;
                lastAggs = AggUtil.getAggregators(aggNames, hasFactCount, keyGenerator);

                // calculate max ,min ,decimal num of measure for value
                // compression
//                caliculateMaxMin(data);

                // Just aggregate with old data
                for(int j = 0;j < lastAggs.length;j++)
                {
                    lastAggs[j].agg(vals[j], vals[lastData.length - 1]);
                }
                return data;
            }

            // Just aggregate with old data
            for(int j = 0;j < lastAggs.length;j++)
            {
                lastAggs[j].agg(vals[j], vals[lastData.length - 1]);
            }
        }

        // Handle left out row
        if(lastKey != null)
        {
            for(int k = 0;k < lastAggs.length;k++)
            {
                lastData[k] = lastAggs[k].getValue();
//                setDecimals(k, lastData[k]);
            }
            data = new Pair(lastKey, lastData);
//            caliculateMaxMin(data);
            lastKey = null;
            lastData = null;
            return data;
        }

        return null;
    }

     
    /**
     * Project Name NSE V3R7C00 
     * Module Name : MOLAP
     * Author :C00900810
     * Created Date :25-Jun-2013
     * FileName : ScannersInputCombiner.java
     * Class Description : 
     * Version 1.0
     */
    private static class RowTempFile
    {
        /**
         * 
         */
        private Pair<byte[], double[]> row;

        /**
         * 
         */
        private int fileNumber;

        /**
         * @param row
         * @param fileNumber
         */
         RowTempFile(Pair row, int fileNumber)
        {
            this.row = row;
            this.fileNumber = fileNumber;
        }
    }

    /**
     * Combine all the scanners and find out
     */
    private Pair getNextSortData()
    {
        Pair retval;

        if(tempRows.size() == 0)
        {
            return null;
        }

        RowTempFile rowTempFile = tempRows.remove(0);
        retval = rowTempFile.row;
        int smallest = rowTempFile.fileNumber;

        // now get another Row for position smallest
        Scanner scanner = scanners.get(smallest);

        if(scanner.isDone())
        {
            scanners.remove(smallest);
            // Write some code to close the stream

            // Also update all file numbers in in data.tempRows if they
            // are larger than smallest.
            for(RowTempFile rtf : tempRows)
            {
                if(rtf.fileNumber > smallest)
                {
                    rtf.fileNumber--;
                }
            }
        }
        else
        {
            KeyValue row = scanner.getNext();
            double[] msrData = null;
            if(null != row.getMsrCols())
            {
                msrData = row.getOriginalValue();
            }
            Pair pair = new Pair(row.getOriginalKey(), msrData);
            RowTempFile extra = new RowTempFile(pair, smallest);

            int index = Collections.binarySearch(tempRows, extra, new KeyComparator());
            if(index < 0)
            {
                tempRows.add(index * (-1) - 1, extra);
            }
            else
            {
                tempRows.add(index, extra);
            }
        }

        return retval;
    }

    /**
     * Project Name NSE V3R7C00 
     * Module Name : MOLAP
     * Author :C00900810
     * Created Date :25-Jun-2013
     * FileName : ScannersInputCombiner.java
     * Class Description : 
     * Version 1.0
     */
    private class KeyComparator implements Comparator<RowTempFile>
    {
        @Override
        public int compare(RowTempFile o1, RowTempFile o2)
        {
            int i = 0;
            int j = 0;
            for(;i < keyGenerator.getKeySizeInBytes();i++, j++)
            {
                int a = (o1.row.getKey()[i] & 0xff);
                int b = (o2.row.getKey()[i] & 0xff);
                if(a != b)
                {
                    return a - b;
                }
            }
            return 0;
        }
    }

    /**
     * @param data
     */
   /* private void caliculateMaxMin(Pair<byte[], double[]> data)
    {
        int m = 0;
        for(double value : data.getValue())
        {
            maxData[m] = (maxData[m] > value ? maxData[m] : value);
            minData[m] = (minData[m] < value ? minData[m] : value);
            m++;
        }
    }*/

    /**
     * @param i
     * @param measure
     * @throws SQLException
     */
  /*  private void setDecimals(int i, double measureVal)
    {
        // calculate decimal num of measure
        String measureString = String.valueOf(measureVal);
        int index = measureString.indexOf(".");
        int num = 0;
        if(index != -1 && !"0".equalsIgnoreCase(measureString.substring(index + 1, measureString.length())))
        {
            num = measureString.length() - index - 1;
        }
//        decimalLength[i] = (decimalLength[i] > num ? decimalLength[i] : num);
    }*/

    @Override
    public void initInput()
    {
        //
        if(scanners.size() > 0)
        {
            Iterator<Scanner> itr = scanners.iterator();
            // for(int f = 0;f < scanners.size();f++)
            int f = 0;
            while(itr.hasNext())
            {
                // Get the each Scanner
                Scanner eachScanner = itr.next();
                f++;
                if(eachScanner.isDone())
                {
                    scanners.remove(f);
                    f--;
                }
                else
                {
                    //
                    KeyValue keyValue = eachScanner.getNext();
                    double[] msrValue = null;
                    if(null != keyValue.getMsrCols())
                    {
                        msrValue = keyValue.getOriginalValue();
                    }
                    Pair pair = new Pair(keyValue.getOriginalKey(), msrValue);
                    tempRows.add(new RowTempFile(pair, f));
                }
            }

            // Sort the data row buffer
            Collections.sort(tempRows, new KeyComparator());
        }
    }

    @Override
    public void closeInput()
    {
        // TODO Auto-generated method stub
    }

    @Override
    public ValueCompressionModel getValueCompressionMode()
    {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * 
     * @see com.huawei.unibi.molap.engine.datastorage.streams.DataInputStream#getLeafNodeInfo()
     * 
     */
    @Override
    public List<LeafNodeInfoColumnar> getLeafNodeInfoColumnar()
    {
        // TODO Auto-generated method stub
        return null;
    }
    
    /**
     * 
     * @see com.huawei.unibi.molap.engine.datastorage.streams.DataInputStream#getLeafNodeInfo()
     * 
     */
    @Override
    public List<LeafNodeInfo> getLeafNodeInfo()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public byte[] getStartKey()
    {
        // TODO Auto-generated method stub
        return null;
    }
}
