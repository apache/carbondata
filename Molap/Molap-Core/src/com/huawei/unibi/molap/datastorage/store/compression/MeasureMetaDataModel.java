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


package com.huawei.unibi.molap.datastorage.store.compression;

public class MeasureMetaDataModel {
    /**
     * maxValue
     */
    private double[] maxValue;

    /**
     * minValue
     */
    private double[] minValue;

    /**
     * decimal
     */
    private int[] decimal;

    /**
     * measureCount
     */
    private int measureCount;

    /**
     * uniqueValue
     */
    private double[] uniqueValue;

    /**
     * type
     */
    private char[] type;

    /**
     * dataTypeSelected
     */
    private byte[] dataTypeSelected;

    private double[] minValueFactForAgg;

    public MeasureMetaDataModel() {

    }

    /**
     * MeasureMetaDataModel Constructor
     *
     * @param minValue
     * @param maxValue
     * @param decimal
     * @param measureCount
     * @param uniqueValue
     * @param type
     */
    public MeasureMetaDataModel(double[] minValue, double[] maxValue, int[] decimal, int measureCount,
                                double[] uniqueValue, char[] type, byte[] dataTypeSelected) {
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.decimal = decimal;
        this.measureCount = measureCount;
        this.uniqueValue = uniqueValue;
        this.type = type;
        this.dataTypeSelected = dataTypeSelected;
    }

    /**
     * get Max value
     *
     * @return
     */
    public double[] getMaxValue() {
        return maxValue;
    }

    /**
     * set max value
     *
     * @param maxValue
     */
    public void setMaxValue(double[] maxValue) {
        this.maxValue = maxValue;
    }

    /**
     * getMinValue
     *
     * @return
     */
    public double[] getMinValue() {
        return minValue;
    }

    /**
     * setMinValue
     *
     * @param minValue
     */
    public void setMinValue(double[] minValue) {
        this.minValue = minValue;
    }

    /**
     * getDecimal
     *
     * @return
     */
    public int[] getDecimal() {
        return decimal;
    }

    /**
     * setDecimal
     *
     * @param decimal
     */
    public void setDecimal(int[] decimal) {
        this.decimal = decimal;
    }

    /**
     * getMeasureCount
     *
     * @return
     */
    public int getMeasureCount() {
        return measureCount;
    }

    /**
     * setMeasureCount
     *
     * @param measureCount
     */
    public void setMeasureCount(int measureCount) {
        this.measureCount = measureCount;
    }

    /**
     * getUniqueValue
     *
     * @return
     */
    public double[] getUniqueValue() {
        return uniqueValue;
    }

    /**
     * setUniqueValue
     *
     * @param uniqueValue
     */
    public void setUniqueValue(double[] uniqueValue) {
        this.uniqueValue = uniqueValue;
    }

    /**
     * @return the type
     */
    public char[] getType() {
        return type;
    }

    /**
     * @param type the type to set
     */
    public void setType(char[] type) {
        this.type = type;
    }

    /**
     * @return the dataTypeSelected
     */
    public byte[] getDataTypeSelected() {
        return dataTypeSelected;
    }

    /**
     * @param dataTypeSelected the dataTypeSelected to set
     */
    public void setDataTypeSelected(byte[] dataTypeSelected) {
        this.dataTypeSelected = dataTypeSelected;
    }

    /**
     * @return the minValueFactForAgg
     */
    public double[] getMinValueFactForAgg() {
        return minValueFactForAgg;
    }

    /**
     * @param minValueFactForAgg the minValueFactForAgg to set
     */
    public void setMinValueFactForAgg(double[] minValueFactForAgg) {
        this.minValueFactForAgg = minValueFactForAgg;
    }

}