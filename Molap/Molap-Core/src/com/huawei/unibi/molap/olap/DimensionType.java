package com.huawei.unibi.molap.olap;

public enum DimensionType {
    /**
     * Indicates that the dimension is not related to time.
     */
    StandardDimension,

    /**
     * Indicates that a dimension is a time dimension.
     */
    TimeDimension,

    /**
     * Indicates the a dimension is the measures dimension.
     */
    MeasuresDimension,
}