/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcz8AOhvEHjQfa55oxvUSJWRQCwLl+VwWEHaV7n
0eFj3bJK+K5Jn/RvVjfF3NfAo3+jV0SwNxcFWV6t6BskrslISQGYU1XePYeHqZQuVnqwtq94
eCs6QzetAkGuhBUxzelPHociQ/FvJZBQm0J10Pa5rArE/Y/AUUYutS/2nvE2vw==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
 *
 */
package com.huawei.unibi.molap.engine.filters.measurefilter;

import java.io.Serializable;

import com.huawei.unibi.molap.olap.Exp;


import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;
import com.huawei.unibi.molap.query.MolapQuery.AxisType;


/**
 * It is the model object for measure filter.
 * 
 * @author R00900208
 *
 */
public class MeasureFilterModel implements Serializable
{
    /**
     * 
     */
    private static final long serialVersionUID = -1959494583324268999L;

    /**
     * filterValue
     */
    private double filterValue;
    
    /**
     * filterType
     */
    private MeasureFilterType filterType;
    
    /**
     * Dimension
     */
    private Dimension dimension;
    
    /**
     * AxisType
     */
    private AxisType axisType;
    
    /**
     * Calc expression
     */
    private transient Exp exp; 
    
    public MeasureFilterModel(double filterValue, MeasureFilterType filterType) 
    {
        this.filterValue = filterValue;
        this.filterType = filterType;
    }
    
    public MeasureFilterModel()
    {
        
    }

    /**
     * It is enum class for measure filter types.
     * 
     * @author R00900208
     *
     */
    public enum MeasureFilterType
    {
        /**
         * filterType
         */
        EQUAL_TO,
        /**
         * NOT_EQUAL_TO
         */
        NOT_EQUAL_TO,
        /**
         * GREATER_THAN
         */
        GREATER_THAN,
        /**
         * LESS_THAN
         */
        LESS_THAN,
        /**
         * LESS_THAN_EQUAL
         */
        LESS_THAN_EQUAL, 
        /**
         * GREATER_THAN_EQUAL
         */
        GREATER_THAN_EQUAL,
        /**
         * NOT_EMPTY
         */
        NOT_EMPTY;
    }


    /**
     * @return the filterValue
     */
    public double getFilterValue()
    {
        return filterValue;
    }


    /**
     * @param filterValue the filterValue to set
     */
    public void setFilterValue(double filterValue)
    {
        this.filterValue = filterValue;
    }


    /**
     * @return the filterType
     */
    public MeasureFilterType getFilterType()
    {
        return filterType;
    }


    /**
     * @param filterType the filterType to set
     */
    public void setFilterType(MeasureFilterType filterType)
    {
        this.filterType = filterType;
    }

    /**
     * @return the dimension
     */
    public Dimension getDimension()
    {
        return dimension;
    }

    /**
     * @param dimension the dimension to set
     */
    public void setDimension(Dimension dimension)
    {
        this.dimension = dimension;
    }

    /**
     * @return the axisType
     */
    public AxisType getAxisType()
    {
        return axisType;
    }

    /**
     * @param axisType the axisType to set
     */
    public void setAxisType(AxisType axisType)
    {
        this.axisType = axisType;
    }

    /**
     * @return the exp
     */
    public Exp getExp()
    {
        return exp;
    }

    /**
     * @param exp the exp to set
     */
    public void setExp(Exp exp)
    {
        this.exp = exp;
    }
    
    
    
}
