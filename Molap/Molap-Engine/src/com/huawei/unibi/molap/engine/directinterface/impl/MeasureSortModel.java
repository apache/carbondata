/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcAIRTtLWBkMMN+iqJ62JNQb/MYFaBoemC1VlrU
n+vkOdf9cazfjBey4t0DjFDYDA1PHBpWSsyYpLgfbzhbpcNKgnmD1K7QIzLlfgbr6FacZiN2
sIo9wGWwOjYO3JiTBPgUm156YCYj54fBslL40nZ0ubSR1PEFX0VkFBiWXJNaOQ==*/
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
package com.huawei.unibi.molap.engine.directinterface.impl;

import java.io.Serializable;

import com.huawei.unibi.molap.metadata.MolapMetadata.Measure;

/**
 * This model object for Molap measure sort.
 * @author R00900208
 *
 */
public class MeasureSortModel implements Serializable
{

    /**
     * 
     */
    private static final long serialVersionUID = -8532335454146149584L;

    /**
     * measure
     */
    private Measure measure;
    
    /**
     * measureIndex
     */
    private int measureIndex;
    
    /**
     * sortOrder
     */
    private int sortOrder;
    
    /**
     * isBreakHeir
     */
    private boolean isBreakHeir;
    
    public MeasureSortModel(Measure measure, int sortOrder)
    {
        this.measure = measure;
        this.sortOrder = sortOrder;
    }

    /**
     * @return the measureIndex
     */
    public int getMeasureIndex()
    {
        return measureIndex;
    }

    /**
     * @param measureIndex the measureIndex to set
     */
    public void setMeasureIndex(int measureIndex)
    {
        this.measureIndex = measureIndex;
    }

    /**
     * @return the measure
     */
    public Measure getMeasure()
    {
        return measure;
    }

    /**
     * @return the sortOrder
     */
    public int getSortOrder()
    {
        return sortOrder;
    }

    /**
     * @return the isBreakHeir
     */
    public boolean isBreakHeir()
    {
        return isBreakHeir;
    }

    /**
     * @param isBreakHeir the isBreakHeir to set
     */
    public void setBreakHeir(boolean isBreakHeir)
    {
        this.isBreakHeir = isBreakHeir;
    }

}
