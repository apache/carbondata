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
package com.huawei.unibi.molap.query.metadata;

/**
 * 
 * @author K00900207
 *
 */
public class MolapDynamicDimension extends MolapDimensionLevel
{

    /**
     * 
     * Comment for <code>serialVersionUID</code>
     * 
     */
    private static final long serialVersionUID = 8361029642215961703L;

    
    public MolapDynamicDimension(String dimensionName, String hierarchyName, String levelName)
    {
        super(dimensionName, hierarchyName, levelName);
    }
    
    @Override
    public MolapLevelType getType()
    {
        return MolapLevelType.DYNAMIC_DIMENSION;
    }
    
}
