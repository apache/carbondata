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
 * Represents DSL transformation which can be added to <code>MolapQuery<code> 
 * 
 * @author K00900207
 *
 */
public class DSLTransformation
{
    /**
     * Name of the transformation. 
     */
    private String name;
    
    /**
     * DSL script 
     */
    private String dslExpression;
    
    /**
     * The new column name if the DSL script is adding one new column 
     */
    private String newColumnName;
    
    /**
     * Flag to set if the transformation script will resulting to add a new column in the original result.
     */
    private boolean addAsColumn;

    public DSLTransformation(String name, String dslExpression, String newColumnName, boolean addAsColumn)
    {
        this.name = name;
        this.dslExpression = dslExpression;
        this.newColumnName = newColumnName;
        this.addAsColumn = addAsColumn;
    }

    /**
     * 
     * @return Returns the name.
     * 
     */
    public String getName()
    {
        return name;
    }

    /**
     * 
     * @param name The name to set.
     * 
     */
    public void setName(String name)
    {
        this.name = name;
    }

    /**
     * 
     * @return Returns the dslExpression.
     * 
     */
    public String getDslExpression()
    {
        return dslExpression;
    }

    /**
     * 
     * @param dslExpression The dslExpression to set.
     * 
     */
    public void setDslExpression(String dslExpression)
    {
        this.dslExpression = dslExpression;
    }

    /**
     * 
     * @return Returns the newColumnName.
     * 
     */
    public String getNewColumnName()
    {
        return newColumnName;
    }

    /**
     * 
     * @param newColumnName The newColumnName to set.
     * 
     */
    public void setNewColumnName(String newColumnName)
    {
        this.newColumnName = newColumnName;
    }

    /**
     * 
     * @return Returns the addAsColumn.
     * 
     */
    public boolean isAddAsColumn()
    {
        return addAsColumn;
    }

    /**
     * 
     * @param addAsColumn The addAsColumn to set.
     * 
     */
    public void setAddAsColumn(boolean addAsColumn)
    {
        this.addAsColumn = addAsColumn;
    }
    
}
