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
package com.huawei.unibi.molap.query.metadata;

/**
 * It is abstract class for MolapLevel interface.
 * @author R00900208
 *
 */
public abstract class AbstractMolapLevel implements MolapLevel
{

	/**
	 * 
	 */
	private static final long serialVersionUID = -1487270452433379657L;
	
	/**
	 * visibility
	 */
	private boolean visible = true;
	

	/**
	 * See interface comments
	 */
	@Override
	public String getDimensionName() 
	{
		return null;
	}

	/**
	 * See interface comments
	 */
	@Override
	public String getHierarchyName() 
	{
			return null;
	}

	/**
	 * @return the visible
	 */
	public boolean isVisible() 
	{
		return visible;
	}

	/**
	 * @param visible the visible to set
	 */
	public void setVisible(boolean visible) 
	{
		this.visible = visible;
	}
}
