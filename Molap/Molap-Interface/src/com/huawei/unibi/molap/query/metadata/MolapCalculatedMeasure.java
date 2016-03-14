/**
 * 
 */
package com.huawei.unibi.molap.query.metadata;


/**
 * Calculated measures can be created by using this class
 * @author R00900208
 *
 */
public class MolapCalculatedMeasure extends MolapMeasure
{

	/**
	 * expression
	 */
	private String expression;
	
    private boolean groupCount;
	
	private String groupDimensionFormula;
	
	private MolapDimensionLevel groupDimensionLevel;
	
    /**
	 * 
	 */
	private static final long serialVersionUID = 4176313704077360543L;
	

	public MolapCalculatedMeasure(String measureName,String expr) 
	{
		super(measureName);
	}

	/**
	 * @return the expression
	 */
	public String getExpression() 
	{
		return expression;
	}

	/**
	 * @param expression the expression to set
	 */
	public void setExpression(String expression) 
	{
		this.expression = expression;
	}
	
	/**
     * 
     * @return Returns the groupCount.
     * 
     */
    public boolean isGroupCount()
    {
        return groupCount;
    }

    /**
     * 
     * @param groupCount The groupCount to set.
     * 
     */
    public void setGroupCount(boolean groupCount)
    {
        this.groupCount = groupCount;
    }

    /**
     * 
     * @return Returns the groupDimensionFormula.
     * 
     */
    public String getGroupDimensionFormula()
    {
        return groupDimensionFormula;
    }

    /**
     * 
     * @param groupDimensionFormula The groupDimensionFormula to set.
     * 
     */
    public void setGroupDimensionFormula(String groupDimensionFormula)
    {
        this.groupDimensionFormula = groupDimensionFormula;
    }
    
    /**
     * 
     * @return Returns the groupDimensionLevel.
     * 
     */
    public MolapDimensionLevel getGroupDimensionLevel()
    {
        return groupDimensionLevel;
    }

    /**
     * 
     * @param groupDimensionLevel The groupDimensionLevel to set.
     * 
     */
    public void setGroupDimensionLevel(MolapDimensionLevel groupDimensionLevel)
    {
        this.groupDimensionLevel = groupDimensionLevel;
    }
    
	/**
	 * See interface comments
	 */
	@Override
	public MolapLevelType getType() 
	{
		return MolapLevelType.CALCULATED_MEASURE;
	}
	
}
