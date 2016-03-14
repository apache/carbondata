/**
 * 
 */
package com.huawei.unibi.molap.metadata;

import com.huawei.unibi.molap.metadata.MolapMetadata.Cube;
import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;
import com.huawei.unibi.molap.metadata.MolapMetadata.Measure;
import com.huawei.unibi.molap.olap.Exp;
import com.huawei.unibi.molap.olap.SqlStatement.Type;


/**
 * Calculated measure instance
 * @author R00900208
 *
 */
public class CalculatedMeasure extends Measure
{
    
    /**
     * 
     */
    private static final long serialVersionUID = 7678164364921738949L;
    
    private transient Exp exp;
    
    private Dimension distCountDim;
    

    public CalculatedMeasure(String colName, int ordinal, String aggName, String aggClassName, String name,
            Type dataType, Cube cube)
    {
//        super(colName, ordinal, aggName, aggClassName, name, dataType, cube, false);
    }
    
    public CalculatedMeasure(Exp exp,String name)
    {
        super(null, -1, null, null, name, null, null, false);
        this.exp = exp;
    }
    
    
    public CalculatedMeasure(String name)
    {
        super(null, -1, null, null, name, null, null, false);
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
    
    
    /**
     * @return the distCountDim
     */
    public Dimension getDistCountDim()
    {
        return distCountDim;
    }
    
    /**
     * @return the distCountDim
     */
    public void setDistCountDim(Dimension distCountDim)
    {
        this.distCountDim = distCountDim;
    }    

    @Override
    public boolean equals(Object obj)
    {
        Measure that = null;

        if(obj instanceof Measure)
        {

            that = (Measure)obj;
            return that.getName().equals(getName());
        }
        // Added this to fix Find bug
        // Symmetric issue
        if(obj instanceof Dimension)
        {
            return super.equals(obj);
        }
        return false;

    }

    @Override
    public int hashCode()
    {
        return getName().hashCode();
    }
    
    
}
