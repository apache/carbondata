/**
 * 
 */
package com.huawei.unibi.molap.metadata;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;

/**
 * @author R00900208
 *
 */
public class SliceMetaData implements Serializable
{
    
    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = 3046237866264840878L;

    /**
     * Array of dimensions declared.
     */
    private String[] dimensions;
    
    private String complexTypeString;
    
    public String getComplexTypeString()
    {
        return complexTypeString;
    }

    public void setComplexTypeString(String complexTypeString)
    {
        this.complexTypeString = complexTypeString;
    }

    /**
     *actualDimensions 
     */
    private String[] actualDimensions;
    
    

    /**
     * Array of measures declared.
     */
    private String[] measures;
    /**
     * measuresAggregator
     */
    private String[] measuresAggregator;
    
    
    /**
     * Array of newDimensions declared.
     */
    private String[] newDimensions;
    
    /**
     *actualNewDimensions 
     */
    private String[] newActualDimensions;
    
    
    /**
     * tableNamesToLoadMandatory
     */
    private HashSet<String> tableNamesToLoadMandatory;

    /**
     * Array of newMeasures declared.
     */
    private String[] newMeasures; 
    /**
     * Array of newMsrDfts declared.
     */
    private double[] newMsrDfts;
    /**
     * newMeasuresAggregator
     */
    private String [] newMeasuresAggregator;
    
    /**
     * heirAnKeySize
     */
    private String heirAnKeySize;
    
    /**
     * KeyGenerator declared.
     */
    private KeyGenerator keyGenerator;
    
    /**
     * dimLens
     */
    private int[] dimLens;
    
    
    /**
     *actualDimLens 
     */
    private int[] actualDimLens;
    
    /**
     * newDimLens
     */
    private int[] newDimLens;
    
    /**
     *newActualDimLens 
     */
    private int[] newActualDimLens;
    
    

    /**
     * oldDimsNewCardinality
     */
    private int[] oldDimsNewCardinality;
    
    /**
     * isDimCarinalityChanged
     */
    private boolean isDimCarinalityChanged;
    
    private String[] newDimsDefVals;
    
    private int[] newDimsSurrogateKeys;
    
    public int[] getNewDimLens()
    {
        return newDimLens;
    }

    public void setNewDimLens(int[] newDimLens)
    {
        this.newDimLens = newDimLens;
    }

    public int[] getDimLens()
    {
        return dimLens;
    }

    public void setDimLens(int[] dimLens)
    {
        this.dimLens = dimLens;
    }

    /**
     * new keygenerator
     */
    private KeyGenerator newKeyGenerator;
    public KeyGenerator getNewKeyGenerator()
    {
        return newKeyGenerator;
    }

    public void setNewKeyGenerator(KeyGenerator newKeyGenerator)
    {
        this.newKeyGenerator = newKeyGenerator;
    }

    /**
     * @return the dimensions
     */
    public String[] getDimensions()
    {
        return dimensions;
    }

    /**
     * @param dimensions the dimensions to set
     */
    public void setDimensions(String[] dimensions)
    {
        this.dimensions = dimensions;
    }

    /**
     * @return the measures
     */
    public String[] getMeasures()
    {
        return measures;
    }

    /**
     * @param measures the measures to set
     */
    public void setMeasures(String[] measures)
    {
        this.measures = measures;
    }

    /**
     * @return the newDimensions
     */
    public String[] getNewDimensions()
    {
        return newDimensions;
    }

    /**
     * @param newDimensions the newDimensions to set
     */
    public void setNewDimensions(String[] newDimensions)
    {
        this.newDimensions = newDimensions;
    }

    /**
     * @return the newMeasures
     */
    public String[] getNewMeasures()
    {
        return newMeasures;
    }

    /**
     * @param newMeasures the newMeasures to set
     */
    public void setNewMeasures(String[] newMeasures)
    {
        this.newMeasures = newMeasures;
    }

    /**
     * @return the newMsrDfts
     */
    public double[] getNewMsrDfts()
    {
        return newMsrDfts;
    }

    /**
     * @param newMsrDfts the newMsrDfts to set
     */
    public void setNewMsrDfts(double[] newMsrDfts)
    {
        this.newMsrDfts = newMsrDfts;
    }

    /**
     * @return the keyGenerator
     */
    public KeyGenerator getKeyGenerator()
    {
        return keyGenerator;
    }

    /**
     * @param keyGenerator the keyGenerator to set
     */
    public void setKeyGenerator(KeyGenerator keyGenerator)
    {
        this.keyGenerator = keyGenerator;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(dimensions);
        result = prime * result + ((keyGenerator == null) ? 0 : keyGenerator.hashCode());
        result = prime * result + Arrays.hashCode(measures);
        result = prime * result + Arrays.hashCode(newDimensions);
        result = prime * result + Arrays.hashCode(newMeasures);
        result = prime * result + Arrays.hashCode(newMsrDfts);
        result = prime * result + Arrays.hashCode(dimLens);
        return result;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj)
    {
        if(obj instanceof SliceMetaData)
        {
            SliceMetaData other = (SliceMetaData)obj;
            if(Arrays.equals(dimensions, other.dimensions)
                    && Arrays.equals(measuresAggregator, other.measuresAggregator)
                    && Arrays.equals(dimLens, other.dimLens))
            {
                return true;
            }
        }
        return false;
    }
    
    public boolean isSameAs(SliceMetaData other)
    {
        return (Arrays.equals(newDimensions, other.newDimensions) && Arrays.equals(newMeasures, other.newMeasures));
    }

 /*   public String[] getNewMeasureAggreagtor()
    {
        return newMeasureAggreagtor;
    }*/

   /* public void setNewMeasureAggreagtor(String[] newMeasureAggreagtor)
    {
        this.newMeasureAggreagtor = newMeasureAggreagtor;
    }*/

    public String[] getMeasuresAggregator()
    {
        return measuresAggregator;
    }

    public void setMeasuresAggregator(String[] measuresAggregator)
    {
        this.measuresAggregator = measuresAggregator;
    }

    public String[] getNewMeasuresAggregator()
    {
        return newMeasuresAggregator;
    }
                
    public void setNewMeasuresAggregator(String[] newMeasuresAggregator)
    {
        this.newMeasuresAggregator = newMeasuresAggregator;
    }

    public String getHeirAnKeySize()
    {
        return heirAnKeySize;
    }

    public void setHeirAnKeySize(String heirAnKeySize)
    {
        this.heirAnKeySize = heirAnKeySize;
    }

//    public int getCountMsrOrdinal()
//    {
//        return countMsrOrdinal;
//    }

//    public void setCountMsrOrdinal(int countMsrOrdinal)
//    {
//        this.countMsrOrdinal = countMsrOrdinal;
//    }

    public boolean isDimCarinalityChanged()
    {
        return isDimCarinalityChanged;
    }

    public void setDimCarinalityChanged(boolean isDimCarinalityChanged)
    {
        this.isDimCarinalityChanged = isDimCarinalityChanged;
    }

    public int[] getOldDimsNewCardinality()
    {
        return oldDimsNewCardinality;
    }

    public void setOldDimsNewCardinality(int[] oldDimsNewCardinality)
    {
        this.oldDimsNewCardinality = oldDimsNewCardinality;
    }

   
    public String[] getNewActualDimensions()
    {
        return newActualDimensions;
    }

    public void setNewActualDimensions(String[] newActualDimensions)
    {
        this.newActualDimensions = newActualDimensions;
    }
    
    public int[] getNewActualDimLens()
    {
        return newActualDimLens;
    }

    public void setNewActualDimLens(int[] newActualDimLens)
    {
        this.newActualDimLens = newActualDimLens;
    }
    
    public String[] getActualDimensions()
    {
        return actualDimensions;
    }

    public void setActualDimensions(String[] actualDimensions)
    {
        this.actualDimensions = actualDimensions;
    }

    public int[] getActualDimLens()
    {
        return actualDimLens;
    }

    public void setActualDimLens(int[] actualDimLens)
    {
        this.actualDimLens = actualDimLens;
    }

    /**
     * 
     * @return Returns the heirAndDimLens.
     * 
     */
//    public String getHeirAndDimLens()
//    {
//        return heirAndDimLens;
//    }

    /**
     * 
     * @param heirAndDimLens The heirAndDimLens to set.
     * 
     */
//    public void setHeirAndDimLens(String heirAndDimLens)
//    {
//        this.heirAndDimLens = heirAndDimLens;
//    }

    /**
     * 
     * @return Returns the tableNamesToLoadMandatory.
     * 
     */
    public HashSet<String> getTableNamesToLoadMandatory()
    {
        return tableNamesToLoadMandatory;
    }

    /**
     * 
     * @param tableNamesToLoadMandatory The tableNamesToLoadMandatory to set.
     * 
     */
    public void addTableNamesToLoadMandatory(String tableNamesToLoadMandatory)
    {
        if(null == this.tableNamesToLoadMandatory)
        {
            this.tableNamesToLoadMandatory = new HashSet<String>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        }
        this.tableNamesToLoadMandatory.add(tableNamesToLoadMandatory);
    }
    
    /**
     * 
     * @param tableNamesToLoadMandatory The tableNamesToLoadMandatory to set.
     * 
     */
    public void setTableNamesToLoadMandatory(HashSet<String> tableNamesToLoadMandatory)
    {
       
        this.tableNamesToLoadMandatory =tableNamesToLoadMandatory;
    }

    /**
     * return the new dimensions default values
     * @return
     */
    public String[] getNewDimsDefVals()
    {
        return newDimsDefVals;
    }

    /**
     * set the default values of new dimensions added
     * @param newDimsDefVals
     */
    public void setNewDimsDefVals(String[] newDimsDefVals)
    {
        this.newDimsDefVals = newDimsDefVals;
    }

    /**
     * return the surrogate keys of new dimension values
     * @return
     */
    public int[] getNewDimsSurrogateKeys()
    {
        return newDimsSurrogateKeys;
    }

    /**
     * set the surrogate keys for the new dimension values
     * @param newDimsSurrogateKeys
     */
    public void setNewDimsSurrogateKeys(int[] newDimsSurrogateKeys)
    {
        this.newDimsSurrogateKeys = newDimsSurrogateKeys;
    }
}
