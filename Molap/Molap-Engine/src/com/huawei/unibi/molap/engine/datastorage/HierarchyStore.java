/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwdh/HjOjN0Brs7b7TRorj6S6iAIeaqK90lj7BAM
GSGxBvh9FWsGxOHNHCEQyqkc7YeyXIkbKZnpAHZugsIQxokJ0PI4LLWXHZyAU8SBlErtcTkj
BK8kMos8tyf4EdPzmxWuvKc1kQXr9frNZsPnCixhdHhaIDn32vnhmRw0m1N0NA==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.datastorage;


import com.huawei.unibi.molap.engine.datastorage.streams.DataInputStream;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;

/**
 * @author R00900208
 * 
 */
public class HierarchyStore
{
    /**
     * 
     */
    private String hierName;

    /**
     * 
     */
    private String tableName;
    
    /**
     * Fact table Name
     */
    private String factTableName;

    /**
     * A null represents the hierarchy has only one level and the hierarchy can
     * be directly loaded from member cache.
     */
    // private Map dimension = new HashMap();

    private HierarchyBtreeStore btreeStore;

    /**
     * 
     */
    private com.huawei.unibi.molap.olap.MolapDef.Hierarchy rolapHierarchy;

    /**
     * 
     */
    private String dimeName;

    public HierarchyStore(com.huawei.unibi.molap.olap.MolapDef.Hierarchy rolapHierarchy, String factTableName, String dimensionName)
    {
        this.dimeName = dimensionName;
        this.hierName = rolapHierarchy.name == null ? dimensionName : rolapHierarchy.name;

        this.rolapHierarchy = rolapHierarchy;
        tableName = hierName.replaceAll(" ", "_") + ".hierarchy";
        this.factTableName = factTableName;
    }

    /**
     * Getter for hierarchy
     */
    public com.huawei.unibi.molap.olap.MolapDef.Hierarchy getRolapHierarchy()
    {
        return rolapHierarchy;
    }

    /**
     * Getter for dimension name in which this hierarchy present
     */
    public String getDimensionName()
    {
        return dimeName;
    }

    /**
     * @param keyGen
     * @param factStream
     */
    public void build(KeyGenerator keyGen, DataInputStream factStream)
    {
        btreeStore = new HierarchyBtreeStore(keyGen);
        btreeStore.build(factStream);
    }

    /**
     * @return the hierName
     */
    public String getHierName()
    {
        return hierName;
    }

    /**
     * @param hierName
     *            the hierName to set
     */
    /*public void setHierName(String hierName)
    {
        this.hierName = hierName;
    }*/

    /**
     * @return the dimension
     */
    // public Map getCache()
    // {
    // return dimension;
    // }

    /**
     * @param dimension
     *            the dimension to set
     */
    // public void setDimension(Map dimension)
    // {
    // this.dimension = dimension;
    // }

//    public String getLevelName()
//    {
//        for(MolapDef.Level level : rolapHierarchy.levels)
//        {
//            if(!level.isAll())
//            {
//                return level.getName();
//            }
//        }
//
//        return null;
//    }

    /**
     * @return
     */
    public String getTableName()
    {
        return tableName;
    }

    /**
     * @return
     */
    /*public HierarchyBtreeStore getHierBTreeStore()
    {
        return btreeStore;
    }*/

    /**
     * 
     * @return Returns the factTableName.
     * 
     */
    public String getFactTableName()
    {
        return factTableName;
    }

    // private Member dummy = new Member(0, "");

    // public boolean findMember(Long[] keys)
    // {
    // Map currentLevelCache = dimension;
    // for(int i=0; i< keys.length; i++)
    // {
    // //dummy.setDimension(keys[i]);
    // //currentLevelCache = (Map) currentLevelCache.get(dummy);
    // if(currentLevelCache == null)
    // {
    // return false;
    // }
    // }
    //
    // return true;
    // }
}
