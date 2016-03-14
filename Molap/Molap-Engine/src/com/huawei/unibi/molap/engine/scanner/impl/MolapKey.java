/**
 * 
 */
package com.huawei.unibi.molap.engine.scanner.impl;

import java.io.Serializable;
import java.util.Arrays;

/**
 * @author R00900208
 *
 */
public class MolapKey implements  Serializable,Comparable<MolapKey>
{
    
    /**
     * 
     */
    private static final long serialVersionUID = -8773813519739848506L;
    
    private Object[] key;
    


    public MolapKey(Object[] key)
    {
        this.key = key;
    }

    /**
     * @return the key
     */
    public Object[] getKey()
    {
        return key;
    }
    
    public  MolapKey getSubKey(int size)
    {
        Object[] crop = new Object[size];
        System.arraycopy(key, 0, crop, 0, size);
        return new MolapKey(crop);
    }
    
    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(key);
        return result;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj)
    {
        if(this == obj)
        {
            return true;
        }
        if(obj == null)
        {
            return false;
        }
        if(getClass() != obj.getClass())
        {
            return false;
        }
        MolapKey other = (MolapKey)obj;
        if(!Arrays.equals(key, other.key))
        {
            return false;
        }
        return true;
    }

    
    @Override
    public String toString()
    {
       return Arrays.toString(key);
    }

    @Override
    public int compareTo(MolapKey other)
    {
        Object[] oKey = other.key;
        
        int l = 0;
        for(int i = 0;i < key.length;i++)
        {
            l = ((Comparable)key[i]).compareTo(oKey[i]);
            if(l != 0)
            {
                return l;
            }
        }
        
        return 0;
    }

}
