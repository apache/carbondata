/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwdh/HjOjN0Brs7b7TRorj6S6iAIeaqK90lj7BAM
GSGxBp3CptKgZR+CmDnfprc4NURttlQ44HomkiyKqCdSMjxhU96vLVMyEzQcCXz887HhlNDU
r00eVy5GA8x+1loakpV4F2PotEmTHLPSFjxD70Zf7F3O3tfI7wClnqx88RrFpw==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.datastorage;

import java.util.Arrays;

import com.huawei.unibi.molap.util.ByteUtil;

/**
 * @author R00900208
 * 
 */
public class Member
{

    /**
     * All the attributes related to member. ordinalCOlumn, caption column,
     * names column and properties. Order is same as how SQLMemberSource reads
     * the columns. Current Store: ordinalCOlumn, Properties.
     */
    protected Object[] attributes;

    /**
     * 
     */
    protected byte[] name;

    public Member(final byte[] name)
    {
        this.name = name;
    }

    /**
     * @return the name
     */
    public byte[] getCharArray()
    {
        return name;
    }

    @Override
    public boolean equals(Object obj)
    {
        if(obj instanceof Member)
        {
            if(this == obj)
            {
                return true;
            }
//            return Arrays.equals(name, ((Member)obj).name);
            return ByteUtil.UnsafeComparer.INSTANCE.equals(name, ((Member)obj).name);
        }
        else
        {
            return false;
        }
    }
    
    /**
     * 
     * @see java.lang.Object#hashCode()
     * 
     */
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(name);
        return result;
    }
    

    @Override
    public String toString()
    {
        // return str;
        return new String(name);
    }

    /**
     * @param properties
     */
    public void setAttributes(final Object[] properties)
    {
        this.attributes = properties;
    }

    /**
     * @return
     */
    public Object[] getAttributes()
    {
        return attributes;
    }

}
