/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwdh/HjOjN0Brs7b7TRorj6S6iAIeaqK90lj7BAM
GSGxBuAVYc3AOgydTJcaqGxA+5qF6dHkXSeqBAEjPcb4cNsJPvhZwswlvZ834tPoSHlBlHSu
nwu46IaXwYLJ6QVj7l02WWOEkTkqM5IYyEhZckvf6hqrcSmyR/GYmvtjf6kzdA==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.schema.metadata;

/**
 * @author R00900208
 * @param <K>
 * @param <V>
 */
public class Pair<K, V>
{
    /**
     * 
     */
    private K key;

    /**
     * 
     */
    private V value;

    /**
     * @param key
     * @param value
     */
    public Pair(K key, V value)
    {
        super();
        this.key = key;
        this.value = value;
    }

    public Pair()
    {

    }

    /**
     * @return the key
     */
    public K getKey()
    {
        return key;
    }

    /**
     * @param key
     *            the key to set
     */
    public void setKey(K key)
    {
        this.key = key;
    }

    /**
     * @return the value
     */
    public V getValue()
    {
        return value;
    }

    /**
     * @param value
     *            the value to set
     */
    public void setValue(V value)
    {
        this.value = value;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((key == null) ? 0 : key.hashCode());
        result = prime * result + ((value == null) ? 0 : value.hashCode());
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj)
    {
        if(obj instanceof Pair)
        {
            if(this == obj)
            {
                return true;
            }
            Pair other = (Pair)obj;
            if(key == null)
            {
                if(other.key != null)
                {
                    return false;
                }
            }
            else if(!key.equals(other.key))
            {
                return false;
            }
            if(value == null)
            {
                if(other.value != null)
                {
                    return false;
                }
            }
            else if(!value.equals(other.value))
            {
                return false;
            }
            return true;
        }
        else
        {
            return false;
        }
    }

}
