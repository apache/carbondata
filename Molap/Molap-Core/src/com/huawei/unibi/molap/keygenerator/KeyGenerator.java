/**
 * 
 */
package com.huawei.unibi.molap.keygenerator;

import java.io.Serializable;
import java.util.Comparator;

/**
 * It generates the key by using multiple keys(typically multiple dimension keys
 * are combined to form a single key). And it can return the individual
 * key(dimensional key) out of combined key.
 * 
 * 
 * @author R00900208
 * 
 */
public interface KeyGenerator extends Serializable, Comparator<byte[]>
{
    /**
     * It generates the single key aka byte array from multiple keys.
     * 
     * @param keys
     * @return byte array
     * @throws KeyGenException
     */
    byte[] generateKey(long[] keys) throws KeyGenException;
    
    /**
     * It generates the single key aka byte array from multiple keys.
     * 
     * @param keys
     * @return
     * @throws KeyGenException
     *
     */
    byte[] generateKey(int[] keys) throws KeyGenException;

    /**
     * It gets array of keys out of single key aka byte array
     * 
     * @param key
     * @return array of keys.
     */
    long[] getKeyArray(byte[] key);
    

    /**
     * It gets array of keys out of single key aka byte array
     * @param key
     * @param maskedByteRanges
     * @return array of keys
     */
    long[] getKeyArray(byte[] key,int[] maskedByteRanges);

    /**
     * It gets the key in the specified index from the single key aka byte array
     * 
     * @param key
     * @param index
     *            of key.
     * @return key
     */
    long getKey(byte[] key, int index);

    /**
     * Set any extra properties if required.
     * 
     * @param properties
     */
    void setProperty(Object key, Object value);

    /**
     * Gives the key size in number of bytes.
     */
    int getKeySizeInBytes();

    /**
     * It gets the specified index and size from the single key aka byte aray
     * 
     * @param key
     * @param index
     * @param size
     * @return
     */
    long[] getSubKeyArray(byte[] key, int index, int size);

    /**
     * returns key bytes offset
     * @param index
     * @return
     */
    int[] getKeyByteOffsets(int index);

    int compare(byte[] key1, int offset1, int length1, byte[] key2, int offset2, int length2);

    /**
     * returns the dimension count
     * @return
     */
    int getDimCount();
    
    void setStartAndEndKeySizeWithOnlyPrimitives(int startAndEndKeySizeWithPrimitives);
    
    int getStartAndEndKeySizeWithOnlyPrimitives();
}