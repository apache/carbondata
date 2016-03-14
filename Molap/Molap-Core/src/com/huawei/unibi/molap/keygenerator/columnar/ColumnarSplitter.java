/**
 * 
 */
package com.huawei.unibi.molap.keygenerator.columnar;

import com.huawei.unibi.molap.keygenerator.KeyGenException;

/**
 * Splits the odometer key to columns.Further these columns can be stored in a columnar storage.
 * 
 * @author R00900208
 *
 */
public interface ColumnarSplitter
{
    /**
     * Splits generated MDKey to multiple columns.
     * @param key MDKey
     * @return Multiple columns in 2 dimensional byte array
     */
    byte[][] splitKey(byte[] key);

    /**
     * It generates and splits key to multiple columns
     * @param keys
     * @return
     * @throws KeyGenException
     */
    byte[][] generateAndSplitKey(long[] keys) throws KeyGenException;

    /**
     * It generates and splits key to multiple columns
     * @param keys
     * @return
     * @throws KeyGenException
     */
    byte[][] generateAndSplitKey(int[] keys) throws KeyGenException;

    /**
     * Takes the split keys and generates the surrogate key array
     * @param key
     * @return
     */
    long[] getKeyArray(byte[][] key);

    /**
     * Takes the split keys and generates the surrogate key array in bytes
     * @param key
     * @return
     */
    byte[] getKeyByteArray(byte[][] key);

    /**
     * Takes the split keys and generates the surrogate key array in bytes
     * @param key
     * @param columnIndexes, takes columnIndexes to consider which columns are present in the key
     * @return
     */
    byte[] getKeyByteArray(byte[][] key, int[] columnIndexes);
    
    /**
     * Takes the split keys and generates the surrogate key array
     * @param key
     * @param columnIndexes, takes columnIndexes to consider which columns are present in the key
     * @return
     */
    long[] getKeyArray(byte[][] key, int[] columnIndexes);
    
    /**
     * Below method will be used to get the block size
     * @return
     */
    int[] getBlockKeySize();
    
    /**
     * Below method will be used to get the total key Size of the particular block
     * @param blockIndexes
     * @return
     */
    int getKeySizeByBlock(int[] blockIndexes);

}
