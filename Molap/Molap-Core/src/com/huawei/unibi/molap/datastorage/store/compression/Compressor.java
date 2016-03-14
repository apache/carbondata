/**
 * 
 */
package com.huawei.unibi.molap.datastorage.store.compression;

/**
 * 
 * @author R00900208
 *
 * @param <T>
 */
public interface Compressor<T>
{

    byte[] compress(T input);

    T unCompress(byte[] input);

}
