/**
 * 
 */
package com.huawei.unibi.molap.keygenerator.util;

/**
 * @author R00900208
 *
 */
public final class KeyGenUtil 
{
	private KeyGenUtil()
	{
	    
	}

    /**
     * toLong method.
     * @param bytes
     * @param offset
     * @param length
     * @return long.
     */
	public static long toLong(byte[] bytes, int offset, final int length) 
	{
		long l = 0;
		for (int i = offset; i < offset + length; i++) 
		{
			l <<= 8;
			l ^= bytes[i] & 0xFF;
		}
		return l;
	}
	
	

}
