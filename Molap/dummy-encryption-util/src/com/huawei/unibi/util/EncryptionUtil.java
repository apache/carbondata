package com.huawei.unibi.util;

import com.huawei.unibi.exception.CipherException;

public final class EncryptionUtil
{
	private EncryptionUtil()
	{
		
	}
	
	/**
	 * For encrypting strings reversibly
	 */
	public static String encryptReversible(String string)
			throws CipherException
	{
		return string;
	}

	/**
	 * For decrypting an encrypted String which was encrypted reversibly.
	 */
	public static String decrypt(String string) throws CipherException
	{
		return string;
	}
}
