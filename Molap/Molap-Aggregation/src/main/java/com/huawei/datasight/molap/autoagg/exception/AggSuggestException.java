package com.huawei.datasight.molap.autoagg.exception;

/**
 * Aggregate suggestion exception
 * @author A00902717
 *
 */
public class AggSuggestException extends Exception 
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	
	public AggSuggestException(String message ,Exception e)
	{
		super(message,e);
	}


	public AggSuggestException(String message) 
	{
		super(message);
	}

}
