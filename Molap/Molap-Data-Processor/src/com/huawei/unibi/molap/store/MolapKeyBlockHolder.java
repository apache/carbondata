package com.huawei.unibi.molap.store;

public class MolapKeyBlockHolder 
{
	private byte[][] keyBlock;
	
	private int counter;
	
	public MolapKeyBlockHolder(int size) 
	{
		keyBlock= new byte[size][];
	}
	
	public void addRowToBlock(int index, byte[] keyBlock)
	{
		this.keyBlock[index]=keyBlock;
		counter++;
	}
	
	public byte[][] getKeyBlock()
	{
		if(counter<keyBlock.length)
		{
			byte[][] temp= new byte[counter][];
			System.arraycopy(keyBlock,0,temp,0,counter);
			return temp;
		}
		return keyBlock;
	}
	
	public void resetCounter()
	{
//	    keyBlock= new byte[keyBlock.length][];
		counter=0;
	}
}
