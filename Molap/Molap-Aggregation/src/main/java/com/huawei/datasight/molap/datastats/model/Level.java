package com.huawei.datasight.molap.datastats.model;

import java.io.Serializable;
import java.util.Map;


/**
 * Dimension ordinal with cardinalities
 * @author A00902717
 *
 */
public class Level implements Comparable<Level>,Serializable
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String name;
	
	private int ordinal;
	
	private int cardinality;
	
	/**
	 * for each data of master dimension , this value will have distinct data in
	 * other dimension
	 */
	private Map<Integer,Integer> otherDimsDistinctData;

	public Level(int ordinal, int cardinality)
	{
		this.ordinal = ordinal;
		this.cardinality = cardinality;
		
	}
	
	public Level(String name,int ordinal)
	{
		this.name=name;
		this.ordinal=ordinal;
	}

	
	public String getName()
	{
		return name;
	}
	
	public void setName(String name)
	{
		this.name = name;
	}
	public int getOrdinal()
	{
		return ordinal;
	}

	public int getCardinality()
	{
		return cardinality;
	}
   
	
	@Override
	public int compareTo(Level o)
	{

		return Integer.compare(o.cardinality,cardinality);
	}
	@Override
	public String toString()
	{	
		//return name+"["+cardinality+"]";
		return name;
	}

	public void setOtherDimesnionDistinctData(Map<Integer,Integer> distinctRel)
	{
		this.otherDimsDistinctData=distinctRel;
		
	}
	
	public Map<Integer,Integer> getOtherDimesnionDistinctData()
	{
		return this.otherDimsDistinctData;
	}

}
