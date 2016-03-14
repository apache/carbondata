package com.huawei.unibi.molap.engine.aggregator.dimension;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;

public class DimensionAggregatorInfo implements Serializable
{
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private String columnName;

    private Dimension dim;

    private List<String> aggList;

    private List<Integer> orderList;

    private byte[] nullValueMdkey;
    
    /**
     * isDimensionPresentInCurrentSlice
     */
    private boolean isDimensionPresentInCurrentSlice=true;

    public DimensionAggregatorInfo()
    {
        aggList = new ArrayList<String>(10);
        orderList = new ArrayList<Integer>(10);
    }

    public String getColumnName()
    {
        return columnName;
    }

    public void setColumnName(String columnName)
    {
        this.columnName = columnName;
    }

    public Dimension getDim()
    {
        return dim;
    }

    public void setDim(Dimension dim)
    {
        this.dim = dim;
    }

    public List<String> getAggList()
    {
        return aggList;
    }

    public void setAggList(List<String> aggList)
    {
        this.aggList = aggList;
    }

    public void addAgg(String agg)
    {
        aggList.add(agg);
    }

    public void setOrder(int index)
    {
        orderList.add(index);
    }

    public byte[] getNullValueMdkey()
    {
        return nullValueMdkey;
    }

    public void setNullValueMdkey(byte[] nullValueMdkey)
    {
        this.nullValueMdkey = nullValueMdkey;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((columnName == null) ? 0 : columnName.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if(this == obj)
        {
            return true;
        }
        if(obj == null)
        {
            return false;
        }       
        if(getClass() != obj.getClass())
        {
            return false;
        }
        DimensionAggregatorInfo other = (DimensionAggregatorInfo)obj;
        if(columnName == null)
        {
            if(other.columnName != null)
            {
                return false;
            }
        }
        else if(!columnName.equals(other.columnName))
        {
            return false;
        }
        return true;
    }

    public List<Integer> getOrderList()
    {
        return orderList;
    }

    public void setOrderList(List<Integer> orderList)
    {
        this.orderList = orderList;
    }

    public boolean isDimensionPresentInCurrentSlice()
    {
        return isDimensionPresentInCurrentSlice;
    }

    public void setDimensionPresentInCurrentSlice(boolean isDimensionPresentInCurrentSlice)
    {
        this.isDimensionPresentInCurrentSlice = isDimensionPresentInCurrentSlice;
    }
}
