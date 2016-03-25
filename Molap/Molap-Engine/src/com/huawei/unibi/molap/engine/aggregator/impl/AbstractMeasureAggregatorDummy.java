/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwdEVzw1icjfRowqz2DW4XzUpEhhSzBOwVynEHjc
u0090WB80p6C2F0BaV/nPfET1YH8WiTmzGmqlGWNJnfkabT2Vmafa4wfhnkaSIrrXKAvI9Ss
fyGrpchxXf0OpbQu1yNmt8TEQOXGMOJXYTrPMP0HALMZhWitjz8eQcofUoPkTw==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
package com.huawei.unibi.molap.engine.aggregator.impl;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * AbstractMeasureAggregatorDummy
 * Used for custom Molap Aggregator dummy
 *
 */
public abstract class AbstractMeasureAggregatorDummy extends AbstractMeasureAggregatorBasic
{
    private static final long serialVersionUID = 1L;

    @Override
    public int compareTo(MeasureAggregator o)
    {
        if(equals(o))
        {
            return 0;
        }
        return -1;
    }
    @Override
    public boolean equals(Object arg0)
    {
        return super.equals(arg0);
    }
    @Override
    public int hashCode()
    {
        return super.hashCode();
    }

    @Override
    public byte[] getByteArray()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void merge(MeasureAggregator aggregator)
    {
        // TODO Auto-generated method stub

    }
    @Override
    public boolean isFirstTime()
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public MeasureAggregator getCopy()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void writeData(DataOutput output) throws IOException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void readData(DataInput inPut) throws IOException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public void merge(byte[] value)
    {
        // TODO Auto-generated method stub

    }
}
