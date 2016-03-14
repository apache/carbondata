package com.huawei.unibi.molap.engine.result.iterator;

import com.huawei.unibi.molap.engine.result.ChunkResult;
import com.huawei.unibi.molap.engine.result.RowResult;
import com.huawei.unibi.molap.iterator.MolapIterator;

public class ChunkRowIterator implements MolapIterator<RowResult>
{
    private MolapIterator<ChunkResult> iterator;
    
    private ChunkResult currentchunk;
    
    public ChunkRowIterator(MolapIterator<ChunkResult> iterator)
    {
        this.iterator=iterator;
        if(iterator.hasNext())
        {
            currentchunk=iterator.next();
        }
    }
    @Override
    public boolean hasNext()
    {
       if(null!=currentchunk)
        {
        if((currentchunk.hasNext()))
        {
            return true;
        }
        else if(!currentchunk.hasNext())
        {
            while(iterator.hasNext())
            {
                currentchunk = iterator.next();
                if(currentchunk!=null && currentchunk.hasNext())
                {
                    return true;
                }
            }
        }
        }
        return false;
    }

    @Override
    public RowResult next()
    {
        return currentchunk.next();
    }

}
