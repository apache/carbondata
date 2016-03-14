package com.huawei.unibi.molap.merger.columnar.impl;

import com.huawei.unibi.molap.factreader.MolapSurrogateTupleHolder;
import com.huawei.unibi.molap.merger.columnar.ColumnarFactFileMerger;
import com.huawei.unibi.molap.merger.columnar.iterator.MolapDataIterator;
import com.huawei.unibi.molap.merger.exeception.SliceMergerException;
import com.huawei.unibi.molap.schema.metadata.MolapColumnarFactMergerInfo;
import com.huawei.unibi.molap.store.writer.exception.MolapDataWriterException;

public class TimeBasedMergerColumnar extends ColumnarFactFileMerger
{

    public TimeBasedMergerColumnar(
            MolapColumnarFactMergerInfo molapColumnarFactMergerInfo, int currentRestructNumber)
    {
        super(molapColumnarFactMergerInfo, currentRestructNumber);
    }

    @Override
    public void mergerSlice() throws SliceMergerException
    {
        try
        {
            dataHandler.initialise();
            for(MolapDataIterator<MolapSurrogateTupleHolder> leaftTupleIterator :leafTupleIteratorList)
            {
                while(true)
                {
                    addRow(leaftTupleIterator.getNextData());
                    if(!leaftTupleIterator.hasNext())
                    {
                        break;
                    }
                    leaftTupleIterator.fetchNextData();
                }
            }
            this.dataHandler.finish();
        }
        catch(MolapDataWriterException e)
        {
            throw new SliceMergerException(
                    "Problem while getting the file channel for Destination file: ",
                    e);
        }
        finally
        {
            this.dataHandler.closeHandler();
        }

    }

}
