package com.huawei.unibi.molap.merger.columnar.impl;

import java.util.AbstractQueue;
import java.util.Comparator;
import java.util.PriorityQueue;

import com.huawei.unibi.molap.factreader.MolapSurrogateTupleHolder;
import com.huawei.unibi.molap.merger.columnar.ColumnarFactFileMerger;
import com.huawei.unibi.molap.merger.columnar.iterator.MolapDataIterator;
import com.huawei.unibi.molap.merger.exeception.SliceMergerException;
import com.huawei.unibi.molap.schema.metadata.MolapColumnarFactMergerInfo;
import com.huawei.unibi.molap.store.writer.exception.MolapDataWriterException;
import com.huawei.unibi.molap.util.ByteUtil;

public class NonTimeBasedMergerColumnar extends ColumnarFactFileMerger
{

    /**
     * record holder heap
     */
    private AbstractQueue<MolapDataIterator<MolapSurrogateTupleHolder>> recordHolderHeap;

    public NonTimeBasedMergerColumnar(
            MolapColumnarFactMergerInfo molapColumnarFactMergerInfo, int currentRestructNumber)
    {
        super(molapColumnarFactMergerInfo, currentRestructNumber);
        if(leafTupleIteratorList.size() > 0)
        {
            recordHolderHeap = new PriorityQueue<MolapDataIterator<MolapSurrogateTupleHolder>>(
                    leafTupleIteratorList.size(), new MolapMdkeyComparator());
        }
    }

    @Override
    public void mergerSlice() throws SliceMergerException
    {
        // index
        int index = 0;
        try
        {
            dataHandler.initialise();
            // add first record from each file
            // CHECKSTYLE:OFF Approval No:Approval-367
            for(MolapDataIterator<MolapSurrogateTupleHolder> leaftTupleIterator : this.leafTupleIteratorList)
            {
                this.recordHolderHeap.add(leaftTupleIterator); // CHECKSTYLE:ON
                index++;
            }
            MolapDataIterator<MolapSurrogateTupleHolder> poll = null;
            while(index > 1)
            {
                // poll the top record
                 poll = this.recordHolderHeap.poll();
                // get the mdkey
                addRow(poll.getNextData());
                // if there is no record in the leaf and all then decrement the
                // index
                if(!poll.hasNext())
                {
                    index--;
                    continue;
                }
                poll.fetchNextData();
                // add record to heap
                this.recordHolderHeap.add(poll);
            }
            // if record holder is not empty then poll the slice holder from
            // heap
            poll = this.recordHolderHeap.poll();
            while(true)
            {
                addRow(poll.getNextData());
                // check if leaf contains no record
                if(!poll.hasNext())
                {
                    break;
                }
                poll.fetchNextData();
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
    
    private class MolapMdkeyComparator implements Comparator<MolapDataIterator<MolapSurrogateTupleHolder>>
    {

        @Override
        public int compare(MolapDataIterator<MolapSurrogateTupleHolder> o1,
                MolapDataIterator<MolapSurrogateTupleHolder> o2)
        {
            return ByteUtil.UnsafeComparer.INSTANCE.compareTo(o1.getNextData().getMdKey(), o2.getNextData().getMdKey());
        }
        
    }
}
